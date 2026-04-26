"""
izibet_scraper.py — Headless Chromium scraper for Izibet outright cotes.

Why this exists:
  Izibet's SBS API uses a complex subscription/push model with cryptic tree
  nodes. The actual outright Market/Outcome cotes are rendered in the
  BuildABet WebView's DOM but are difficult to extract via the API alone.

  This module uses Playwright (headless Chromium) to navigate the WebView
  exactly like a real user, then scrapes the rendered cotes from the DOM.

Activation:
  - Disabled by default (IZIBET_SCRAPER_ENABLED=0)
  - Requires Playwright + Chromium installed in the deploy environment
  - Activated cleanly on Oracle Cloud Free Tier (post-Railway migration)

Detected markets:
  - All outright markets visible in Football → Largo Plazo (Long Term)
  - Includes FA Cup Winner, Premier League outrights, etc.
  - Will include CDM 2026 Winner, Top Scorer, Group Winner once Izibet adds
    them (expected 2-4 weeks before tournament).
"""
from __future__ import annotations
import logging
import re
from dataclasses import dataclass
from typing import Optional

try:
    from playwright.sync_api import sync_playwright
    from playwright.sync_api import TimeoutError as PWTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PWTimeout = Exception  # type: ignore


# ----------------------------------------------------------------------
# DOM selectors (validated against Izibet BuildABet WebView v15.0.0)
# ----------------------------------------------------------------------
BUILD_A_BET_URL    = "https://rma.my-bettracker.com/Izibet/BuildABet/"
SPORT_FOOTBALL_ID  = "1523221699"  # button.sports-link[data-item-id]
LARGO_PLAZO_ID     = "1574333735"  # button.league-link[data-event-id]


@dataclass
class OutrightCote:
    """A single outright market selection (one team + its decimal cote)."""
    market_event_id: str   # data-event-id of the market button (e.g. "11231983945")
    selection: str         # team / player name as displayed (e.g. "Manchester City")
    odds: float            # decimal cote (e.g. 1.32)
    market_title: str = "" # market name if detectable from surrounding DOM
    market_type: str = ""  # canonical: "winner" / "top_scorer" / "group_winner_X"


# ----------------------------------------------------------------------
# Market type inference from market title
# ----------------------------------------------------------------------
MARKET_TYPE_PATTERNS = [
    (re.compile(r"goleador|top.?scorer|golden.?boot|máximo.?goleador|maximo.?goleador", re.I), "top_scorer"),
    (re.compile(r"finalist|finaliste|reach.*final|to reach the final", re.I), "finalist"),
    (re.compile(r"semi[\s-]?final|semifinal|reach.*semi", re.I), "semi"),
    (re.compile(r"cuart|quarter|reach.*quarter", re.I), "quarter"),
    (re.compile(r"octavo|r16|round of 16|last 16", re.I), "r16"),
    (re.compile(r"grupo\s*([a-l])|group\s*([a-l]).*winner", re.I), "group_winner"),
    (re.compile(r"vencedor|ganador|winner|campeón|campeon", re.I), "winner"),
]


def infer_market_type(title: str) -> str:
    """Map a free-text market title to canonical market_type used in MODEL."""
    if not title:
        return "winner"  # safe default
    for pat, mtype in MARKET_TYPE_PATTERNS:
        m = pat.search(title)
        if m:
            if mtype == "group_winner":
                # Capture group letter A-L
                letter = ((m.group(1) or m.group(2) or "")).upper()
                return f"group_winner_{letter}" if letter else "group_winner"
            return mtype
    return "winner"


# ----------------------------------------------------------------------
# Cote text parser
# ----------------------------------------------------------------------
COTE_TEXT_PAT = re.compile(r"^(.+?)\s+([\d]+[,.]?\d*)$")


def parse_runner_button_text(txt: str) -> Optional[tuple[str, float]]:
    """Parse a runner button's textContent like 'Manchester City 1,32' → (name, odds)."""
    if not txt:
        return None
    norm = re.sub(r"\s+", " ", txt).strip()
    m = COTE_TEXT_PAT.match(norm)
    if not m:
        return None
    sel = m.group(1).strip()
    try:
        odds = float(m.group(2).replace(",", "."))
    except ValueError:
        return None
    if odds <= 1.0 or odds > 1000.0:
        return None
    if len(sel) < 2:
        return None
    return sel, odds


# ----------------------------------------------------------------------
# Main scraping function (sync — safe to run in thread)
# ----------------------------------------------------------------------
def scrape_largo_plazo(timeout_ms: int = 60_000) -> list[OutrightCote]:
    """Open Izibet BuildABet headless, navigate Football → Largo Plazo,
    scrape all outright cotes visible in the DOM.

    Returns empty list on any failure (and logs)."""
    if not PLAYWRIGHT_AVAILABLE:
        logging.error("[SCRAPER] Playwright not installed (pip install playwright && playwright install chromium)")
        return []

    cotes: list[OutrightCote] = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )
            context = browser.new_context(
                user_agent=("Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36"),
                locale="es-ES",
                viewport={"width": 414, "height": 896},
                ignore_https_errors=True,
            )
            page = context.new_page()
            page.set_default_timeout(timeout_ms)

            # 1. Load BuildABet
            page.goto(BUILD_A_BET_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(5000)

            # 2. Click Football
            try:
                page.locator(f'button.sports-link[data-item-id="{SPORT_FOOTBALL_ID}"]').first.click()
            except PWTimeout:
                logging.warning("[SCRAPER] Football button not found")
                browser.close()
                return []
            page.wait_for_timeout(4500)

            # 3. Click Largo Plazo
            try:
                page.locator(f'button.league-link[data-event-id="{LARGO_PLAZO_ID}"]').first.click()
            except PWTimeout:
                logging.warning("[SCRAPER] Largo Plazo button not found")
                browser.close()
                return []
            page.wait_for_timeout(7000)

            # 4. Scrape all selection buttons from DOM
            #
            # Each runner button has:
            #   - data-event-id    : the parent market id
            #   - text content     : "TeamName 1,32" (Spanish decimal w/ comma)
            #   - oddsChange_eXXX  : selection-id encoded in className
            #
            # Group runners by market_event_id to reconstruct full market.
            runners_data = page.evaluate(
                """() => Array.from(
                    document.querySelectorAll('button.eventbutton[data-action="toggleSelection"]')
                ).map(b => ({
                    eid: b.getAttribute('data-event-id') || '',
                    text: (b.innerText || b.textContent || '').trim(),
                    className: b.className || '',
                    disabled: b.classList.contains('isDisabled') ||
                              /isDisabled_/.test(b.className || ''),
                }))"""
            )

            # 5. Find market titles by walking the parent container
            market_titles = page.evaluate(
                """() => {
                    const out = {};
                    const buttons = Array.from(
                        document.querySelectorAll('button.event-link[data-event-type="event"]')
                    );
                    for (const b of buttons) {
                        const id = b.getAttribute('data-event-id');
                        if (!id) continue;
                        // Walk up looking for a heading or league name
                        let cur = b;
                        for (let i = 0; i < 6 && cur; i++) {
                            const heading = cur.querySelector('.league-title, .event-title, h2, h3, [class*="title"]');
                            if (heading && heading.textContent) {
                                out[id] = heading.textContent.trim().replace(/\\s+/g, ' ').slice(0, 200);
                                break;
                            }
                            cur = cur.parentElement;
                        }
                    }
                    return out;
                }"""
            )

            browser.close()

        # 6. Build OutrightCote list (Python-side parsing)
        market_to_title = market_titles or {}
        for r in runners_data or []:
            if r.get("disabled"):
                continue
            parsed = parse_runner_button_text(r.get("text", ""))
            if not parsed:
                continue
            sel, odds = parsed
            eid = r.get("eid", "")
            title = market_to_title.get(eid, "")
            cotes.append(OutrightCote(
                market_event_id=eid,
                selection=sel,
                odds=odds,
                market_title=title,
                market_type=infer_market_type(title),
            ))

    except Exception as e:
        logging.exception("[SCRAPER] scrape_largo_plazo error: %s", e)
        return []

    logging.info("[SCRAPER] Scraped %d outright cotes from Largo Plazo "
                 "across %d markets", len(cotes),
                 len({c.market_event_id for c in cotes}))
    return cotes
