"""
Izibet Bet Tracker — client Python pour le sportsbook white-label BGT/TerminalCT.

Reverse-engineered depuis APK com.izibet.retailmobileapp.android v3.5.14.
Voir izibet_api_spec.md pour les details du protocole.

Flow:
  1. JWebCreateSession  → channelSessionId (UUID)
  2. JWebGetContentCouponV3?couponId=6316  → events list snapshot
  3. JWebGetContentEventNodesV3?eventNodeIds=...  → markets/outcomes pour event
  4. Boucle: JWebGetUpdatesV3?since=<actRevision>  → deltas

L'API utilise un modele subscription + long-polling. Le serveur renvoie
des entries typees ("State", "Event", "League", "Sports", "Market", "Outcome", ...).
"""
from __future__ import annotations
import json
import logging
import threading
import time
import unicodedata
from dataclasses import dataclass
from typing import Optional

import requests


SBS_BASE = "https://scores.my-bettracker.com/SportsBookService.svc/sbs/"

IZIBET_PARAMS = {
    "partnerId": "24838",
    "nodeId":    "42882",
    "systemId":  "16",
    "country":   "es",
    "language":  "es",
}

COUPON_HOME       = 777
COUPON_LIVE       = 779
COUPON_DEFAULT    = 6154
COUPON_OVERVIEW   = 6316

# Football "Largo Plazo" (Long Term / Outright markets) league ID.
# Discovered via DOM scan: button.league-link[data-event-id="1574333735"].
# Currently contains FA Cup Winner, Premier League outrights, etc.
# CDM 2026 Winner / Top Scorer / Group Winner expected to appear here ~2-4 weeks
# before tournament start (June 2026).
LARGO_PLAZO_NODE_ID = "1574333735"

# Keywords (lowercased) that indicate a CDM 2026 outright market title.
CDM_OUTRIGHT_KEYWORDS = [
    "mundial 2026",
    "copa mundial 2026",
    "copa mundial fifa 2026",
    "fifa world cup 2026",
    "world cup 2026",
    "wm 2026",
    "coupe du monde 2026",
    "vencedor mundial",
    "ganador mundial",
    "campeón mundial",
    "campeon mundial",
    "winner mundial",
    "máximo goleador mundial",
    "maximo goleador mundial",
    "top scorer mundial",
    "top goalscorer world cup",
    "grupo a 2026", "grupo b 2026", "grupo c 2026", "grupo d 2026",
    "grupo e 2026", "grupo f 2026", "grupo g 2026", "grupo h 2026",
    "grupo i 2026", "grupo j 2026", "grupo k 2026", "grupo l 2026",
]

# Mapping ES → canonical EN pour matching avec MODEL/FMD_DATA dans main.py
CDM_NATIONALS_ES = {
    "España": "Spain", "Espana": "Spain",
    "Francia": "France",
    "Argentina": "Argentina",
    "Inglaterra": "England",
    "Brasil": "Brazil",
    "Portugal": "Portugal",
    "Alemania": "Germany",
    "Holanda": "Netherlands", "Países Bajos": "Netherlands", "Paises Bajos": "Netherlands",
    "Bélgica": "Belgium", "Belgica": "Belgium",
    "Uruguay": "Uruguay",
    "México": "Mexico", "Mexico": "Mexico",
    "Croacia": "Croatia",
    "Ecuador": "Ecuador",
    "Japón": "Japan", "Japon": "Japan",
    "Estados Unidos": "USA",
    "Suiza": "Switzerland",
    "Colombia": "Colombia",
    "Noruega": "Norway",
    "Austria": "Austria",
    "Senegal": "Senegal",
    "Marruecos": "Morocco",
    "Turquía": "Turkey", "Turquia": "Turkey",
    "Canadá": "Canada", "Canada": "Canada",
    "Paraguay": "Paraguay",
    "Escocia": "Scotland",
    "Suecia": "Sweden",
    "Egipto": "Egypt",
    "Chequia": "Czechia",
    "Corea del Sur": "Korea Republic",
    "Australia": "Australia",
    "Costa de Marfil": "Ivory Coast",
    "Irán": "Iran", "Iran": "Iran",
    "Argelia": "Algeria",
    "Bosnia": "Bosnia",
    "Túnez": "Tunisia", "Tunez": "Tunisia",
    "Ghana": "Ghana",
    "Congo": "DR Congo", "RD Congo": "DR Congo",
    "Sudáfrica": "South Africa", "Sudafrica": "South Africa",
    "Panamá": "Panama", "Panama": "Panama",
    "Arabia Saudí": "Saudi Arabia", "Arabia Saudita": "Saudi Arabia",
    "Uzbekistán": "Uzbekistan", "Uzbekistan": "Uzbekistan",
    "Catar": "Qatar", "Qatar": "Qatar",
    "Nueva Zelanda": "New Zealand",
    "Jordania": "Jordan",
    "Cabo Verde": "Cabo Verde",
    "Iraq": "Iraq", "Irak": "Iraq",
    "Haití": "Haiti", "Haiti": "Haiti",
    "Curazao": "Curacao", "Curaçao": "Curacao",
}


def _strip_accents(s: str) -> str:
    if not s: return ""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def normalize_team_es(name: str) -> Optional[str]:
    if not name: return None
    if name in CDM_NATIONALS_ES:
        return CDM_NATIONALS_ES[name]
    stripped = _strip_accents(name)
    if stripped in CDM_NATIONALS_ES:
        return CDM_NATIONALS_ES[stripped]
    for k, v in CDM_NATIONALS_ES.items():
        if _strip_accents(k).lower() == stripped.lower():
            return v
    return None


@dataclass
class IzibetEvent:
    event_id: str
    title: str
    home: Optional[str]
    away: Optional[str]
    start_date: str
    sport: str
    in_play: bool
    ui_id: str
    home_canonical: Optional[str] = None
    away_canonical: Optional[str] = None

    def is_cdm_match(self) -> bool:
        return (self.sport == "soccer"
                and self.home_canonical is not None
                and self.away_canonical is not None)


class IzibetClient:
    def __init__(self, params: dict = None):
        self.params = {**IZIBET_PARAMS, **(params or {})}
        self.session_id: Optional[str] = None
        self.act_revision: str = "0"
        self.events: dict[str, IzibetEvent] = {}
        self.subscribed_coupons: set = set()
        self.session_created_at: float = 0
        self.s = requests.Session()
        self.s.headers.update({
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Linux; Android 11) Mobile Bet Tracker",
        })
        self.lock = threading.Lock()

    def create_session(self) -> str:
        url = SBS_BASE + "JWebCreateSession"
        qs = "&".join(f"{k}={v}" for k, v in self.params.items())
        r = self.s.get(f"{url}?{qs}", timeout=15)
        r.raise_for_status()
        data = r.json()
        state = next((e for e in data if e.get("type") == "State" or e.get("ty") == "State"), None)
        if not state:
            raise RuntimeError(f"CreateSession: no State entry: {data}")
        self.session_id = state.get("session") or state.get("csi")
        self.act_revision = state.get("actRevision") or state.get("ar") or "0"
        self.session_created_at = time.time()
        logging.info("[IZIBET] Session created rev=%s", self.act_revision[-8:] if self.act_revision else "?")
        return self.session_id

    def ensure_session(self):
        if not self.session_id or (time.time() - self.session_created_at) > 50 * 60:
            self.create_session()

    def _common_qs(self) -> str:
        self.ensure_session()
        params = dict(self.params)
        params["channelSessionId"] = self.session_id
        params["useAbbreviations"] = "true"
        return "&".join(f"{k}={v}" for k, v in params.items())

    def subscribe_coupon(self, coupon_id: int):
        url = SBS_BASE + f"JWebGetContentCouponV3?{self._common_qs()}&couponId={coupon_id}"
        r = self.s.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        self._ingest(data)
        with self.lock:
            self.subscribed_coupons.add(coupon_id)
        logging.info("[IZIBET] Subscribed coupon %d (+%d items, %d events total)",
                     coupon_id, len(data), len(self.events))
        return data

    def unregister_all(self):
        url = SBS_BASE + f"JWebUnregisterContentAllV3?cache={int(time.time()*1000)}&{self._common_qs()}"
        try:
            self.s.get(url, timeout=10)
        except Exception:
            pass
        with self.lock:
            self.subscribed_coupons.clear()
            self.events.clear()

    def poll_updates(self) -> list:
        url = SBS_BASE + f"JWebGetUpdatesV3?{self._common_qs()}&since={self.act_revision}"
        try:
            r = self.s.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logging.warning("[IZIBET] Poll error: %s", e)
            return []
        self._ingest(data)
        return data

    def _ingest(self, items: list):
        if not items:
            return
        with self.lock:
            for it in items:
                ty = it.get("ty") or it.get("type")
                if ty == "State":
                    new_rev = it.get("ar") or it.get("actRevision")
                    if new_rev:
                        self.act_revision = new_rev
                elif ty == "Event":
                    eid = it.get("dgn") or it.get("designation") or it.get("ui") or ""
                    if not eid:
                        continue
                    home = it.get("th"); away = it.get("ta")
                    evt = IzibetEvent(
                        event_id=str(eid),
                        title=it.get("ti", "") or "",
                        home=home,
                        away=away,
                        start_date=it.get("sd", "") or "",
                        sport=it.get("sh", "") or "",
                        in_play=bool(it.get("ip", False)),
                        ui_id=str(it.get("ui", "")),
                        home_canonical=normalize_team_es(home) if home else None,
                        away_canonical=normalize_team_es(away) if away else None,
                    )
                    self.events[evt.event_id] = evt

    def cdm_matches(self) -> list:
        with self.lock:
            return [e for e in self.events.values() if e.is_cdm_match()]

    # ------------------------------------------------------------------
    # Outright watcher (CDM 2026 winner, top scorer, group winners)
    # ------------------------------------------------------------------
    def scan_outright_section(self) -> list:
        """Scan the football Largo Plazo (long-term/outright) section and return
        any market entries whose title looks like a CDM 2026 outright.

        Returns a list of dicts with keys: title, event_id, type, sport,
        runner_count_hint. Empty list if no CDM outrights are currently listed
        on Izibet's shop catalog (expected before ~3-4 weeks pre-tournament).
        """
        try:
            qs = self._common_qs()
            url = (SBS_BASE +
                   f"JWebGetContentEventNodesV3?{qs}"
                   f"&eventNodeIds={LARGO_PLAZO_NODE_ID}")
            r = self.s.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logging.warning("[IZIBET] scan_outright_section error: %s", e)
            return []

        # Update revision from any State entries observed
        with self.lock:
            for it in data:
                if (it.get("ty") == "State" or it.get("type") == "State"):
                    new_rev = it.get("ar") or it.get("actRevision")
                    if new_rev:
                        self.act_revision = new_rev

        candidates = []
        for it in data:
            ty = it.get("ty") or it.get("type")
            if ty not in ("Event", "League", "Sports"):
                continue
            title = (it.get("ti") or it.get("title") or "")
            tl = title.lower()
            if any(kw in tl for kw in CDM_OUTRIGHT_KEYWORDS):
                candidates.append({
                    "title": title,
                    "event_id": str(it.get("dgn") or it.get("designation")
                                    or it.get("ui") or ""),
                    "type": ty,
                    "sport": it.get("sh") or it.get("sport"),
                })
        return candidates


def izibet_refresh_loop_blocking(client: IzibetClient,
                                 coupon_ids: list = None,
                                 poll_sec: int = 30,
                                 stop_event: threading.Event = None):
    coupon_ids = coupon_ids or [COUPON_OVERVIEW]
    stop_event = stop_event or threading.Event()
    last_resub = 0

    while not stop_event.is_set():
        try:
            if (time.time() - last_resub) > 30 * 60:
                client.unregister_all()
                client.create_session()
                for cid in coupon_ids:
                    client.subscribe_coupon(cid)
                    time.sleep(0.5)
                last_resub = time.time()
                cdm = client.cdm_matches()
                logging.info("[IZIBET] Snapshot: %d events, %d CDM matches",
                             len(client.events), len(cdm))
                for m in cdm[:5]:
                    logging.info("  CDM: %s vs %s sport=%s", m.home, m.away, m.sport)

            client.poll_updates()
        except Exception as e:
            logging.exception("[IZIBET] Loop error: %s", e)
            time.sleep(10)
        stop_event.wait(poll_sec)
