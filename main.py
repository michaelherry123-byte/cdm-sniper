"""
CDM 2026 SNIPER - version single-file
======================================

Service qui ecoute les messages forwardes depuis @izibet_bet_tracker vers
ton bot Telegram, parse les cotes, compare avec le modele, et t'envoie une
alerte si value bet detecte.

Setup (2 commandes) :
  pip install python-telegram-bot requests python-dotenv
  python cdm_sniper_single.py

Tout est pre-configure pour Michael (bot token + chat_id deja dedans).
"""
from __future__ import annotations
import asyncio
import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# ============================================================================
# CONFIG
# ============================================================================
BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "8749769198:AAFg7wvVGrg1ahgmJfDJUqWmeJ5A6iHzGo0")
CHAT_ID     = os.getenv("TELEGRAM_ALERT_CHAT_ID", "1103779200")
TARGET      = os.getenv("TELEGRAM_TARGET", "izibet_bet_tracker")
MIN_EDGE    = float(os.getenv("MIN_EDGE", "0.08"))
MIN_ODDS    = float(os.getenv("MIN_ODDS", "1.8"))
MAX_ODDS    = float(os.getenv("MAX_ODDS", "50"))
BANKROLL    = float(os.getenv("BANKROLL_UNITS", "100"))
KELLY_FRAC  = float(os.getenv("KELLY_FRAC", "0.25"))
MAX_STAKE   = float(os.getenv("MAX_STAKE_UNITS", "5"))
BLEND_W     = float(os.getenv("BLEND_MARKET_WEIGHT", "0.6"))
DEDUP_MIN   = int(os.getenv("DEDUP_COOLDOWN_MIN", "360"))
MAX_ALERT_H = int(os.getenv("MAX_ALERTS_PER_HOUR", "20"))

DB_PATH = Path(__file__).parent / "cdm_sniper.db"

# ============================================================================
# MODEL - Probabilites pre-calculees (Monte Carlo 50k + Polymarket live)
# Regenerables via scripts/monte_carlo.py separe, mais ici on a une snapshot.
# ============================================================================
MODEL = {
    "teams": {
        "Spain":       {"mc": {"winner":0.180, "finalist":0.280, "semi":0.412, "group_winner":0.688},
                        "market": {"winner":0.161}},
        "France":      {"mc": {"winner":0.155, "finalist":0.248, "semi":0.383, "group_winner":0.614},
                        "market": {"winner":0.162}},
        "Argentina":   {"mc": {"winner":0.154, "finalist":0.250, "semi":0.382, "group_winner":0.626},
                        "market": {"winner":0.090}},
        "Brazil":      {"mc": {"winner":0.129, "finalist":0.221, "semi":0.350, "group_winner":0.657},
                        "market": {"winner":0.085}},
        "Portugal":    {"mc": {"winner":0.052, "finalist":0.114, "semi":0.218, "group_winner":0.547},
                        "market": {"winner":0.078}},
        "Netherlands": {"mc": {"winner":0.049, "finalist":0.104, "semi":0.204, "group_winner":0.523},
                        "market": {"winner":0.034}},
        "England":     {"mc": {"winner":0.046, "finalist":0.100, "semi":0.205, "group_winner":0.551},
                        "market": {"winner":0.111}},
        "Germany":     {"mc": {"winner":0.038, "finalist":0.090, "semi":0.185, "group_winner":0.462},
                        "market": {"winner":0.054}},
        "Belgium":     {"mc": {"winner":0.027, "finalist":0.067, "semi":0.147, "group_winner":0.451},
                        "market": {"winner":0.020}},
        "Croatia":     {"mc": {"winner":0.016, "finalist":0.065, "semi":0.170, "group_winner":0.420},
                        "market": {"winner":0.009}},
        "Denmark":     {"mc": {"winner":0.013, "finalist":0.055, "semi":0.155, "group_winner":0.250}},
        "USA":         {"mc": {"winner":0.023, "finalist":0.060, "semi":0.134, "group_winner":0.520},
                        "market": {"winner":0.013}},
        "Uruguay":     {"mc": {"winner":0.012, "finalist":0.050, "semi":0.145, "group_winner":0.28},
                        "market": {"winner":0.010}},
        "Morocco":     {"mc": {"winner":0.018, "finalist":0.070, "semi":0.180, "group_winner":0.450},
                        "market": {"winner":0.019}},
        "Mexico":      {"mc": {"winner":0.016, "finalist":0.065, "semi":0.170, "group_winner":0.500},
                        "market": {"winner":0.011}},
        "Colombia":    {"mc": {"winner":0.012, "finalist":0.050, "semi":0.145, "group_winner":0.320},
                        "market": {"winner":0.017}},
        "Switzerland": {"mc": {"winner":0.014, "finalist":0.060, "semi":0.165, "group_winner":0.300},
                        "market": {"winner":0.010}},
        "Senegal":     {"mc": {"winner":0.010, "finalist":0.045, "semi":0.135, "group_winner":0.380}},
        "Austria":     {"mc": {"winner":0.009, "finalist":0.040, "semi":0.125, "group_winner":0.300}},
        "Ecuador":     {"mc": {"winner":0.007, "finalist":0.035, "semi":0.115, "group_winner":0.220}},
        "Japan":       {"mc": {"winner":0.022, "finalist":0.058, "semi":0.134, "group_winner":0.300},
                        "market": {"winner":0.023}},
        "Korea Republic": {"mc": {"winner":0.007, "finalist":0.032, "semi":0.108, "group_winner":0.280}},
        "Serbia":      {"mc": {"winner":0.006, "finalist":0.028, "semi":0.095, "group_winner":0.320}},
        "Ukraine":     {"mc": {"winner":0.005, "finalist":0.025, "semi":0.085, "group_winner":0.200}},
        "Norway":      {"mc": {"winner":0.025, "finalist":0.065, "semi":0.158, "group_winner":0.450},
                        "market": {"winner":0.025}},
        "Iran":        {"mc": {"winner":0.003, "finalist":0.018, "semi":0.068, "group_winner":0.220}},
        "Turkey":      {"mc": {"winner":0.006, "finalist":0.028, "semi":0.095, "group_winner":0.300},
                        "market": {"winner":0.007}},
        "Canada":      {"mc": {"winner":0.005, "finalist":0.020, "semi":0.080, "group_winner":0.400},
                        "market": {"winner":0.006}},
        "Tunisia":     {"mc": {"winner":0.002, "finalist":0.012, "semi":0.050, "group_winner":0.150}},
        "Australia":   {"mc": {"winner":0.003, "finalist":0.016, "semi":0.062, "group_winner":0.220}},
        "Ghana":       {"mc": {"winner":0.002, "finalist":0.012, "semi":0.050, "group_winner":0.180}},
        "Egypt":       {"mc": {"winner":0.003, "finalist":0.018, "semi":0.068, "group_winner":0.250}},
        "Chile":       {"mc": {"winner":0.003, "finalist":0.018, "semi":0.068, "group_winner":0.200}},
        "Cameroon":    {"mc": {"winner":0.002, "finalist":0.010, "semi":0.042, "group_winner":0.120}},
        "Ivory Coast": {"mc": {"winner":0.004, "finalist":0.022, "semi":0.078, "group_winner":0.300}},
        "Paraguay":    {"mc": {"winner":0.004, "finalist":0.022, "semi":0.078, "group_winner":0.320}},
        "Peru":        {"mc": {"winner":0.002, "finalist":0.010, "semi":0.042, "group_winner":0.100}},
        "Saudi Arabia":{"mc": {"winner":0.002, "finalist":0.010, "semi":0.042, "group_winner":0.150}},
        "Costa Rica":  {"mc": {"winner":0.001, "finalist":0.008, "semi":0.035, "group_winner":0.080}},
        "South Africa":{"mc": {"winner":0.002, "finalist":0.012, "semi":0.050, "group_winner":0.200}},
        "Uzbekistan":  {"mc": {"winner":0.002, "finalist":0.010, "semi":0.042, "group_winner":0.250}},
        "Venezuela":   {"mc": {"winner":0.002, "finalist":0.010, "semi":0.042, "group_winner":0.150}},
        "Panama":      {"mc": {"winner":0.001, "finalist":0.006, "semi":0.028, "group_winner":0.120}},
        "Jamaica":     {"mc": {"winner":0.001, "finalist":0.006, "semi":0.028, "group_winner":0.080}},
        "Qatar":       {"mc": {"winner":0.001, "finalist":0.006, "semi":0.028, "group_winner":0.100}},
        "UAE":         {"mc": {"winner":0.001, "finalist":0.005, "semi":0.022, "group_winner":0.080}},
        "New Zealand": {"mc": {"winner":0.001, "finalist":0.005, "semi":0.022, "group_winner":0.100}},
    },
    "top_scorer": {
        "Kylian Mbappe":  {"mc": 0.16, "team": "France"},
        "Erling Haaland": {"mc": 0.10, "team": "Norway"},
        "Harry Kane":     {"mc": 0.11, "team": "England"},
        "Lamine Yamal":   {"mc": 0.13, "team": "Spain"},
        "Lionel Messi":   {"mc": 0.07, "team": "Argentina"},
        "Vinicius Jr":    {"mc": 0.08, "team": "Brazil"},
        "Jude Bellingham":{"mc": 0.07, "team": "England"},
        "Ousmane Dembele":{"mc": 0.06, "team": "France"},
        "Julian Alvarez": {"mc": 0.06, "team": "Argentina"},
        "Pedri":          {"mc": 0.05, "team": "Spain"},
    }
}

TEAM_ALIAS = {
    "south korea": "Korea Republic", "korea": "Korea Republic",
    "czechia": "Czechia", "cezchia": "Czechia", "czech republic": "Czechia",
    "turkiye": "Turkey", "cote d'ivoire": "Ivory Coast",
    "usa": "USA", "united states": "USA",
    "uae": "UAE", "bosnia-herzegovina": "Bosnia",
    "curaçao": "Curacao", "dr congo": "DR Congo", "congo dr": "DR Congo",
    "new zealand": "New Zealand",
}


def normalize_team(name: str) -> str:
    if not name:
        return ""
    k = name.strip().lower()
    return TEAM_ALIAS.get(k, name.strip().title())


def blend_prob(mc: float | None, market: float | None) -> float | None:
    if mc is None and market is None:
        return None
    if mc is None: return market
    if market is None: return mc
    return BLEND_W * market + (1 - BLEND_W) * mc


def get_prob(team: str, market: str) -> float | None:
    team_n = normalize_team(team)
    if market == "top_scorer":
        rec = MODEL["top_scorer"].get(team_n)
        return rec["mc"] if rec else None
    if market == "reach_final":
        market = "finalist"
    rec = MODEL["teams"].get(team_n)
    if not rec:
        return None
    mc = (rec.get("mc") or {}).get(market)
    mk = (rec.get("market") or {}).get(market)
    return blend_prob(mc, mk)


# ============================================================================
# PARSER
# ============================================================================
@dataclass
class ParsedBet:
    team: str
    market: str
    odds_dec: float
    raw: str


MARKET_PATTERNS = [
    (re.compile(r"world\s*cup\s*winner|winner\s*(world\s*cup|wc|cdm)|outright\s*winner|tournament\s*winner|vainqueur", re.I), "winner"),
    (re.compile(r"reach\s*(the\s*)?final|finaliste|to\s*reach\s*the\s*final", re.I), "finalist"),
    (re.compile(r"reach\s*(the\s*)?semi|demi\s*final|semi\s*final", re.I), "semi"),
    (re.compile(r"reach\s*(the\s*)?(quarter|quarts)", re.I), "quarter"),
    (re.compile(r"reach\s*(the\s*)?(last\s*16|r16|huitiemes?)", re.I), "r16"),
    (re.compile(r"golden\s*boot|top\s*(goal)?scorer|meilleur\s*buteur", re.I), "top_scorer"),
    (re.compile(r"group\s+([a-l])\s*(winner|win)|vainqueur\s*groupe\s+([a-l])", re.I), "group_winner"),
]
ODDS_PATTERN = re.compile(r"(?P<sel>[A-Za-z][A-Za-z'\s\-\.]{1,30}?)\s*[@\|]\s*(?P<odds>\d+(?:\.\d{1,2})?)")
ODDS_PATTERN2 = re.compile(r"(?P<sel>[A-Za-z][A-Za-z'\s\-\.]{1,30}?)\s+(?P<odds>\d+\.\d{1,2})")


def parse_message(text: str) -> list[ParsedBet]:
    if not text:
        return []
    markets = []
    for pat, key in MARKET_PATTERNS:
        m = pat.search(text)
        if m:
            if key == "group_winner":
                g = (m.group(1) or m.group(3) or "").upper()
                markets.append("group_winner" + (f"_{g}" if g else ""))
            else:
                markets.append(key)
    if not markets:
        markets = ["winner"]

    bets = []
    seen = set()
    for market in markets:
        base_market = market.split("_")[0] + ("_" + market.split("_")[1] if "_" in market else "")
        for pattern in [ODDS_PATTERN, ODDS_PATTERN2]:
            for m in pattern.finditer(text):
                sel = m.group("sel").strip()
                try:
                    odds = float(m.group("odds"))
                except ValueError:
                    continue
                if odds < 1.2 or odds > 1000:
                    continue
                if len(sel) < 2 or sel.lower() in {"winner","final","goal","stake","odds","at","to","the","a","an","wc","cdm"}:
                    continue
                team_n = normalize_team(sel)
                key = (team_n, market, round(odds, 2))
                if key in seen:
                    continue
                seen.add(key)
                bets.append(ParsedBet(team=team_n, market=market, odds_dec=odds, raw=text))
    return bets


# ============================================================================
# DETECTOR
# ============================================================================
def evaluate(bet: ParsedBet) -> dict:
    res = {"value": False, "edge": 0.0, "kelly": 0.0, "stake_units": 0.0,
           "p_model": None, "reason": "", "bet": bet}

    if bet.odds_dec < MIN_ODDS:
        res["reason"] = f"cote {bet.odds_dec} < {MIN_ODDS}"
        return res
    if bet.odds_dec > MAX_ODDS:
        res["reason"] = f"cote {bet.odds_dec} > {MAX_ODDS}"
        return res

    p = get_prob(bet.team, bet.market)
    if not p or p <= 0:
        res["reason"] = f"no prob {bet.market}/{bet.team}"
        return res
    res["p_model"] = p

    edge = bet.odds_dec * p - 1
    res["edge"] = edge
    if edge < MIN_EDGE:
        res["reason"] = f"edge {edge:.2%} < {MIN_EDGE:.2%}"
        return res

    b = bet.odds_dec - 1
    kelly = max(0, (b * p - (1 - p)) / b)
    res["kelly"] = kelly
    res["stake_units"] = min(MAX_STAKE, round(BANKROLL * kelly * KELLY_FRAC, 1))
    res["value"] = res["stake_units"] > 0
    res["reason"] = f"VALUE edge={edge:.2%} stake={res['stake_units']}u"
    return res


# ============================================================================
# STATE (SQLite)
# ============================================================================
def db_connect():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, market TEXT, team TEXT,
        odds REAL, edge REAL, stake_units REAL, prob_model REAL, raw_msg TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, source TEXT,
        raw TEXT, parsed_count INTEGER
    )""")
    return conn


def was_alerted(market: str, team: str, odds: float) -> bool:
    cutoff = int(time.time()) - DEDUP_MIN * 60
    with db_connect() as c:
        row = c.execute(
            "SELECT 1 FROM alerts WHERE market=? AND team=? AND abs(odds-?)<0.05 AND ts>=?",
            (market, team, odds, cutoff)
        ).fetchone()
        return row is not None


def record_alert(market, team, odds, edge, stake, p_model, raw=""):
    with db_connect() as c:
        c.execute("INSERT INTO alerts (ts,market,team,odds,edge,stake_units,prob_model,raw_msg) VALUES (?,?,?,?,?,?,?,?)",
                  (int(time.time()), market, team, odds, edge, stake, p_model, raw))


def alerts_1h() -> int:
    cutoff = int(time.time()) - 3600
    with db_connect() as c:
        row = c.execute("SELECT COUNT(*) FROM alerts WHERE ts>=?", (cutoff,)).fetchone()
        return row[0] if row else 0


def log_msg(source: str, raw: str, parsed_count: int):
    with db_connect() as c:
        c.execute("INSERT INTO messages (ts,source,raw,parsed_count) VALUES (?,?,?,?)",
                  (int(time.time()), source, raw, parsed_count))


# ============================================================================
# ALERTER (via Bot API direct)
# ============================================================================
def format_alert(res: dict) -> str:
    b = res["bet"]
    p = res["p_model"]
    return (
        f"🎯 *VALUE BET DETECTE*\n\n"
        f"*{b.team}*  —  _{b.market.replace('_',' ').title()}_\n"
        f"🏪 Cote Izibet: *{b.odds_dec:.2f}*\n"
        f"⚖️ Cote fair: {1/p:.2f}\n"
        f"📈 Edge: *+{res['edge']*100:.1f}%*\n"
        f"🎰 Kelly: {res['kelly']*100:.1f}%\n"
        f"💰 *Mise: {res['stake_units']:.1f} units*\n"
        f"   _(br {int(BANKROLL)}u · {int(KELLY_FRAC*100)}% Kelly · cap {int(MAX_STAKE)}u)_\n\n"
        f"🕒 {datetime.now().strftime('%d/%m %H:%M')}"
    )


def send_alert(text: str) -> bool:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": CHAT_ID, "text": text,
            "parse_mode": "Markdown", "disable_web_page_preview": True,
        }, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        logging.error("Telegram send failed: %s", e)
        return False


# ============================================================================
# RECEIVER (python-telegram-bot long polling)
# ============================================================================
async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"✅ *CDM Sniper receiver up*\n\n"
        f"Ton chat_id: `{update.effective_chat.id}`\n"
        f"Target: @{TARGET}\n"
        f"Forward les messages de @{TARGET} ici pour declencher les analyses.\n\n"
        f"Commandes: /status",
        parse_mode="Markdown"
    )


async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    nb = alerts_1h()
    await update.message.reply_text(
        f"📊 *CDM Sniper*\n"
        f"Teams model: {len(MODEL['teams'])}\n"
        f"Alerts (1h): {nb}/{MAX_ALERT_H}\n"
        f"Edge min: {MIN_EDGE*100:.0f}% · Cotes {MIN_ODDS}-{MAX_ODDS}",
        parse_mode="Markdown"
    )


async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.text:
        return

    fwd_chat = msg.forward_from_chat
    fwd_user = msg.forward_from
    src_name = None
    if fwd_chat: src_name = fwd_chat.username or fwd_chat.title
    elif fwd_user: src_name = fwd_user.username or fwd_user.first_name
    elif msg.forward_sender_name: src_name = msg.forward_sender_name

    target = TARGET.lstrip("@").lower()
    # Accept aussi test direct (sans forward) pour debug
    from_target = src_name and target in src_name.lower()
    is_test = not src_name  # message direct au bot

    if not from_target and not is_test:
        return  # ignore

    raw = msg.text
    logging.info("Msg recu (source=%s, %d chars)", src_name or "DIRECT_TEST", len(raw))
    bets = parse_message(raw)
    log_msg(src_name or "direct_test", raw, len(bets))

    if not bets:
        if is_test:
            await msg.reply_text(f"⚠️ Aucun pari detecte dans : {raw[:80]!r}")
        return

    if alerts_1h() >= MAX_ALERT_H:
        return

    for bet in bets:
        if was_alerted(bet.market, bet.team, bet.odds_dec):
            continue
        res = evaluate(bet)
        logging.info("  %s/%s @%.2f -> %s", bet.market, bet.team, bet.odds_dec, res["reason"])
        if not res["value"]:
            if is_test:
                await msg.reply_text(f"❌ {bet.market}/{bet.team} @{bet.odds_dec} : {res['reason']}")
            continue
        text = format_alert(res)
        if send_alert(text):
            record_alert(bet.market, bet.team, bet.odds_dec, res["edge"], res["stake_units"], res["p_model"], raw)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s : %(message)s")
    if not BOT_TOKEN or not CHAT_ID:
        logging.error("Missing BOT_TOKEN or CHAT_ID")
        return
    logging.info("Starting CDM Sniper (single-file, receiver mode)")
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    # Startup notification
    send_alert(
        f"✅ *CDM Sniper ON (single-file)*\n"
        f"Ecoute les forwards de @{TARGET}\n"
        f"Edge min: {MIN_EDGE*100:.0f}% · cotes {MIN_ODDS}-{MAX_ODDS}\n"
        f"/status pour l'etat"
    )
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
