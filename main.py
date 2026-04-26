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

import threading
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Izibet client (separate module - voir izibet_client.py)
try:
    from izibet_client import IzibetClient, izibet_refresh_loop_blocking, COUPON_OVERVIEW, normalize_team_es
    IZIBET_AVAILABLE = True
except ImportError:
    IZIBET_AVAILABLE = False
    def normalize_team_es(name):  # fallback no-op
        return None

# Headless Chromium scraper (optional - requires playwright + chromium installed)
try:
    from izibet_scraper import scrape_largo_plazo, PLAYWRIGHT_AVAILABLE
    IZIBET_SCRAPER_AVAILABLE = PLAYWRIGHT_AVAILABLE
except ImportError:
    IZIBET_SCRAPER_AVAILABLE = False

# State tracking for outright watcher (in-memory; resets on Railway redeploy = OK).
_izibet_outright_seen = set()
_izibet_scraper_seen_alerts = set()  # dedup scraper alerts

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
BLEND_W     = float(os.getenv("BLEND_MARKET_WEIGHT", "0.35"))
FMD_W       = float(os.getenv("BLEND_FMD_WEIGHT", "0.50"))
MC_W        = float(os.getenv("BLEND_MC_WEIGHT", "0.15"))
DEDUP_MIN   = int(os.getenv("DEDUP_COOLDOWN_MIN", "360"))
MAX_ALERT_H = int(os.getenv("MAX_ALERTS_PER_HOUR", "20"))
IZIBET_ENABLED = os.getenv("IZIBET_ENABLED", "1") == "1"
IZIBET_POLL_SEC = int(os.getenv("IZIBET_POLL_SEC", "30"))
IZIBET_DIGEST_MIN = int(os.getenv("IZIBET_DIGEST_MIN", "60"))  # send Telegram digest every X min
IZIBET_OUTRIGHT_SCAN_MIN = int(os.getenv("IZIBET_OUTRIGHT_SCAN_MIN", "60"))  # scan Largo Plazo every X min
IZIBET_SCRAPER_ENABLED = os.getenv("IZIBET_SCRAPER_ENABLED", "0") == "1"  # default off — needs Playwright
IZIBET_SCRAPER_INTERVAL_MIN = int(os.getenv("IZIBET_SCRAPER_INTERVAL_MIN", "30"))

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
    "curaÃÂ§ao": "Curacao", "dr congo": "DR Congo", "congo dr": "DR Congo",
    "new zealand": "New Zealand",
    "cabo verde": "Cabo Verde", "cape verde": "Cabo Verde",
    "bih": "Bosnia", "curacao": "Curacao", "congo-kinshasa": "DR Congo",
}

FMD_DATA = {
    "Spain":          {"winner":0.197, "finalist":0.312, "semi":0.460, "quarter":0.584, "r16":0.759},
    "France":         {"winner":0.132, "finalist":0.219, "semi":0.370, "quarter":0.527, "r16":0.765},
    "Argentina":      {"winner":0.129, "finalist":0.223, "semi":0.365, "quarter":0.523, "r16":0.667},
    "England":        {"winner":0.109, "finalist":0.198, "semi":0.333, "quarter":0.522, "r16":0.749},
    "Brazil":         {"winner":0.083, "finalist":0.156, "semi":0.278, "quarter":0.460, "r16":0.678},
    "Portugal":       {"winner":0.083, "finalist":0.163, "semi":0.295, "quarter":0.498, "r16":0.717},
    "Germany":        {"winner":0.052, "finalist":0.109, "semi":0.216, "quarter":0.360, "r16":0.676},
    "Netherlands":    {"winner":0.042, "finalist":0.090, "semi":0.187, "quarter":0.352, "r16":0.517},
    "Belgium":        {"winner":0.024, "finalist":0.061, "semi":0.138, "quarter":0.345, "r16":0.623},
    "Uruguay":        {"winner":0.016, "finalist":0.044, "semi":0.106, "quarter":0.217, "r16":0.391},
    "Mexico":         {"winner":0.015, "finalist":0.042, "semi":0.102, "quarter":0.254, "r16":0.567},
    "Croatia":        {"winner":0.014, "finalist":0.042, "semi":0.101, "quarter":0.214, "r16":0.481},
    "Ecuador":        {"winner":0.013, "finalist":0.036, "semi":0.093, "quarter":0.200, "r16":0.489},
    "Japan":          {"winner":0.012, "finalist":0.032, "semi":0.083, "quarter":0.192, "r16":0.352},
    "USA":            {"winner":0.010, "finalist":0.028, "semi":0.073, "quarter":0.202, "r16":0.450},
    "Switzerland":    {"winner":0.008, "finalist":0.024, "semi":0.070, "quarter":0.214, "r16":0.517},
    "Colombia":       {"winner":0.008, "finalist":0.024, "semi":0.068, "quarter":0.158, "r16":0.386},
    "Norway":         {"winner":0.007, "finalist":0.022, "semi":0.063, "quarter":0.157, "r16":0.362},
    "Austria":        {"winner":0.006, "finalist":0.020, "semi":0.054, "quarter":0.133, "r16":0.274},
    "Senegal":        {"winner":0.006, "finalist":0.019, "semi":0.055, "quarter":0.148, "r16":0.340},
    "Morocco":        {"winner":0.005, "finalist":0.019, "semi":0.057, "quarter":0.159, "r16":0.343},
    "Turkey":         {"winner":0.005, "finalist":0.016, "semi":0.045, "quarter":0.135, "r16":0.360},
    "Canada":         {"winner":0.004, "finalist":0.014, "semi":0.050, "quarter":0.166, "r16":0.437},
    "Paraguay":       {"winner":0.003, "finalist":0.013, "semi":0.041, "quarter":0.126, "r16":0.341},
    "Scotland":       {"winner":0.003, "finalist":0.010, "semi":0.034, "quarter":0.110, "r16":0.272},
    "Sweden":         {"winner":0.003, "finalist":0.009, "semi":0.030, "quarter":0.089, "r16":0.208},
    "Egypt":          {"winner":0.002, "finalist":0.007, "semi":0.026, "quarter":0.094, "r16":0.302},
    "Czechia":        {"winner":0.001, "finalist":0.008, "semi":0.031, "quarter":0.120, "r16":0.345},
    "Korea Republic": {"winner":0.001, "finalist":0.007, "semi":0.026, "quarter":0.101, "r16":0.304},
    "Australia":      {"winner":0.001, "finalist":0.005, "semi":0.020, "quarter":0.075, "r16":0.236},
    "Ivory Coast":    {"winner":0.001, "finalist":0.004, "semi":0.017, "quarter":0.066, "r16":0.239},
    "Iran":           {"winner":0.001, "finalist":0.005, "semi":0.019, "quarter":0.076, "r16":0.267},
    "Algeria":        {"winner":0.001, "finalist":0.004, "semi":0.020, "quarter":0.063, "r16":0.168},
    "Bosnia":         {"winner":0.001, "finalist":0.003, "semi":0.014, "quarter":0.068, "r16":0.237},
    "Tunisia":        {"winner":0.0005,"finalist":0.002, "semi":0.008, "quarter":0.029, "r16":0.090},
    "Ghana":          {"winner":0.0005,"finalist":0.002, "semi":0.008, "quarter":0.028, "r16":0.106},
    "DR Congo":       {"winner":0.0005,"finalist":0.001, "semi":0.007, "quarter":0.028, "r16":0.116},
    "South Africa":   {"winner":0.0005,"finalist":0.001, "semi":0.006, "quarter":0.035, "r16":0.139},
    "Panama":         {"winner":0.0005,"finalist":0.001, "semi":0.007, "quarter":0.030, "r16":0.110},
    "Saudi Arabia":   {"winner":0.0005,"finalist":0.001, "semi":0.005, "quarter":0.024, "r16":0.085},
    "Uzbekistan":     {"winner":0.0005,"finalist":0.001, "semi":0.004, "quarter":0.021, "r16":0.092},
    "Qatar":          {"winner":0.0005,"finalist":0.001, "semi":0.005, "quarter":0.032, "r16":0.134},
    "New Zealand":    {"winner":0.0005,"finalist":0.0005,"semi":0.005, "quarter":0.025, "r16":0.128},
    "Jordan":         {"winner":0.0001,"finalist":0.0005,"semi":0.001, "quarter":0.012, "r16":0.049},
    "Cabo Verde":     {"winner":0.0001,"finalist":0.0005,"semi":0.002, "quarter":0.010, "r16":0.045},
    "Iraq":           {"winner":0.0001,"finalist":0.0005,"semi":0.001, "quarter":0.009, "r16":0.043},
    "Haiti":          {"winner":0.0001,"finalist":0.0005,"semi":0.001, "quarter":0.009, "r16":0.043},
    "Haiti":          {"winner":0.0001,"finalist":0.0005,"semi":0.001, "quarter":0.004, "r16":0.023},
    "Curacao":        {"winner":0.0001,"finalist":0.0001,"semi":0.0005,"quarter":0.003, "r16":0.021},
}



def normalize_team(name: str) -> str:
    if not name:
        return ""
    k = name.strip().lower()
    return TEAM_ALIAS.get(k, name.strip().title())


def blend_prob(fmd: float | None, mc: float | None, market: float | None) -> float | None:
    """Bayesian blend: FMD + Market + MC. Rebalance when sources missing."""
    sources = []
    if fmd is not None:    sources.append((FMD_W, fmd))
    if market is not None: sources.append((BLEND_W, market))
    if mc is not None:     sources.append((MC_W, mc))
    if not sources:
        return None
    total_w = sum(w for w, _ in sources)
    return sum(w * p for w, p in sources) / total_w


def get_prob(team: str, market: str) -> float | None:
    team_n = normalize_team(team)
    if market == "top_scorer":
        rec = MODEL["top_scorer"].get(team_n)
        return rec["mc"] if rec else None
    if market == "reach_final":
        market = "finalist"
    rec = MODEL["teams"].get(team_n) or {}
    mc = (rec.get("mc") or {}).get(market)
    mk = (rec.get("market") or {}).get(market)
    fmd = (FMD_DATA.get(team_n) or {}).get(market)
    if mc is None and mk is None and fmd is None:
        return None
    return blend_prob(fmd, mc, mk)


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
        f"Ã°ÂÂÂ¯ *VALUE BET DETECTE*\n\n"
        f"*{b.team}*  Ã¢ÂÂ  _{b.market.replace('_',' ').title()}_\n"
        f"Ã°ÂÂÂª Cote Izibet: *{b.odds_dec:.2f}*\n"
        f"Ã¢ÂÂÃ¯Â¸Â Cote fair: {1/p:.2f}\n"
        f"Ã°ÂÂÂ Edge: *+{res['edge']*100:.1f}%*\n"
        f"Ã°ÂÂÂ° Kelly: {res['kelly']*100:.1f}%\n"
        f"Ã°ÂÂÂ° *Mise: {res['stake_units']:.1f} units*\n"
        f"   _(br {int(BANKROLL)}u ÃÂ· {int(KELLY_FRAC*100)}% Kelly ÃÂ· cap {int(MAX_STAKE)}u)_\n\n"
        f"Ã°ÂÂÂ {datetime.now().strftime('%d/%m %H:%M')}"
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
        f"Ã¢ÂÂ *CDM Sniper receiver up*\n\n"
        f"Ton chat_id: `{update.effective_chat.id}`\n"
        f"Target: @{TARGET}\n"
        f"Forward les messages de @{TARGET} ici pour declencher les analyses.\n\n"
        f"Commandes: /status",
        parse_mode="Markdown"
    )


async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    nb = alerts_1h()
    await update.message.reply_text(
        f"Ã°ÂÂÂ *CDM Sniper*\n"
        f"Teams model: {len(MODEL['teams'])}\n"
        f"Alerts (1h): {nb}/{MAX_ALERT_H}\n"
        f"Edge min: {MIN_EDGE*100:.0f}% ÃÂ· Cotes {MIN_ODDS}-{MAX_ODDS}",
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
            await msg.reply_text(f"Ã¢ÂÂ Ã¯Â¸Â Aucun pari detecte dans : {raw[:80]!r}")
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
                await msg.reply_text(f"Ã¢ÂÂ {bet.market}/{bet.team} @{bet.odds_dec} : {res['reason']}")
            continue
        text = format_alert(res)
        if send_alert(text):
            record_alert(bet.market, bet.team, bet.odds_dec, res["edge"], res["stake_units"], res["p_model"], raw)


# ============================================================================
# LIVE MARKET POLLING (Polymarket every 30 min)
# ============================================================================
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com/markets"
TEAM_RE_POLY = re.compile(r"Will (.+?) win the 2026 FIFA World Cup\??", re.I)
REFRESH_INTERVAL_SEC = int(os.getenv("REFRESH_INTERVAL_SEC", "1800"))


def fetch_polymarket_winner() -> dict:
    out = {}
    try:
        for offset in range(0, 2000, 500):
            r = requests.get(POLYMARKET_GAMMA, params={
                "closed": "false", "active": "true",
                "limit": "500", "offset": str(offset),
            }, timeout=15)
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            for m in data:
                q = m.get("question") or ""
                match = TEAM_RE_POLY.match(q)
                if not match:
                    continue
                team = normalize_team(match.group(1))
                bid = m.get("bestBid")
                ask = m.get("bestAsk")
                mid = ((bid + ask) / 2) if (bid and ask) else m.get("lastTradePrice")
                if mid:
                    out[team] = mid
            if len(data) < 500:
                break
    except Exception as e:
        logging.exception("Polymarket fetch failed: %s", e)
    return out


async def refresh_market_loop():
    while True:
        try:
            logging.info("[POLL] Refreshing Polymarket winner market...")
            poly_probs = await asyncio.to_thread(fetch_polymarket_winner)
            updated = 0
            for team, mid_prob in poly_probs.items():
                rec = MODEL["teams"].setdefault(team, {"mc": {}, "market": {}})
                rec.setdefault("market", {})["winner"] = mid_prob
                updated += 1
            logging.info("[POLL] Polymarket: updated %d teams", updated)
        except Exception as e:
            logging.exception("[POLL] Refresh loop error: %s", e)
        await asyncio.sleep(REFRESH_INTERVAL_SEC)


# ============================================================================
# IZIBET CLIENT (background thread + async digest)
# ============================================================================
_izibet_client = None
_izibet_thread = None
_izibet_stop = None


def start_izibet_thread():
    """Lance le client Izibet dans un thread en background (sync requests)."""
    global _izibet_client, _izibet_thread, _izibet_stop
    if not IZIBET_AVAILABLE or not IZIBET_ENABLED:
        logging.info("[IZIBET] Disabled or module not available")
        return
    if _izibet_thread and _izibet_thread.is_alive():
        return
    _izibet_client = IzibetClient()
    _izibet_stop = threading.Event()
    _izibet_thread = threading.Thread(
        target=izibet_refresh_loop_blocking,
        args=(_izibet_client, [COUPON_OVERVIEW], IZIBET_POLL_SEC, _izibet_stop),
        daemon=True,
        name="izibet-poller",
    )
    _izibet_thread.start()
    logging.info("[IZIBET] Background thread started (poll=%ds)", IZIBET_POLL_SEC)


async def izibet_digest_loop():
    """Envoie un digest Telegram des matches CDM 2026 detectes sur Izibet."""
    while True:
        await asyncio.sleep(IZIBET_DIGEST_MIN * 60)
        if not _izibet_client:
            continue
        try:
            cdm = _izibet_client.cdm_matches()
            total_events = len(_izibet_client.events)
            if not cdm and total_events == 0:
                continue
            lines = [
                f"🎯 *Izibet Bet Tracker — CDM 2026 watch*",
                f"Events catalog: {total_events}",
                f"CDM-eligible matches: {len(cdm)}",
                "",
            ]
            for m in cdm[:15]:
                lines.append(f"• {m.home_canonical or m.home} vs {m.away_canonical or m.away}  · {m.start_date[:16]}")
            text = "\n".join(lines)
            send_alert(text)
            logging.info("[IZIBET] Digest sent (%d CDM matches)", len(cdm))
        except Exception as e:
            logging.exception("[IZIBET] Digest loop error: %s", e)


async def izibet_outright_watcher_loop():
    """Periodically scan Izibet's "Largo Plazo" (Long Term / outright) section
    looking for new CDM 2026 outright markets (Winner, Top Scorer, Group Winner).

    Currently Izibet shop catalog has no CDM 2026 outrights listed; they are
    expected to appear ~2-4 weeks before tournament kickoff (June 2026). This
    watcher pings via Telegram the FIRST time a matching market is detected,
    so Michael can manually go check the cotes (and we can then extend the
    client to auto-parse outright Market/Outcome entries).
    """
    # Initial delay so the bot has time to create its Izibet session
    await asyncio.sleep(60)
    while True:
        try:
            if _izibet_client:
                outrights = await asyncio.to_thread(_izibet_client.scan_outright_section)
                new_ones = []
                for o in outrights:
                    eid = o.get("event_id") or o.get("title")
                    if eid and eid not in _izibet_outright_seen:
                        _izibet_outright_seen.add(eid)
                        new_ones.append(o)
                if new_ones:
                    lines = [
                        "🚨 *IZIBET — OUTRIGHT CDM 2026 DETECTÉ*",
                        "",
                        "Nouveaux marchés outright disponibles dans la section "
                        "*Largo Plazo* (Long Term) sur l'app Izibet Bet Tracker :",
                        "",
                    ]
                    for o in new_ones[:10]:
                        lines.append(f"• {o.get('title', '?')} ({o.get('type', '?')})")
                    lines.append("")
                    lines.append("➡️ Vas voir les cotes dans l'app maintenant.")
                    lines.append("➡️ Reviens me dire de coder le parser auto.")
                    send_alert("\n".join(lines))
                    logging.info("[IZIBET] Outright detected: %d new markets",
                                 len(new_ones))
                else:
                    logging.info("[IZIBET] Outright scan: no CDM markets yet "
                                 "(found %d total outrights)", len(outrights))
        except Exception as e:
            logging.exception("[IZIBET] Outright watcher error: %s", e)
        await asyncio.sleep(IZIBET_OUTRIGHT_SCAN_MIN * 60)


async def izibet_scraper_loop():
    """Headless Chromium scraper for Izibet outright cotes.

    Disabled by default (IZIBET_SCRAPER_ENABLED=0). Activate when running on
    Oracle Cloud Free or any host with Playwright + Chromium installed:
        pip install playwright && playwright install chromium
        IZIBET_SCRAPER_ENABLED=1
    """
    if not IZIBET_SCRAPER_AVAILABLE:
        logging.warning("[SCRAPER] Playwright not available — install with "
                        "`pip install playwright && playwright install chromium`")
        return
    await asyncio.sleep(180)  # initial delay
    while True:
        try:
            cotes = await asyncio.to_thread(scrape_largo_plazo)
            value_bets_found = 0
            for c in cotes:
                # Map team/player name to canonical
                canonical = normalize_team_es(c.selection) or c.selection
                # Only evaluate against teams in our MODEL (CDM nations)
                if canonical not in MODEL["teams"] and c.market_type != "top_scorer":
                    continue
                bet = ParsedBet(
                    team=canonical,
                    market=c.market_type or "winner",
                    odds_dec=c.odds,
                    raw=f"izibet-scraper:{c.market_event_id}:{c.selection}@{c.odds}",
                )
                if was_alerted(bet.market, bet.team, bet.odds_dec):
                    continue
                res = evaluate(bet)
                logging.info("[SCRAPER] %s/%s @%.2f → %s",
                             bet.market, bet.team, bet.odds_dec, res["reason"])
                if not res["value"]:
                    continue
                # dedup the same alert this session (avoid pinging if scraper sees same cote twice)
                key = f"{bet.market}|{bet.team}|{round(bet.odds_dec, 2)}"
                if key in _izibet_scraper_seen_alerts:
                    continue
                _izibet_scraper_seen_alerts.add(key)
                text = format_alert(res)
                if send_alert(text):
                    record_alert(bet.market, bet.team, bet.odds_dec,
                                 res["edge"], res["stake_units"],
                                 res["p_model"], bet.raw)
                    value_bets_found += 1
            logging.info("[SCRAPER] Cycle done: %d cotes scraped, %d value bets alerted",
                         len(cotes), value_bets_found)
        except Exception as e:
            logging.exception("[SCRAPER] loop error: %s", e)
        await asyncio.sleep(IZIBET_SCRAPER_INTERVAL_MIN * 60)


async def post_init(app):
    asyncio.create_task(refresh_market_loop())
    asyncio.create_task(refresh_betfair_loop())
    if IZIBET_AVAILABLE and IZIBET_ENABLED:
        start_izibet_thread()
        # Hourly digest disabled by default (only listed match metadata, no value).
        # Set IZIBET_DIGEST_ENABLED=1 in env to re-enable. Will become useful once
        # the client is extended to fetch outright Market/Outcome cotes.
        if os.getenv("IZIBET_DIGEST_ENABLED", "0") == "1":
            asyncio.create_task(izibet_digest_loop())
        # Always-on outright watcher: pings when CDM outrights appear on Izibet.
        asyncio.create_task(izibet_outright_watcher_loop())
    if IZIBET_SCRAPER_ENABLED and IZIBET_SCRAPER_AVAILABLE:
        asyncio.create_task(izibet_scraper_loop())
        logging.info("[SCRAPER] Headless scraper enabled, interval=%dmin",
                     IZIBET_SCRAPER_INTERVAL_MIN)
    elif IZIBET_SCRAPER_ENABLED and not IZIBET_SCRAPER_AVAILABLE:
        logging.warning("[SCRAPER] Enabled but Playwright not installed — skipping")
    logging.info("Background polling started (every %ds)", REFRESH_INTERVAL_SEC)


# ============================================================================
# BETFAIR EXCHANGE POLLING
# ============================================================================
BETFAIR_USERNAME = os.getenv("BETFAIR_USERNAME", "")
BETFAIR_PASSWORD = os.getenv("BETFAIR_PASSWORD", "")
BETFAIR_APP_KEY = os.getenv("BETFAIR_APP_KEY", "")
BETFAIR_COMP_NAME = os.getenv("BETFAIR_COMPETITION_NAME", "FIFA World Cup")

_BF_SESSION = {"token": None, "expires": 0}


def betfair_login() -> str | None:
    if not (BETFAIR_USERNAME and BETFAIR_PASSWORD and BETFAIR_APP_KEY):
        return None
    if _BF_SESSION["token"] and _BF_SESSION["expires"] > time.time():
        return _BF_SESSION["token"]
    try:
        r = requests.post("https://identitysso.betfair.com/api/login",
            headers={"X-Application": BETFAIR_APP_KEY,
                     "Content-Type": "application/x-www-form-urlencoded",
                     "Accept": "application/json"},
            data={"username": BETFAIR_USERNAME, "password": BETFAIR_PASSWORD},
            timeout=15)
        r.raise_for_status()
        j = r.json()
        if j.get("status") != "SUCCESS":
            logging.error("Betfair login failed: %s", j)
            return None
        _BF_SESSION["token"] = j["token"]
        _BF_SESSION["expires"] = time.time() + 3600 * 3  # 3h
        return j["token"]
    except Exception as e:
        logging.exception("Betfair login error: %s", e)
        return None


def bf_api(endpoint: str, payload: dict, token: str):
    r = requests.post(f"https://api.betfair.com/exchange/betting/rest/v1.0/{endpoint}",
        json=payload,
        headers={"X-Application": BETFAIR_APP_KEY, "X-Authentication": token,
                 "Content-Type": "application/json", "Accept": "application/json"},
        timeout=15)
    r.raise_for_status()
    return r.json()


def fetch_betfair_all() -> dict:
    """Returns {(market_key, selection): mid_prob}."""
    out = {}
    token = betfair_login()
    if not token:
        return out
    try:
        # Find WC competition
        comps = bf_api("listCompetitions/",
            {"filter": {"eventTypeIds": ["1"], "textQuery": BETFAIR_COMP_NAME}}, token)
        if not comps:
            return out
        comp_id = sorted(comps, key=lambda c: -c.get("marketCount", 0))[0]["competition"]["id"]

        # List markets
        markets = bf_api("listMarketCatalogue/", {
            "filter": {"competitionIds": [comp_id]},
            "marketProjection": ["EVENT", "MARKET_DESCRIPTION", "RUNNER_DESCRIPTION"],
            "maxResults": "200", "sort": "FIRST_TO_START",
        }, token)

        ids = [m["marketId"] for m in markets]
        prices = {}
        for i in range(0, len(ids), 40):
            batch = ids[i:i+40]
            j = bf_api("listMarketBook/", {
                "marketIds": batch,
                "priceProjection": {"priceData": ["EX_BEST_OFFERS"]},
            }, token)
            for mb in j:
                prices[mb["marketId"]] = mb

        for m in markets:
            mname = (m.get("marketName") or "").lower()
            if "outright winner" in mname or "world cup winner" in mname:
                mk = "winner"
            elif "to reach the final" in mname:
                mk = "finalist"
            elif "to reach the semi" in mname:
                mk = "semi"
            elif "top goalscorer" in mname or "golden boot" in mname:
                mk = "top_scorer"
            else:
                continue
            runners = {r["selectionId"]: r["runnerName"] for r in m.get("runners", [])}
            mb = prices.get(m["marketId"])
            if not mb:
                continue
            for r in mb.get("runners", []):
                sel = normalize_team(runners.get(r["selectionId"], ""))
                ex = r.get("ex", {})
                backs = ex.get("availableToBack") or []
                if not backs:
                    continue
                back = backs[0]["price"]
                lays = ex.get("availableToLay") or []
                lay = lays[0]["price"] if lays else None
                mid_odds = ((back + lay) / 2) if (back and lay) else back
                if mid_odds and mid_odds > 1:
                    out[(mk, sel)] = 1 / mid_odds
    except Exception as e:
        logging.exception("Betfair fetch error: %s", e)
    return out


async def refresh_betfair_loop():
    while True:
        try:
            logging.info("[POLL] Refreshing Betfair markets...")
            bf_probs = await asyncio.to_thread(fetch_betfair_all)
            updated = 0
            for (mk, team), prob in bf_probs.items():
                if mk == "top_scorer":
                    ts = MODEL.setdefault("top_scorer", {}).setdefault(team, {"mc": 0, "team": ""})
                    ts.setdefault("market", 0)
                    ts["market"] = prob
                else:
                    rec = MODEL["teams"].setdefault(team, {"mc": {}, "market": {}})
                    rec.setdefault("market", {})[mk] = prob
                updated += 1
            logging.info("[POLL] Betfair: updated %d pairs", updated)
        except Exception as e:
            logging.exception("[POLL] Betfair refresh error: %s", e)
        await asyncio.sleep(REFRESH_INTERVAL_SEC)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s : %(message)s")
    if not BOT_TOKEN or not CHAT_ID:
        logging.error("Missing BOT_TOKEN or CHAT_ID")
        return
    logging.info("Starting CDM Sniper (single-file, receiver mode)")
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    # Startup notification
    send_alert(
        f"Ã¢ÂÂ *CDM Sniper ON (single-file)*\n"
        f"Ecoute les forwards de @{TARGET}\n"
        f"Edge min: {MIN_EDGE*100:.0f}% ÃÂ· cotes {MIN_ODDS}-{MAX_ODDS}\n"
        f"/status pour l'etat"
    )
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
