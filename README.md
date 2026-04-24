# CDM 2026 Sniper — Railway Deploy

Bot Telegram qui ecoute les forwards de @izibet_bet_tracker et envoie des alertes value bet.

## Deployment (Railway)

1. Fork ce repo sur GitHub (ou upload les fichiers)
2. Va sur https://railway.app → New Project → Deploy from GitHub
3. Selectionne ce repo
4. Dans Variables, ajoute :
   - `TELEGRAM_BOT_TOKEN` = ton bot token
   - `TELEGRAM_ALERT_CHAT_ID` = ton chat id numerique
   - `TELEGRAM_TARGET` = izibet_bet_tracker (defaut)
5. Railway deploy auto

Le service tournera 24/7 (plan Starter $5/mois ou free tier avec $5 credit).

## Fichiers

- `main.py` — single-file sniper (parser + detector + receiver Telegram)
- `requirements.txt` — deps Python
- `Procfile` / `railway.json` — config deployment
- `runtime.txt` — Python 3.11

## Env vars supplementaires (optionnel)

- `MIN_EDGE` = 0.08
- `MIN_ODDS` = 1.8
- `MAX_ODDS` = 50
- `BANKROLL_UNITS` = 100
- `KELLY_FRAC` = 0.25
- `MAX_STAKE_UNITS` = 5

## Usage

Forward un message de @izibet_bet_tracker vers ton bot Telegram. Le bot parse, detecte value bets, et envoie une alerte formatee.

Commandes bot : `/start`, `/status`
