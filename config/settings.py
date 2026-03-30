# ============================================================
#  SPORTS COMMENTARY ENGINE — CENTRAL CONFIG
#  All values can be overridden via environment variables.
#  e.g.  SPORT=nfl LLM_PROVIDER=claude python -m ingestion.producer
# ============================================================

import os
from dotenv import load_dotenv

load_dotenv()

# ── SPORT ──────────────────────────────────────────────────
# Options: "nba" | "nfl" | "nhl" | "soccer" | "cricket"
SPORT = os.getenv("SPORT", "nba")

# ── LLM ────────────────────────────────────────────────────
# Options: "openai" | "claude" | "ollama"
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")

# ── API KEYS (fill in or set via env) ─────────────────────
OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OLLAMA_MODEL      = os.getenv("OLLAMA_MODEL", "llama3")

# ── KAFKA ──────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW_EVENTS   = os.getenv("TOPIC_RAW_EVENTS", "game-events")
TOPIC_ENRICHED     = os.getenv("TOPIC_ENRICHED", "enriched-events")
TOPIC_COMMENTARY   = os.getenv("TOPIC_COMMENTARY", "commentary-output")
TOPIC_DLQ          = os.getenv("TOPIC_DLQ", "commentary-dlq")

# ── POLLING ────────────────────────────────────────────────
POLL_INTERVAL_SEC  = int(os.getenv("POLL_INTERVAL_SEC", "10"))
# Cricket uses longer default interval (no ball-by-ball, reduce noise)
CRICKET_POLL_INTERVAL = int(os.getenv("CRICKET_POLL_INTERVAL", "30"))

# ── PERSISTENT STATE ──────────────────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "/tmp")

# ── ESPN API BASE (free, no key needed) ────────────────────
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
ESPN_SPORT_PATHS = {
    "nba":    "basketball/nba",
    "nfl":    "football/nfl",
    "nhl":    "hockey/nhl",
    "soccer": "soccer/eng.1",   # EPL — change to "usa.1" for MLS
}

# ── CRICKET LEAGUES (ESPN uses numeric IDs for cricket) ───
# Set CRICKET_LEAGUE env var to switch leagues
# Common IDs: ipl=8048, psl=8679, bbl=8044, t20i=various
CRICKET_LEAGUE = os.getenv("CRICKET_LEAGUE", "ipl")
CRICKET_LEAGUE_IDS = {
    "ipl":     "8048",    # Indian Premier League
    "psl":     "8679",    # Pakistan Super League
    "bbl":     "8044",    # Big Bash League
    "shield":  "8043",    # Sheffield Shield (AUS)
    "hundred": "8171",    # The Hundred (ENG)
    "cpl":     "8173",    # Caribbean Premier League
    "sa20":    "11375",   # SA20 (South Africa)
}

# ── MULTI-LEAGUE CRICKET (poll multiple leagues simultaneously) ──
# Comma-separated list of league keys, or "all" for header-based discovery
CRICKET_LEAGUES = os.getenv("CRICKET_LEAGUES", "")  # e.g. "ipl,psl,bbl"

# ── CRICKET SCOREBOARD HEADER (all live cricket in one call) ──
ESPN_CRICKET_HEADER = "https://site.api.espn.com/apis/personalized/v2/scoreboard/header"

# ── COMMENTARY STYLE ─────────────────────────────────────
# Options: "default" | "bhogle" | "bishop" | "shastri" | "benaud"
COMMENTARY_STYLE = os.getenv("COMMENTARY_STYLE", "default")

# ── TTS (text-to-speech) ─────────────────────────────────
# Options: "off" | "pyttsx3" | "gtts"
TTS_ENGINE = os.getenv("TTS_ENGINE", "off")
