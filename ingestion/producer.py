"""
ingestion/producer.py
─────────────────────
Polls ESPN for live game events and publishes them
to the Kafka 'game-events' topic every POLL_INTERVAL_SEC.

Run:  python -m ingestion.producer
"""

import json
import time
import hashlib
import signal
import traceback
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config.settings import (
    KAFKA_BOOTSTRAP,
    TOPIC_RAW_EVENTS,
    POLL_INTERVAL_SEC,
    CRICKET_POLL_INTERVAL,
    SPORT,
    DATA_DIR,
    CRICKET_LEAGUES,
    CRICKET_LEAGUE_IDS,
)
from ingestion.espn_client import ESPNClient


# ── Kafka setup ─────────────────────────────────────────────

def make_producer(retries: int = 5) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            print(f"[Producer] Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            wait = 5 * (attempt + 1)
            print(f"[Producer] Kafka not ready, retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("[Producer] Could not connect to Kafka after retries.")


# ── Persistent deduplication ────────────────────────────────

SEEN_FILE = Path(DATA_DIR) / "seen_hashes.jsonl"
MAX_SEEN_HASHES = 10_000


def load_seen() -> dict:
    """Load previously-seen play hashes from disk (dict preserves insertion order)."""
    seen = {}
    if SEEN_FILE.exists():
        with open(SEEN_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    seen[line] = None
        # Truncate if file has grown too large
        if len(seen) > MAX_SEEN_HASHES:
            _truncate_seen(seen)
        print(f"[Producer] Loaded {len(seen)} seen hashes from {SEEN_FILE}")
    return seen


def _truncate_seen(seen: dict):
    """Keep only the most recent hashes and rewrite the file."""
    # Keep the last MAX_SEEN_HASHES / 2 entries to avoid frequent rewrites
    keep = MAX_SEEN_HASHES // 2
    all_hashes = list(seen)
    # dict preserves insertion order, so [-keep:] gives the most recent
    recent = all_hashes[-keep:]
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SEEN_FILE, "w") as f:
        for h in recent:
            f.write(h + "\n")
    seen.clear()
    for h in recent:
        seen[h] = None
    print(f"[Producer] Truncated seen hashes from {len(all_hashes)} to {len(seen)}")


def persist_hash(h: str):
    """Append a single hash to the persistent file."""
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SEEN_FILE, "a") as f:
        f.write(h + "\n")


def play_hash(play: dict) -> str:
    """Unique fingerprint for a play so we don't re-publish duplicates."""
    key = f"{play.get('play_id')}|{play.get('description')}"
    return hashlib.md5(key.encode()).hexdigest()


# ── Graceful shutdown ───────────────────────────────────────

_running = True


def _shutdown_handler(signum, frame):
    global _running
    print(f"\n[Producer] Received signal {signum}, shutting down gracefully...")
    _running = False


signal.signal(signal.SIGTERM, _shutdown_handler)
signal.signal(signal.SIGINT, _shutdown_handler)


# ── Main loop ───────────────────────────────────────────────

def _get_poll_interval(game: dict | None = None) -> int:
    """Return adaptive poll interval. Cricket uses longer base interval,
    shortened during exciting moments (last 5 overs, close chases)."""
    if SPORT != "cricket":
        return POLL_INTERVAL_SEC

    base = CRICKET_POLL_INTERVAL
    if not game:
        return base

    # Shorten poll during exciting moments
    situation = game.get("situation", "").lower()
    overs_str = game.get("clock", "0")
    try:
        overs = float(str(overs_str).replace(" ov", ""))
    except (ValueError, TypeError):
        overs = 0

    # Last 5 overs of a T20 — poll faster
    if overs >= 15:
        base = min(base, 15)

    # Close chase or tight match — poll faster
    if "target" in situation:
        base = min(base, 15)

    # Powerplay (first 6 overs) — slightly faster
    if overs <= 6:
        base = min(base, 20)

    return base


def _build_clients() -> list[ESPNClient]:
    """Build ESPN client(s). For cricket with CRICKET_LEAGUES, create one per league."""
    if SPORT == "cricket" and CRICKET_LEAGUES:
        clients = []
        for key in CRICKET_LEAGUES.split(","):
            key = key.strip()
            if key in CRICKET_LEAGUE_IDS:
                # Temporarily override the league for each client
                import os
                orig = os.environ.get("CRICKET_LEAGUE", "")
                os.environ["CRICKET_LEAGUE"] = key
                # Reload won't work, so we construct manually
                from config.settings import ESPN_BASE
                client = ESPNClient.__new__(ESPNClient)
                client.sport = "cricket"
                client.is_cricket = True
                league_id = CRICKET_LEAGUE_IDS[key]
                client.path = f"cricket/{league_id}"
                client.base = f"{ESPN_BASE}/{client.path}"
                client._prev_scores = {}
                clients.append(client)
                os.environ["CRICKET_LEAGUE"] = orig
                print(f"[Producer] Added cricket league: {key} (ID: {league_id})")
        return clients if clients else [ESPNClient(sport=SPORT)]
    return [ESPNClient(sport=SPORT)]


def run():
    clients  = _build_clients()
    producer = make_producer()
    seen     = load_seen()

    effective_interval = CRICKET_POLL_INTERVAL if SPORT == "cricket" else POLL_INTERVAL_SEC
    leagues_info = f", leagues={CRICKET_LEAGUES}" if CRICKET_LEAGUES else ""
    print(f"[Producer] Starting — sport={SPORT}{leagues_info}, base poll interval={effective_interval}s")
    print(f"[Producer] Publishing to topic: {TOPIC_RAW_EVENTS}")
    print(f"[Producer] Dedup file: {SEEN_FILE}\n")

    try:
        while _running:
            try:
                games = []
                for client in clients:
                    for g in client.get_live_games():
                        g["_client_base"] = client.base
                        games.append(g)

                if not games:
                    print(f"[{_now()}] No live games right now. Waiting...")
                else:
                    print(f"[{_now()}] Found {len(games)} live game(s)")

                for game in games:
                    game_id = game["game_id"]
                    if SPORT == "cricket":
                        overs = game.get("clock", "")
                        situation = game.get("situation", "")
                        print(f"  → {game['away_team']} vs {game['home_team']} "
                              f"({game['away_score']} / {game['home_score']}) "
                              f"| {situation or overs}")
                    else:
                        print(f"  → {game['away_team']} @ {game['home_team']} "
                              f"({game['away_score']}–{game['home_score']}) "
                              f"| Period {game['period']} {game['clock']}")

                    # Publish game state update (strip internal _client_base key)
                    game_event = {
                        "event_type": "game_state",
                        "timestamp":  _now(),
                        **{k: v for k, v in game.items() if not k.startswith("_")},
                    }
                    producer.send(TOPIC_RAW_EVENTS, key=game_id, value=game_event)

                    # Match the client that found this game
                    game_client = clients[0]
                    game_base = game.get("_client_base", "")
                    if game_base:
                        for c in clients:
                            if c.base == game_base:
                                game_client = c
                                break
                    plays = game_client.get_plays(game_id)
                    new_plays = 0
                    for play in plays:
                        h = play_hash(play)
                        if h in seen:
                            continue
                        seen[h] = None
                        persist_hash(h)
                        new_plays += 1

                        play_event = {
                            "event_type": "play",
                            "timestamp":  _now(),
                            "game_id":    game_id,
                            "sport":      SPORT,
                            "home_team":  game["home_team"],
                            "away_team":  game["away_team"],
                            "home_score": game["home_score"],
                            "away_score": game["away_score"],
                            **play,
                        }
                        producer.send(TOPIC_RAW_EVENTS, key=game_id, value=play_event)
                        print(f"    ▶ [{play.get('clock')}] {play.get('description', '')[:80]}")

                    if new_plays == 0:
                        print(f"    (no new plays since last poll)")

                    # Truncate dedup set periodically
                    if len(seen) > MAX_SEEN_HASHES:
                        _truncate_seen(seen)

                producer.flush()

            except Exception as e:
                print(f"[Producer] Error: {e}")
                traceback.print_exc()

            # Adaptive sleep — cricket adjusts based on match situation
            last_game = games[-1] if games else None
            interval = _get_poll_interval(last_game)
            for _ in range(interval):
                if not _running:
                    break
                time.sleep(1)

    finally:
        print("[Producer] Closing Kafka producer...")
        producer.flush()
        producer.close()
        print("[Producer] Shutdown complete.")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")


if __name__ == "__main__":
    run()
