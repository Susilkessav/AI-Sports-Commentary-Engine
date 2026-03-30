"""
streaming/enricher.py
─────────────────────
Layer 3: Stream Processing / Event Enrichment

Consumes raw game events from 'game-events' topic, enriches them with:
  - Rolling context window (last 5 plays per game)
  - Game state tracking (score trends, momentum)
  - Duplicate / low-signal filtering (skip routine single-run plays)
  - Event importance scoring (prioritize scoring events)
  - Player context (recent performance in this game)

Publishes enriched events to 'enriched-events' topic.

Run:  python -m streaming.enricher
"""

import json
import signal
import time
import traceback
from collections import deque, defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from config.settings import (
    KAFKA_BOOTSTRAP,
    TOPIC_RAW_EVENTS,
    TOPIC_ENRICHED,
    SPORT,
)


# ── Configuration ──────────────────────────────────────────────
CONTEXT_WINDOW = 5          # rolling window of recent plays per game
MIN_EVENT_GAP_SEC = 3       # suppress events within N seconds of identical type
LOW_SIGNAL_TYPES = {        # event types to filter (unless score changed)
    "runs": 1,              # single runs are low-signal for cricket
}


# ── Kafka setup ────────────────────────────────────────────────

def make_consumer(retries: int = 5) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC_RAW_EVENTS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=False,
                group_id="stream-enricher",
            )
            print(f"[Enricher] Consumer connected, reading '{TOPIC_RAW_EVENTS}'")
            return consumer
        except NoBrokersAvailable:
            wait = 5 * (attempt + 1)
            print(f"[Enricher] Kafka not ready, retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("[Enricher] Could not connect to Kafka.")


def make_producer(retries: int = 5) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            print(f"[Enricher] Producer connected, writing to '{TOPIC_ENRICHED}'")
            return producer
        except NoBrokersAvailable:
            wait = 5 * (attempt + 1)
            print(f"[Enricher] Kafka producer not ready, retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("[Enricher] Could not connect Kafka producer.")


# ── Game state tracker ─────────────────────────────────────────

class GameState:
    """Tracks rolling context for a single game."""

    def __init__(self):
        self.recent_plays: deque = deque(maxlen=CONTEXT_WINDOW)
        self.player_stats: dict = defaultdict(lambda: {"mentions": 0, "scoring": 0})
        self.score_history: list = []   # [(timestamp, home_score, away_score)]
        self.last_event_types: dict = {}  # type -> last_timestamp (dedup)
        self.total_events: int = 0
        self.scoring_events: int = 0
        self.home_team: str = ""
        self.away_team: str = ""

    def add_play(self, event: dict):
        """Add a play to the rolling context."""
        self.recent_plays.append({
            "clock": event.get("clock", ""),
            "period": event.get("period", 1),
            "description": event.get("description", ""),
            "type": event.get("type", ""),
            "score_value": event.get("score_value", False),
        })
        self.total_events += 1
        if event.get("score_value"):
            self.scoring_events += 1

        # Track players
        for athlete in event.get("athletes", []):
            if athlete:
                self.player_stats[athlete]["mentions"] += 1
                if event.get("score_value"):
                    self.player_stats[athlete]["scoring"] += 1

        # Track score progression
        home = event.get("home_score", "0")
        away = event.get("away_score", "0")
        if home or away:
            self.score_history.append({
                "ts": event.get("timestamp", ""),
                "home": str(home),
                "away": str(away),
            })
            # Keep last 20 score snapshots
            if len(self.score_history) > 20:
                self.score_history = self.score_history[-20:]

        self.home_team = event.get("home_team", self.home_team)
        self.away_team = event.get("away_team", self.away_team)

    def get_context(self) -> dict:
        """Return enrichment context for this game."""
        return {
            "recent_plays": list(self.recent_plays),
            "play_count": self.total_events,
            "scoring_count": self.scoring_events,
            "scoring_pct": round(self.scoring_events / max(self.total_events, 1) * 100, 1),
            "top_players": self._top_players(),
            "score_trend": self._score_trend(),
        }

    def _top_players(self, limit=3) -> list:
        """Return most-mentioned players."""
        sorted_players = sorted(
            self.player_stats.items(),
            key=lambda x: x[1]["mentions"],
            reverse=True,
        )
        return [
            {"name": name, "mentions": s["mentions"], "scoring_plays": s["scoring"]}
            for name, s in sorted_players[:limit]
        ]

    def _score_trend(self) -> str:
        """Describe recent score momentum."""
        if len(self.score_history) < 2:
            return "early"
        recent = self.score_history[-5:]
        if len(recent) < 2:
            return "steady"

        # Check if scores are changing rapidly
        changes = 0
        for i in range(1, len(recent)):
            if recent[i]["home"] != recent[i - 1]["home"] or recent[i]["away"] != recent[i - 1]["away"]:
                changes += 1

        if changes >= 3:
            return "high_scoring"
        elif changes >= 1:
            return "active"
        return "quiet"


# ── Event filtering ────────────────────────────────────────────

def should_publish(event: dict, game_state: GameState) -> bool:
    """Decide if an event is worth publishing to enriched topic.
    Filters low-signal noise while keeping all important events.
    """
    event_type = event.get("event_type", "")

    # Always pass through game_state pings (score updates)
    if event_type == "game_state":
        return True

    play_type = event.get("type", "").lower()

    # Always publish scoring events, wickets, goals, touchdowns, milestones
    if event.get("score_value"):
        return True
    high_priority = {"wicket", "boundary", "six", "milestone", "century", "fifty",
                     "five_wickets", "powerplay", "touchdown", "goal", "three point shot"}
    if play_type in high_priority:
        return True

    # Filter low-signal events: single runs in cricket
    sport = event.get("sport", SPORT)
    if sport == "cricket" and play_type == "runs":
        desc = event.get("description", "")
        # Allow if it's 2+ runs (slightly more interesting)
        if "add 1 run" in desc.lower():
            return False  # skip single-run updates

    # Dedup: suppress same event type within MIN_EVENT_GAP_SEC
    now = time.time()
    last_time = game_state.last_event_types.get(play_type, 0)
    if now - last_time < MIN_EVENT_GAP_SEC and play_type not in high_priority:
        return False
    game_state.last_event_types[play_type] = now

    return True


def compute_importance(event: dict) -> int:
    """Score event importance 1-10 for downstream prioritization."""
    score = 3  # baseline
    play_type = event.get("type", "").lower()
    sport = event.get("sport", SPORT)

    if event.get("score_value"):
        score += 2
    if play_type in ("wicket", "touchdown", "goal"):
        score += 3
    if play_type in ("boundary", "six", "century", "five_wickets"):
        score += 2
    if play_type in ("milestone", "fifty", "powerplay"):
        score += 1
    if play_type in ("required_rate",):
        score += 1

    # Cap at 10
    return min(score, 10)


# ── Graceful shutdown ──────────────────────────────────────────

_running = True


def _shutdown_handler(signum, frame):
    global _running
    print(f"\n[Enricher] Received signal {signum}, shutting down...")
    _running = False


signal.signal(signal.SIGTERM, _shutdown_handler)
signal.signal(signal.SIGINT, _shutdown_handler)


# ── Main loop ──────────────────────────────────────────────────

def run():
    consumer = make_consumer()
    producer = make_producer()
    game_states: dict[str, GameState] = {}
    stats = {"processed": 0, "published": 0, "filtered": 0}

    print(f"[Enricher] Running — sport={SPORT}")
    print(f"[Enricher] {TOPIC_RAW_EVENTS} → enrich + filter → {TOPIC_ENRICHED}\n")

    try:
        while _running:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for tp, messages in records.items():
                for msg in messages:
                    event = msg.value
                    stats["processed"] += 1
                    game_id = event.get("game_id", "unknown")

                    # Initialize game state if needed
                    if game_id not in game_states:
                        game_states[game_id] = GameState()

                    gs = game_states[game_id]

                    # Update game state with this event
                    if event.get("event_type") == "play":
                        gs.add_play(event)

                    # Filter decision
                    if not should_publish(event, gs):
                        stats["filtered"] += 1
                        continue

                    # Enrich the event
                    enriched = {
                        **event,
                        "enrichment": gs.get_context(),
                        "importance": compute_importance(event),
                        "enriched_at": datetime.now(timezone.utc).isoformat(),
                    }

                    # Publish to enriched topic
                    producer.send(TOPIC_ENRICHED, key=game_id, value=enriched)
                    stats["published"] += 1

                    if event.get("event_type") == "play":
                        importance = enriched["importance"]
                        desc = event.get("description", "")[:70]
                        print(f"  [imp={importance}] {desc}")

            consumer.commit()
            producer.flush()

            # Log stats periodically
            if stats["processed"] % 50 == 0 and stats["processed"] > 0:
                pct = round(stats["published"] / max(stats["processed"], 1) * 100)
                print(f"[Enricher] Stats: {stats['processed']} processed, "
                      f"{stats['published']} published ({pct}%), "
                      f"{stats['filtered']} filtered")

    finally:
        print(f"\n[Enricher] Final stats: {stats}")
        print("[Enricher] Closing...")
        consumer.close()
        producer.flush()
        producer.close()
        print("[Enricher] Shutdown complete.")


if __name__ == "__main__":
    run()
