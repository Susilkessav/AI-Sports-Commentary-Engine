"""
llm/commentator.py
──────────────────
Reads enriched game events from Kafka and generates
live commentary using the configured LLM provider.

Run:  python -m llm.commentator
"""

import json
import signal
import time
import traceback
from collections import deque

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config.settings import (
    KAFKA_BOOTSTRAP,
    TOPIC_ENRICHED,
    TOPIC_COMMENTARY,
    TOPIC_DLQ,
    LLM_PROVIDER,
    OPENAI_API_KEY,
    ANTHROPIC_API_KEY,
    OLLAMA_MODEL,
    SPORT,
    COMMENTARY_STYLE,
    TTS_ENGINE,
)

# Which topic to consume: enriched-events if enricher is running, else raw
import os
CONSUME_TOPIC = os.getenv("COMMENTATOR_TOPIC", TOPIC_ENRICHED)


# ── Context window ──────────────────────────────────────────
# Keeps the last N plays per game so the LLM has continuity
CONTEXT_WINDOW = 8
game_contexts: dict[str, deque] = {}


# ── LLM client singletons ──────────────────────────────────

_openai_client = None
_anthropic_client = None


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client


def _get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        import anthropic
        _anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    return _anthropic_client


# ── LLM Providers ──────────────────────────────────────────

class TransientLLMError(Exception):
    """Raised for retryable LLM errors (timeouts, rate limits, server errors)."""
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type(TransientLLMError),
    reraise=True,
)
def call_llm(prompt: str) -> str:
    """Route to the correct LLM based on config. Retries up to 3x with backoff."""
    if LLM_PROVIDER == "openai":
        return _call_openai(prompt)
    elif LLM_PROVIDER == "claude":
        return _call_claude(prompt)
    elif LLM_PROVIDER == "ollama":
        return _call_ollama(prompt)
    else:
        raise ValueError(f"Unknown LLM_PROVIDER: {LLM_PROVIDER}")


def _call_openai(prompt: str) -> str:
    try:
        client = _get_openai_client()
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": get_system_prompt()},
                {"role": "user",   "content": prompt},
            ],
            max_tokens=120,
            temperature=0.85,
        )
        return resp.choices[0].message.content.strip()
    except (TimeoutError, ConnectionError) as e:
        raise TransientLLMError(str(e)) from e
    except Exception as e:
        if "rate" in str(e).lower() or "timeout" in str(e).lower() or "server" in str(e).lower():
            raise TransientLLMError(str(e)) from e
        raise


def _call_claude(prompt: str) -> str:
    try:
        client = _get_anthropic_client()
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=120,
            system=get_system_prompt(),
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()
    except (TimeoutError, ConnectionError) as e:
        raise TransientLLMError(str(e)) from e
    except Exception as e:
        if "rate" in str(e).lower() or "timeout" in str(e).lower() or "overloaded" in str(e).lower():
            raise TransientLLMError(str(e)) from e
        raise


def _call_ollama(prompt: str) -> str:
    import requests
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": f"{get_system_prompt()}\n\n{prompt}",
        "stream": False,
    }
    try:
        resp = requests.post("http://localhost:11434/api/generate", json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except (requests.ConnectionError, requests.Timeout) as e:
        raise TransientLLMError(str(e)) from e
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code >= 500:
            raise TransientLLMError(str(e)) from e
        raise


# ── Prompt builder ──────────────────────────────────────────

SYSTEM_PROMPT = """You are an enthusiastic, witty sports broadcaster.
Generate 1-2 sentences of live play-by-play commentary for the given event.
Be energetic, natural, and specific to the play. Vary your sentence structure.
Never repeat the exact same phrasing. Keep it under 40 words."""

CRICKET_SYSTEM_PROMPT = """You are an enthusiastic, knowledgeable cricket commentator.
Generate 1-2 sentences of live cricket commentary for the given event.
Use cricket terminology naturally (overs, wickets, boundaries, crease, yorker, etc.).
Be energetic and insightful. Vary your style — mix excitement with analysis.
Never repeat the exact same phrasing. Keep it under 50 words."""

# ── Commentary Style Personas ─────────────────────────────────

COMMENTARY_STYLES = {
    "bhogle": """You are Harsha Bhogle, the beloved Indian cricket commentator.
Your style: articulate, analytical, warm. You find the story within the game.
Use elegant phrasing with occasional Hindi cricket terms. Balance stats with emotion.
You love celebrating young talent and finding narrative arcs in matches.
Generate 1-2 sentences. Keep it under 50 words.""",

    "bishop": """You are Ian Bishop, the legendary West Indian cricket commentator.
Your style: explosive excitement on big moments, deep technical knowledge.
Famous for calls like "Remember the name!" Use dramatic pauses (—) for emphasis.
On wickets and sixes, be electric. On quiet moments, offer bowling analysis.
Generate 1-2 sentences. Keep it under 50 words.""",

    "shastri": """You are Ravi Shastri, the iconic Indian cricket commentator.
Your style: bold, dramatic, larger-than-life. Everything is TRACER BULLET and
LIKE A MILLION DOLLARS. Use superlatives freely. Repeat key words for emphasis.
Boundaries are "into the stands!" Wickets are "GONE! That's plumb!"
Generate 1-2 sentences. Keep it under 50 words.""",

    "benaud": """You are Richie Benaud, the master of understated cricket commentary.
Your style: minimal, precise, letting the game speak. Less is more.
Use dry wit. Never over-commentate. "Marvellous" is your highest praise.
Describe what happened simply, then add one insightful observation.
Generate 1-2 sentences. Keep it under 40 words.""",
}


def get_system_prompt() -> str:
    """Return the appropriate system prompt based on sport and commentary style."""
    if SPORT == "cricket":
        if COMMENTARY_STYLE in COMMENTARY_STYLES:
            base_prompt = COMMENTARY_STYLES[COMMENTARY_STYLE]
        else:
            base_prompt = CRICKET_SYSTEM_PROMPT
    else:
        base_prompt = SYSTEM_PROMPT

    return base_prompt + "\n\n" + \
           "IMPORTANT: You MUST respond in valid JSON format with exactly three keys:\n" + \
           "1. 'hype_score': An integer from 1 to 10 rating the excitement and impact of the play.\n" + \
           "2. 'home_win_prob': An integer from 0 to 100 estimating the home team's win probability logically based on the current score and time remaining.\n" + \
           "3. 'commentary': The text commentary generation.\n" + \
           "Match the tone of your commentary strictly to the assigned hype score (e.g., a 2/10 should be calm and analytical, a 10/10 should be absolute explosive hype and unhinged)."


def build_prompt(event: dict, context: list[dict]) -> str:
    if SPORT == "cricket":
        return _build_cricket_prompt(event, context)
    return _build_standard_prompt(event, context)


def _build_standard_prompt(event: dict, context: list[dict]) -> str:
    ctx_lines = "\n".join(
        f"  - [{p.get('clock','')}] {p.get('description','')}"
        for p in context
    )
    return f"""GAME: {event.get('away_team')} @ {event.get('home_team')}
SCORE: {event.get('away_score')}–{event.get('home_score')} | Period {event.get('period',1)} {event.get('clock','')}

RECENT PLAYS:
{ctx_lines if ctx_lines else '  (start of game)'}

NEW PLAY TO COMMENTATE:
  [{event.get('clock','')}] {event.get('description','')}
  Players involved: {', '.join(event.get('athletes', [])) or 'N/A'}
  Scoring play: {event.get('score_value', False)}

Generate commentary for this specific play now:"""


def _build_cricket_prompt(event: dict, context: list[dict]) -> str:
    ctx_lines = "\n".join(
        f"  - [{p.get('clock','')}] {p.get('description','')}"
        for p in context
    )
    event_type = event.get("type", "update")
    athletes = ', '.join(event.get('athletes', [])) or 'N/A'

    return f"""MATCH: {event.get('away_team')} vs {event.get('home_team')}
SCORE: {event.get('away_score')} / {event.get('home_score')} | Innings {event.get('period', 1)} | {event.get('clock', '')}

RECENT EVENTS:
{ctx_lines if ctx_lines else '  (start of innings)'}

NEW EVENT TO COMMENTATE:
  [{event.get('clock', '')}] {event.get('description', '')}
  Event type: {event_type}
  Players: {athletes}

Generate cricket commentary for this event now:"""


# ── Graceful shutdown ───────────────────────────────────────

_running = True


def _shutdown_handler(signum, frame):
    global _running
    print(f"\n[Commentator] Received signal {signum}, shutting down gracefully...")
    _running = False


signal.signal(signal.SIGTERM, _shutdown_handler)
signal.signal(signal.SIGINT, _shutdown_handler)


# ── Text-to-Speech ────────────────────────────────────────────

_tts_engine = None


def _speak(text: str):
    """Speak commentary aloud if TTS is enabled. Non-blocking."""
    if TTS_ENGINE == "off":
        return

    import threading

    def _do_speak():
        global _tts_engine
        try:
            if TTS_ENGINE == "pyttsx3":
                import pyttsx3
                if _tts_engine is None:
                    _tts_engine = pyttsx3.init()
                    _tts_engine.setProperty("rate", 175)
                _tts_engine.say(text)
                _tts_engine.runAndWait()
            elif TTS_ENGINE == "gtts":
                from gtts import gTTS
                import tempfile
                import os
                tts = gTTS(text=text, lang="en", slow=False)
                with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
                    tts.save(f.name)
                    os.system(f"afplay {f.name} 2>/dev/null || mpg123 -q {f.name} 2>/dev/null")
                    os.unlink(f.name)
        except Exception as e:
            print(f"[TTS] Error: {e}")

    threading.Thread(target=_do_speak, daemon=True).start()


# ── Main consumer loop ──────────────────────────────────────

def _make_consumer(retries: int = 5) -> KafkaConsumer:
    """Create Kafka consumer with retry logic for startup."""
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                CONSUME_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=False,
                group_id="commentary-generator",
            )
            print(f"[Commentator] Consumer connected to Kafka")
            return consumer
        except NoBrokersAvailable:
            wait = 5 * (attempt + 1)
            print(f"[Commentator] Kafka not ready, retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("[Commentator] Could not connect to Kafka after retries.")


def _make_producer(retries: int = 5) -> KafkaProducer:
    """Create Kafka producer with retry logic for startup."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[Commentator] Producer connected to Kafka")
            return producer
        except NoBrokersAvailable:
            wait = 5 * (attempt + 1)
            print(f"[Commentator] Kafka producer not ready, retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("[Commentator] Could not connect Kafka producer after retries.")


def run():
    consumer = _make_consumer()
    producer = _make_producer()

    print(f"[Commentator] Listening on '{CONSUME_TOPIC}' using {LLM_PROVIDER.upper()}")
    print(f"[Commentator] Publishing commentary to '{TOPIC_COMMENTARY}'")
    print(f"[Commentator] Dead-letter topic: '{TOPIC_DLQ}'")
    print(f"[Commentator] Manual offset commit enabled\n")

    try:
        while _running:
            # Poll with timeout so we can check _running flag
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for tp, messages in records.items():
                for msg in messages:
                    if not _running:
                        break

                    event = msg.value

                    # Only commentate on actual plays, skip pure game_state pings
                    if event.get("event_type") != "play":
                        continue

                    description = event.get("description", "").strip()
                    if not description:
                        continue

                    game_id = event.get("game_id", "unknown")

                    # Maintain rolling context per game
                    if game_id not in game_contexts:
                        game_contexts[game_id] = deque(maxlen=CONTEXT_WINDOW)
                    ctx = list(game_contexts[game_id])

                    print(f"[Commentator] Generating for: {description[:60]}...")

                    try:
                        prompt      = build_prompt(event, ctx)
                        llm_output  = call_llm(prompt)

                        import re
                        commentary = llm_output
                        hype_score = 5
                        home_win_prob = 50

                        try:
                            # Try to cleanly parse JSON (strip markdown if present)
                            clean_out = re.sub(r'```(?:json)?\n?(.*?)\n?```', r'\1', llm_output, flags=re.DOTALL).strip()
                            parsed = json.loads(clean_out)
                            commentary = parsed.get("commentary", commentary)
                            hype_score = int(parsed.get("hype_score", 5))
                            if "home_win_prob" in parsed:
                                home_win_prob = int(parsed["home_win_prob"])
                        except Exception:
                            # Fallback if the LLM didn't return valid JSON
                            match_hype = re.search(r'"hype_score"\s*:\s*(\d+)', llm_output)
                            if match_hype:
                                hype_score = int(match_hype.group(1))
                            match_prob = re.search(r'"home_win_prob"\s*:\s*(\d+)', llm_output)
                            if match_prob:
                                home_win_prob = int(match_prob.group(1))
                            match_text = re.search(r'"commentary"\s*:\s*"(.*?)"', llm_output, flags=re.DOTALL)
                            if match_text:
                                commentary = match_text.group(1).replace('\\"', '"').replace('\\n', '\n')

                        # Add this play to context for next call
                        game_contexts[game_id].append({
                            "clock":       event.get("clock", ""),
                            "description": description,
                        })

                        output = {
                            "game_id":    game_id,
                            "sport":      event.get("sport", SPORT),
                            "home_team":  event.get("home_team"),
                            "away_team":  event.get("away_team"),
                            "home_score": event.get("home_score"),
                            "away_score": event.get("away_score"),
                            "period":     event.get("period"),
                            "clock":      event.get("clock"),
                            "play":       description,
                            "commentary": commentary,
                            "hype_score": hype_score,
                            "home_win_prob": home_win_prob,
                            "timestamp":  event.get("timestamp"),
                            "type":       event.get("type", ""),
                            "score_value": event.get("score_value", False),
                            "importance": event.get("importance", 5),
                        }

                        producer.send(TOPIC_COMMENTARY, value=output)
                        producer.flush()
                        print(f'  🎙  "{commentary}"\n')

                        # Text-to-speech (non-blocking)
                        _speak(commentary)

                    except Exception as e:
                        # All retries exhausted — send to Dead Letter Queue
                        print(f"[Commentator] LLM failed after retries: {e}")
                        traceback.print_exc()
                        print(f"[Commentator] Sending play to DLQ: {TOPIC_DLQ}")
                        dlq_payload = {
                            "original_event": event,
                            "error": str(e),
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                        }
                        try:
                            producer.send(TOPIC_DLQ, value=dlq_payload)
                            producer.flush()
                        except Exception as dlq_err:
                            print(f"[Commentator] DLQ publish also failed: {dlq_err}")

            # Commit offsets after processing the entire batch
            consumer.commit()

    finally:
        print("[Commentator] Closing Kafka consumer & producer...")
        consumer.close()
        producer.flush()
        producer.close()
        print("[Commentator] Shutdown complete.")


if __name__ == "__main__":
    run()
