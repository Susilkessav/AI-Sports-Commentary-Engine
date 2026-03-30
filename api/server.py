"""
api/server.py
─────────────
FastAPI server with:
  - WebSocket endpoint  → pushes live commentary to browsers
  - REST endpoint       → last N commentary lines (for page refresh)

Run:  uvicorn api.server:app --reload --port 8000
"""

import json
import asyncio
import os
from pathlib import Path
from typing import Set
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
import threading

from config.settings import KAFKA_BOOTSTRAP, TOPIC_COMMENTARY, SPORT
from ingestion.espn_client import ESPNClient

# ── Shared state ────────────────────────────────────────────
_lock = threading.Lock()
active_connections: Set[WebSocket] = set()
recent_commentary:  list[dict]     = []   # rolling buffer for REST
MAX_HISTORY = 50
_main_loop: asyncio.AbstractEventLoop = None
_consumer: KafkaConsumer = None
_shutdown_event = threading.Event()

ALLOWED_ORIGINS = os.getenv(
    "CORS_ORIGINS", "http://localhost:8501,http://localhost:3000"
).split(",")


# ── Kafka listener (runs in background thread) ──────────────

def kafka_listener():
    """Background thread: reads from Kafka, pushes to WebSocket clients."""
    global _consumer
    _consumer = KafkaConsumer(
        TOPIC_COMMENTARY,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id="websocket-broadcaster",
    )
    print(f"[Server] Kafka listener ready on '{TOPIC_COMMENTARY}' (manual commit)")

    while not _shutdown_event.is_set():
        records = _consumer.poll(timeout_ms=1000)
        if not records:
            continue

        for tp, messages in records.items():
            for msg in messages:
                data = msg.value
                with _lock:
                    recent_commentary.append(data)
                    if len(recent_commentary) > MAX_HISTORY:
                        recent_commentary.pop(0)

                print(f"[Server] New commentary: {data.get('play', '')[:60]}")

                # Broadcast to all active WebSocket connections via the main event loop
                if _main_loop and active_connections:
                    asyncio.run_coroutine_threadsafe(_broadcast(data), _main_loop)

        _consumer.commit()

    # Thread exiting — close consumer
    print("[Server] Kafka listener thread exiting, closing consumer...")
    _consumer.close()
    print("[Server] Kafka consumer closed.")


async def _broadcast(data: dict):
    """Send data to all connected WebSocket clients."""
    dead = set()
    for ws in list(active_connections):
        try:
            await ws.send_json(data)
        except Exception:
            dead.add(ws)
    active_connections -= dead


# ── Lifespan (startup + shutdown) ───────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    t = threading.Thread(target=kafka_listener, daemon=True)
    t.start()
    print("[Server] Background Kafka listener started.")

    yield

    # Shutdown
    print("[Server] Shutting down — signaling Kafka listener to stop...")
    _shutdown_event.set()

    # Close all active WebSocket connections
    for ws in list(active_connections):
        try:
            await ws.close()
        except Exception:
            pass
    active_connections.clear()
    print("[Server] All WebSocket connections closed.")
    print("[Server] Shutdown complete.")


app = FastAPI(title="Sports Commentary Engine", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── WebSocket endpoint ──────────────────────────────────────

@app.websocket("/ws/commentary")
async def commentary_ws(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    print(f"[Server] Client connected. Total: {len(active_connections)}")

    # Send recent history to newly connected client
    with _lock:
        history = list(recent_commentary[-10:])
    for item in history:
        await websocket.send_json(item)

    try:
        while True:
            await websocket.receive_text()   # keep connection alive
    except WebSocketDisconnect:
        active_connections.discard(websocket)
        print(f"[Server] Client disconnected. Total: {len(active_connections)}")


# ── REST endpoints ──────────────────────────────────────────

@app.get("/commentary")
def get_recent():
    """Return the last 50 commentary lines."""
    with _lock:
        items = list(recent_commentary[-50:])
    return {"items": items, "count": len(recent_commentary)}


@app.get("/health")
def health():
    return {"status": "ok", "connections": len(active_connections)}


# ── Game schedule endpoints ────────────────────────────────────

_espn_cache: dict = {}
_cache_ttl = 120  # seconds (scoreboard does lookback, so cache longer)


def _cached_espn(key: str, fetcher, ttl: int = _cache_ttl):
    """Simple TTL cache for ESPN API calls."""
    now = _time_mod.time()
    if key in _espn_cache:
        data, ts = _espn_cache[key]
        if now - ts < ttl:
            return data
    data = fetcher()
    _espn_cache[key] = (data, now)
    return data


import time as _time_mod


@app.get("/games/scoreboard")
def games_scoreboard(sport: str = None):
    """Full scoreboard: live, recent completed, and upcoming games."""
    s = sport or SPORT
    def fetch():
        client = ESPNClient(sport=s)
        return client.get_scoreboard_all()
    return _cached_espn(f"scoreboard_{s}", fetch)


@app.get("/games/recent")
def games_recent(sport: str = None):
    """Recently completed games with results."""
    s = sport or SPORT
    def fetch():
        client = ESPNClient(sport=s)
        scoreboard = client.get_scoreboard_all()
        return scoreboard.get("recent", [])
    return {"games": _cached_espn(f"recent_{s}", fetch)}


@app.get("/games/upcoming")
def games_upcoming(sport: str = None):
    """Upcoming scheduled games."""
    s = sport or SPORT
    def fetch():
        client = ESPNClient(sport=s)
        scoreboard = client.get_scoreboard_all()
        return scoreboard.get("upcoming", [])
    return {"games": _cached_espn(f"upcoming_{s}", fetch)}


@app.get("/games/live")
def games_live(sport: str = None):
    """Currently live games (short cache for real-time score updates)."""
    s = sport or SPORT
    def fetch():
        client = ESPNClient(sport=s)
        scoreboard = client.get_scoreboard_all()
        return scoreboard.get("live", [])
    return {"games": _cached_espn(f"live_{s}", fetch, ttl=5)}


_DASHBOARD_HTML = (
    Path(__file__).resolve().parent.parent / "dashboard" / "index.html"
)


@app.get("/", response_class=HTMLResponse)
def root():
    """Serve the live commentary dashboard."""
    return _DASHBOARD_HTML.read_text()


@app.get("/api", response_class=HTMLResponse)
def api_index():
    """API index page with endpoint links."""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AI Sports Commentary — API</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: #0f1117; color: #fafafa;
                display: flex; justify-content: center; align-items: center;
                min-height: 100vh;
            }
            .container { max-width: 600px; padding: 2rem; text-align: center; }
            h1 { font-size: 2.5rem; margin-bottom: 0.5rem; }
            .subtitle { color: #888; margin-bottom: 2rem; }
            .links { display: flex; flex-direction: column; gap: 1rem; margin-top: 1.5rem; }
            a {
                display: block; padding: 1rem 1.5rem; border-radius: 8px;
                background: #1a1d2e; color: #58a6ff; text-decoration: none;
                border: 1px solid #30363d; transition: all 0.2s;
            }
            a:hover { background: #21262d; border-color: #58a6ff; }
            .desc { color: #888; font-size: 0.85rem; margin-top: 0.3rem; }
            .status { margin-top: 2rem; padding: 0.7rem; border-radius: 6px;
                      background: #1a3a1a; color: #3fb950; font-size: 0.9rem; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>API Endpoints</h1>
            <p class="subtitle">AI Sports Commentary Engine</p>
            <div class="status">Server Running</div>
            <div class="links">
                <a href="/">Dashboard<div class="desc">Live commentary dashboard</div></a>
                <a href="/health">/health<div class="desc">Server health & WebSocket connections</div></a>
                <a href="/commentary">/commentary<div class="desc">Recent commentary (last 50, JSON)</div></a>
                <a href="/games/scoreboard">/games/scoreboard<div class="desc">Full scoreboard: live, recent, upcoming</div></a>
                <a href="/games/live">/games/live<div class="desc">Currently live games</div></a>
                <a href="/games/recent">/games/recent<div class="desc">Recently completed games</div></a>
                <a href="/games/upcoming">/games/upcoming<div class="desc">Upcoming scheduled games</div></a>
                <a href="/docs">/docs<div class="desc">Interactive Swagger documentation</div></a>
            </div>
        </div>
    </body>
    </html>
    """
