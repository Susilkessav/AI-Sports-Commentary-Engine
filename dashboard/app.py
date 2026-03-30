"""
dashboard/app.py
────────────────
Streamlit live commentary dashboard.
Connects to the FastAPI backend via WebSocket (primary) and REST (fallback):
  - Sport selector (NBA, NFL, NHL, Soccer, Cricket)
  - Live score card with team branding
  - Stats row (plays, scoring %, last updated)
  - Scrolling commentary ticker with event-type badges

Run:  streamlit run dashboard/app.py
"""

import streamlit as st
import websocket
import requests
import json
import time
import os
import threading
from collections import deque
from datetime import datetime

st.set_page_config(
    page_title="AI Sports Commentary",
    page_icon="🎙",
    layout="wide",
)

# ── Custom CSS for dark polished theme ──────────────────────
st.markdown("""
<style>
    /* Dark theme overrides */
    .stApp {
        background-color: #0e1117;
    }

    /* Top bar styling */
    .topbar {
        display: flex;
        align-items: center;
        gap: 1rem;
        padding: 0.5rem 0;
        margin-bottom: 0.5rem;
    }
    .topbar h1 {
        font-size: 1.6rem;
        font-weight: 700;
        margin: 0;
        color: #fafafa;
    }
    .topbar .subtitle {
        color: #888;
        font-size: 0.85rem;
    }

    /* Live badge */
    .live-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 14px;
        border-radius: 999px;
        font-size: 0.78rem;
        font-weight: 600;
    }
    .live-badge.on {
        background: #1b3a1b;
        color: #4caf50;
    }
    .live-badge.off {
        background: #3a2a1b;
        color: #ff9800;
    }
    .live-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        display: inline-block;
        animation: pulse 1.5s infinite;
    }
    .live-dot.on { background: #4caf50; }
    .live-dot.off { background: #ff9800; animation: none; }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.4; }
    }

    /* Score card */
    .score-card {
        background: #1a1d2e;
        border: 1px solid #2a2d3e;
        border-radius: 16px;
        padding: 1.5rem 2rem;
        margin: 1rem 0;
    }
    .score-row {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 2rem;
    }
    .team-block {
        text-align: center;
        min-width: 120px;
    }
    .team-logo {
        width: 52px;
        height: 52px;
        border-radius: 50%;
        margin: 0 auto 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 0.8rem;
        font-weight: 700;
        color: #fff;
    }
    .team-name {
        font-weight: 600;
        font-size: 0.95rem;
        color: #e0e0e0;
    }
    .team-score {
        font-size: 2.8rem;
        font-weight: 800;
        color: #fafafa;
        letter-spacing: 1px;
    }
    .score-center {
        text-align: center;
        padding: 0 1rem;
    }
    .score-period {
        font-size: 0.85rem;
        color: #888;
        margin-bottom: 4px;
    }
    .score-separator {
        font-size: 1.2rem;
        color: #555;
    }

    /* Stats row */
    .stats-row {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 0.75rem;
        margin: 1rem 0;
    }
    .stat-box {
        text-align: center;
        padding: 0.75rem;
        background: #1a1d2e;
        border-radius: 12px;
        border: 1px solid #2a2d3e;
    }
    .stat-value {
        font-size: 1.4rem;
        font-weight: 700;
        color: #fafafa;
    }
    .stat-label {
        font-size: 0.72rem;
        color: #888;
        margin-top: 2px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    /* Commentary card */
    .commentary-card {
        background: #1a1d2e;
        border: 1px solid #2a2d3e;
        border-radius: 14px;
        padding: 1rem 1.25rem;
        margin-bottom: 0.75rem;
        transition: border-color 0.2s;
    }
    .commentary-card:hover {
        border-color: #3a3d5e;
    }
    .commentary-card.wicket { border-left: 3px solid #e53935; }
    .commentary-card.boundary { border-left: 3px solid #ff9800; }
    .commentary-card.six { border-left: 3px solid #ffd600; }
    .commentary-card.milestone { border-left: 3px solid #ab47bc; }
    .commentary-card.scoring { border-left: 3px solid #4caf50; }
    .commentary-card.touchdown { border-left: 3px solid #ff6d00; }
    .commentary-card.goal { border-left: 3px solid #2196f3; }

    .commentary-meta {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin-bottom: 6px;
        font-size: 0.78rem;
        color: #888;
    }

    /* Event type badges */
    .event-badge {
        display: inline-block;
        padding: 1px 8px;
        border-radius: 6px;
        font-size: 0.68rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .event-badge.wicket   { background: #3a1515; color: #e53935; }
    .event-badge.boundary { background: #3a2a15; color: #ff9800; }
    .event-badge.six      { background: #3a3515; color: #ffd600; }
    .event-badge.milestone{ background: #2a1535; color: #ab47bc; }
    .event-badge.scoring  { background: #153a15; color: #4caf50; }
    .event-badge.touchdown{ background: #3a2215; color: #ff6d00; }
    .event-badge.goal     { background: #15253a; color: #2196f3; }
    .event-badge.default  { background: #252530; color: #888; }

    .commentary-text {
        font-size: 1rem;
        font-weight: 500;
        line-height: 1.6;
        color: #e0e0e0;
        margin-bottom: 4px;
        max-width: 700px;
    }
    .commentary-play {
        font-size: 0.82rem;
        color: #777;
        font-style: italic;
    }

    /* Section header */
    .section-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin: 1.5rem 0 1rem;
    }
    .section-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #ff8a65;
    }

    /* Empty state */
    .empty-state {
        text-align: center;
        padding: 3rem 1rem;
        color: #666;
    }
    .empty-state .icon { font-size: 2.5rem; margin-bottom: 0.75rem; }
    .empty-state .msg { font-size: 0.95rem; }
    .empty-state .hint { font-size: 0.8rem; color: #555; margin-top: 0.5rem; }

    /* Responsive */
    @media (max-width: 640px) {
        .score-row { gap: 1rem; }
        .team-score { font-size: 2rem; }
        .stats-row { grid-template-columns: repeat(2, 1fr); }
    }

    /* Hide Streamlit defaults we replace */
    header[data-testid="stHeader"] { display: none; }
    .block-container { padding-top: 1rem; max-width: 900px; }
</style>
""", unsafe_allow_html=True)


API_BASE = os.getenv("API_BASE", "http://localhost:8000")
WS_URL = os.getenv("WS_URL", "ws://localhost:8000/ws/commentary")
MAX_ITEMS = 30

SPORT_OPTIONS = {
    "nba":     "🏀 NBA",
    "nfl":     "🏈 NFL",
    "nhl":     "🏒 NHL",
    "soccer":  "⚽ Soccer (EPL)",
    "cricket": "🏏 Cricket",
}

# Team colors for logo circles
TEAM_COLORS = {
    # NBA
    "lakers": "#552583", "celtics": "#007a33", "warriors": "#1d428a",
    "nuggets": "#0e2240", "mavericks": "#00538c", "thunder": "#007ac1",
    "bucks": "#00471b", "suns": "#1d1160", "76ers": "#006bb6",
    "heat": "#98002e", "knicks": "#f58426", "cavaliers": "#860038",
    "pacers": "#002d62", "nets": "#000000", "bulls": "#ce1141",
    "grizzlies": "#5d76a9", "kings": "#5a2d81", "clippers": "#c8102e",
    "rockets": "#ce1141", "spurs": "#c4ced4",
    # NFL
    "chiefs": "#e31837", "eagles": "#004c54", "cowboys": "#003594",
    "bills": "#00338d", "ravens": "#241773", "49ers": "#aa0000",
    "packers": "#203731", "steelers": "#ffb612",
    # Cricket IPL
    "mumbai indians": "#004ba0", "chennai super kings": "#fdb913",
    "royal challengers": "#d4213d", "kolkata knight riders": "#3a225d",
    "rajasthan royals": "#e73895", "delhi capitals": "#004c93",
    "sunrisers hyderabad": "#ff822a", "punjab kings": "#ed1b24",
    "gujarat titans": "#1c1c1c", "lucknow super giants": "#a72056",
    # Cricket international
    "india": "#0066b3", "australia": "#ffcd00", "england": "#1c3a6e",
    "pakistan": "#01411c", "south africa": "#007749", "new zealand": "#000000",
    "west indies": "#7b0041", "sri lanka": "#0c2340", "bangladesh": "#006a4e",
    # Soccer EPL
    "arsenal": "#ef0107", "chelsea": "#034694", "liverpool": "#c8102e",
    "manchester city": "#6cabdd", "manchester united": "#da291c",
    "tottenham": "#132257",
}


def get_team_color(name):
    name_lower = name.lower()
    for key, color in TEAM_COLORS.items():
        if key in name_lower:
            return color
    return "#555"


def get_team_abbr(name):
    words = name.split()
    if len(words) == 1:
        return name[:3].upper()
    return words[-1][:3].upper()


def _esc(text):
    """HTML-escape a string for safe embedding."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ── Session state ────────────────────────────────────────────
if "feed" not in st.session_state:
    st.session_state.feed = deque(maxlen=MAX_ITEMS)
if "connected" not in st.session_state:
    st.session_state.connected = False
if "latest_game" not in st.session_state:
    st.session_state.latest_game = None
if "active_sport" not in st.session_state:
    st.session_state.active_sport = os.getenv("SPORT", "nba")
if "last_update" not in st.session_state:
    st.session_state.last_update = None
if "total_plays" not in st.session_state:
    st.session_state.total_plays = 0
if "scoring_plays" not in st.session_state:
    st.session_state.scoring_plays = 0


# ── WebSocket thread ─────────────────────────────────────────

def ws_thread():
    backoff = 3

    def on_open(ws):
        nonlocal backoff
        st.session_state.connected = True
        backoff = 3  # reset on successful connection

    def on_message(ws, message):
        try:
            data = json.loads(message)
            msg_sport = data.get("sport", st.session_state.active_sport)
            if msg_sport == st.session_state.active_sport or st.session_state.active_sport == "all":
                st.session_state.feed.appendleft(data)
                st.session_state.latest_game = data
                st.session_state.last_update = datetime.now()
                st.session_state.total_plays += 1
                if data.get("score_value"):
                    st.session_state.scoring_plays += 1
            st.session_state.connected = True
        except Exception:
            pass

    def on_error(ws, error):
        st.session_state.connected = False

    def on_close(ws, *args):
        st.session_state.connected = False

    while True:
        try:
            ws_app = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws_app.run_forever()
        except Exception:
            st.session_state.connected = False
        time.sleep(backoff)
        backoff = min(backoff * 1.5, 30)  # exponential backoff, max 30s


if "ws_started" not in st.session_state:
    st.session_state.ws_started = True
    t = threading.Thread(target=ws_thread, daemon=True)
    t.start()


# ── Header ──────────────────────────────────────────────────

st.markdown("""
<div class="topbar">
    <h1>🎙 AI Sports Commentary Engine</h1>
    <span class="subtitle">Real-time · Kafka · LLM-powered</span>
</div>
""", unsafe_allow_html=True)

# ── Sport selector ───────────────────────────────────────────
current_sport = st.session_state.active_sport
default_idx = list(SPORT_OPTIONS.keys()).index(current_sport) if current_sport in SPORT_OPTIONS else 0
selected = st.selectbox(
    "Sport",
    options=list(SPORT_OPTIONS.keys()),
    format_func=lambda x: SPORT_OPTIONS[x],
    index=default_idx,
    label_visibility="collapsed",
)
if selected != st.session_state.active_sport:
    st.session_state.active_sport = selected
    st.session_state.feed.clear()
    st.session_state.latest_game = None
    st.session_state.total_plays = 0
    st.session_state.scoring_plays = 0


# ── Helper: sport-aware formatting ───────────────────────────

def is_cricket():
    return st.session_state.active_sport == "cricket"


def format_period(item):
    """Format period/clock display based on sport."""
    sport = item.get("sport", st.session_state.active_sport)
    if sport == "cricket":
        inn = item.get("period", "?")
        clock = item.get("clock", "")
        return f"Inn {inn} · {clock}" if clock else f"Inn {inn}"
    elif sport == "soccer":
        return f"{item.get('clock', '')}'"
    elif sport == "nhl":
        return f"P{item.get('period', '?')} · {item.get('clock', '')}"
    else:
        return f"Q{item.get('period', '?')} · {item.get('clock', '')}"


def format_score_center(game):
    """Format the center column of the score card."""
    sport = game.get("sport", st.session_state.active_sport)
    if sport == "cricket":
        inn = game.get("period", 1)
        overs = game.get("clock", "")
        return f"Innings {inn}" + (f" · {overs}" if overs else "")
    elif sport == "soccer":
        clock = game.get("clock", "")
        return f"{clock}'" if clock else ""
    elif sport == "nhl":
        return f"Period {game.get('period', '1')} · {game.get('clock', '')}"
    else:
        return f"Q{game.get('period', '1')} · {game.get('clock', '')}"


def get_event_class(item):
    """Return CSS class for event type styling."""
    sport = item.get("sport", st.session_state.active_sport)
    event_type = item.get("type", "").lower()
    play_text = item.get("play", "").lower()

    if sport == "cricket":
        if "wicket" in event_type or "wicket" in play_text:
            return "wicket"
        if "six" in event_type or "six!" in play_text:
            return "six"
        if "boundary" in event_type or "four!" in play_text:
            return "boundary"
        if "milestone" in event_type or "fifty" in event_type or "century" in event_type:
            return "milestone"
    elif sport == "soccer":
        if "goal" in event_type or "goal" in play_text:
            return "goal"
    elif sport in ("nba", "nfl", "nhl"):
        if "touchdown" in event_type or "touchdown" in play_text:
            return "touchdown"
        if item.get("score_value"):
            return "scoring"

    if item.get("score_value"):
        return "scoring"
    return "default"


def get_event_label(item):
    """Return a short label for the event badge."""
    sport = item.get("sport", st.session_state.active_sport)
    event_type = item.get("type", "").lower()
    play_text = item.get("play", "").lower()

    if sport == "cricket":
        if "wicket" in event_type or "wicket" in play_text:
            return "WICKET"
        if "six" in event_type or "six!" in play_text:
            return "SIX"
        if "boundary" in event_type or "four!" in play_text:
            return "FOUR"
        if "century" in event_type:
            return "CENTURY"
        if "fifty" in event_type or "milestone" in event_type:
            return "MILESTONE"
    elif sport == "soccer":
        if "goal" in event_type or "goal" in play_text:
            return "GOAL"
    elif sport == "nfl":
        if "touchdown" in event_type or "touchdown" in play_text:
            return "TOUCHDOWN"
    elif sport == "nba":
        if "three point" in event_type or "three" in play_text:
            return "3PT"

    if item.get("score_value"):
        return "SCORE"
    return None


def format_time_ago(dt):
    """Format a datetime as relative time string."""
    if not dt:
        return "—"
    delta = (datetime.now() - dt).total_seconds()
    if delta < 5:
        return "just now"
    if delta < 60:
        return f"{int(delta)}s ago"
    if delta < 3600:
        return f"{int(delta // 60)}m ago"
    return f"{int(delta // 3600)}h ago"


# ── No-live-game fallback: recent results + upcoming schedule ──

def _render_no_live_fallback():
    """When no live commentary exists, fetch and display recent results + upcoming games."""
    sport = st.session_state.active_sport
    sport_name = SPORT_OPTIONS.get(sport, "")

    # Try fetching scoreboard from API
    scoreboard = None
    try:
        resp = requests.get(f"{API_BASE}/games/scoreboard", params={"sport": sport}, timeout=3)
        if resp.status_code == 200:
            scoreboard = resp.json()
    except requests.RequestException:
        pass

    if not scoreboard:
        sport_icon = sport_name.split()[0] if sport_name else "🏟"
        st.markdown(f"""
        <div class="empty-state">
            <div class="icon">{sport_icon}</div>
            <div class="msg">No live {sport_name} games right now</div>
            <div class="hint">Connect the API server to see recent results and upcoming schedule.</div>
        </div>
        """, unsafe_allow_html=True)
        return

    live = scoreboard.get("live", [])
    recent = scoreboard.get("recent", [])
    upcoming = scoreboard.get("upcoming", [])

    # Build all fallback HTML in one string to render in a single call
    all_html = ""

    # If there ARE live games but no commentary yet
    if live:
        games_html = ""
        for g in live:
            away = _esc(g.get("away_team", "Away"))
            home = _esc(g.get("home_team", "Home"))
            a_score = _esc(str(g.get("away_score", "0")))
            h_score = _esc(str(g.get("home_score", "0")))
            detail = _esc(g.get("status_detail", "In Progress"))
            away_color = get_team_color(g.get("away_team", ""))
            home_color = get_team_color(g.get("home_team", ""))
            away_abbr = get_team_abbr(g.get("away_team", "Away"))
            home_abbr = get_team_abbr(g.get("home_team", "Home"))
            sep = "vs" if is_cricket() else "@"
            games_html += (
                f'<div class="commentary-card scoring" style="border-left:3px solid #4caf50;padding:1rem 1.25rem;">'
                f'<div class="commentary-meta"><span class="event-badge scoring">LIVE</span> {detail}</div>'
                f'<div style="display:flex;align-items:center;justify-content:space-between;margin-top:0.5rem;">'
                f'<div style="display:flex;align-items:center;gap:0.6rem;">'
                f'<div class="team-logo" style="background:{away_color};width:36px;height:36px;font-size:0.65rem;">{away_abbr}</div>'
                f'<div><div style="font-weight:600;font-size:0.9rem;color:#e0e0e0;">{away}</div>'
                f'<div style="font-size:1.5rem;font-weight:700;color:#fafafa;">{a_score}</div></div></div>'
                f'<div style="color:#555;font-size:0.85rem;">{sep}</div>'
                f'<div style="display:flex;align-items:center;gap:0.6rem;flex-direction:row-reverse;">'
                f'<div class="team-logo" style="background:{home_color};width:36px;height:36px;font-size:0.65rem;">{home_abbr}</div>'
                f'<div style="text-align:right;"><div style="font-weight:600;font-size:0.9rem;color:#e0e0e0;">{home}</div>'
                f'<div style="font-size:1.5rem;font-weight:700;color:#fafafa;">{h_score}</div></div></div></div>'
                f'</div>'
            )
        all_html += (
            f'<div style="margin-bottom:1.5rem;">'
            f'<div class="section-title" style="color:#4caf50;margin-bottom:0.75rem;">'
            f'🟢 Live Games — waiting for commentary pipeline...</div>'
            f'{games_html}</div>'
        )

    # Recent completed games
    if recent:
        recent_html = ""
        for g in recent[:6]:
            away = _esc(g.get("away_team", "Away"))
            home = _esc(g.get("home_team", "Home"))
            a_score = _esc(str(g.get("away_score", "0")))
            h_score = _esc(str(g.get("home_score", "0")))
            detail = _esc(g.get("status_detail", "Final"))
            headline = _esc(g.get("headline", ""))
            winner = g.get("winner", "")
            date_disp = _esc(g.get("date_display", ""))
            notes = g.get("notes", [])
            note_text = _esc(notes[0]) if notes else ""
            away_color = get_team_color(g.get("away_team", ""))
            home_color = get_team_color(g.get("home_team", ""))
            away_abbr = get_team_abbr(g.get("away_team", "Away"))
            home_abbr = get_team_abbr(g.get("home_team", "Home"))
            sep = "vs" if is_cricket() else "@"

            # Highlight winner's score
            away_weight = "800" if winner and winner in g.get("away_team", "") else "700"
            home_weight = "800" if winner and winner in g.get("home_team", "") else "700"
            away_color_text = "#4caf50" if winner and winner in g.get("away_team", "") else "#fafafa"
            home_color_text = "#4caf50" if winner and winner in g.get("home_team", "") else "#fafafa"

            # Summary line
            summary_line = ""
            if headline:
                summary_line = f'<div style="text-align:center;color:#b0b0b0;font-size:0.78rem;margin-top:0.4rem;font-style:italic;">{headline}</div>'
            elif note_text:
                summary_line = f'<div style="text-align:center;color:#b0b0b0;font-size:0.78rem;margin-top:0.4rem;">{note_text}</div>'

            recent_html += (
                f'<div class="commentary-card" style="padding:1rem 1.25rem;margin-bottom:0.5rem;">'
                f'<div style="display:flex;align-items:center;justify-content:space-between;">'
                f'<div style="display:flex;align-items:center;gap:0.75rem;">'
                f'<div class="team-logo" style="background:{away_color};width:38px;height:38px;font-size:0.65rem;">{away_abbr}</div>'
                f'<div><div style="font-weight:600;font-size:0.88rem;color:#e0e0e0;">{away}</div>'
                f'<div style="font-size:1.5rem;font-weight:{away_weight};color:{away_color_text};">{a_score}</div></div></div>'
                f'<div style="text-align:center;color:#555;font-size:0.8rem;padding:0 0.5rem;">{sep}</div>'
                f'<div style="display:flex;align-items:center;gap:0.75rem;flex-direction:row-reverse;">'
                f'<div class="team-logo" style="background:{home_color};width:38px;height:38px;font-size:0.65rem;">{home_abbr}</div>'
                f'<div style="text-align:right;"><div style="font-weight:600;font-size:0.88rem;color:#e0e0e0;">{home}</div>'
                f'<div style="font-size:1.5rem;font-weight:{home_weight};color:{home_color_text};">{h_score}</div></div></div></div>'
                f'<div style="text-align:center;color:#888;font-size:0.72rem;margin-top:0.5rem;">{detail}</div>'
                f'{summary_line}'
                f'<div style="text-align:center;color:#555;font-size:0.65rem;margin-top:0.25rem;">{date_disp}</div>'
                f'</div>'
            )

        all_html += (
            f'<div style="margin-bottom:1.5rem;">'
            f'<div class="section-title" style="margin-bottom:0.75rem;">📋 Recent Results</div>'
            f'{recent_html}</div>'
        )
    elif not live:
        sport_icon = sport_name.split()[0] if sport_name else "🏟"
        all_html += (
            f'<div class="empty-state">'
            f'<div class="icon">{sport_icon}</div>'
            f'<div class="msg">No recent {_esc(sport_name)} games found</div>'
            f'</div>'
        )

    # Upcoming games
    if upcoming:
        upcoming_html = ""
        for g in upcoming[:6]:
            away = _esc(g.get("away_team", "Away"))
            home = _esc(g.get("home_team", "Home"))
            date_display = _esc(g.get("date_display", g.get("status_detail", "Scheduled")))
            notes = g.get("notes", [])
            note_text = _esc(notes[0]) if notes else ""
            away_color = get_team_color(g.get("away_team", ""))
            home_color = get_team_color(g.get("home_team", ""))
            away_abbr = get_team_abbr(g.get("away_team", "Away"))
            home_abbr = get_team_abbr(g.get("home_team", "Home"))
            sep = "vs" if is_cricket() else "@"

            note_line = ""
            if note_text:
                note_line = f'<div style="text-align:center;color:#888;font-size:0.68rem;margin-top:0.2rem;">{note_text}</div>'

            upcoming_html += (
                f'<div class="commentary-card" style="padding:0.85rem 1.25rem;margin-bottom:0.5rem;">'
                f'<div style="display:flex;align-items:center;justify-content:space-between;">'
                f'<div style="display:flex;align-items:center;gap:0.6rem;">'
                f'<div class="team-logo" style="background:{away_color};width:34px;height:34px;font-size:0.6rem;">{away_abbr}</div>'
                f'<span style="font-weight:600;font-size:0.9rem;color:#e0e0e0;">{away}</span></div>'
                f'<span style="color:#555;font-size:0.85rem;">{sep}</span>'
                f'<div style="display:flex;align-items:center;gap:0.6rem;flex-direction:row-reverse;">'
                f'<div class="team-logo" style="background:{home_color};width:34px;height:34px;font-size:0.6rem;">{home_abbr}</div>'
                f'<span style="font-weight:600;font-size:0.9rem;color:#e0e0e0;">{home}</span></div></div>'
                f'<div style="text-align:center;color:#ff8a65;font-size:0.78rem;margin-top:0.5rem;">📅 {date_display}</div>'
                f'{note_line}'
                f'</div>'
            )

        all_html += (
            f'<div style="margin-bottom:1.5rem;">'
            f'<div class="section-title" style="margin-bottom:0.75rem;">📅 Upcoming Matches</div>'
            f'{upcoming_html}</div>'
        )

    # Single render call for all fallback content
    if all_html:
        st.markdown(all_html, unsafe_allow_html=True)


# ── Live feed fragment (only THIS block rerenders every 3s) ──

@st.fragment(run_every=3)
def live_feed():
    """Scoped rerun: refreshes score card, connection status, and commentary feed."""

    # ── REST fallback: poll /commentary if WebSocket hasn't delivered data ──
    if not st.session_state.connected or not st.session_state.feed:
        try:
            resp = requests.get(f"{API_BASE}/commentary", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                st.session_state.connected = True
                for item in reversed(data.get("items", [])):
                    if item not in list(st.session_state.feed)[:5]:
                        st.session_state.feed.appendleft(item)
                        st.session_state.latest_game = item
                        st.session_state.last_update = datetime.now()
                        st.session_state.total_plays += 1
                        if item.get("score_value"):
                            st.session_state.scoring_plays += 1
        except requests.RequestException:
            pass

    # Wrap all visual output in st.empty() so each rerun replaces previous content
    wrapper = st.empty()
    with wrapper.container():

        # ── Connection status ──
        if st.session_state.connected:
            badge_class = "on"
            badge_text = "LIVE"
        else:
            badge_class = "off"
            badge_text = "CONNECTING"

        last_update_text = format_time_ago(st.session_state.last_update)

        st.markdown(f"""
        <div style="display:flex; align-items:center; gap:1rem; margin-bottom:0.5rem;">
            <span class="live-badge {badge_class}">
                <span class="live-dot {badge_class}"></span>
                {badge_text}
            </span>
            <span style="color:#666; font-size:0.8rem;">Last update: {last_update_text}</span>
        </div>
        """, unsafe_allow_html=True)

        # ── Score card ──
        game = st.session_state.latest_game
        if game:
            away_name = game.get("away_team", "Away")
            home_name = game.get("home_team", "Home")
            away_score = game.get("away_score", "0")
            home_score = game.get("home_score", "0")
            away_color = get_team_color(away_name)
            home_color = get_team_color(home_name)
            away_abbr = get_team_abbr(away_name)
            home_abbr = get_team_abbr(home_name)
            period_label = format_score_center(game)
            sep = "vs" if is_cricket() else "@"

            win_prob_html = ""
            home_win = game.get("home_win_prob")
            if home_win is not None:
                try:
                    hw = int(home_win)
                    aw = 100 - hw
                    win_prob_html = f'''
                    <div style="margin-top:1.5rem; padding: 0 1rem;">
                        <div style="display:flex; justify-content:space-between; font-size:0.75rem; color:#888; margin-bottom:6px; font-weight:600;">
                            <span>{aw}%</span>
                            <span style="letter-spacing:1px; color:#555; font-size:0.65rem;">WIN PROBABILITY</span>
                            <span>{hw}%</span>
                        </div>
                        <div style="display:flex; height:6px; border-radius:3px; overflow:hidden; background:#2a2d3e;">
                            <div style="width:{aw}%; background:{away_color}; transition: width 0.5s ease;"></div>
                            <div style="width:{hw}%; background:{home_color}; transition: width 0.5s ease;"></div>
                        </div>
                    </div>
                    '''
                except Exception:
                    pass

            st.markdown(f"""
            <div class="score-card">
                <div class="score-row">
                    <div class="team-block">
                        <div class="team-logo" style="background:{away_color};">{away_abbr}</div>
                        <div class="team-name">{away_name}</div>
                        <div class="team-score">{away_score}</div>
                    </div>
                    <div class="score-center">
                        <div class="score-period">{period_label}</div>
                        <div class="score-separator">{sep}</div>
                    </div>
                    <div class="team-block">
                        <div class="team-logo" style="background:{home_color};">{home_abbr}</div>
                        <div class="team-name">{home_name}</div>
                        <div class="team-score">{home_score}</div>
                    </div>
                </div>
                {win_prob_html}
            </div>
            """, unsafe_allow_html=True)

        # ── Stats row ──
        total = st.session_state.total_plays
        scoring = st.session_state.scoring_plays
        scoring_pct = f"{round(scoring / total * 100)}%" if total > 0 else "0%"
        feed_count = len(st.session_state.feed)

        st.markdown(f"""
        <div class="stats-row">
            <div class="stat-box">
                <div class="stat-value">{total}</div>
                <div class="stat-label">Total Plays</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{scoring_pct}</div>
                <div class="stat-label">Scoring %</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{game.get('period', '—') if game else '—'}</div>
                <div class="stat-label">{"Innings" if is_cricket() else "Period"}</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{feed_count}</div>
                <div class="stat-label">In Feed</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Commentary feed ──
        st.markdown("""
        <div class="section-header">
            <span class="section-title">🎤 Live Commentary</span>
        </div>
        """, unsafe_allow_html=True)

        feed = list(st.session_state.feed)
        if not feed:
            # ── No live commentary — show recent results + upcoming schedule ──
            _render_no_live_fallback()
        else:
            cards_html = ""
            for item in feed:
                event_class = get_event_class(item)
                event_label = get_event_label(item)
                period_text = format_period(item)
                commentary = item.get("commentary", "").replace("<", "&lt;").replace(">", "&gt;")
                play = item.get("play", "").replace("<", "&lt;").replace(">", "&gt;")

                badge_html = ""
                if event_label:
                    badge_html = f'<span class="event-badge {event_class}">{event_label}</span>'

                hype_score = item.get("hype_score")
                hype_html = ""
                if hype_score is not None:
                    try:
                        h = int(hype_score)
                        if h >= 9:
                            color = "#ff3d00"
                            icon = "🔥🔥🔥"
                        elif h >= 7:
                            color = "#ff9800"
                            icon = "🔥🔥"
                        elif h >= 5:
                            color = "#ffc107"
                            icon = "🔥"
                        else:
                            color = "#9e9e9e"
                            icon = "🧊"
                        hype_html = f'<span style="border:1px solid {color};color:{color};padding:1px 6px;border-radius:4px;font-size:0.65rem;font-weight:700;margin-left:auto;white-space:nowrap;">{icon} HYPE: {h}/10</span>'
                    except Exception:
                        pass

                cards_html += f"""
                <div class="commentary-card {event_class}">
                    <div class="commentary-meta">
                        {period_text}
                        {badge_html}
                        {hype_html}
                    </div>
                    <div class="commentary-text">{commentary}</div>
                    <div class="commentary-play">{play}</div>
                </div>
                """

            st.markdown(cards_html, unsafe_allow_html=True)


live_feed()
