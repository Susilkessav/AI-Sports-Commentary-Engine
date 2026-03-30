import asyncio
import json
from datetime import datetime
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
import random

app = FastAPI()

# Fake Game Data
fake_game = {
    "game_id": "test_game_1",
    "sport": "nba",
    "home_team": "Lakers",
    "away_team": "Celtics",
    "home_score": "102",
    "away_score": "98",
    "period": 4,
    "clock": "05:12",
}

fake_commentary = []

def generate_fake_play():
    score_change = random.choice([True, False, False])
    home_win = random.randint(45, 95)
    hype = random.randint(2, 10)
    
    if score_change:
        fake_game["home_score"] = str(int(fake_game["home_score"]) + 2)
        desc = "LeBron James drives to the basket for a massive DUNK!"
        comm = "Absolutely explosive! He takes flight and shatters the defense!! UNREAL ATHLETICISM!" if hype > 8 else "A solid drive and finish at the rim."
        play_type = "scoring"
    else:
        desc = "Smart misses the 3-pointer. Rebound by Davis."
        comm = "Good defensive hustle right there. They needed that stop."
        play_type = "miss"

    return {
        **fake_game,
        "play": desc,
        "commentary": comm,
        "hype_score": hype,
        "home_win_prob": home_win,
        "timestamp": datetime.now().strftime("%H:%M:%S UTC"),
        "type": play_type,
        "score_value": score_change
    }

for _ in range(5):
    fake_commentary.append(generate_fake_play())

@app.websocket("/ws/commentary")
async def commentary_ws(websocket: WebSocket):
    await websocket.accept()
    # Send historical
    for msg in fake_commentary:
        await websocket.send_json(msg)
    
    try:
        while True:
            await asyncio.sleep(4)
            new_play = generate_fake_play()
            fake_commentary.append(new_play)
            if len(fake_commentary) > 50:
                fake_commentary.pop(0)
            await websocket.send_json(new_play)
    except Exception:
        pass

@app.get("/commentary")
def get_recent():
    return {"items": fake_commentary, "count": len(fake_commentary)}

@app.get("/games/scoreboard")
def games_scoreboard(sport: str = None):
    return {
        "live": [fake_game],
        "recent": [],
        "upcoming": []
    }

@app.get("/games/live")
def games_live(sport: str = None):
    return {"games": [fake_game]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9091)
