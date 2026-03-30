# 🎙 Real-Time AI Sports Commentary Engine

> Stream live sports data → Apache Kafka → LLM → Live Dashboard

---

## Project Structure

```
sports-commentary-engine/
├── config/
│   └── settings.py          ← 🔧 change SPORT and LLM_PROVIDER here
├── ingestion/
│   ├── espn_client.py        ← ESPN API client (free, no key)
│   └── producer.py           ← Kafka producer (polls ESPN)
├── llm/
│   └── commentator.py        ← LLM consumer (generates commentary)
├── api/
│   └── server.py             ← FastAPI + WebSocket server
├── dashboard/
│   └── app.py                ← Streamlit live dashboard
├── scripts/
│   └── test_api.py           ← Run this first to verify ESPN API
├── docker-compose.yml        ← Kafka + Kafka UI
└── requirements.txt
```

---

## Setup (Day 1 — 30 minutes)

### 1. Create a virtual environment
```bash
cd sports-commentary-engine
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Test ESPN API (no Kafka needed yet)
```bash
python scripts/test_api.py
```
If you see live game data, you're good. If no live games, try during game time.

### 3. Start Kafka
```bash
docker-compose up -d
```
- Kafka runs on `localhost:9092`
- Kafka UI at `http://localhost:8080` (visual topic browser)

### 4. Start the producer (publishes live events to Kafka)
```bash
python -m ingestion.producer
```
You should see play-by-play events printed every 10 seconds.

### 5. Configure your LLM

Create a `.env` file in the project root (it is gitignored):

**Option A — Ollama (free, local, recommended to start)**
```bash
# Install Ollama: https://ollama.com
ollama pull llama3
echo 'LLM_PROVIDER=ollama' > .env
```

**Option B — OpenAI**
```bash
cat > .env <<EOF
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...
EOF
```

**Option C — Claude (Anthropic)**
```bash
cat > .env <<EOF
LLM_PROVIDER=claude
ANTHROPIC_API_KEY=sk-ant-...
EOF
```

### 6. Start the LLM commentator
```bash
python -m llm.commentator
```
Watch commentary lines appear as plays stream in!

### 7. Start the API server
```bash
uvicorn api.server:app --reload --port 8000
```

### 8. Start the dashboard
```bash
streamlit run dashboard/app.py
```
Open `http://localhost:8501` — live commentary ticker! 🎉

---

## Changing Sport

Open `config/settings.py` and change:
```python
SPORT = "nba"    # options: "nba" | "nfl" | "nhl" | "soccer"
```

## Changing LLM

Open `config/settings.py` and change:
```python
LLM_PROVIDER = "ollama"    # options: "ollama" | "openai" | "claude"
```

---

## Architecture

```
ESPN API
   │  (poll every 10s)
   ▼
Kafka: game-events topic
   │
   ▼
LLM Commentator (reads from Kafka, calls LLM)
   │
   ▼
Kafka: commentary-output topic
   │
   ▼
FastAPI WebSocket Server
   │
   ▼
Streamlit Dashboard  ←  Browser
```

---

## Next Steps (Week 2)

- [ ] Add TTS voice output (ElevenLabs or Coqui)
- [ ] Tune prompts for different commentary styles (hype / analyst)
- [ ] Add multi-language support
- [ ] Deploy to Fly.io or Railway (free tier)
- [ ] Add Grafana dashboard for pipeline monitoring
