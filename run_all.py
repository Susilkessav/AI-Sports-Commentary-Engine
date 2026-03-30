"""
run_all.py
──────────
Launch all pipeline components locally (without Docker):
  1. ESPN Producer (ingestion)
  2. Stream Enricher (Layer 3)
  3. LLM Commentator
  4. FastAPI Server & Dashboard UI

Requires Kafka to be running (docker-compose up -d kafka zookeeper).
"""

import subprocess
import sys
import time
import signal

python_bin = sys.executable
uvicorn_bin = "uvicorn"
streamlit_bin = "streamlit"

cwd = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()

processes = []

print("Starting AI Sports Commentary Engine pipeline...")
print("=" * 50)

# 1. ESPN Producer
print("[1/5] Starting ESPN Producer...")
p1 = subprocess.Popen([python_bin, "-m", "ingestion.producer"], cwd=cwd, stdin=subprocess.DEVNULL)
processes.append(("Producer", p1))

# 2. Stream Enricher
print("[2/5] Starting Stream Enricher...")
p2 = subprocess.Popen([python_bin, "-m", "streaming.enricher"], cwd=cwd, stdin=subprocess.DEVNULL)
processes.append(("Enricher", p2))

# 3. LLM Commentator
print("[3/5] Starting LLM Commentator...")
p3 = subprocess.Popen([python_bin, "-m", "llm.commentator"], cwd=cwd, stdin=subprocess.DEVNULL)
processes.append(("Commentator", p3))

# 4. FastAPI Server
print("[4/5] Starting FastAPI Server on port 8000...")
p4 = subprocess.Popen(
    [python_bin, "-m", uvicorn_bin, "api.server:app", "--port", "8000"],
    cwd=cwd, stdin=subprocess.DEVNULL,
)
processes.append(("API Server", p4))

print("=" * 50)
print("All components started!")
print("  API Server:  http://localhost:8000")
print("  API Docs:    http://localhost:8000/docs")
print("  Dashboard:   http://localhost:8000/")
print("\nPress Ctrl+C to stop all components.\n")


def shutdown(signum=None, frame=None):
    print("\nShutting down all components...")
    for name, proc in processes:
        try:
            proc.terminate()
            print(f"  Stopped {name} (PID {proc.pid})")
        except Exception:
            pass
    # Wait for graceful shutdown
    for name, proc in processes:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            print(f"  Force killed {name}")
    print("Shutdown complete.")
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

try:
    while True:
        # Check if any process has died
        for name, proc in processes:
            ret = proc.poll()
            if ret is not None:
                print(f"[WARNING] {name} exited with code {ret}")
        time.sleep(2)
except KeyboardInterrupt:
    shutdown()
