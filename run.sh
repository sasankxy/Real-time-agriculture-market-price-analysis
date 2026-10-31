#!/usr/bin/env bash
# Quickstart script for KrishiStream Real-Time Analytics Pipeline

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

echo "=========================================================="
echo "🌱 KrishiStream: Real-Time Agricultural Market Price Analytics"
echo "=========================================================="

# Check virtual environment
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
else
    source .venv/bin/activate
fi

# Preprocessing & Baseline calculation check
if [ ! -f "data/baselines.db" ]; then
    echo "Processing data and computing statistical baselines..."
    python3 src/baselines/preprocessor.py
    python3 src/baselines/baseline_calculator.py
fi

echo "Starting KrishiStream FastAPI + WebSocket Gateway..."
echo "Open your browser at: http://localhost:8000"
echo "Press Ctrl+C to stop."
exec uvicorn src.backend.main:app --host 0.0.0.0 --port 8000 --reload
