# KrishiStream: Real-Time Agricultural Market Price Analytics & Anomaly Detection

A production-grade, real-time streaming pipeline and analytics platform for AGMARKNET agricultural market data.

Built according to the 4-phase methodology pipeline:
1. **Historical & Baseline Engine (Batch)**: Data normalization, ISO date parsing, season extraction, and 5-tier context baseline computation ($\mu, \sigma, Q_1, Q_3, \text{IQR}$) saved into SQLite, DuckDB, and in-memory cache.
2. **Kafka Streaming Pipeline (Ingestion & Validation)**: Dual-mode broker adapter supporting Apache Kafka / Redpanda via Docker as well as a zero-dependency async event bus.
3. **Real-Time Analytics Core**: Context Z-score & IQR anomaly detection, cross-market arbitrage/spread calculation, EWMA rolling volatility, and concept drift adaptive baselines.
4. **Serving & UI Layer**: FastAPI + WebSockets real-time gateway with an interactive dark-mode dashboard (Leaflet.js geospatial map, Chart.js stream ticker, live anomaly feed, and cross-market spread matrix).

---

## Project Structure

```
SPA lab/
├── data/
│   ├── agmarknet_raw.csv           # Raw government dataset (14,455 records)
│   ├── agmarknet_cleaned.csv       # Normalized schema & geo-enriched
│   ├── baselines.db                # SQLite indexed baselines
│   ├── baselines.duckdb            # DuckDB parquet baselines
│   └── baselines_cache.json        # Sub-millisecond lookup cache (36,301 keys)
├── docker/
│   └── docker-compose.yml          # Redpanda (Kafka-compatible) + Redpanda Console
├── src/
│   ├── baselines/
│   │   ├── preprocessor.py         # Schema cleaning, XML entity removal, geo coordinates
│   │   └── baseline_calculator.py  # 5-tier context baseline stats generator
│   ├── streaming/
│   │   ├── broker_adapter.py       # Dual-mode (Kafka / Redpanda + async bus fallback)
│   │   └── analytics_engine.py     # Z-Score, IQR, Arbitrage, EWMA drift core
│   ├── producer/
│   │   └── stream_producer.py      # Replayer with interactive anomaly injection
│   └── backend/
│       └── main.py                 # FastAPI + WebSocket server
├── dashboard/
│   ├── index.html                  # Responsive dark-theme dashboard
│   ├── css/style.css               # Glassmorphism & micro-animations
│   └── js/app.js                   # WebSocket client, Leaflet map, Chart.js
├── requirements.txt
└── run.sh                          # Quickstart launcher
```

---

## Quickstart

### 1. Launch the Pipeline & Dashboard
```bash
./run.sh
```
Or manually:
```bash
source .venv/bin/activate
uvicorn src.backend.main:app --host 0.0.0.0 --port 8000 --reload
```

### 2. Open the Live Dashboard
Navigate to **`http://localhost:8000`** in any web browser.

---

## Live Demo Triggers
On the top right of the dashboard:
- Click **⚡ Spike Tomato**: Injects a +180% unseasonal price surge at Kolar APMC to immediately trigger a **CRITICAL** anomaly alert.
- Click **📉 Crash Onion**: Injects a 70% market glut crash at Lasalgaon APMC.
- Adjust **Speed**: Replay the stream at 1x, 2x, 5x, or 10x real-time speed.
