"""
Serving & Gateway Layer (FastAPI + WebSockets).
Matches Phase 4 of the project architecture.
"""

import asyncio
import json
import os
import sqlite3
from typing import List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.producer.stream_producer import producer
from src.producer.gov_api_client import gov_client
from src.streaming.analytics_engine import AnalyticsEngine
from src.streaming.broker_adapter import broker

app = FastAPI(title="Agri Market Price Stream Analytics API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = AnalyticsEngine()
connected_websockets: List[WebSocket] = []
recent_events_cache = []
recent_alerts_cache = []


class ControlRequest(BaseModel):
    action: str  # "start", "stop", "speed", "source"
    speed: Optional[float] = 2.0
    source: Optional[str] = None  # "LOCAL_DATASET" or "LIVE_GOV_API"


class AnomalyRequest(BaseModel):
    type: str = "PRICE_SPIKE"  # "PRICE_SPIKE", "PRICE_CRASH", "FAT_FINGER"
    commodity: str = "Tomato"
    market: str = "Kolar APMC"


@app.on_event("startup")
async def startup_event():
    # 1. Connect message broker (Kafka or internal bus)
    await broker.connect()

    # 2. Start Stream Processing Loop in background
    asyncio.create_task(stream_processing_pipeline())

    # 3. Auto-start Producer with default speed (2 records/sec)
    asyncio.create_task(producer.start())


@app.on_event("shutdown")
async def shutdown_event():
    producer.stop()
    await broker.close()


async def stream_processing_pipeline():
    """
    Subscribes to 'mandi-raw-events', enriches via AnalyticsEngine,
    and broadcasts to WebSocket clients and alert topics.
    """
    print("[Pipeline] Stream processing pipeline listening for events...")
    async for raw_event in broker.subscribe("mandi-raw-events"):
        try:
            enriched, alert = engine.evaluate_event(raw_event)

            # Publish to processed topic
            await broker.send("mandi-processed-events", enriched)

            # If anomaly, publish to alert topic
            if alert:
                await broker.send("mandi-alerts", alert)
                recent_alerts_cache.insert(0, alert)
                if len(recent_alerts_cache) > 50:
                    recent_alerts_cache.pop()

            recent_events_cache.insert(0, enriched)
            if len(recent_events_cache) > 100:
                recent_events_cache.pop()

            # Broadcast over active WebSockets
            payload = json.dumps({
                "type": "STREAM_EVENT",
                "event": enriched,
                "alert": alert,
                "stats": engine.stats
            })

            dead_sockets = []
            for ws in connected_websockets:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead_sockets.append(ws)

            for dead in dead_sockets:
                if dead in connected_websockets:
                    connected_websockets.remove(dead)

        except Exception as e:
            print(f"[Pipeline] Processing error: {e}")


@app.websocket("/ws/live-stream")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_websockets.append(websocket)
    try:
        # Send initial snapshot of recent events, alerts, and counters
        initial_payload = json.dumps({
            "type": "INITIAL_SNAPSHOT",
            "recent_events": recent_events_cache[:20],
            "recent_alerts": recent_alerts_cache[:15],
            "stats": engine.stats
        })
        await websocket.send_text(initial_payload)

        while True:
            # Keep socket alive and receive client control ping/messages
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get("action") == "inject":
                producer.inject_synthetic_anomaly(
                    anomaly_type=msg.get("type", "PRICE_SPIKE"),
                    commodity=msg.get("commodity", "Tomato"),
                    market=msg.get("market", "Kolar APMC")
                )
    except WebSocketDisconnect:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)
    except Exception:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)


@app.get("/api/stats")
async def get_stats():
    return {
        "engine_stats": engine.stats,
        "is_producer_running": producer.is_running,
        "source_mode": producer.source_mode,
        "current_speed_hz": round(1.0 / producer.delay_seconds, 1),
        "connected_clients": len(connected_websockets),
        "is_kafka_connected": broker.is_kafka_connected,
        "total_baselines_loaded": len(engine.baselines_cache),
        "gov_api_configured": bool(gov_client.api_key)
    }


@app.post("/api/producer/control")
async def producer_control(req: ControlRequest):
    if req.action == "start" and not producer.is_running:
        asyncio.create_task(producer.start())
    elif req.action == "stop" and producer.is_running:
        producer.stop()
    elif req.action == "speed" and req.speed:
        producer.set_speed(req.speed)
    elif req.action == "source" and req.source:
        producer.set_source_mode(req.source)

    return {
        "status": "success",
        "is_running": producer.is_running,
        "source_mode": producer.source_mode,
        "speed_hz": round(1.0 / producer.delay_seconds, 1)
    }


@app.get("/api/gov-api/test")
async def test_gov_api(limit: int = 5):
    """Directly queries data.gov.in using the configured API key."""
    return await gov_client.fetch_records(limit=limit)


@app.post("/api/inject-anomaly")
async def inject_anomaly(req: AnomalyRequest):
    producer.inject_synthetic_anomaly(
        anomaly_type=req.type,
        commodity=req.commodity,
        market=req.market
    )
    return {"status": "queued", "anomaly": req.dict()}


@app.get("/api/baselines")
async def query_baselines(commodity: Optional[str] = None, market: Optional[str] = None, limit: int = 50):
    db_path = "data/baselines.db"
    if not os.path.exists(db_path):
        return {"error": "Baselines database not found"}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    query = "SELECT * FROM baselines WHERE 1=1"
    params = []
    if commodity:
        query += " AND commodity LIKE ?"
        params.append(f"%{commodity}%")
    if market:
        query += " AND market LIKE ?"
        params.append(f"%{market}%")
    query += " LIMIT ?"
    params.append(limit)

    cursor = conn.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"count": len(rows), "results": rows}


@app.get("/api/commodities")
async def get_commodities():
    db_path = "data/baselines.db"
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT DISTINCT commodity, COUNT(*) as count FROM baselines GROUP BY commodity ORDER BY count DESC LIMIT 25")
    results = [{"commodity": r[0], "records": r[1]} for r in cursor.fetchall()]
    conn.close()
    return results


# Mount static assets for the Dashboard UI
dashboard_dir = os.path.abspath("dashboard")
if os.path.exists(dashboard_dir):
    app.mount("/static", StaticFiles(directory=dashboard_dir), name="static")

@app.get("/")
async def serve_dashboard():
    index_path = os.path.join(dashboard_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Agri Market Stream Analytics API is running. Dashboard file not found."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.backend.main:app", host="0.0.0.0", port=8000, reload=True)
