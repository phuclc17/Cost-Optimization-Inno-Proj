import os
import sys
import time
import socket
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import uvicorn

sys.path.insert(0, "/app")
from pipeline.storage.db_client import DatabaseClient
from worker.metrics import (
    events_total,
    processing_latency,
    active_connections,
    worker_info,
)

# ─── CONFIG ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

WORKER_ID = os.getenv(
    "WORKER_ID",
    f"worker-{socket.gethostname()[:8]}"
)

# Xử lý time khác nhau theo channel (ms)
PROCESSING_TIME = {
    "web":         0.005,
    "pos":         0.003,
    "marketplace": 0.008,
}

app = FastAPI(
    title="Retail Event Worker",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

db = DatabaseClient()
worker_info.labels(worker_id=WORKER_ID).set(1)


# ─── MODELS ──────────────────────────────────────────────

class RetailEvent(BaseModel):
    event_id:   str
    channel:    str
    event_type: str
    user_id:    str
    product_id: str
    category:   str
    amount:     float
    region:     str
    device:     str
    created_at: str


class ProcessResult(BaseModel):
    status:     str
    event_id:   str
    worker_id:  str
    latency_ms: float
    timestamp:  str


# ─── ENDPOINTS ───────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check — Docker dùng endpoint này."""
    return {
        "status":       "healthy",
        "worker_id":    WORKER_ID,
        "db_connected": db.health_check()
    }


@app.get("/metrics")
async def metrics():
    """Prometheus scrape endpoint."""
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


@app.post("/process", response_model=ProcessResult)
async def process_event(event: RetailEvent):
    """
    Nhận và xử lý 1 retail event.
    Simulator gọi endpoint này liên tục.
    """
    start = time.perf_counter()
    active_connections.inc()

    try:
        # Validate channel
        if event.channel not in PROCESSING_TIME:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid channel: {event.channel}"
            )

        # Simulate processing time theo channel
        time.sleep(PROCESSING_TIME[event.channel])

        # Tính latency
        latency_ms = (time.perf_counter() - start) * 1000

        # Lưu vào DB
        db.insert_event({
            "event_id":   event.event_id,
            "channel":    event.channel,
            "event_type": event.event_type,
            "user_id":    event.user_id,
            "product_id": event.product_id,
            "category":   event.category,
            "amount":     event.amount,
            "region":     event.region,
            "device":     event.device,
            "created_at": event.created_at,
            "processed_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "worker_id":  WORKER_ID,
            "latency_ms": int(latency_ms)
        })

        # Update Prometheus
        events_total.labels(
            channel=event.channel,
            event_type=event.event_type,
            status="success"
        ).inc()

        processing_latency.labels(
            channel=event.channel
        ).observe(latency_ms)

        return ProcessResult(
            status="processed",
            event_id=event.event_id,
            worker_id=WORKER_ID,
            latency_ms=round(latency_ms, 2),
            timestamp=datetime.now(
                timezone.utc
            ).isoformat()
        )

    except HTTPException:
        events_total.labels(
            channel=event.channel,
            event_type=event.event_type,
            status="error"
        ).inc()
        raise

    except Exception as e:
        logger.error(f"Processing error: {e}")
        events_total.labels(
            channel=getattr(event, "channel", "unknown"),
            event_type="unknown",
            status="error"
        ).inc()
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    finally:
        active_connections.dec()


@app.get("/stats")
async def stats():
    """
    Thống kê nhanh của worker này.
    Dùng để debug và validate.
    """
    try:
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        channel,
                        COUNT(*)         AS total,
                        AVG(latency_ms)  AS avg_lat,
                        MAX(latency_ms)  AS max_lat,
                        PERCENTILE_CONT(0.99)
                            WITHIN GROUP (ORDER BY latency_ms)
                                         AS p99_lat
                    FROM retail_events
                    WHERE worker_id = %s
                    GROUP BY channel
                """, (WORKER_ID,))
                rows = cur.fetchall()

        return {
            "worker_id": WORKER_ID,
            "channels": [
                {
                    "channel":      r[0],
                    "total_events": r[1],
                    "avg_latency":  round(r[2] or 0, 2),
                    "max_latency":  r[3],
                    "p99_latency":  round(r[4] or 0, 2)
                }
                for r in rows
            ]
        }
    except Exception as e:
        return {"worker_id": WORKER_ID, "error": str(e)}


# ─── ENTRY POINT ─────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "worker.main:app",
        host="0.0.0.0",
        port=8000,
        workers=1,
        log_level="info"
    )