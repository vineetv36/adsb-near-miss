"""
ADS-B Near-Miss Detection – FastAPI application.

Connects to:
  - PostGIS (asyncpg connection pool) for historical event + hotspot queries
  - Redis (async client)  for live aircraft state and real-time alert pub/sub
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from src.api.routes import hotspots, live_traffic, near_misses, stats, ws_alerts

log = logging.getLogger("adsb.api")

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "frontend")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://adsb:adsb_secret@postgres:5432/adsb",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# psycopg2 needs "postgres://" not "postgresql://"
_PG_DSN = DATABASE_URL.replace("postgresql://", "postgres://", 1)

# How often to re-run clustering (seconds)
_CLUSTER_INTERVAL = 60


def _run_clustering_sync() -> int:
    """
    Synchronous clustering job — runs in a thread-pool executor so it
    doesn't block the async event loop.  Returns the number of clusters written.
    """
    # Import here so startup isn't slowed by optional heavy deps (hdbscan/shapely)
    _src = os.path.join(os.path.dirname(__file__), "..", "..")
    if _src not in sys.path:
        sys.path.insert(0, _src)

    try:
        import psycopg2
        from src.analysis.hotspot_clustering import (
            build_cluster_records, fetch_events, run_hdbscan, write_hotspots,
        )
    except ImportError as exc:
        log.debug("Hotspot clustering deps not available: %s", exc)
        return 0

    try:
        conn = psycopg2.connect(_PG_DSN)
        try:
            events = fetch_events(conn)
            if len(events) < 3:
                return 0
            labels = run_hdbscan(events, min_cluster_size=3, min_samples=2)
            records = build_cluster_records(events, labels)
            if records:
                write_hotspots(conn, records)
            return len(records)
        finally:
            conn.close()
    except Exception as exc:
        log.warning("Hotspot clustering error: %s", exc)
        return 0


async def _clustering_loop() -> None:
    """Background task: cluster near-miss events into hotspot polygons."""
    await asyncio.sleep(20)   # let the DB settle before first run
    while True:
        try:
            n = await asyncio.get_event_loop().run_in_executor(None, _run_clustering_sync)
            if n:
                log.info("Hotspot clustering: %d clusters written", n)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.warning("Clustering loop error: %s", exc)
        await asyncio.sleep(_CLUSTER_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    task = asyncio.create_task(_clustering_loop())
    yield
    task.cancel()
    await app.state.db.close()
    await app.state.redis.aclose()


app = FastAPI(
    title="ADS-B Near-Miss API",
    version="1.0.0",
    description="Real-time airspace separation events, hotspot analysis, and live traffic.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(live_traffic.router, prefix="/api", tags=["live-traffic"])
app.include_router(near_misses.router, prefix="/api", tags=["near-misses"])
app.include_router(hotspots.router, prefix="/api", tags=["hotspots"])
app.include_router(stats.router, prefix="/api", tags=["stats"])
app.include_router(ws_alerts.router, tags=["websocket"])


@app.get("/health")
async def health():
    return {"status": "ok"}


# Serve the Deck.gl frontend at / — must be mounted AFTER all API routes
# so /api/* requests are handled first.
if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
