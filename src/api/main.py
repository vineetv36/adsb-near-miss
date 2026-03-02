"""
ADS-B Near-Miss Detection – FastAPI application.

Connects to:
  - PostGIS (asyncpg connection pool) for historical event + hotspot queries
  - Redis (async client)  for live aircraft state and real-time alert pub/sub
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from src.api.routes import hotspots, live_traffic, near_misses, stats, ws_alerts

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "frontend")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://adsb:adsb_secret@postgres:5432/adsb",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield
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
