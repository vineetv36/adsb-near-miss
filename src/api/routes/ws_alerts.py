"""WS /ws/alerts — real-time push of LoS / near-miss alerts via Redis pub/sub."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()

_CHANNEL = "adsb:alerts"
_POLL_TIMEOUT = 1.0  # seconds between Redis poll attempts


@router.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    """
    Subscribes to the ``adsb:alerts`` Redis pub/sub channel and forwards every
    message to the connected WebSocket client.  The Spark job publishes a JSON
    blob to this channel whenever it writes a NEAR_MISS or LOSS_OF_SEPARATION
    event.
    """
    await websocket.accept()
    redis = websocket.app.state.redis
    pubsub = redis.pubsub()
    await pubsub.subscribe(_CHANNEL)

    try:
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=_POLL_TIMEOUT
            )
            if message:
                await websocket.send_text(message["data"])
            else:
                # Yield control briefly so other coroutines can run
                await asyncio.sleep(0)
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(_CHANNEL)
        await pubsub.aclose()
