import logging
from collections import deque
from typing import Any

from fastapi import FastAPI, HTTPException, Request

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Webhook mock")
received: deque[dict[str, Any]] = deque(maxlen=1000)


@app.post("/webhook")
async def webhook(request: Request):
    payload = await request.json()
    received.append({"webhook_id": request.headers.get("X-Webhook-Id"), **payload})
    logger.info("webhook received: %s", payload)
    return {"status": "ok"}


@app.post("/webhook/fail")
async def webhook_fail(request: Request):
    logger.info("webhook/fail called: %s", await request.json())
    raise HTTPException(status_code=500, detail="Simulated failure")


@app.get("/webhook/received")
async def webhook_received():
    return list(received)
