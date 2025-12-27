# worker/main.py

from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI

from .client import registration_and_heartbeat_loop
from .config import load_config
from .api import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    This replaces @app.on_event("startup") and @app.on_event("shutdown").
    """
    cfg = load_config()

    # Start background heartbeat thread BEFORE the app starts serving
    thread = threading.Thread(
        target=registration_and_heartbeat_loop,
        args=(cfg,),
        daemon=True,
    )
    thread.start()

    yield


app = FastAPI(title="Worker", lifespan=lifespan)
app.include_router(router)

@app.get("/health")
def health():
    return {"ok": True}
