from fastapi import FastAPI, HTTPException
from .models import HeartbeatRequest, RegisterRequest
from .worker_registry import WorkerRegistry
from dataclasses import asdict

app = FastAPI(title="orchestrator")
registry = WorkerRegistry(ttl_seconds=45)

@app.post("/workers/register")
def register(req: RegisterRequest):
    registry.upsert(req.worker_id, str(req.base_url), req.load)
    return {"ok": True}


@app.post("/workers/heartbeat")
def heartbeat(req: HeartbeatRequest):
    try:
        registry.heartbeat(req.worker_id, str(req.base_url) if req.base_url else None, req.load)
    except KeyError:
        raise HTTPException(status_code=404, detail="worker not registered")
    return {"ok": True}


@app.get("/workers")
def list_workers():
    active = registry.list_active()
    return {"active": [asdict(w) for w in active]}