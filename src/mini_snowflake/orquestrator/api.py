from fastapi import FastAPI, HTTPException
from mini_snowflake.orquestrator.orquestrator import route_external_query
from .models import ExternalQueryRequest, ExternalQueryResponse, HeartbeatRequest, RegisterRequest
from dataclasses import asdict
from .worker_registry import registry


app = FastAPI(title="orchestrator")

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


@app.post("/query", response_model=ExternalQueryResponse)
def query(req: ExternalQueryRequest) -> ExternalQueryResponse:
    routed = route_external_query(req.path, req.query)
    return ExternalQueryResponse(**routed)