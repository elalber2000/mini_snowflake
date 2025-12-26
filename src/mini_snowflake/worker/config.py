import os
from dataclasses import dataclass

@dataclass(frozen=True)
class WorkerConfig:
    worker_id: str
    base_url: str
    orchestrator_url: str
    heartbeat_seconds: float


def load_config() -> WorkerConfig:
    return WorkerConfig(
        worker_id=os.getenv("WORKER_ID", "w1"),
        base_url=os.getenv("BASE_URL", "http://worker1:8000").rstrip("/"),
        orchestrator_url=os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000").rstrip("/"),
        heartbeat_seconds=float(os.getenv("HEARTBEAT_SECONDS", "10")),
    )