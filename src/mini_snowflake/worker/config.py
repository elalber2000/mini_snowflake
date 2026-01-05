import os
from dataclasses import dataclass
import socket


@dataclass(frozen=True)
class WorkerConfig:
    worker_id: str
    orchestrator_url: str
    heartbeat_seconds: float


def load_config() -> WorkerConfig:
    worker_id = os.getenv("WORKER_ID") or socket.gethostname()
    return WorkerConfig(
        worker_id=worker_id,
        orchestrator_url=os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000").rstrip("/"),
        heartbeat_seconds=float(os.getenv("HEARTBEAT_SECONDS", "5")),
    )
