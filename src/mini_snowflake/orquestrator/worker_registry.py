from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass
class WorkerInfo:
    worker_id: str
    base_url: str
    last_seen: datetime
    load: float = 0.0


class WorkerRegistry:
    def __init__(self, ttl_seconds: int = 45):
        self.ttl = timedelta(seconds=ttl_seconds)
        self.workers: dict[str, WorkerInfo] = {}

    def upsert(self, worker_id: str, base_url: str, load: float) -> None:
        self.workers[worker_id] = WorkerInfo(
            worker_id=worker_id,
            base_url=base_url.rstrip("/"),
            last_seen=datetime.now(UTC),
            load=float(load),
        )

    def heartbeat(
        self, worker_id: str, base_url: str | None, load: float | None
    ) -> None:
        if worker_id not in self.workers:
            raise KeyError(worker_id)
        w = self.workers[worker_id]
        w.last_seen = datetime.now(UTC)
        if base_url:
            w.base_url = base_url.rstrip("/")
        if load is not None:
            w.load = float(load)

    def list_active(self) -> list[WorkerInfo]:
        now = datetime.now(UTC)
        return [w for w in self.workers.values() if (now - w.last_seen) <= self.ttl]


registry = WorkerRegistry(ttl_seconds=45)
