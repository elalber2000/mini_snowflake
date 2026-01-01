from typing import Any, Literal

from pydantic import AnyHttpUrl, BaseModel, Field

KindType = Literal["create", "drop", "insert", "select", "unknown"]


class RegisterRequest(BaseModel):
    worker_id: str
    base_url: str | None = None
    load: float = 0.0


class HeartbeatRequest(BaseModel):
    worker_id: str
    base_url: str | None = None
    load: float | None = None


# user -> orchestrator
class ExternalQueryRequest(BaseModel):
    path: str = Field(..., description="DB root path, e.g. db/")
    query: str = Field(..., description="Raw SQL/DDL query string")


class ExternalQueryResponse(BaseModel):
    ok: bool
    kind: KindType
    worker_id: str | None = None
    worker_url: str | None = None
    result: Any = None
    error: str | None = None
