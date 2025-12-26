from pydantic import AnyHttpUrl, BaseModel

class RegisterRequest(BaseModel):
    worker_id: str
    base_url: AnyHttpUrl
    load: float = 0.0


class HeartbeatRequest(BaseModel):
    worker_id: str
    base_url: AnyHttpUrl | None = None
    load: float | None = None