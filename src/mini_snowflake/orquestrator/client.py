from pydantic import BaseModel
import requests

from mini_snowflake.common.utils import setup_logging
from .models import ExternalQueryRequest, ExternalQueryResponse, KindType
import logging

setup_logging()
logger = logging.getLogger("")

def send_task(
    worker_base_url: str,
    task: BaseModel,
    kind: KindType,
    timeout_seconds: float = 15.0
) -> ExternalQueryResponse:
    """
    Orchestrator -> Worker client call.
    """
    url = worker_base_url.rstrip("/") + "/tasks/execute"

    logger.info(f"Sending {kind} request: {task.model_dump()}")
    r = requests.post(url, json=task.model_dump(), timeout=timeout_seconds)
    logger.info(f"Recieved {str(r)}")

    # Normalize errors
    if r.status_code >= 400:
        return ExternalQueryResponse(ok=False, error=r.text, kind=kind)

    try:
        return ExternalQueryResponse(kind=kind, **r.json())
    except Exception:
        return ExternalQueryResponse(ok=False, error=f"Invalid worker response: {r.text}", kind=kind)