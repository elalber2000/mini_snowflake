from __future__ import annotations

from fastapi import APIRouter

from mini_snowflake.common.utils import setup_logging

from .models import DropRequest, InsertRequest, SelectRequest, TaskResponse, CreateRequest
from mini_snowflake.worker.worker import worker_create, worker_drop, worker_insert

from mini_snowflake.common.db_conn import DBConn
import logging

setup_logging()
logger = logging.getLogger("")

router = APIRouter()


@router.post("/tasks/execute", response_model=TaskResponse)
def execute_task(task: CreateRequest | DropRequest | InsertRequest | SelectRequest) -> TaskResponse:
    """
    Single internal endpoint for all tasks.
    For now only CreateQuery is wired.
    """
    logger.info(f"Executing request {task}")
    try:
        conn = DBConn(task.db_path)
        logger.info("Established conn")

        if isinstance(task, CreateRequest):
            result = worker_create(conn, task)
        if isinstance(task, DropRequest):
            result = worker_drop(conn, task)
        if isinstance(task, InsertRequest):
            result = worker_insert(conn, task)

        if result is not None:
            return TaskResponse(ok=True, result=result)

        return TaskResponse(ok=False, error=f"Unsupported task type: {type(task).__name__}")

    except Exception as e:
        logger.exception("Failed task")
        return TaskResponse(ok=False, error=str(e))
