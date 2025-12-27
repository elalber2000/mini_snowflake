from typing import Any
from mini_snowflake.common.db_conn import DBConn
from mini_snowflake.common.utils import setup_logging
from mini_snowflake.worker.models import CreateRequest, DropRequest, InsertRequest, SelectRequest
from .client import send_task
from .worker_registry import registry
from mini_snowflake.parser.models import AggExpr, ColumnRef, CreateQuery, DropQuery, InsertQuery, PredicateTerm, SelectQuery
from mini_snowflake.parser.parser import parse
import logging

setup_logging()
logger = logging.getLogger("")

def orchestrate_create(query: CreateQuery, conn: DBConn) -> dict[str, Any]:
    logger.info("Create request")
    workers = registry.list_active()
    if not workers:
        return {"ok": False, "error": "No active workers"}

    chosen = workers[0]
    task = CreateRequest(
        db_path=str(conn.path),
        table=query.table,
        table_schema=query.schema,
        if_not_exists=query.if_not_exists,
    )

    resp = send_task(chosen.base_url, task, "create")

    return {
        "ok": resp.ok,
        "worker_id": chosen.worker_id,
        "worker_url": chosen.base_url,
        "result": resp.result,
        "error": resp.error,
    }

def orchestrate_drop(query: DropQuery, conn: DBConn) -> dict[str, Any]:
    logger.info("Drop request")
    workers = registry.list_active()
    if not workers:
        return {"ok": False, "error": "No active workers"}

    chosen = workers[0]
    task = DropRequest(
        db_path=str(conn.path),
        table=query.table,
        if_exists=query.if_exists,
    )

    resp = send_task(chosen.base_url, task, "drop")

    return {
        "ok": resp.ok,
        "worker_id": chosen.worker_id,
        "worker_url": chosen.base_url,
        "result": resp.result,
        "error": resp.error,
    }

def orchestrate_insert(query: InsertQuery, conn: DBConn) -> dict[str, Any]:
    logger.info("Insert request")
    workers = registry.list_active()
    if not workers:
        return {"ok": False, "error": "No active workers"}

    chosen = workers[0]
    task = InsertRequest(
        db_path=str(conn.path),
        table=query.table,
        src_path=query.src_path,
        rows_per_shard=query.rows_per_shard,
    )

    resp = send_task(chosen.base_url, task, "create")

    return {
        "ok": resp.ok,
        "worker_id": chosen.worker_id,
        "worker_url": chosen.base_url,
        "result": resp.result,
        "error": resp.error,
    }


def route_external_query(path: str, raw_query: str) -> dict[str, Any]:
    """
    External routing: parse and dispatch.
    For now: only CreateQuery is wired.
    """
    conn = DBConn(path)
    query = parse(raw_query)
    out = None

    if isinstance(query, CreateQuery):
        out = orchestrate_create(query, conn)
        out["kind"] = "create"
    if isinstance(query, DropQuery):
        out = orchestrate_drop(query, conn)
        out["kind"] = "drop"
    if isinstance(query, InsertQuery):
        out = orchestrate_insert(query, conn)
        out["kind"] = "insert"
    
    if out is not None:
        return out

    # Not implemented yet
    return {"ok": False, "kind": "unknown", "error": f"Unsupported query type: {type(query).__name__}"}
