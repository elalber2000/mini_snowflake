import logging
import math
import shutil
from pathlib import Path
from time import monotonic, sleep
from typing import Any

from mini_snowflake.common.db_conn import DBConn
from mini_snowflake.common.manifest import Manifest
from mini_snowflake.common.utils import setup_logging
from mini_snowflake.orchestrator.models import ExternalQueryResponse, KindType
from mini_snowflake.orchestrator.query_maker import (
    create_final_reduce_job,
    create_intermediate_reduce_job,
    create_map_job,
)
from mini_snowflake.parser.models import (
    CreateQuery,
    DropQuery,
    InsertQuery,
    SelectQuery,
)
from mini_snowflake.parser.parser import parse
from mini_snowflake.worker.models import (
    CreateRequest,
    DropRequest,
    InsertRequest,
    SelectRequest,
)

from .client import send_task
from .worker_registry import registry

setup_logging()
logger = logging.getLogger("")


def orchestrate_create(query: CreateQuery, conn: DBConn) -> ExternalQueryResponse:
    logger.info("Create request")
    kind: KindType = "select"
    workers = registry.list_active()
    if not workers:
        return ExternalQueryResponse(
            ok=False,
            kind=kind,
            error="No active workers",
        )

    chosen = workers[0]
    task = CreateRequest(
        db_path=str(conn.path),
        table=query.table,
        table_schema=query.schema,
        if_not_exists=query.if_not_exists,
    )

    resp = send_task(chosen.base_url, task, "create")

    return ExternalQueryResponse(
        ok=resp.ok,
        kind=kind,
        worker_id=chosen.worker_id,
        worker_url=chosen.base_url,
        result=resp.result,
        error=resp.error,
    )


def orchestrate_drop(query: DropQuery, conn: DBConn) -> ExternalQueryResponse:
    logger.info("Drop request")
    kind: KindType = "drop"
    workers = registry.list_active()
    if not workers:
        return ExternalQueryResponse(
            ok=False,
            kind=kind,
            error="No active workers",
        )

    chosen = workers[0]
    task = DropRequest(
        db_path=str(conn.path),
        table=query.table,
        if_exists=query.if_exists,
    )

    resp = send_task(chosen.base_url, task, "drop")

    return ExternalQueryResponse(
        ok=resp.ok,
        kind=kind,
        worker_id=chosen.worker_id,
        worker_url=chosen.base_url,
        result=resp.result,
        error=resp.error,
    )


def orchestrate_insert(query: InsertQuery, conn: DBConn) -> ExternalQueryResponse:
    logger.info("Insert request")
    kind: KindType = "insert"
    workers = registry.list_active()
    if not workers:
        return ExternalQueryResponse(
            ok=False,
            kind=kind,
            error="No active workers",
        )

    chosen = workers[0]
    task = InsertRequest(
        db_path=str(conn.path),
        table=query.table,
        src_path=query.src_path,
        rows_per_shard=query.rows_per_shard,
    )

    resp = send_task(chosen.base_url, task, "create")

    return ExternalQueryResponse(
        ok=resp.ok,
        kind=kind,
        worker_id=chosen.worker_id,
        worker_url=chosen.base_url,
        result=resp.result,
        error=resp.error,
    )


def _get_fanout(
    num_rows_per_shard: int,
    R_target: int = 16_000_000,
    k_min: int = 2,
    k_max: int = 256,
) -> int:
    ratio = R_target / max(1, num_rows_per_shard)
    k = 1 if ratio <= 1 else 1 << int(round(math.log2(ratio)))
    return max(k_min, min(k, k_max))


def _planner(
    query: SelectQuery,
    conn: "DBConn",
    shards: list[str],
    out_path: str | Path,
    tmp_path: str | Path,
) -> list[list[str]]:
    fanout = _get_fanout(len(shards))
    logger.info(f"Fanout = {fanout}")
    data_path = Path(conn.path)

    plan_levels: list[list[str]] = []

    map_sqls: list[str] = []
    current_paths: list[Path] = []

    for s in shards:
        sql, tmp_out_path = create_map_job(
            q=query,
            shard_name=s,
            data_path=data_path,
            tmp_dir=tmp_path,
            fmt="parquet",
        )
        map_sqls.append(sql)
        current_paths.append(tmp_out_path)

    plan_levels.append(map_sqls)

    level = 0
    while len(current_paths) > fanout:
        next_sqls: list[str] = []
        next_paths: list[Path] = []

        for i in range(0, len(current_paths), fanout):
            chunk = current_paths[i : i + fanout]
            sql, tmp_out_path = create_intermediate_reduce_job(
                q=query,
                map_outputs=chunk,
                tmp_dir=tmp_path,
                tag=f"r{level}_{i//fanout}",
                fmt="parquet",
            )
            next_sqls.append(sql)
            next_paths.append(tmp_out_path)

        plan_levels.append(next_sqls)

        current_paths = next_paths
        level += 1

    final_sql, _ = create_final_reduce_job(
        q=query,
        inputs=current_paths,
        out_path=Path(out_path),
        fmt="parquet",
        inputs_level="interm" if len(current_paths) > fanout else "map",
    )
    plan_levels.append([final_sql])
    logger.info(f"plan_levels: {plan_levels}")

    return plan_levels


def _get_first_worker_blocking(
    wait_start: float,
    wait_timeout_s: float | None,
) -> Any:
    while True:
        workers = registry.list_active()
        if workers:
            return registry.choose_worker()

        logger.error("No active workers; waiting...")
        sleep(0.5)

        if wait_timeout_s is not None and (monotonic() - wait_start) > wait_timeout_s:
            raise TimeoutError("No active workers became available before timeout")


def orchestrate_select(
    query: SelectQuery,
    conn: DBConn,
    wait_timeout_s: float | None = 60.0,
) -> ExternalQueryResponse:
    logger.info("Select request")
    kind: KindType = "select"

    tmp_path = conn.path / "tmp"
    tmp_path.mkdir(parents=True, exist_ok=True)

    out_path = conn.path / "out.parquet"

    table_manifest = Manifest.load(conn.path / query.table / "manifest.json")
    shards = list(table_manifest.shards)
    if not shards:
        return ExternalQueryResponse(
            ok=False,
            kind=kind,
            error=f"No shards found for table {query.table}",
        )

    plan_levels = _planner(
        query=query,
        conn=conn,
        shards=shards,
        out_path=out_path,
        tmp_path=tmp_path,
    )

    logger.info(f"Plan: {plan_levels}")

    executions: list[dict[str, Any]] = []
    job_i = 0

    wait_start = monotonic()

    try:
        for level_i, level_sqls in enumerate(plan_levels):
            for sql in level_sqls:
                worker = _get_first_worker_blocking(
                    wait_start=wait_start,
                    wait_timeout_s=wait_timeout_s,
                )

                task = SelectRequest(
                    db_path=str(conn.path),
                    raw_query=sql,
                )

                resp = send_task(worker.base_url, task, "select")
                rec = {
                    "job": job_i,
                    "level": level_i,
                    "worker_id": getattr(worker, "worker_id", None),
                    "worker_url": getattr(worker, "base_url", None),
                    "ok": resp.ok,
                    "error": resp.error,
                    "result": resp.result,
                }
                executions.append(rec)
                job_i += 1

                if not resp.ok:
                    # On failure, retry after refreshing workers.
                    return ExternalQueryResponse(
                        ok=False,
                        kind=kind,
                        error=f"""
                            Execution failed at level {level_i}
                            Failed_step: {rec}
                            Executions: {executions}""",
                    )

            logger.info("Completed level %s (%s statements)", level_i, len(level_sqls))

    except TimeoutError as e:
        return ExternalQueryResponse(
            ok=False,
            kind=kind,
            error=f"{e!s}\nExecutions: {executions}",
        )

    shutil.rmtree(tmp_path)

    return ExternalQueryResponse(
        ok=resp.ok,
        kind=kind,
        result=f"Successfully executed select, result in {out_path}",
    )


def route_external_query(path: str, raw_query: str) -> ExternalQueryResponse:
    """
    External routing: parse and dispatch.
    For now: only CreateQuery is wired.
    """
    conn = DBConn(path)
    query = parse(raw_query)
    out = None
    kind: KindType = "unknown"

    if isinstance(query, CreateQuery):
        out = orchestrate_create(query, conn)
    if isinstance(query, DropQuery):
        out = orchestrate_drop(query, conn)
    if isinstance(query, InsertQuery):
        out = orchestrate_insert(query, conn)
    if isinstance(query, SelectQuery):
        out = orchestrate_select(query, conn)

    if out is not None:
        return out

    # Not implemented yet
    return ExternalQueryResponse(
        ok=False,
        kind=kind,
        error=f"Unsupported query type: {type(query).__name__}",
    )
