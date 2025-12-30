from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Literal

from mini_snowflake.common.utils import setup_logging
from mini_snowflake.parser.models import AggExpr, ColumnRef, PredicateTerm, SelectQuery

setup_logging()
logger = logging.getLogger("")

InputsLevelLiteral = Literal["interm", "map"]


def _safe_ident(x: str | None) -> str:
    if x is None:
        return ""
    else:
        return x.replace("*", "star").replace(".", "_").replace("-", "_")


def _unparse_where_clause(clause: PredicateTerm) -> str:
    op = clause.op.replace("_", " ").lower()

    if op in {"is null", "is not null"}:
        return f"{clause.col} {op}"

    if clause.value is None:
        raise ValueError(f"Predicate {clause.col} {op} requires a value")

    val = clause.value
    if isinstance(val, str) and not (val.startswith("'") and val.endswith("'")):
        val = f"'{val}'"

    return f"{clause.col} {op} {val}"


def _group_cols(q: SelectQuery) -> list[str]:
    if q.group_by is not None:
        return list(q.group_by)

    if any(isinstance(s, AggExpr) for s in q.select):
        return [s.name for s in q.select if isinstance(s, ColumnRef)]

    return []


def _iter_aggs(q: SelectQuery) -> Iterable[AggExpr]:
    for s in q.select:
        if isinstance(s, AggExpr):
            yield s


def _has_sum_for_col(q: SelectQuery, col: str | None) -> AggExpr | None:
    if col is None:
        col = ""
    for a in _iter_aggs(q):
        if a.func.lower() == "sum" and a.col == col:
            return a
    return None


def _sql_union_all_select_star(sources: Sequence[str | Path]) -> str:
    return " UNION ALL ".join(f"SELECT * FROM { _sql_source(src) }" for src in sources)


def _sql_source(src: str | Path) -> str:
    if isinstance(src, Path) or (
        isinstance(src, str) and ("/" in src or src.endswith((".parquet", ".csv")))
    ):
        return f"'{src}'"
    return str(src)


def _materialize(select_sql: str, out_path: str | Path, fmt: str = "parquet") -> str:
    """
    Materialize a SELECT into a file.
    """
    query = f"COPY ({select_sql}) TO '{out_path}' (FORMAT {fmt.upper()});"
    return " ".join(query.split())


def _map_alias(func: str, col: str | None) -> str:
    if col is None:
        col = ""
    func = func.lower()
    col_id = _safe_ident(col)
    if func == "count":
        return f"c_{col_id}"
    if func == "sum":
        return f"s_{col_id}"
    if func == "min":
        return f"min_{col_id}"
    if func == "max":
        return f"max_{col_id}"
    raise ValueError(f"Unsupported aggregate func: {func!r}")


def _merge_func(func: str) -> str:
    func = func.lower()
    if func in {"count", "sum"}:
        return "sum"
    if func in {"min", "max"}:
        return func
    raise ValueError(f"Unsupported aggregate func: {func!r}")


def _required_map_measures(q: SelectQuery) -> list[tuple[str, str]]:
    """
    Deduped list of (func, col) needed in MAP.
    avg(x) expands into sum(x) + count(x).
    """
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []

    for agg in _iter_aggs(q):
        f = agg.func.lower()
        if f == "avg":
            for mf in ("sum", "count"):
                key = (mf, agg.col if agg.col is not None else "")
                if key not in seen:
                    seen.add(key)
                    out.append(key)
        else:
            key = (f, agg.col if agg.col is not None else "")
            if key not in seen:
                seen.add(key)
                out.append(key)

    return out


def _has_avg_for_col(q: SelectQuery, col: str) -> AggExpr | None:
    for a in _iter_aggs(q):
        if a.func.lower() == "avg" and a.col == col:
            return a
    return None


def create_map_select(q: SelectQuery, shard_name: str, path: str | Path) -> str:
    group = _group_cols(q)
    select_parts: list[str] = []

    for s in q.select:
        if isinstance(s, ColumnRef):
            if s.name not in select_parts:
                select_parts.append(s.name)

    for c in group:
        if c not in select_parts:
            select_parts.append(c)

    for func, col in _required_map_measures(q):
        select_parts.append(f"{func}({col}) AS {_map_alias(func, col)}")

    if not select_parts:
        select_parts.append("*")

    sql = f"""
        SELECT
            {", ".join(select_parts)}
        FROM
            '{Path(path) / q.table / shard_name}'
    """.strip()

    if q.where:
        sql += "\nWHERE " + " AND ".join(_unparse_where_clause(w) for w in q.where)

    if group:
        sql += "\nGROUP BY " + ", ".join(group)

    return sql


def create_map_job(
    q: SelectQuery,
    shard_name: str,
    data_path: str | Path,
    tmp_dir: str | Path,
    fmt: str = "parquet",
) -> tuple[str, Path]:
    """
    Returns (sql_to_execute, output_path).
    """

    out_path = Path(tmp_dir) / f"map__{q.table}__{_safe_ident(shard_name)}.{fmt}"
    select_sql = create_map_select(q, shard_name, data_path)
    return _materialize(select_sql, out_path, fmt=fmt), out_path


def create_intermediate_reduce_select(
    q: SelectQuery, map_outputs: Sequence[str | Path]
) -> str:
    group = _group_cols(q)

    union_sql = _sql_union_all_select_star(map_outputs)

    if not any(isinstance(s, AggExpr) for s in q.select):
        if group:
            return f"""
                WITH partial AS (
                    {union_sql}
                )
                SELECT
                    {", ".join(group)}
                FROM partial
                GROUP BY {", ".join(group)}
            """.strip()
        return f"""
            WITH partial AS (
                {union_sql}
            )
            SELECT * FROM partial
        """.strip()

    reduce_select: list[str] = []
    for c in group:
        reduce_select.append(c)

    for item in q.select:
        if isinstance(item, ColumnRef):
            continue
        if not isinstance(item, AggExpr):
            raise TypeError(f"Unsupported select item: {item!r}")

        func = item.func.lower()

        if func == "avg":
            avg_alias = item.alias or f"avg_{_safe_ident(item.col)}"
            cnt_map = _map_alias("count", item.col)
            reduce_select.append(f"sum({cnt_map}) AS {avg_alias}_count_partial")

            if _has_sum_for_col(q, item.col) is None:
                sum_map = _map_alias("sum", item.col)
                reduce_select.append(f"sum({sum_map}) AS {avg_alias}_sum_partial")
            continue

        map_col = _map_alias(func, item.col)
        merge = _merge_func(func)

        if func == "count" and item.col == "*":
            out_alias = "count_star_partial"
        elif item.alias:
            out_alias = f"{item.alias}_partial"
        else:
            out_alias = f"{func}_{_safe_ident(item.col)}_partial"

        reduce_select.append(f"{merge}({map_col}) AS {out_alias}")

    sql = f"""
        WITH partial AS (
            {union_sql}
        )
        SELECT
            {", ".join(reduce_select)}
        FROM partial
    """.strip()

    if group:
        sql += "\nGROUP BY " + ", ".join(group)

    return sql


def create_intermediate_reduce_job(
    q: SelectQuery,
    map_outputs: Sequence[str | Path],
    tmp_dir: str | Path,
    tag: str,
    fmt: str = "parquet",
) -> tuple[str, Path]:
    """
    Returns (sql_to_execute, output_path).
    """
    tmp_dir = Path(tmp_dir)
    out_path = tmp_dir / f"reduce__{q.table}__{_safe_ident(tag)}.{fmt}"
    select_sql = create_intermediate_reduce_select(q, map_outputs)
    return _materialize(select_sql, out_path, fmt=fmt), out_path


def create_final_reduce_select(
    q: SelectQuery,
    inputs: Sequence[str | Path],
    *,
    inputs_level: InputsLevelLiteral = "interm",
) -> str:
    group = _group_cols(q)
    union_sql = _sql_union_all_select_star(inputs)

    if not any(isinstance(s, AggExpr) for s in q.select):
        if group:
            return f"""
                WITH partial AS (
                    {union_sql}
                )
                SELECT
                    {", ".join(group)}
                FROM partial
                GROUP BY {", ".join(group)}
            """.strip()
        return f"""
            WITH partial AS (
                {union_sql}
            )
            SELECT * FROM partial
        """.strip()

    final_select: list[str] = []
    for c in group:
        final_select.append(c)

    for item in q.select:
        if isinstance(item, ColumnRef):
            continue
        if not isinstance(item, AggExpr):
            raise TypeError(f"Unsupported select item: {item!r}")

        func = item.func.lower()

        if inputs_level == "map":
            if func == "avg":
                avg_alias = item.alias or f"avg_{_safe_ident(item.col)}"
                s_col = _map_alias("sum", item.col)
                c_col = _map_alias("count", item.col)
                final_select.append(
                    f"sum({s_col}) / nullif(sum({c_col}), 0) AS {avg_alias}"
                )
                continue

            if func == "count" and item.col == "*":
                out_col = item.alias or "count_star"
                final_select.append(f"sum(c_star) AS {out_col}")
                continue

            if func == "sum":
                out_col = item.alias or f"sum_{_safe_ident(item.col)}"
                final_select.append(f"sum({_map_alias('sum', item.col)}) AS {out_col}")
                continue

            if func == "count":
                out_col = item.alias or f"count_{_safe_ident(item.col)}"
                final_select.append(
                    f"sum({_map_alias('count', item.col)}) AS {out_col}"
                )
                continue

            if func == "min":
                out_col = item.alias or f"min_{_safe_ident(item.col)}"
                final_select.append(f"min({_map_alias('min', item.col)}) AS {out_col}")
                continue

            if func == "max":
                out_col = item.alias or f"max_{_safe_ident(item.col)}"
                final_select.append(f"max({_map_alias('max', item.col)}) AS {out_col}")
                continue

            raise ValueError(f"Unsupported aggregate func: {func!r}")

        if func == "avg":
            avg_alias = item.alias or f"avg_{_safe_ident(item.col)}"

            sum_agg = _has_sum_for_col(q, item.col)
            if sum_agg is not None:
                sum_partial_col = (
                    f"{(sum_agg.alias or f'sum_{_safe_ident(sum_agg.col)}')}_partial"
                )
            else:
                sum_partial_col = f"{avg_alias}_sum_partial"

            cnt_partial_col = f"{avg_alias}_count_partial"

            final_select.append(
                f"sum({sum_partial_col}) / nullif(sum({cnt_partial_col}), 0) AS {avg_alias}"
            )
            continue

        if func == "count" and item.col == "*":
            out_col = item.alias or "count_star"
            final_select.append(f"sum(count_star_partial) AS {out_col}")
            continue

        in_col = (
            f"{item.alias}_partial"
            if item.alias
            else f"{func}_{_safe_ident(item.col)}_partial"
        )
        out_col = item.alias or f"{func}_{_safe_ident(item.col)}"
        merge = _merge_func(func)
        final_select.append(f"{merge}({in_col}) AS {out_col}")

    sql = f"""
        WITH partial AS (
            {union_sql}
        )
        SELECT
            {", ".join(final_select)}
        FROM partial
    """.strip()

    if group:
        sql += "\nGROUP BY " + ", ".join(group)

    return sql


def create_final_reduce_job(
    q: SelectQuery,
    inputs: Sequence[str | Path],
    out_path: str | Path,
    fmt: str = "parquet",
    *,
    inputs_level: InputsLevelLiteral = "interm",
) -> tuple[str, Path]:
    """
    Returns (sql_to_execute, output_path).
    """
    select_sql = create_final_reduce_select(q, inputs, inputs_level=inputs_level)
    return _materialize(select_sql, out_path, fmt=fmt), Path(out_path)


if __name__ == "__main__":
    query = SelectQuery(
        table="events",
        select=[
            ColumnRef(name="country"),
            # AggExpr(func="count", col="*"),
            # AggExpr(func="count", col="user_id", alias="n_user_id_nonnull"),
            # AggExpr(func="sum", col="value", alias="total_value"),
            # AggExpr(func="avg", col="value", alias="avg_value"),
            # AggExpr(func="min", col="event_time", alias="first_seen"),
            # AggExpr(func="max", col="event_time", alias="last_seen"),
        ],
        where=[
            PredicateTerm(
                col="event_time",
                op=">=",
                value="2025-01-01T00:00:00Z",
            ),
            PredicateTerm(
                col="event_time",
                op="<",
                value="2025-02-01T00:00:00Z",
            ),
            PredicateTerm(
                col="value",
                op=">=",
                value=0,
            ),
            PredicateTerm(
                col="user_id",
                op="is_not_null",
                value=None,
            ),
        ],
        group_by=None,  # ["event_type"],
    )

    shards = ["shard-0", "shard-1", "shard-2"]
    data_path = "src/data"
    tmp_dir = f"{data_path}/tmp"

    map_jobs: list[tuple[str, Path]] = []
    for s in shards:
        sql, out = create_map_job(query, s, data_path, tmp_dir, fmt="parquet")
        map_jobs.append((sql, out))

    print("\n--- MAP ---\n")
    for sql, out in map_jobs:
        print(f"- {out}\n{sql}\n")

    map_outputs = [out for _, out in map_jobs]

    interm_sql, interm_out = create_intermediate_reduce_job(
        query, map_outputs, tmp_dir, tag="r0", fmt="parquet"
    )
    print("\n--- INTERMEDIATE REDUCE ---\n")
    print(f"- {interm_out}\n{interm_sql}\n")
    final_sql, final_out = create_final_reduce_job(
        query, [interm_out], out_path="src/data/out/final_events.parquet", fmt="parquet"
    )
    print("\n--- FINAL REDUCE ---\n")
    print(f"- {final_out}\n{final_sql}\n")
