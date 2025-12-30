import re
from typing import cast

from mini_snowflake.common.manifest import ColType, ColumnInfo
from mini_snowflake.parser.models import (
    AggExpr,
    AggFunc,
    AggFuncStr,
    Cmp,
    CmpStr,
    ColumnRef,
    CreateQuery,
    DropQuery,
    InsertQuery,
    NullCond,
    NullCondStr,
    PredicateTerm,
    SelectQuery,
)


def lower_outside_quotes(s: str) -> str:
    parts = re.split(r"('.*?')", s)
    return "".join(part if part.startswith("'") else part.lower() for part in parts)


def preprocess_query(query: str) -> str:
    query = lower_outside_quotes(query)
    return (
        query.replace("group by", "group_by")
        .replace(",", " , ")
        .replace("(", " ( ")
        .replace(")", " ) ")
        .replace("is null", "is_null")
        .replace("is not null", "is_not_null")
        .replace("if not exists", "if_not_exists")
        .replace("if exists", "if_exists")
        .replace("rows per shard", "rows_per_shard")
    )


def parse(query: str) -> SelectQuery | CreateQuery | InsertQuery | DropQuery:
    query = preprocess_query(query)

    toks = query.split()
    if toks[0] == "select":
        return parse_select(toks[1:])
    elif toks[0] == "create":
        return parse_create(toks[1:])
    elif toks[0] == "insert":
        return parse_insert(toks[1:])
    elif toks[0] == "drop":
        return parse_drop(toks[1:])
    raise ValueError(f"Error at token (0) {toks[0]}")


def parse_drop(toks: list[str]) -> DropQuery:
    assert toks[0] == "table"
    if len(toks) == 2:
        return DropQuery(table=toks[1], if_exists=False)
    elif len(toks) == 3:
        assert toks[2] == "if_exists"
        return DropQuery(table=toks[1], if_exists=True)
    else:
        raise TypeError(f"Error parsing '{' '.join(toks)}': Invalid tokenization")


def parse_insert(toks: list[str]) -> InsertQuery:
    try:
        assert toks[0] == "into"
        table = toks[1]
        assert toks[2] == "from"
        src_path = toks[3]
        if len(toks) == 4:
            return InsertQuery(table=table, src_path=src_path)
        elif len(toks) == 6:
            assert toks[4] == "rows_per_shard"
            return InsertQuery(
                table=table, src_path=src_path, rows_per_shard=int(toks[5])
            )
        raise TypeError("Tokenization Error")
    except Exception as e:
        raise TypeError(f"Error parsing '{' '.join(toks)}': {e}") from e


def parse_create(toks: list[str]) -> CreateQuery:
    assert toks[0] == "table"

    if toks[-1] == "if_not_exists":
        if_not_exists = True
        toks = toks[:-1]
    else:
        if_not_exists = False

    assert toks[2] == "(" and toks[-1] == ")"

    table = toks[1]
    schema = []
    col = []
    selected_toks = toks[3:-1]
    for count, tok in enumerate(selected_toks):
        if count == len(selected_toks) - 1:
            col.append(tok)
        if tok == "," or count == len(selected_toks) - 1:
            schema.append(parse_create_col(col))
            col = []
        else:
            col.append(tok)

    return CreateQuery(
        table=table,
        schema=schema,
        if_not_exists=if_not_exists,
    )


def parse_create_col(toks: list[str]) -> ColumnInfo:
    try:
        if len(toks) == 2:
            return ColumnInfo(
                name=toks[0],
                type=cast(ColType, toks[1]),
            )
        elif len(toks) == 3:
            assert toks[2] == "is_not_null"
            return ColumnInfo(
                name=toks[0],
                type=cast(ColType, toks[1]),
                nullable=False,
            )
        else:
            raise ValueError("Invalid Tokenization")
    except Exception as e:
        raise TypeError(f"Error parsing '{' '.join(toks)}': {e}") from e


def parse_select(toks: list[str]) -> SelectQuery:
    select_rows = parse_select_cols(toks[: toks.index("from")])
    table_name = toks[toks.index("from") + 1]
    if "where" in toks:
        where_rows = parse_where(toks[toks.index("where") + 1 : toks.index("group_by")])
    else:
        where_rows = None
    if "group_by" in toks:
        groupby_tables = parse_groupby(toks[toks.index("group_by") + 1 : -1])
    else:
        groupby_tables = None

    return SelectQuery(
        table=table_name,
        select=select_rows,
        where=where_rows,
        group_by=groupby_tables,
    )


def parse_select_cols(toks: list[str]):
    res = []
    col = []
    for count, tok in enumerate(toks):
        if count == len(toks) - 1:
            col.append(tok)
        if tok == "," or count == len(toks) - 1:
            res.append(parse_select_col(col))
            col = []
        else:
            col.append(tok)
    return res


def parse_select_col(toks: list[str]):
    try:
        if toks[0] in AggFuncStr:
            assert toks[1] == "(" and toks[3] == ")"
            if len(toks) <= 4:
                return AggExpr(
                    func=cast(AggFunc, toks[0]),
                    col=toks[2],
                )
            assert toks[4] == "as"
            return AggExpr(
                func=cast(AggFunc, toks[0]),
                col=toks[2],
                alias=toks[5],
            )
        else:
            if len(toks) == 1:
                return ColumnRef(name=toks[0])
            elif len(toks) == 3:
                assert toks[1] == "as"
                return ColumnRef(name=toks[0], alias=toks[2])
        raise ValueError("Invalid Tokenization")
    except Exception as e:
        print(f"Error parsing '{' '.join(toks)}': {e}")


def parse_where(toks: list[str]):
    res = []
    expr = []
    for count, tok in enumerate(toks):
        if count == len(toks) - 1:
            expr.append(tok)
        if tok == "and" or count == len(toks) - 1:
            res.append(parse_where_expr(expr))
            expr = []
        else:
            expr.append(tok)
    return res


def num_cast(val: str) -> int | float | str:
    if val.isdigit():
        return int(val)
    elif val.count(".") == 1 and val.replace(".", "").isdigit():
        return float(val)
    elif val.startswith("'") and val.endswith("'"):
        return val[1:-1]
    raise ValueError(f"Cannot parse expresion {val}")


def parse_where_expr(toks: list[str]):
    try:
        if len(toks) == 2:
            assert toks[1] in NullCondStr
            return PredicateTerm(
                col=toks[0],
                op=cast(NullCond, toks[1]),
            )
        elif len(toks) == 3:
            assert toks[1] in CmpStr

            value = num_cast(toks[2])

            return PredicateTerm(
                col=toks[0],
                op=cast(Cmp, toks[1]),
                value=value,
            )
        raise ValueError("Invalid Tokenization")
    except Exception as e:
        print(f"Error parsing '{' '.join(toks)}': {e}")


def parse_groupby(toks: list[str]):
    if len(toks) == 1:
        return toks

    try:
        res = []
        for count, tok in enumerate(toks):
            if count % 2 == 0:
                res.append(tok)
            else:
                assert tok == ","
        raise ValueError("Invalid Tokenization")
    except Exception as e:
        print(f"Error parsing '{' '.join(toks)}': {e}")


if __name__ == "__main__":
    select_query_str = """
        SELECT
            event_type,
            COUNT(*),
            COUNT(user_id) as n_user_id_nonnull,
            SUM(value) as total_value,
            AVG(value) as avg_value,
            MIN(event_time) as first_seen,
            MAX(event_time) as last_seen
        from events
        where event_time >= '2025-01-01T00:00:00Z'
            AND event_time <  '2025-02-01T00:00:00Z'
            AND value >= 0
            AND user_id IS NOT NULL
        group by event_type
    """

    select_query_class = SelectQuery(
        table="events",
        select=[
            ColumnRef(name="event_type"),
            AggExpr(func="count", col="*"),
            AggExpr(func="count", col="user_id", alias="n_user_id_nonnull"),
            AggExpr(func="sum", col="value", alias="total_value"),
            AggExpr(func="avg", col="value", alias="avg_value"),
            AggExpr(func="min", col="event_time", alias="first_seen"),
            AggExpr(func="max", col="event_time", alias="last_seen"),
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
        group_by=["event_type"],
    )

    print(parse(select_query_str) == select_query_class)

    create_query_str = """
        CREATE TABLE events(
            event_id    INT,
            user_id     INT,
            event_type  VARCHAR,
            value       DOUBLE IS NOT NULL,
            event_time  TIMESTAMP
        ) IF NOT EXISTS
    """

    create_query_class = CreateQuery(
        table="events",
        schema=[
            ColumnInfo(name="event_id", type="int"),
            ColumnInfo(name="user_id", type="int"),
            ColumnInfo(name="event_type", type="varchar"),
            ColumnInfo(
                name="value",
                type="double",
                nullable=False,
            ),
            ColumnInfo(name="event_time", type="timestamp"),
        ],
        if_not_exists=True,
    )

    print(parse(create_query_str) == create_query_class)

    insert_query_str = "INSERT INTO events FROM data/path ROWS_PER_SHARD 2"
    insert_query_class = InsertQuery(
        table="events", src_path="data/path", rows_per_shard=2
    )

    print(parse(create_query_str) == create_query_class)
