from mini_snowflake.common.db_conn import DBConn
from mini_snowflake.parser.models import AggExpr, ColumnRef, CreateQuery, DropQuery, InsertQuery, PredicateTerm, SelectQuery
from mini_snowflake.parser.parser import parse

def orquestrate(raw_query: str, path: str):
    conn = DBConn(path)
    query = parse(raw_query)
    if isinstance(query, CreateQuery):
        return orquestrate_create(query, conn)
    if isinstance(query, DropQuery):
        return orquestrate_drop(query, conn)
    if isinstance(query, SelectQuery):
        return orquestrate_create(query, conn)
    

def orquestrate_create(query: CreateQuery, conn: DBConn):
    pass


def orquestrate_drop(query: DropQuery, conn: DBConn):
    pass


def orquestrate_select(query: SelectQuery, conn: DBConn):

    SelectQuery(
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
    pass