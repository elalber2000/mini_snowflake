from mini_snowflake.parser.models import AggExpr, AggFunc, AggFuncStr, CmpStr, ColumnRef, NullCondStr, PredicateTerm, Query
import re

def lower_outside_quotes(s: str) -> str:
    parts = re.split(r"('.*?')", s)
    return "".join(
        part if part.startswith("'") else part.lower()
        for part in parts
    )

def preprocess_query(query: str) -> str:
    query = lower_outside_quotes(query)
    return (
        query
        .replace("group by", "group_by")
        .replace(",", " , ")
        .replace("(", " ( ")
        .replace(")", " ) ")
        .replace(";", " ; ")
        .replace("is null", "is_null")
        .replace("is not null", "is_not_null")
    )

def parse(query: str) -> Query:
    query = preprocess_query(query)

    toks = query.split()
    if toks[0]=="select":
        return parse_select(toks[1:])
    raise ValueError(f"Error at token (0) {toks[0]}")


def parse_select(toks: list[str]) -> Query:
    select_rows = parse_select_cols(toks[:toks.index("from")])
    table_name = toks[toks.index("from")+1]
    where_rows = parse_where(toks[toks.index("where")+1:toks.index("group_by")])
    groupby_tables = parse_groupby(toks[toks.index("group_by")+1:toks.index(";")])

    return Query(
        table=table_name,
        select=select_rows,
        where=where_rows,
        group_by=groupby_tables,
    )

def parse_select_cols(toks: list[str]):
    res = []
    col = []
    for count, tok in enumerate(toks):
        if count==len(toks)-1:
            col.append(tok)
        if tok == "," or count==len(toks)-1:
            res.append(parse_select_col(col))
            col = []
        else:
            col.append(tok)
    return res

def parse_select_col(toks: list[str]):
    try:
        if toks[0] in AggFuncStr:
            assert toks[1] == "(" and toks[3] == ")"
            if len(toks)<=4:
                return AggExpr(
                    func=toks[0],
                    col=toks[2],
                )
            assert toks[4] == "as"
            return AggExpr(
                    func=toks[0],
                    col=toks[2],
                    alias=toks[5],
                )
        else:
            if len(toks)==1:
                return ColumnRef(name=toks[0])
            elif len(toks)==3:
                assert toks[1]=="as"
                return ColumnRef(name=toks[0], alias=toks[2])
        raise ValueError("Invalid Tokenization")
    except Exception as e:
        print(f"Error parsing '{' '.join(toks)}': {e}")

def parse_where(toks: list[str]):
    res = []
    expr = []
    for count, tok in enumerate(toks):
        if count==len(toks)-1:
            expr.append(tok)
        if tok == "and" or count==len(toks)-1:
            res.append(parse_where_expr(expr))
            expr = []
        else:
            expr.append(tok)
    return res

def parse_where_expr(toks: list[str]):
    try:
        if len(toks) == 2:
            assert(toks[1] in NullCondStr)
            return PredicateTerm(
                    col=toks[0],
                    op=toks[1],
                )
        elif len(toks) == 3:
            assert toks[1] in CmpStr

            if toks[2].isdigit():
                value = int(toks[2])
            elif (
                toks[2].count(".") == 1 and
                toks[2].replace(".", "").isdigit()
            ):
                value = float(toks[2])
            elif(
                toks[2].startswith("'") and
                toks[2].endswith("'")
            ):
                value = toks[2][1:-1]

            return PredicateTerm(
                    col=toks[0],
                    op=toks[1],
                    value=value,
                )
        raise ValueError("Invalid Tokenization")
    except Exception as e:
        print(f"Error parsing '{' '.join(toks)}': {e}")


def parse_groupby(toks: list[str]):
    if len(toks)==1:
        return toks

    try:
        res = []
        for count, tok in enumerate(toks):
            if count%2==0:
                res.append(tok)
            else:
                assert tok==","
        raise ValueError("Invalid Tokenization")
    except Exception as e:
        print(f"Error parsing '{' '.join(toks)}': {e}")
            


if __name__=="__main__":
    query_str = """
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
        group by event_type;
    """

    query_class = Query(
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

    print(parse(query_str)==query_class)