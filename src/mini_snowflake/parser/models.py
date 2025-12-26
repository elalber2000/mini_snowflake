from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, Final

from mini_snowflake.common.models import ColumnInfo

AggFuncStr: Final = ("count", "sum", "min", "max", "avg")
AggFunc = Literal["count", "sum", "min", "max", "avg"]

CmpStr = ("=", "!=", "<", "<=", ">", ">=")
Cmp = Literal["=", "!=", "<", "<=", ">", ">="]

NullCondStr = ("is_null", "is_not_null")
NullCond = Literal["is_null", "is_not_null"]

@dataclass(frozen=True)
class ColumnRef:
    name: str
    alias: str | None = None

@dataclass(frozen=True)
class AggExpr:
    func: AggFunc
    col: str | None
    alias: str | None = None

@dataclass(frozen=True)
class PredicateTerm:
    col: str
    op: Cmp | NullCond
    value: int | float | str | None | bool = None

@dataclass(frozen=True)
class SelectQuery:
    table: str
    select: list[ColumnRef | AggExpr] | Literal["*"]
    where: list[PredicateTerm] | None
    group_by: list[str] | None

@dataclass(frozen=True)
class CreateQuery:
    table: str
    schema: list[ColumnInfo]

@dataclass(frozen=True)
class InsertQuery:
    table: str

@dataclass(frozen=True)
class DropQuery:
    table: str
    if_exists: bool