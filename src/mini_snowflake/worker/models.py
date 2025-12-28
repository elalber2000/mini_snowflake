from pydantic import BaseModel
from typing import Any

from mini_snowflake.common.manifest import ColumnInfo

class CreateRequest(BaseModel):
    db_path: str
    table: str
    table_schema: list[ColumnInfo]
    if_not_exists: bool = False

class DropRequest(BaseModel):
    db_path: str
    table: str
    if_exists: bool | None = None

class InsertRequest(BaseModel):
    db_path: str
    table: str
    src_path: str
    rows_per_shard: int | None = None

class SelectRequest(BaseModel):
    db_path: str
    raw_query: str


class TaskResponse(BaseModel):
    ok: bool
    result: Any = None
    error: str | None = None