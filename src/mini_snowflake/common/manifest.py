from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Literal

from mini_snowflake.common.utils import _atomic_write_text, _curr_date
from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass

ColType = Literal[
    "tinyint",
    "smallint",
    "integer",
    "int",
    "bigint",
    "hugeint",
    "bignum",
    "utinyint",
    "usmallint",
    "uinteger",
    "ubigint",
    "uhugeint",
    "float",
    "real",
    "double",
    "decimal",
    "numeric",
    "boolean",
    "bool",
    "varchar",
    "text",
    "string",
    "char",
    "uuid",
    "bit",
    "blob",
    "bytea",
    "varbinary",
    "date",
    "time",
    "timestamp",
    "timestamptz",
    "interval",
]


@dataclass(frozen=True, config=ConfigDict(extra="forbid"))
class ColumnInfo:
    name: str
    type: ColType
    nullable: bool = True

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ColumnInfo:
        return cls(**d)

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type, "nullable": self.nullable}


@dataclass(config=ConfigDict(extra="forbid"))
class Manifest:
    manifest_version: int = 1
    table_name: str = ""
    table_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rows_per_shard: int = 100_000
    created_at: str = Field(default_factory=_curr_date)
    schema: list[ColumnInfo] = Field(default_factory=list)
    shards: list[str] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "table_name": self.table_name,
            "table_id": self.table_id,
            "rows_per_shard": self.rows_per_shard,
            "created_at": self.created_at,
            "schema": [c.to_dict() for c in self.schema],
            "shards": list(self.shards),
        }

    @classmethod
    def load(cls, manifest_path: str | Path) -> Manifest:
        manifest_path = Path(manifest_path)
        data = json.loads(manifest_path.read_text(encoding="utf-8"))

        data["schema"] = [ColumnInfo.from_dict(c) for c in data.get("schema", [])]
        data["shards"] = list(data.get("shards", []))

        return cls(**data)

    def save(self, manifest_path: str | Path) -> Path:
        manifest_path = Path(manifest_path)
        text = (
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n"
        )
        _atomic_write_text(manifest_path, text)
        return manifest_path
