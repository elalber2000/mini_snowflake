from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import json
import uuid

from mini_snowflake.common.utils import _atomic_write_text, _curr_date, _validate_types

@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type: str
    nullable: bool = True

    def validate(self) -> None:
        _validate_types(
            (
                (self.name, str),
                (self.type, str),
                (self.nullable, bool),
            )
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ColumnInfo:
        obj = cls(
            name=str(d["name"]),
            type=str(d["type"]),
            nullable=bool(d.get("nullable", True)),
        )
        obj.validate()
        return obj

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type, "nullable": self.nullable}

@dataclass
class Manifest:
    manifest_version: int = 1
    table_name: str = ""
    table_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    rows_per_shard: int = 100_000
    created_at: str = field(default_factory=_curr_date)
    schema: list[ColumnInfo] = field(default_factory=list)
    shards: list[str] = field(default_factory=list)

    def validate(self) -> None:
        _validate_types(
            (
                (self.manifest_version, int),
                (self.table_name, str),
                (self.table_id, str),
                (self.rows_per_shard, int),
                (self.created_at, str),
            )
        )
        for shard in self.shards:
            _validate_types(((shard, str),))
        for col in self.schema:
            col.validate()

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "table_name": self.table_name,
            "table_id": self.table_id,
            "rows_per_shard": self.rows_per_shard,
            "created_at": self.created_at,
            "schema": [c.to_dict() for c in self.schema],
            "shards": [s for s in self.shards],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Manifest:
        m = cls(
            manifest_version=int(d.get("manifest_version", 1)),
            table_name=str(d.get("table_name", "")),
            table_id=str(d.get("table_id", str(uuid.uuid4()))),
            rows_per_shard=int(d.get("rows_per_shard", 100)),
            created_at=str(d.get("created_at", _curr_date())),
            schema=[ColumnInfo.from_dict(x) for x in d.get("schema", [])],
            shards=[x for x in d.get("shards", [])],
        )
        m.validate()
        return m

    @classmethod
    def load(cls, manifest_path: str | Path) -> Manifest:
        manifest_path = Path(manifest_path)
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        return cls.from_dict(data)

    def save(self, manifest_path: str | Path) -> Path:
        manifest_path = Path(manifest_path)
        self.validate()
        text = json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        _atomic_write_text(manifest_path, text)
        return manifest_path