from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
import json
import os
import uuid

from mini_snowflake.common.utils import _atomic_write_text


def _curr_date() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )

def _validate_types(spec: tuple[tuple[object, type], ...]) -> None:
    """
    Validate that each value matches its expected type.

    Example:
        validate_types((
            ("asd", str),
            (123, int),
        ))
    """
    for value, expected_type in spec:
        if not isinstance(value, expected_type):
            raise TypeError(
                f"Expected type {expected_type.__name__}, "
                f"got {type(value).__name__} ({value!r})"
            )


@dataclass(frozen=True)
class TableEntry:
    table_id: str

    def validate(self) -> None:
        _validate_types(
            (
                (self.table_id, str),
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_id": self.table_id,
        }


@dataclass
class Catalog:
    version: int = 1
    created_at: str = field(default_factory=_curr_date)
    tables: dict[str, TableEntry] = field(default_factory=dict)

    def validate(self) -> None:
        assert self.version==1, "Expected version 1"
        _validate_types(
            (
                (self.version, int),
                (self.created_at, str)
            )
        )
        for table in self.tables.values():
            table.validate()

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "created_at": self.created_at,
            "tables": {name: entry.to_dict() for name, entry in self.tables.items()},
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Catalog:
        c = cls(
            version=int(d.get("version", 1)),
            created_at=str(d.get("created_at", _curr_date())),
            tables={k: TableEntry.from_dict(v) for k, v in dict(d.get("tables", {})).items()},
        )
        c.validate()
        return c

    @classmethod
    def load(cls, catalog_path: str | Path) -> Catalog:
        catalog_path = Path(catalog_path)
        if not catalog_path.exists():
            # Default empty catalog (useful for first run)
            return cls()
        data = json.loads(catalog_path.read_text(encoding="utf-8"))
        return cls.from_dict(data)

    def save(self, catalog_path: str | Path) -> Path:
        catalog_path = Path(catalog_path)
        self.validate()
        text = json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        _atomic_write_text(catalog_path, text)
        return catalog_path
    
    def create_table(
        self,
        table_name: str,
        table_id: str,
    ):
        if table_name in self.tables:
            raise IndexError(f"Table '{table_name}' already exists in catalog")
        self.tables[table_name] = TableEntry(
            table_id=table_id,
        )
    
    def drop_table(
        self,
        table_name: str,
        exist_ok: bool = False,
    ):
        if table_name not in self.tables:
            if exist_ok:
                return
            raise IndexError(f"Table '{table_name}' doesn't exist in catalog")
        del(self.tables[table_name])

    
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
    rows_per_shard: int = 100
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
            _validate_types(((shard, str)))
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