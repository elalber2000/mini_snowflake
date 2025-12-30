from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mini_snowflake.common.utils import _atomic_write_text, _curr_date
from pydantic import ConfigDict, Field, field_validator
from pydantic.dataclasses import dataclass


@dataclass(frozen=True, config=ConfigDict(extra="forbid"))
class TableEntry:
    table_id: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TableEntry:
        return cls(**d)

    def to_dict(self) -> dict[str, Any]:
        return {"table_id": self.table_id}


@dataclass(config=ConfigDict(extra="forbid"))
class Catalog:
    version: int = 1
    created_at: str = Field(default_factory=_curr_date)
    tables: dict[str, TableEntry] = Field(default_factory=dict)

    @field_validator("version")
    @classmethod
    def _version_must_be_1(cls, v: int) -> int:
        if v != 1:
            raise ValueError("Expected version 1")
        return v

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "created_at": self.created_at,
            "tables": {name: entry.to_dict() for name, entry in self.tables.items()},
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Catalog:
        raw_tables = dict(d.get("tables", {}))
        d2 = dict(d)
        d2["tables"] = {k: TableEntry.from_dict(v) for k, v in raw_tables.items()}
        return cls(**d2)

    @classmethod
    def load(cls, catalog_path: str | Path) -> Catalog:
        catalog_path = Path(catalog_path)
        if not catalog_path.exists():
            return cls()
        data = json.loads(catalog_path.read_text(encoding="utf-8"))
        return cls.from_dict(data)

    def save(self, catalog_path: str | Path) -> Path:
        catalog_path = Path(catalog_path)
        text = (
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n"
        )
        _atomic_write_text(catalog_path, text)
        return catalog_path

    def create_table(self, table_name: str, table_id: str) -> None:
        if table_name in self.tables:
            raise IndexError(f"Table '{table_name}' already exists in catalog")
        self.tables[table_name] = TableEntry(table_id=table_id)

    def _table_in_catalog(self, table_name: str, exist_ok: bool = False) -> bool:
        if table_name not in self.tables:
            if exist_ok:
                return False
            raise IndexError(f"Table '{table_name}' doesn't exist in catalog")
        return True

    def get_table(self, table_name: str, exist_ok: bool) -> TableEntry | None:
        if self._table_in_catalog(table_name, exist_ok):
            return self.tables[table_name]
        return None

    def drop_table(self, table_name: str, exist_ok: bool = False) -> None:
        if self._table_in_catalog(table_name, exist_ok=exist_ok):
            del self.tables[table_name]
