from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mini_snowflake.common.utils import _atomic_write_text, _curr_date, _validate_types


@dataclass(frozen=True)
class TableEntry:
    table_id: str

    def validate(self) -> None:
        _validate_types(((self.table_id, str),))

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_id": self.table_id,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TableEntry:
        c = cls(
            table_id=str(d.get("table_id", "")),
        )
        c.validate()
        return c


@dataclass
class Catalog:
    version: int = 1
    created_at: str = field(default_factory=_curr_date)
    tables: dict[str, TableEntry] = field(default_factory=dict)

    def validate(self) -> None:
        assert self.version == 1, "Expected version 1"
        _validate_types(((self.version, int), (self.created_at, str)))
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
            tables={
                k: TableEntry.from_dict(v) for k, v in dict(d.get("tables", {})).items()
            },
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
        text = (
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n"
        )
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

    def _table_in_catalog(
        self,
        table_name: str,
        exist_ok: bool = False,
    ):
        if table_name not in self.tables:
            if exist_ok:
                return False
            raise IndexError(f"Table '{table_name}' doesn't exist in catalog")
        return True

    def get_table(
        self,
        table_name: str,
        exist_ok: bool,
    ):
        if self._table_in_catalog(table_name, exist_ok):
            return self.tables[table_name]

    def drop_table(
        self,
        table_name: str,
        exist_ok: bool = False,
    ):
        if self._table_in_catalog(table_name, exist_ok=exist_ok):
            del self.tables[table_name]
