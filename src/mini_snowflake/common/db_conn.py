from pathlib import Path
from mini_snowflake.common.catalog import Catalog


class DBConn():
    def __init__(
        self,
        path: str,
        exist_ok: bool = False,
    ):
        # Crear db
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=exist_ok)

        # Crear catalog
        self.catalog_path = self.path / "catalog.json"

        if self.catalog_path.exists():
            self.catalog = Catalog.load(self.catalog_path)
        else:
            self.catalog = Catalog()
            self.catalog.save(self.catalog_path)
