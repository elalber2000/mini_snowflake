import json
from pathlib import Path
import shutil
from mini_snowflake.common.db_conn import DBConn
from mini_snowflake.common.models import Catalog, ColumnInfo, Manifest
from mini_snowflake.common.utils import MSF_PATH
from mini_snowflake.parser.models import CreateQuery, DropQuery

def drop_command(conn: DBConn, query: DropQuery):
    # Update catalog
    conn.catalog.drop_table(
        query.table,
        exist_ok=query.if_exists,
    )

    # Drop table
    drop_path = conn.path / query.table
    if drop_path.exists():
        shutil.rmtree(drop_path)


def create_command(conn: DBConn, query: CreateQuery):
    # Crear tabla
    table_path = conn.path / query.table
    table_path.mkdir(parents=True)

    # - Crea manifest
    manifest_path = table_path / "manifest.json"

    manifest = Manifest(
        table_name=query.table,
        schema=query.schema,
    )
    manifest.save(manifest_path)
    
    # - Update/Crea catalog
    conn.catalog.create_table(
        table_name=query.table,
        table_id=manifest.table_id,
    )
    
    # Save catalog and manifest
    conn.catalog.save(conn.catalog_path)

if __name__=="__main__":
    path = MSF_PATH / "data" / "dummy_db"
    conn = DBConn(
        path=path,
        exist_ok=True,
    )
    drop_command(
        conn,
        DropQuery(
            table="dummy_table",
            if_exists=True,
        ),
    )
    create_command(
        conn,
        CreateQuery(
            table="dummy_table",
            schema=[
                ColumnInfo(
                    name="col1",
                    type="int",
                ),
                ColumnInfo(
                    name="col2",
                    type="varchar",
                ),
            ]
        ),
    )