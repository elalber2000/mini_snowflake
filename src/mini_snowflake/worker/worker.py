import re
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from mini_snowflake.common.db_conn import DBConn
from mini_snowflake.common.manifest import ColumnInfo, Manifest
from mini_snowflake.common.utils import MSF_PATH
from mini_snowflake.parser.models import CreateQuery, DropQuery, InsertQuery, SelectQuery


def worker_create(conn: DBConn, query: CreateQuery):
    # Create table
    table_path = conn.path / query.table
    table_path.mkdir(parents=True)

    # - Create manifest
    manifest_path = table_path / "manifest.json"

    manifest = Manifest(
        table_name=query.table,
        schema=query.schema,
    )
    manifest.save(manifest_path)

    # - Update/Create catalog
    conn.catalog.create_table(
        table_name=query.table,
        table_id=manifest.table_id,
    )

    # Save catalog and manifest
    conn.catalog.save(conn.catalog_path)

    # Output
    return f"Successfully created table '{query.table}'"


def worker_drop(conn: DBConn, query: DropQuery):
    # Update catalog
    conn.catalog.drop_table(
        query.table,
        exist_ok=query.if_exists,
    )

    # Drop table
    drop_path = conn.path / query.table
    if drop_path.exists():
        shutil.rmtree(drop_path)

    # Output
    return f"Successfully dropped table '{query.table}'"


def _get_shard_i(path: str) -> int:
    match = re.search(r"shard-([^.]+)\.parquet", path)
    return int(match.group(1)) if match else 0


def worker_insert(
    conn: DBConn,
    query: InsertQuery,
    df: pd.DataFrame,
    rows_per_shard: int | None = None,
):
    table_path = conn.path / query.table
    if not table_path.exists():
        raise NameError(f"Table '{query.table}' doesn't exist")

    manifest_path = table_path / "manifest.json"
    if not manifest_path.exists():
        raise NameError(f"Table '{query.table}' doesn't have a Manifest")

    manifest = Manifest.load(manifest_path)
    if manifest.shards == []:
        last_shard = 0
    else:
        last_shard = max([_get_shard_i(s) for s in manifest.shards]) + 1

    rows_per_shard = (
        rows_per_shard if rows_per_shard is not None else manifest.rows_per_shard
    )

    # Convert to Arrow
    tb = pa.Table.from_pandas(df, preserve_index=False)

    # Write shards
    ds.write_dataset(
        data=tb,
        base_dir=str(table_path),
        format="parquet",
        max_rows_per_file=rows_per_shard,
        max_rows_per_group=rows_per_shard,
        existing_data_behavior="overwrite_or_ignore",
        basename_template="tmp_shard-{i}.parquet",
    )

    for p in sorted(table_path.glob("tmp_shard-*.parquet")):
        i = int(p.stem.split("-")[1])
        shard_i_name = f"shard-{last_shard + i}.parquet"
        p.rename(table_path / shard_i_name)
        manifest.shards += [shard_i_name]

    # Manifest save
    manifest.save(manifest_path)

    # Output
    return f"Successfully inserted data into table '{query.table}'"


if __name__ == "__main__":
    path = MSF_PATH / "data" / "dummy_db"
    conn = DBConn(
        path=path,
        exist_ok=True,
    )

    print(
        worker_drop(
            conn,
            DropQuery(
                table="dummy_table",
                if_exists=True,
            ),
        )
    )

    print(
        worker_create(
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
                ],
            ),
        )
    )

    print(
        worker_insert(
            conn,
            InsertQuery(table="dummy_table"),
            pd.DataFrame(
                {
                    "col1": [1, 2],
                    "col2": ["foo", "bar"],
                }
            ),
            rows_per_shard=3,
        )
    )

    print(
        worker_insert(
            conn,
            InsertQuery(table="dummy_table"),
            pd.DataFrame(
                {
                    "col1": [3, 4, 5, 6],
                    "col2": ["a", "b", "c", "d"],
                }
            ),
            rows_per_shard=3,
        )
    )
