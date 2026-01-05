import logging
import re
import shutil
from functools import lru_cache
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from mini_snowflake.common.db_conn import DBConn
from mini_snowflake.common.manifest import Manifest
from mini_snowflake.common.utils import setup_logging

from .models import CreateRequest, DropRequest, InsertRequest, SelectRequest

setup_logging()
logger = logging.getLogger("")

DUCKDB_TO_ARROW: dict[str, pa.DataType] = {
    # integers
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    "hugeint": pa.int64(),
    "bignum": pa.decimal128(38, 0),
    "utinyint": pa.uint8(),
    "usmallint": pa.uint16(),
    "uinteger": pa.uint32(),
    "ubigint": pa.uint64(),
    "uhugeint": pa.uint64(),
    # floats / decimals
    "float": pa.float32(),
    "real": pa.float32(),
    "double": pa.float64(),
    "decimal": pa.decimal128(
        38, 10
    ),  # if you don't store precision/scale, pick a default
    "numeric": pa.decimal128(38, 10),
    # boolean
    "boolean": pa.bool_(),
    "bool": pa.bool_(),
    # strings
    "varchar": pa.string(),
    "text": pa.string(),
    "string": pa.string(),
    "char": pa.string(),
    "uuid": pa.string(),
    "bit": pa.string(),
    # binary
    "blob": pa.binary(),
    "bytea": pa.binary(),
    "varbinary": pa.binary(),
    # temporal
    "date": pa.date32(),
    "time": pa.time64("us"),
    "timestamp": pa.timestamp("us"),
    "timestamptz": pa.timestamp("us", tz="UTC"),
    "interval": pa.duration("us"),
}

def init_worker(db_path: str, threads: int = 1):
    global DUCK
    DUCK = duckdb.connect(db_path)
    DUCK.execute(f"PRAGMA threads={threads}")

def get_conn() -> duckdb.DuckDBPyConnection:
    if DUCK is None:
        raise RuntimeError("DuckDB not initialized. Call init_worker() at startup.")
    return DUCK


def worker_create(
    conn: DBConn,
    request: CreateRequest,
):
    logger.info(f"worker_create({request})")
    table = request.table
    schema = request.table_schema

    if conn.catalog.get_table(table, exist_ok=True) is not None:
        return f"Table {table} already created"

    logger.info("Creating table")
    table_path = conn.path / table
    table_path.mkdir(parents=True)

    logger.info("Create manifest")
    manifest_path = table_path / "manifest.json"

    manifest = Manifest(
        table_name=table,
        schema=schema,
    )

    logger.info("Update/Create catalog")
    conn.catalog.create_table(
        table_name=table,
        table_id=manifest.table_id,
    )

    logger.info("Save catalog and manifest")
    manifest.save(manifest_path)
    conn.catalog.save(conn.catalog_path)

    return f"Successfully created table '{table}'"


def worker_drop(
    conn: DBConn,
    request: DropRequest,
):
    logger.info(f"worker_drop({request})")
    table = request.table
    if_exists = request.if_exists if request.if_exists is not None else False

    # Update catalog
    conn.catalog.drop_table(
        table,
        exist_ok=if_exists,
    )

    # Drop table
    drop_path = conn.path / table
    if drop_path.exists():
        shutil.rmtree(drop_path)

    conn.catalog.save(conn.catalog_path)

    # Output
    return f"Successfully dropped table '{table}'"


def _get_shard_i(path: str) -> int:
    match = re.search(r"shard-([^.]+)\.parquet", path)
    return int(match.group(1)) if match else 0


def _validate_insert_table(tb: pa.Table, manifest: Manifest) -> pa.Table:
    """
    Validates:
      - all manifest columns exist in the incoming table
      - nullability constraints (nullable=False)
      - cocast columns to target Arrow types
    Returns a casted table (so downstream parquet files have canonical types).
    """
    expected = {c.name: c for c in manifest.schema}
    incoming_names = set(tb.schema.names)

    missing = [name for name in expected.keys() if name not in incoming_names]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Forbid extras
    extras = [name for name in tb.schema.names if name not in expected]
    if extras:
        raise ValueError(f"Unexpected columns: {extras}")

    tb = tb.select([c.name for c in manifest.schema])

    # Nullability + castability
    cast_arrays = []
    for col in manifest.schema:
        arr = tb[col.name]
        if not col.nullable:
            if arr.null_count > 0:
                raise ValueError(
                    f"Column '{col.name}' is NOT NULL but has {arr.null_count} nulls"
                )

        target_type = DUCKDB_TO_ARROW[str(col.type)]
        try:
            cast_arrays.append(pa.compute.cast(arr, target_type, safe=True))
        except Exception as e:
            raise TypeError(
                f"Column '{col.name}' cannot be safely cast from {arr.type} to {target_type}"
            ) from e

    return pa.table(cast_arrays, names=[c.name for c in manifest.schema])


def worker_insert(
    conn: DBConn,
    request: InsertRequest,
):
    logger.info(f"worker_insert({request})")
    table = request.table
    src_path = Path(request.src_path)
    rows_per_shard = request.rows_per_shard

    suffix = src_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(src_path)
    elif suffix in {".parquet", ".pq"}:
        df = pd.read_parquet(src_path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    table_path = conn.path / table
    if not table_path.exists():
        raise NameError(f"Table '{table}' doesn't exist")

    manifest_path = table_path / "manifest.json"
    if not manifest_path.exists():
        raise NameError(f"Table '{table}' doesn't have a Manifest")

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
    tb = _validate_insert_table(tb, manifest)

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
    return f"Successfully inserted data into table '{table}'"


def worker_select(
    conn: DBConn,
    request: SelectRequest,
) -> str:
    duckconn = get_conn()
    duckconn.execute(request.raw_query).fetchall()
    return f"Successfully executed query {' '.join(request.raw_query.split())}"
