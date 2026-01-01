from datetime import date, datetime, timezone
from pathlib import Path
import random
from typing import Any, Callable, Literal
from pydantic import BaseModel
import yaml
import pyarrow as pa
import pyarrow.dataset as ds
from mini_snowflake.common.db_conn import DBConn
from mini_snowflake.common.manifest import ColumnInfo, Manifest
from mini_snowflake.common.utils import MSF_PATH
from mini_snowflake.parser.parser import parse
from mini_snowflake.worker.models import CreateRequest
from mini_snowflake.worker.worker import DUCKDB_TO_ARROW, worker_create

class DataGenConfig(BaseModel):
    rows_per_shard: int
    num_shards: int
    db_path: str | Path
    create_ddl: str

SynthDataType = Literal[
    "int",
    "float",
    "bool",
    "varchar",
    "date",
    "timestamp",
]

def load_config(config_path: str) -> DataGenConfig:
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
    if not config_dict["db_path"].startswith("/"):
        config_dict["db_path"] = MSF_PATH / config_dict["db_path"]
    config = DataGenConfig(**config_dict)
    return config

def generate_synthetic_value(col: ColumnInfo) -> Any:
    return {
        "int": random.randint(0, 100),
        "float": round(random.uniform(0, 100), 2),
        "bool": random.choice([True, False]),
        "varchar": f"{col.name}_{random.randint(0, 10_000)}",
        "date": date.today(),
        "timestamp": datetime.now(tz=timezone.utc),
    }[col.type]

def generate_row(manifest: Manifest) -> dict[str, Any]:
    return {
        col.name: generate_synthetic_value(col)
        for col in manifest.schema
    }

def manifest_to_arrow_schema(manifest) -> pa.Schema:
    fields: list[pa.Field] = []
    for col in manifest.schema:
        arrow_type = DUCKDB_TO_ARROW.get(col.type)
        if arrow_type is None:
            raise ValueError(f"Unsupported type for Parquet: {col.type}")
        # Arrow Field.nullable=True means "may be null". If your col is non-nullable, set False.
        fields.append(pa.field(col.name, arrow_type, nullable=col.nullable))
    return pa.schema(fields)


def write_sharded_parquet(
    *,
    manifest: Manifest,
    generate_row_fn: Callable[[Any], dict[str, Any]],
    table_name: str,
    conn: DBConn,
    config: DataGenConfig,
) -> list[Path]:
    dataset_dir: Path = Path(conn.path) / table_name
    dataset_dir.mkdir(parents=True, exist_ok=True)

    schema = manifest_to_arrow_schema(manifest)
    total_rows = config.num_shards * config.rows_per_shard

    rows = [generate_row_fn(manifest) for _ in range(total_rows)]
    data = pa.Table.from_pylist(rows, schema=schema)

    write_kwargs: dict[str, Any] = dict(
        data=data,
        base_dir=str(dataset_dir),
        format="parquet",
        max_rows_per_file=config.rows_per_shard,
        max_rows_per_group=config.rows_per_shard,
        existing_data_behavior="overwrite_or_ignore",
        basename_template="tmp_shard-{i}.parquet",
    )

    ds.write_dataset(**write_kwargs)

    written_files: list[Path] = sorted(dataset_dir.glob("*.parquet"))
    return dataset_dir, [str(f).split("/")[-1] for f in written_files]

if __name__=="__main__":
    config = load_config(MSF_PATH / "benchmark/data_gen.yml")
    print(config)
    conn = DBConn(config.db_path)
    query = parse(config.create_ddl)
    worker_create(
        conn,
        CreateRequest(
            db_path=str(conn.path),
            table=query.table,
            table_schema=query.schema,
            if_not_exists=query.if_not_exists,
        )
    )
    manifest_path = conn.path / query.table / "manifest.json"
    manifest = Manifest.load(manifest_path)
    _, shards = write_sharded_parquet(
        manifest=manifest,
        generate_row_fn=generate_row,
        table_name=query.table,
        conn=conn,
        config=config,
    )
    manifest.shards = shards
    manifest.save(manifest_path)

