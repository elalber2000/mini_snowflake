from pathlib import Path

import duckdb
import pandas as pd


def delete_parquet(parquet_path: str | Path) -> None:
    """
    Delete a parquet file if it exists.

    This is intentionally explicit (no silent overwrite here),
    so callers can choose when deletion is acceptable.
    """
    parquet_path = Path(parquet_path)

    if parquet_path.exists():
        parquet_path.unlink()


def write_parquet_overwrite(df: pd.DataFrame, parquet_path: str | Path) -> Path:
    parquet_path = Path(parquet_path)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # Create
    con = duckdb.connect()
    try:
        con.register("df", df)

        con.execute(
            """
            COPY (
                SELECT *
                FROM df
            )
            TO ?
            (FORMAT PARQUET)
            """,
            [str(parquet_path)],
        )
    finally:
        con.close()

    return parquet_path


def read_parquet(parquet_path: str | Path) -> pd.DataFrame:
    parquet_path = Path(parquet_path)

    if not parquet_path.exists():
        raise FileNotFoundError(parquet_path)

    con = duckdb.connect()
    try:
        df = con.execute(
            "SELECT * FROM read_parquet(?)",
            [str(parquet_path)],
        ).df()
    finally:
        con.close()

    return df
