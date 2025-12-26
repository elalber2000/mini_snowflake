import pandas as pd
from mini_snowflake.common.io_parquet import (
    delete_parquet,
    read_parquet,
    write_parquet_overwrite,
)
from mini_snowflake.common.utils import MSF_PATH


def main() -> None:
    table_path = MSF_PATH / "data" / "test.parquet"

    # Create
    df0 = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "value": [10.0, 20.5, 30.25],
        }
    )
    write_parquet_overwrite(df0, table_path)
    print(f"Table created at: {table_path.resolve()}")

    # Read
    df1 = read_parquet(table_path)
    print("Read table:")
    print(df1)

    # Delete
    delete_parquet(table_path)
    print("Table deleted.")

    # Read
    try:
        _ = read_parquet(table_path)
        raise RuntimeError("Expected FileNotFoundError, but read_parquet succeeded.")
    except FileNotFoundError:
        print("Confirmed: table is deleted (FileNotFoundError raised).")


if __name__ == "__main__":
    main()
