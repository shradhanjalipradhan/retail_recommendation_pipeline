
import pandas as pd
from pathlib import Path

expected_counts = {
    "customers": 5,
    "products": 5,
    "customer_interactions": 10
}

for table, expected_count in expected_counts.items():
    path = Path(f"data/raw/{table}.parquet")

    df = pd.read_parquet(path)

    print(f"\nTable: {table}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    assert len(df) == expected_count, (
        f"{table}: expected {expected_count} rows, "
        f"found {len(df)}"
    )

print("\nAll Parquet validation checks passed!")
