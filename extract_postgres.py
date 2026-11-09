
import os
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

# Create a local folder for extracted data
output_dir = Path("data/raw")
output_dir.mkdir(parents=True, exist_ok=True)

# Tables we want to extract
tables = [
    "customers",
    "products",
    "customer_interactions"
]

# Connect to PostgreSQL
with psycopg.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
) as conn:

    for table in tables:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table}")

            rows = cursor.fetchall()
            columns = [
                desc.name for desc in cursor.description
            ]

        # Convert PostgreSQL records into a DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # Save as Parquet
        output_path = output_dir / f"{table}.parquet"
        df.to_parquet(output_path, index=False)

        print(
            f"Extracted {len(df)} rows from {table} "
            f"to {output_path}"
        )

print("PostgreSQL extraction completed!")
