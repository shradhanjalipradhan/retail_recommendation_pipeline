
import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

PIPELINE = "customer_interactions_to_s3"
OUTPUT_DIR = Path("data/incremental")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

with psycopg.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
) as conn:

    with conn.cursor() as cursor:

        # Read the last successful watermark
        cursor.execute("""
            SELECT last_processed_id
            FROM ingestion_watermarks
            WHERE pipeline_name = %s
        """, (PIPELINE,))

        result = cursor.fetchone()

        if result is None:
            raise RuntimeError(
                "Watermark not initialized"
            )

        last_id = result[0]
        print(f"Previous watermark: {last_id}")

        # Extract only records newer than the watermark
        cursor.execute("""
            SELECT *
            FROM customer_interactions
            WHERE interaction_id > %s
            ORDER BY interaction_id
        """, (last_id,))

        rows = cursor.fetchall()
        columns = [
            desc.name for desc in cursor.description
        ]

        df = pd.DataFrame(rows, columns=columns)

        if df.empty:
            print("No new records to extract.")
        else:
            max_id = int(df["interaction_id"].max())

            timestamp = datetime.now(
                timezone.utc
            ).strftime("%Y%m%dT%H%M%S%fZ")

            output_file = (
                OUTPUT_DIR /
                f"interactions_{last_id + 1}_"
                f"{max_id}_{timestamp}.parquet"
            )

            df.to_parquet(output_file, index=False)

            print(f"New records: {len(df)}")
            print(f"Extracted IDs: {df['interaction_id'].tolist()}")
            print(f"Saved to: {output_file}")
            print(
                "Watermark not advanced yet. "
                "S3 upload must succeed first."
            )
