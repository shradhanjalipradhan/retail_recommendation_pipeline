
import os
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

BUCKET = "retail-recommendation-1"
PREFIX = "raw/postgres/incremental/"
PIPELINE = "customer_interactions_to_s3"

with psycopg.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
) as conn:

    with conn.cursor() as cursor:
        # Lock the watermark row for this run
        cursor.execute("""
            SELECT last_processed_id
            FROM ingestion_watermarks
            WHERE pipeline_name = %s
            FOR UPDATE
        """, (PIPELINE,))

        result = cursor.fetchone()

        if result is None:
            raise RuntimeError("Watermark not initialized")

        last_id = result[0]
        print(f"Current watermark: {last_id}")

        # Extract records newer than the watermark
        cursor.execute("""
            SELECT interaction_id,
                   customer_id,
                   product_id,
                   interaction_type,
                   interaction_timestamp
            FROM customer_interactions
            WHERE interaction_id > %s
            ORDER BY interaction_id
        """, (last_id,))

        rows = cursor.fetchall()
        columns = [desc.name for desc in cursor.description]

        if not rows:
            print("No new records. Nothing to upload.")
        else:
            df = pd.DataFrame(rows, columns=columns)
            max_id = int(df["interaction_id"].max())

            s3_key = (
                f"{PREFIX}interactions_{last_id + 1}_{max_id}.parquet"
            )

            # Use a temporary local file for this batch
            with tempfile.TemporaryDirectory() as tmp:
                local_file = Path(tmp) / "batch.parquet"
                df.to_parquet(local_file, index=False)

                expected_size = local_file.stat().st_size

                s3 = boto3.client("s3")
                s3.upload_file(
                    str(local_file),
                    BUCKET,
                    s3_key
                )

                response = s3.head_object(
                    Bucket=BUCKET,
                    Key=s3_key
                )

                if response["ContentLength"] != expected_size:
                    raise RuntimeError(
                        "S3 file size does not match local file"
                    )

            # Only advance after upload verification
            cursor.execute("""
                UPDATE ingestion_watermarks
                SET last_processed_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_name = %s
            """, (max_id, PIPELINE))

            conn.commit()

            print(f"Uploaded {len(df)} records.")
            print(f"S3 key: {s3_key}")
            print(f"Watermark advanced: {last_id} -> {max_id}")

print("Pipeline finished.")
