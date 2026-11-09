
import os
from pathlib import Path

import boto3
import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

BUCKET = "retail-recommendation-1"
PREFIX = "raw/postgres/incremental/"
PIPELINE = "customer_interactions_to_s3"

# 1. Find the most recently generated incremental file
files = list(Path("data/incremental").glob("*.parquet"))

if not files:
    raise FileNotFoundError("No incremental Parquet files found")

file_path = max(files, key=lambda p: p.stat().st_mtime)

df = pd.read_parquet(file_path)

if df.empty:
    raise ValueError("Incremental file is empty")

min_id = int(df["interaction_id"].min())
max_id = int(df["interaction_id"].max())

print(f"Preparing to upload IDs {min_id} through {max_id}")

# 2. Connect to PostgreSQL
with psycopg.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
) as conn:

    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT last_processed_id
            FROM ingestion_watermarks
            WHERE pipeline_name = %s
            FOR UPDATE
        """, (PIPELINE,))

        result = cursor.fetchone()

        if result is None:
            raise RuntimeError("Watermark not initialized")

        watermark = result[0]

        if max_id <= watermark:
            print("This batch has already been processed.")
        else:
            # 3. Upload with a deterministic object key
            s3_key = (
                f"{PREFIX}"
                f"interactions_{watermark + 1}_{max_id}.parquet"
            )

            s3 = boto3.client("s3")

            s3.upload_file(
                str(file_path),
                BUCKET,
                s3_key
            )

            # 4. Verify object exists in S3
            response = s3.head_object(
                Bucket=BUCKET,
                Key=s3_key
            )

            if response["ContentLength"] <= 0:
                raise RuntimeError("Uploaded S3 object is empty")

            print(f"Upload verified: s3://{BUCKET}/{s3_key}")

            # 5. Advance watermark only after successful upload
            cursor.execute("""
                UPDATE ingestion_watermarks
                SET last_processed_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_name = %s
            """, (max_id, PIPELINE))

            conn.commit()

            print(f"Watermark advanced: {watermark} → {max_id}")

print("Incremental ingestion completed!")
