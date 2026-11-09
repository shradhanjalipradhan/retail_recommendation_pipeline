
from pathlib import Path
import boto3

BUCKET = "retail-recommendation-1"
LOCAL_DIR = Path("data/raw")
S3_PREFIX = "raw/postgres/"

# Create an S3 client using configured AWS credentials
s3 = boto3.client("s3")

files = [
    "customers.parquet",
    "products.parquet",
    "customer_interactions.parquet"
]

for filename in files:
    local_path = LOCAL_DIR / filename
    s3_key = f"{S3_PREFIX}{filename}"

    if not local_path.exists():
        raise FileNotFoundError(f"Missing file: {local_path}")

    s3.upload_file(
        str(local_path),
        BUCKET,
        s3_key
    )

    print(f"Uploaded: s3://{BUCKET}/{s3_key}")

print("All files uploaded successfully!")
