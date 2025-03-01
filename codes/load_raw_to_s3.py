from dotenv import load_dotenv
from pathlib import Path

import boto3
import os
import tqdm

# Paths to local datasets
raw_data_orders_path = Path("../datasets/raw_data/orders")
raw_data_registers_path = Path("../datasets/raw_data/registers")

# Remove any old environment variables before loading new ones
for var in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_BUCKET_NAME', 'AWS_REGION']:
    os.environ.pop(var, None)

# Load credentials from .env file
load_dotenv(override=True)

# Retrieve environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# Initialize boto3 client for S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Function to upload parquet files to S3
def load_parquet_to_s3(path, prefix):
    parquet_files = list(path.glob('*.parquet'))  # Get all parquet files in the directory

    for file in tqdm.tqdm(parquet_files, desc=f"Uploading {prefix}"):
        # Ensure the full path is converted to a string and the S3 path is properly defined
        s3_client.upload_file(str(file), AWS_BUCKET_NAME, f"{prefix}/{file.name}")
# Upload the parquet files
try:
    load_parquet_to_s3(raw_data_registers_path, 'raw_registers')
    load_parquet_to_s3(raw_data_orders_path, 'raw_orders')
    print("✅ All uploads completed successfully!")
except Exception as ex:
    print('❌ Error during upload:', ex)
