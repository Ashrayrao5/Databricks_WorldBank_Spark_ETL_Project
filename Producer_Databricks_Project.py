import boto3
import requests
from datetime import datetime

# -----------------------------
# CONFIG — EDIT THESE
# -----------------------------
S3_BUCKET = "world-bank-api"
S3_PREFIX = "worldbank/bronze"
AWS_REGION = "us-east-2"

# World Bank API endpoint
API_URL = "https://api.worldbank.org/v2/country?format=json&per_page=300"

# -----------------------------
# FETCH API DATA (RAW)
# -----------------------------
response = requests.get(API_URL)
response.raise_for_status()

raw_json = response.text   # raw response EXACTLY as received

# -----------------------------
# GENERATE TIMESTAMPED FILE NAME
# -----------------------------
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
file_key = f"{S3_PREFIX}/countries_raw_{timestamp}.json"

# -----------------------------
# UPLOAD RAW JSON TO S3
# -----------------------------
s3 = boto3.client("s3", region_name=AWS_REGION)

s3.put_object(
    Bucket=S3_BUCKET,
    Key=file_key,
    Body=raw_json,
    ContentType="application/json"
)

print(f"✅ Raw JSON dumped to s3://{S3_BUCKET}/{file_key}")
