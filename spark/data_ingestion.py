import io
import os
import sys
import boto3
import pandas as pd
from sqlalchemy import create_engine

# 确保能 import 到 utils.constants（建议从项目根目录用: python -m spark.data_ingestion）
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from utils.constants import (
    AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY, AWS_REGION, AWS_BUCKET_NAME,
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
)

BASE_PREFIX = "Business_raw_data/"          # 桶内前缀（不要重复写 bucket 名）
FILES = ["customers.csv", "accounts.csv", "transactions.csv"]
TABLES = {"customers.csv":"customers", "accounts.csv":"accounts", "transactions.csv":"transactions"}

def get_engine():
    uri = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(uri)

def read_s3_csv(s3, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def main():
    # S3 & Postgres 连接
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    engine = get_engine()

    # 按父→子顺序写入，避免外键顺序问题（即使没外键，这个顺序也更自然）
    ordered = ["customers.csv", "accounts.csv", "transactions.csv"]

    for fname in ordered:
        key = BASE_PREFIX + fname
        print(f"Reading s3://{AWS_BUCKET_NAME}/{key}")
        df = read_s3_csv(s3, key)

        table = TABLES[fname]
        print(f"Writing to Postgres table: {table}")
        # 追加写入：if_exists='append'；若想每次覆盖请改 'replace'
        df.to_sql(table, engine, if_exists='append', index=False)
        print(f"Done: {fname} -> {table}")

    print("All done ✅")

if __name__ == "__main__":
    main()