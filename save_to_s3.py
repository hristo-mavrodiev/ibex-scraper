import boto3
import io
from datetime import datetime
import pandas as pd

import logging
logger = logging.getLogger(__name__)


def save_raw_data_to_s3(raw_data: str, date: datetime, bucket: str):
    date_str = date.strftime("%Y_%m_%d")
    prefix = f"raw/ibex/{date.strftime('%Y/%m/%d')}"
    key = f"{prefix}/{date_str}_ibex_raw_data.html"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=raw_data)
    print(f"Uploaded raw data to s3://{bucket}/{key}")


def save_clean_data_to_s3(df: pd.DataFrame, date: datetime, bucket: str):
    date_str = date.strftime("%Y_%m_%d")
    prefix = f"clean/ibex/{date.strftime('%Y/%m/%d')}"
    key = f"{prefix}/{date_str}_ibex_clean_data.parquet"

    s3 = boto3.client("s3")
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"Uploaded clean data to s3://{bucket}/{key}")
