"""
Các tiện ích S3: tạo client, upload/download bytes, upload DataFrame theo partition.
"""
import time
from typing import Union, Optional, List
import io
import uuid
import boto3
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from src.utils.logger import get_logger

logger = get_logger(__name__)

def get_s3_client(region: str = None):
    """
    Trả về boto3 S3 client; nếu không truyền region sẽ dùng cấu hình mặc định.
    """
    if region:
        return boto3.client("s3", region_name=region)
    return boto3.client("s3")

def upload_bytes(
    data: Union[bytes, bytearray],
    bucket: str,
    key: str,
    region: str = None,
    max_retries: int = 3,
    retry_sleep: float = 2.0
) -> None:
    """
    Upload bytes lên S3 với retry/backoff đơn giản.
    Ném TypeError nếu dữ liệu không phải bytes, và RuntimeError khi retry hết.
    """
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("data must be bytes or bytearray")

    s3 = get_s3_client(region)
    attempt = 0
    while attempt < max_retries:
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=data)
            logger.info("Uploaded to s3://%s/%s", bucket, key)
            return
        except Exception as exc:
            attempt += 1
            logger.warning(
                "Attempt %d/%d failed to upload s3://%s/%s: %s",
                attempt, max_retries, bucket, key, exc
            )
            if attempt < max_retries:
                time.sleep(retry_sleep)
    logger.exception("Failed to upload s3://%s/%s after %d attempts", bucket, key, max_retries)
    # Raise so callers (pipeline) can decide to fail fast
    raise RuntimeError(f"Failed to upload s3://{bucket}/{key} after {max_retries} attempts")

def download_bytes(
    bucket: str,
    key: str,
    region: str = None
) -> bytes:
    """
    Tải object từ S3, trả về bytes.
    """
    s3 = get_s3_client(region)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()

def upload_dataframe_to_s3(
    df: pd.DataFrame,
    bucket: str,
    base_path: str,
    partition_col: str,
    required_columns: Optional[List[str]] = None,
    region: Optional[str] = None,
    filename_prefix: str = "part-",
    file_format: str = "parquet",
) -> List[str]:
    """
    Ghi DataFrame lên S3 theo partition và trả về danh sách key đã upload.
    Chỉ hỗ trợ Parquet (cần pyarrow hoặc fastparquet).
    """

    if df is None or df.empty:
        logger.warning("DataFrame empty, nothing to upload to S3")
        return []

    # --- hợp đồng schema ---
    if required_columns:
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    if partition_col not in df.columns:
        raise ValueError(f"Partition column '{partition_col}' not found")

    df = df.copy()

    # --- hợp đồng partition: không ép kiểu tại đây ---
    series = df[partition_col]
    if not is_datetime64_any_dtype(series):
        raise AssertionError(
            f"upload_dataframe_to_s3 expects '{partition_col}' datetime64[ns], got {series.dtype}"
        )

    # derive partition string without re-parsing/coercion
    df["_partition"] = series.dt.strftime("%Y-%m-%d")

    uploaded_keys: List[str] = []

    # --- ghi từng partition ---
    for partition_value, part_df in df.groupby("_partition"):
        key_prefix = f"{base_path}/{partition_col}={partition_value}"
        filename = f"{filename_prefix}{uuid.uuid4().hex}.parquet" if file_format == "parquet" else f"{filename_prefix}{uuid.uuid4().hex}"
        key = f"{key_prefix}/{filename}"
        buf = io.BytesIO()
        try:
            if file_format == "parquet":
                # requires pyarrow or fastparquet
                part_df.drop(columns=["_partition"], errors="ignore").to_parquet(buf, index=False)
            else:
                raise ValueError(f"Unsupported file_format: {file_format}")
            buf.seek(0)
            upload_bytes(buf.getvalue(), bucket=bucket, key=key, region=region)
            uploaded_keys.append(key)
        except Exception as exc:
            logger.exception("Ghi/upload partition=%s thất bại: %s", partition_value, exc)
            # re-raise so caller can handle failure deterministically
            raise

    return uploaded_keys

