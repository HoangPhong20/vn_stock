from datetime import date
import pandas as pd
from typing import List
from src.utils.logger import get_logger
from src.utils.s3_utils import upload_dataframe_to_s3, upload_bytes

logger = get_logger(__name__)


def save_silver_stock_price(
    df: pd.DataFrame,
    bucket: str,
    base_path: str,
    region: str,
    run_date: date
) -> List[str] | None:
    """
    Lưu bảng SILVER lên S3 dạng Parquet phân vùng theo trading_date.
    Sau khi upload thành công, tạo marker run_date=_SUCCESS để audit.
    Trả về danh sách key đã ghi (bao gồm marker).
    """
    if df is None or df.empty:
        logger.warning("No silver data to save")
        return None

    # Ensure partition column exists; prefer trading_date, allow common alternatives
    partition_col = None
    for candidate in ("trading_date", "date", "time"):
        if candidate in df.columns:
            partition_col = candidate
            break

    if partition_col is None:
        raise ValueError("Missing partition column: one of trading_date/date/time must be present")

    # Normalize partition column name to 'trading_date' (do NOT coerce types here)
    if partition_col != "trading_date":
        df = df.rename(columns={partition_col: "trading_date"})

    # CONTRACT: clean_stock_price is responsible for parsing trading_date -> datetime64[ns]
    # The save layer must NOT coerce/repair types. Enforce the contract and normalize time-of-day.
    if not pd.api.types.is_datetime64_any_dtype(df["trading_date"]):
        raise ValueError(
            "Partition column 'trading_date' must be pandas datetime64[ns]. "
            "Ensure upstream clean_stock_price produced and normalized trading_date."
        )
    # normalize to midnight (keeps dtype)
    df["trading_date"] = df["trading_date"].dt.normalize()

    # base_path is expected to already point at the silver prefix (e.g. "silver/stock_price")
    try:
        uploaded = upload_dataframe_to_s3(
            df=df,
            bucket=bucket,
            base_path=base_path,
            partition_col="trading_date",
            file_format="parquet",
            region=region,
            required_columns=["trading_date", "symbol"]  # lightweight sanity check
        )

        # write run_date success marker
        success_key = f"{base_path}/run_date={run_date.isoformat()}/_SUCCESS"
        upload_bytes(b"", bucket=bucket, key=success_key, region=region)

        # combine returned partition keys + success marker
        result_keys = (uploaded or []) + [success_key]
        logger.info("Saved silver stock price to s3: %s (success=%s)", base_path, success_key)
        return result_keys
    except Exception as exc:
        logger.exception("Failed to save silver stock price to s3: %s", exc)
        raise
