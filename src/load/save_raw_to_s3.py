import json
import pandas as pd
from datetime import date
from src.utils.s3_utils import upload_bytes
from src.utils.logger import get_logger

logger = get_logger(__name__)


def serialize_df_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    HỢP ĐỒNG RAW:
    - Chỉ format những cột thực sự là datetime64
    - Tuyệt đối không parse suy diễn từ object
    """
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
    return df


def save_raw_stock_price(
    df: pd.DataFrame,
    bucket: str,
    base_path: str,
    region: str,
    run_date: date
):
    if df.empty:
        logger.warning("No raw data to save")
        return []

    # ưu tiên các ứng viên dùng để partition
    date_col = next(
        (c for c in ("trading_date", "date", "time") if c in df.columns),
        None,
    )

    if date_col is None:
        # RAW save must have a date column per contract
        raise AssertionError("RAW save requires a date column (one of trading_date/date/time)")

    series = df[date_col]

    # --- Hợp đồng: bắt buộc cột ngày đã ở dạng datetime64[ns] ---
    if not pd.api.types.is_datetime64_any_dtype(series):
        raise AssertionError(
            f"RAW save expects '{date_col}' datetime64[ns], got {series.dtype}"
        )

    uploaded_keys = []

    df = df.copy()
    # dẫn xuất giá trị partition dạng python.date để ghi key ISO
    df["data_date"] = series.dt.date

    for data_date, part in df.groupby("data_date"):
        part = part.drop(columns=["data_date"])
        part = serialize_df_for_json(part)

        payload = part.to_dict(orient="records")
        key = f"{base_path}/data_date={data_date.isoformat()}/stock_price.json"

        upload_bytes(
            data=json.dumps(payload).encode("utf-8"),
            bucket=bucket,
            key=key,
            region=region
        )
        uploaded_keys.append(key)

    success_key = f"{base_path}/run_date={run_date.isoformat()}/_SUCCESS"
    upload_bytes(b"", bucket=bucket, key=success_key, region=region)
    uploaded_keys.append(success_key)

    logger.info("Saved RAW stock_price to S3 (%d objects)", len(uploaded_keys))
    return uploaded_keys
