import pandas as pd
from typing import Optional, List
from src.utils.logger import get_logger
from src.utils.s3_utils import upload_dataframe_to_s3

logger = get_logger(__name__)


def save_gold_stock_price_daily(
    df: pd.DataFrame,
    bucket: str,
    base_path: str = "gold/stock_price_daily",
    partition_col: str = "trading_date",
    region: Optional[str] = None,
) -> Optional[List[str]]:
    """
    Lưu bảng GOLD stock_price_daily lên S3 dạng Parquet phân vùng theo trading_date.
    Trả về danh sách key đã upload, None nếu không có dữ liệu.
    """
    if df is None or df.empty:
        logger.warning("GOLD DataFrame is empty. Nothing to save.")
        return None

    if partition_col not in df.columns:
        raise ValueError(f"Partition column '{partition_col}' not found in DataFrame")

    logger.info(
        "Đang lưu GOLD stock_price_daily lên s3://%s/%s (số dòng=%d)",
        bucket,
        base_path,
        len(df),
    )

    uploaded = upload_dataframe_to_s3(
        df=df,
        bucket=bucket,
        base_path=base_path,
        partition_col=partition_col,
        # validate core GOLD columns before upload
        required_columns=["symbol", "trading_date", "close_price", "volume"],
        region=region,
        filename_prefix="part-",
        file_format="parquet",
    )

    logger.info("Lưu GOLD stock_price_daily hoàn tất; các key đã ghi: %s", uploaded)
    return uploaded
