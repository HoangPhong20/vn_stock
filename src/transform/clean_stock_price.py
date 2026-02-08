import pandas as pd
from datetime import datetime
from pandas.api.types import is_datetime64_any_dtype
from src.utils.logger import get_logger

logger = get_logger(__name__)


def clean_stock_price(
    df: pd.DataFrame,
    exchange: str = "HOSE",
    source: str = "vnstock",
) -> pd.DataFrame:

    if df is None or df.empty:
        logger.warning("Input DataFrame is empty")
        return pd.DataFrame()

    df = df.copy()

    # ------------------------------------------------------------------
    # 1. Xác định cột ngày giao dịch (quy tắc cứng)
    # ------------------------------------------------------------------
    DATE_CANDIDATES = ("time", "date")
    date_col = next((c for c in DATE_CANDIDATES if c in df.columns), None)
    if date_col is None:
        raise ValueError("Neither `time` nor `date` column found in raw data")

    df["trading_date"] = df[date_col]
    df = df.drop(columns=[c for c in DATE_CANDIDATES if c in df.columns])
    logger.info("Resolved trading_date from column: %s", date_col)

    # rename close
    if "close" in df.columns:
        df = df.rename(columns={"close": "close_price"})

    # ------------------------------------------------------------------
    # 2. Chuẩn hóa trading_date (an toàn)
    # ------------------------------------------------------------------
    if is_datetime64_any_dtype(df["trading_date"]):
        df["trading_date"] = df["trading_date"].dt.normalize()
    else:
        parsed = pd.to_datetime(df["trading_date"], errors="coerce")

        if parsed.isna().any():
            bad = (
                df.loc[parsed.isna(), "trading_date"]
                .astype(str)
                .unique()[:5]
            )
            logger.error("Invalid trading_date values: %s", bad)
            raise ValueError("Unparseable trading_date values")

        df["trading_date"] = parsed.dt.normalize()

    # ------------------------------------------------------------------
    # 3. Ép kiểu số
    # ------------------------------------------------------------------
    for col in ["open", "high", "low", "close_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0)
    df.loc[df["volume"] < 0, "volume"] = 0

    # ------------------------------------------------------------------
    # 4. Ghi metadata
    # ------------------------------------------------------------------
    df["exchange"] = exchange
    df["source"] = source
    df["ingestion_time"] = datetime.utcnow()

    # ------------------------------------------------------------------
    # 5. Loại dòng lỗi
    # ------------------------------------------------------------------
    df = df.dropna(subset=["symbol", "trading_date", "close_price"])
    df = df.reset_index(drop=True)

    logger.info(
        "Cleaned SILVER stock_price: %d rows, %d symbols",
        len(df),
        df["symbol"].nunique(),
    )

    return df
