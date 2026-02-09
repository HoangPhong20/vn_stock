import pandas as pd
import numpy as np
from pandas.api.types import is_datetime64_any_dtype
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_stock_price_daily(silver_df: pd.DataFrame) -> pd.DataFrame:
    """
    Dựng bảng GOLD stock_price_daily từ dữ liệu SILVER.
    """

    if silver_df is None or silver_df.empty:
        logger.warning("Silver DataFrame is empty. Skip building GOLD table.")
        return pd.DataFrame()

    df = silver_df.copy()

    # --- 0. kiểm tra cột bắt buộc ---
    required_cols = {"symbol", "exchange", "trading_date", "close_price", "volume"}
    missing = required_cols - set(df.columns)
    if missing:
        logger.warning("Missing required columns for GOLD build: %s. Skip.", missing)
        return pd.DataFrame()

    # --- đảm bảo hợp đồng trading_date ---
    if not is_datetime64_any_dtype(df["trading_date"]):
        logger.error("silver_df.trading_date dtype is %s; expected datetime64[ns]", df["trading_date"].dtype)
        raise ValueError("Silver.trading_date must be datetime64[ns]")

    # --- 1. ép kiểu số an toàn ---
    # handle string/objects that represent numbers
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # --- loại bỏ dòng thiếu close_price hoặc volume ---
    before_drop = len(df)
    df = df.dropna(subset=["close_price", "volume"]).reset_index(drop=True)
    dropped = before_drop - len(df)
    if dropped > 0:
        logger.warning("Dropped %d rows missing close_price or volume", dropped)

    # --- sắp xếp ổn định trước khi tạo feature ---
    df = df.sort_values(
        by=["symbol", "exchange", "trading_date"],
        kind="mergesort"
    )

    # --- loại trùng khóa (symbol, exchange, trading_date) ---
    before_dups = len(df)
    df = df.drop_duplicates(subset=["symbol", "exchange", "trading_date"], keep="last")
    removed_dups = before_dups - len(df)
    if removed_dups > 0:
        logger.warning("Dropped %d duplicate (symbol, exchange, trading_date) rows", removed_dups)

    # --- ma trận feature: prev_close + daily_return ---
    df["prev_close"] = df.groupby(["symbol", "exchange"])["close_price"].shift(1)
    valid_prev = df["prev_close"].notna() & (df["prev_close"] != 0)
    df["daily_return"] = np.where(valid_prev, (df["close_price"] - df["prev_close"]) / df["prev_close"], np.nan)

    # --- MA 5 ngày ---
    df["ma_5"] = (
        df.groupby(["symbol", "exchange"])["close_price"]
        .rolling(window=5, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    )

    # --- MA 20 ngày ---
    df["ma_20"] = (
        df.groupby(["symbol", "exchange"])["close_price"]
        .rolling(window=20, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    )

    # --- chọn cột GOLD ---
    gold_df = df[
        [
            "symbol",
            "exchange",
            "trading_date",
            "close_price",
            "volume",
            "daily_return",
            "ma_5",
            "ma_20",
        ]
    ].copy()

    gold_df["symbol"] = gold_df["symbol"].astype(str)
    gold_df["exchange"] = gold_df["exchange"].astype(str)

    # --- đảm bảo dtype cuối ---
    # single astype excluding symbol
    gold_df = gold_df.astype({
        "close_price": "float64",
        "daily_return": "float64",
        "ma_5": "float64",
        "ma_20": "float64",
        "volume": "Int64",
    })

    # --- sắp xếp cuối cùng ---
    # Final sort (important for downstream BI / parquet consistency)
    gold_df = gold_df.sort_values(by=["symbol", "exchange", "trading_date"])

    gold_df.reset_index(drop=True, inplace=True)


    logger.info(
        "Built GOLD stock_price_daily: %d rows, %d symbols",
        len(gold_df),
        gold_df["symbol"].nunique()
    )

    return gold_df
