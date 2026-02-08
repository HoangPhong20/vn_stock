import time
import random
import re
from datetime import date
from typing import List, Optional

import pandas as pd
from vnstock import Vnstock
from tenacity import RetryError

from src.utils.logger import get_logger

logger = get_logger(__name__)

SLEEP_PER_SYMBOL = 1.1
MAX_RETRY = 3
RETRY_SLEEP = 60
API_SOURCE = "vnstock:VCI"


# -------------------------------------------------------------------
# Các hàm trợ giúp
# -------------------------------------------------------------------

def is_valid_stock_symbol(symbol: str) -> bool:
    if not symbol:
        return False
    normalized = symbol.strip().upper()
    return bool(re.fullmatch(r"[A-Z0-9]{3}", normalized))


def _normalize_date_str(d: Optional[str]) -> Optional[str]:
    if d is None:
        return None
    try:
        return pd.to_datetime(d, format="%Y-%m-%d").date().isoformat()
    except Exception as exc:
        raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {d}") from exc


def fetch_hose_symbols() -> List[str]:
    vn = Vnstock().stock(symbol="VNM", source="KBS")
    symbols_df = vn.listing.symbols_by_exchange()
    hose_df = symbols_df[symbols_df["exchange"] == "HOSE"]
    symbols = hose_df["symbol"].dropna().unique().tolist()
    logger.info("Fetched %d HOSE symbols", len(symbols))
    return symbols


# -------------------------------------------------------------------
# BRONZE / RAW – THỰC THI RÀO CHẮN NGÀY TẠI ĐÂY
# -------------------------------------------------------------------

def fetch_hose_stock_prices(
    start_date: str,
    end_date: str,
    run_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    RÀNG BUỘC BRONZE:
    - Bắt buộc lọc cứng dữ liệu trong [start_date, end_date]
    - trading_date phải là datetime64[ns], đã normalize
    - Downstream không được lọc lại bằng ngày
    """

    start_date = _normalize_date_str(start_date)
    end_date = _normalize_date_str(end_date)

    start_ts = pd.to_datetime(start_date).normalize()
    end_ts = pd.to_datetime(end_date).normalize()

    symbols = fetch_hose_symbols()
    if not symbols:
        return pd.DataFrame()

    vn = Vnstock().stock(symbol="VNM", source="VCI")
    results: list[pd.DataFrame] = []

    for i, symbol in enumerate(symbols, start=1):
        if not is_valid_stock_symbol(symbol):
            logger.info("Skip non-stock symbol: %s", symbol)
            continue

        retry = 0
        while retry < MAX_RETRY:
            try:
                logger.info(
                    "(%d/%d) Fetching %s [%s → %s]",
                    i,
                    len(symbols),
                    symbol,
                    start_date,
                    end_date,
                )
                df = vn.quote.history(
                    symbol=symbol,
                    start=start_date,
                    end=end_date,
                )

                if df is not None and not df.empty:
                    df = df.copy()
                    df["symbol"] = symbol
                    results.append(df)
                break

            except RetryError:
                logger.warning("Retry exhausted for %s, skip", symbol)
                break

            except Exception as e:
                msg = str(e).lower()
                if "rate limit" in msg or "429" in msg:
                    retry += 1
                    sleep_sec = RETRY_SLEEP + random.uniform(0, 5)
                    logger.warning(
                        "Rate limited %s — sleep %.1fs (%d/%d)",
                        symbol, sleep_sec, retry, MAX_RETRY
                    )
                    time.sleep(sleep_sec)
                else:
                    logger.exception("Failed symbol %s", symbol)
                    break

        time.sleep(SLEEP_PER_SYMBOL)

    if not results:
        return pd.DataFrame()

    df = pd.concat(results, ignore_index=True)

    # ----------------------------------------------------------------
    # PARSE NGÀY NGHIÊM NGẶT
    # ----------------------------------------------------------------
    if "time" not in df.columns:
        raise KeyError("Expected column `time` from vnstock API")

    df["trading_date"] = (
        pd.to_datetime(df["time"], errors="coerce")
        .dt.normalize()
    )

    before = len(df)
    df = df.dropna(subset=["trading_date"])
    dropped = before - len(df)

    if dropped > 0:
        logger.warning("Dropped %d rows with invalid trading_date", dropped)

    # ----------------------------------------------------------------
    # LỌC CỨNG – LUẬT BẮT BUỘC
    # ----------------------------------------------------------------
    before_filter = len(df)

    df = df[
        (df["trading_date"] >= start_ts)
        & (df["trading_date"] <= end_ts)
    ].reset_index(drop=True)

    logger.info(
        "BRONZE date filter %s → %s: kept %d / %d rows",
        start_date,
        end_date,
        len(df),
        before_filter,
    )

    # ----------------------------------------------------------------
    # KIỂM TRA HỢP ĐỒNG CUỐI CÙNG
    # ----------------------------------------------------------------
    if not df.empty:
        min_d = df["trading_date"].min()
        max_d = df["trading_date"].max()
        if min_d < start_ts or max_d > end_ts:
            raise AssertionError(
                f"Bronze date contract violated: {min_d} → {max_d}"
            )

    # ----------------------------------------------------------------
    # METADATA PHỤC VỤ AUDIT
    # ----------------------------------------------------------------
    df["api_source"] = API_SOURCE
    df["api_start_date"] = start_date
    df["api_end_date"] = end_date
    df["api_run_date"] = (
        run_date.isoformat() if run_date else pd.NA
    )

    return df
