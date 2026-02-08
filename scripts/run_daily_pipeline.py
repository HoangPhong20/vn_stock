import yaml
from pathlib import Path
import sys
repo_root = Path(__file__).resolve().parents[1]
from datetime import date
from typing import Optional
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from src.utils.logger import get_logger
from src.extract.fetch_stock_price import fetch_hose_stock_prices
from src.transform.clean_stock_price import clean_stock_price
from src.transform.build_stock_price_daily import build_stock_price_daily
from src.transform.validate_gold_schema import validate_stock_price_daily
from src.transform.validate_silver_schema import validate_dataframe_schema
from src.load.save_raw_to_s3 import save_raw_stock_price
from src.load.save_gold_to_s3 import save_gold_stock_price_daily
from src.load.save_silver_to_s3 import save_silver_stock_price

logger = get_logger("run_daily_pipeline")

def load_config(path: str = "config/config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_daily_pipeline(run_date: Optional[date] = None) -> None:
    """
    Run daily stock price pipeline (RAW -> SILVER -> GOLD)
    """
    cfg = load_config()
    run_date = run_date or date.today()

    # --- phạm vi lấy dữ liệu từ API (hint) vs phạm vi nghiệp vụ ---
    source_range = cfg["source"]["date_range"]
    dataset_range = cfg.get("dataset", {}).get("stock_price", {}).get("date_range", source_range)

    logger.info(
        "=== START DAILY PIPELINE | run_date=%s | source_range=%s → %s | business_range=%s → %s ===",
        run_date,
        source_range.get("start"),
        source_range.get("end"),
        dataset_range.get("start"),
        dataset_range.get("end"),
    )

    # --- TRÍCH XUẤT ---
    try:
        raw_df = fetch_hose_stock_prices(
            start_date=source_range.get("start"),
            end_date=source_range.get("end"),
            run_date=run_date,
        )
    except Exception:
        logger.exception("Failed to fetch raw data")
        sys.exit(1)

    if raw_df is None or raw_df.empty:
        logger.warning("No rows fetched from source. Exiting pipeline.")
        return

    # --- LƯU RAW (bronze) ---
    try:
        raw_result = save_raw_stock_price(
            df=raw_df,
            bucket=cfg["storage"]["bucket"],
            base_path=cfg["paths"]["bronze"],
            region=cfg["storage"]["region"],
            run_date=run_date,
        )
        logger.info("Raw upload result: %s", raw_result)
    except Exception:
        logger.exception("Failed to save raw data")
        sys.exit(1)

    # --- CHUẨN HÓA -> SILVER ---
    try:
        silver_df = clean_stock_price(raw_df)
    except Exception:
        logger.exception("Transformation (clean_stock_price) failed")
        sys.exit(1)

    # --- Enforce data_date range (keep only trading_date within configured business dataset range) ---
    try:
        data_start = dataset_range.get("start")
        data_end = dataset_range.get("end")

        # parse business range as pandas timestamps and normalize to midnight
        data_start_date = pd.to_datetime(data_start).normalize()
        data_end_date = pd.to_datetime(data_end).normalize()

        if (
            silver_df is not None
            and not silver_df.empty
            and "trading_date" in silver_df.columns
        ):
            # CONTRACT: silver_df.trading_date MUST already be datetime64[ns]
            if not is_datetime64_any_dtype(silver_df["trading_date"]):
                logger.error(
                    "Silver.trading_date dtype is %s; clean_stock_price must produce datetime64[ns]",
                    silver_df["trading_date"].dtype,
                )
                raise AssertionError("Silver.trading_date must be datetime64[ns] (do not coerce in pipeline)")

            # NOTE: business date_range is recorded but NOT applied here.
            logger.info(
                "Business date_range noted (not applied in pipeline): %s -> %s",
                data_start_date.date(),
                data_end_date.date(),
            )
    except Exception:
        logger.exception("Failed to process SILVER business_range check")
        sys.exit(1)

    # --- KIỂM TRA SILVER ---
    try:
        # compute schema path relative to repo root
        # use top-level repo_root defined above (removed redundant reassignment)
        silver_schema = repo_root / "schemas" / "stock_price_silver.yaml"
        if not silver_schema.exists():
            logger.error("Silver schema file not found: %s", silver_schema)
            raise FileNotFoundError(f"Schema not found: {silver_schema}")
        # pass strict=True to enforce schema by default
        validate_dataframe_schema(silver_df, str(silver_schema), strict=True)
    except Exception:
        logger.exception("SILVER validation failed")
        sys.exit(1)

    # --- LƯU SILVER ---
    try:
        silver_result = save_silver_stock_price(
            df=silver_df,
            bucket=cfg["storage"]["bucket"],
            base_path=cfg["paths"]["silver"],
            region=cfg["storage"]["region"],
            run_date=run_date,
        )
        logger.info("Silver upload result: %s", silver_result)
    except Exception:
        logger.exception("Failed to save SILVER data")
        sys.exit(1)

    # --- XÂY GOLD ---
    try:
        gold_df = build_stock_price_daily(silver_df)
    except Exception:
        logger.exception("Failed to build GOLD table")
        sys.exit(1)

    if gold_df is None or gold_df.empty:
        logger.info("No GOLD rows to save. Pipeline finished.")
        return

    # --- KIỂM TRA GOLD ---
    try:
        validate_stock_price_daily(gold_df)
    except Exception:
        logger.exception("GOLD validation failed")
        sys.exit(1)

    # --- LƯU GOLD ---
    try:
        gold_result = save_gold_stock_price_daily(
            df=gold_df,
            bucket=cfg["storage"]["bucket"],
            base_path=cfg["paths"]["gold"],
            region=cfg["storage"]["region"],
        )
        logger.info("Gold upload result: %s", gold_result)
    except Exception:
        logger.exception("Failed to save GOLD data")
        sys.exit(1)

    logger.info("=== DAILY PIPELINE SUCCESS ===")


if __name__ == "__main__":
    run_daily_pipeline()
