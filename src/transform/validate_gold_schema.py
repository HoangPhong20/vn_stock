from pathlib import Path
import pandas as pd
from src.utils.logger import get_logger
from src.transform.validate_silver_schema import validate_dataframe_schema

logger = get_logger(__name__)

def validate_stock_price_daily(
    df: pd.DataFrame,
    schema_path: str = "schemas/stock_price_gold.yaml",
    strict: bool = True,
) -> None:
    """
    Trình bao mỏng để kiểm tra GOLD theo schema (không mutate DataFrame).
    strict=True => raise khi vi phạm; strict=False => chỉ cảnh báo.
    """
    if df is None or df.empty:
        raise ValueError("GOLD DataFrame is empty")

    # xác định đường dẫn schema: ưu tiên theo tham số, fallback repo root
    schema_file = Path(schema_path)
    if not schema_file.exists():
        repo_root = Path(__file__).resolve().parents[2]  # repo root (parents: transform->src->repo)
        schema_file = repo_root / schema_path

    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    # delegate to generic validator (does not mutate df)
    validate_dataframe_schema(df, str(schema_file), strict=strict)

    logger.info("Kiểm tra schema GOLD thành công (%d dòng)", len(df))
