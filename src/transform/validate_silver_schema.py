import pandas as pd
import yaml
from src.utils.logger import get_logger
from pandas.api import types as pdt

logger = get_logger(__name__)

def _handle(msg: str, strict: bool):
    if strict:
        raise ValueError(msg)
    else:
        logger.warning(msg)

def _is_integer_valued(series: pd.Series) -> bool:
    # allow integer-valued floats or numeric strings that parse to integers
    s = series.dropna()
    if s.empty:
        return True
    if pdt.is_integer_dtype(s):
        return True
    if pdt.is_float_dtype(s):
        return (s % 1 == 0).all()
    coerced = pd.to_numeric(s, errors="coerce")
    if coerced.isna().any():
        return False
    return (coerced % 1 == 0).all()

def validate_dataframe_schema(
    df: pd.DataFrame,
    schema_path: str,
    strict: bool = True
) -> None:
    """
    Kiểm tra DataFrame theo schema Silver (YAML).
    strict=True -> raise khi vi phạm, strict=False -> cảnh báo.
    """

    if df is None or df.empty:
        _handle("DataFrame is empty", strict)
        return

    with open(schema_path, "r", encoding="utf-8") as f:
        schema = yaml.safe_load(f)

    columns_schema = schema.get("columns", {})

    # =====================================================
    # 1. Kiểm tra thiếu cột
    # =====================================================
    expected_cols = set(columns_schema.keys())
    actual_cols = set(df.columns)

    missing_cols = expected_cols - actual_cols
    if missing_cols:
        _handle(f"Missing columns: {missing_cols}", strict)

    # =====================================================
    # 1.b Đảm bảo tồn tại cột partition
    # =====================================================
    partition_cols = schema.get("partition_by", [])
    for pcol in partition_cols:
        if pcol not in df.columns:
            _handle(f"Missing partition column from schema: {pcol}", strict)

    # =====================================================
    # 2. Kiểu dữ liệu & nullable
    # =====================================================
    for col, rules in columns_schema.items():
        if col not in df.columns:
            # already reported above; skip further checks for missing col
            continue

        meta_type = rules.get("type")
        nullable = rules.get("nullable", True)
        series = df[col]

        # --- nullable ---
        if not nullable and series.isnull().any():
            _handle(f"Column {col} contains NULL but nullable=false", strict)

        # --- type checks: tolerant ---
        if meta_type == "string":
            if not (pdt.is_object_dtype(series) or pdt.is_string_dtype(series)):
                _handle(f"{col} expected string-like type", strict)

        elif meta_type == "float":
            if not (pdt.is_float_dtype(series) or pdt.is_integer_dtype(series)):
                # allow integers where floats expected
                _handle(f"{col} expected float-like type", strict)

        elif meta_type in ("int", "bigint"):
            if not _is_integer_valued(series):
                _handle(f"{col} expected integer-valued data (allowing integer-valued floats/strings)", strict)

        elif meta_type == "date":
            if not pdt.is_datetime64_any_dtype(series):
                _handle(f"{col} must be datetime64[ns] (date), got {series.dtype}", strict)
            else:
                normalized = series.dt.normalize()
                if not normalized.equals(series):
                    _handle(f"{col} must be normalized to midnight (date semantics)", strict)

        elif meta_type == "timestamp":
            if not pdt.is_datetime64_any_dtype(series):
                _handle(f"{col} must be datetime64[ns] (timestamp), got {series.dtype}", strict)

        else:
            # unknown type in schema — warn/raise
            _handle(f"Unknown schema type for column {col}: {meta_type}", strict)

    # =====================================================
    # 3. Duy nhất theo khóa chính
    # =====================================================
    pk_cols = schema.get("primary_key", [])
    if pk_cols:
        for pk_col in pk_cols:
            if pk_col in df.columns and df[pk_col].isnull().any():
                _handle(f"Primary key column {pk_col} contains NULL", strict)

        if df.duplicated(subset=pk_cols).any():
            _handle(f"Duplicate primary key found: {pk_cols}", strict)

    # =====================================================
    # 4. Kiểm tra chất lượng
    # =====================================================
    quality = schema.get("quality_checks", {})

    # --- not null ---
    for col in quality.get("not_null", []):
        if col in df.columns and df[col].isnull().any():
            _handle(f"Quality check failed: {col} contains NULL", strict)

    # --- positive values ---
    for col in quality.get("positive_values", []):
        if col in df.columns and (df[col].dropna() < 0).any():
            _handle(f"Quality check failed: {col} contains negative values", strict)

    logger.info("Kiểm tra schema & quality PASS (schema=%s, strict=%s)", schema.get("table"), strict)
