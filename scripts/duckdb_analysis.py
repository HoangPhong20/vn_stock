import duckdb
import os
from dotenv import load_dotenv
import logging

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

con = duckdb.connect("duckdb/vnstock.duckdb")
con.execute("LOAD httpfs;")

aws_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

if not aws_key or not aws_secret:
    raise RuntimeError("AWS credentials not loaded from .env")

con.execute(f"SET s3_access_key_id='{aws_key}';")
con.execute(f"SET s3_secret_access_key='{aws_secret}';")
con.execute("SET s3_region='ap-southeast-1';")

def create_gold_views(con: duckdb.DuckDBPyConnection) -> None:
    # Tạo view GOLD từ parquet
    con.execute("""
    CREATE OR REPLACE VIEW v_gold_stock_price_daily AS
    SELECT
        trading_date,
        symbol,
        exchange,
        close_price,
        volume,
        daily_return,
        ma_5,
        ma_20
    FROM read_parquet(
      's3://vnstock-project/gold/stock_price_daily/trading_date=*/part-*.parquet'
    )
    WHERE exchange IN ('HOSE', 'HNX')
    """)

def create_dimensions(con: duckdb.DuckDBPyConnection) -> None:
    # DIM_DATE: chuẩn DW cho scope dự án, bổ sung is_trading_day
    con.execute("""
    CREATE OR REPLACE TABLE dim_date AS
    SELECT *
    FROM (
        SELECT
            d::DATE AS date_key,
            EXTRACT(year FROM d) AS year,
            EXTRACT(month FROM d) AS month,
            EXTRACT(day FROM d) AS day,
            EXTRACT(quarter FROM d) AS quarter,
            EXTRACT(week FROM d) AS week_of_year,
            STRFTIME(d, '%w') AS day_of_week,
            STRFTIME(d, '%w') IN ('0','6') AS is_weekend,
            STRFTIME(d, '%w') NOT IN ('0','6') AS is_trading_day
        FROM generate_series(
            DATE '2015-01-01',
            DATE '2035-12-31',
            INTERVAL 1 DAY
        ) t(d)
    )
    """)
    # DIM_SYMBOL: dimension nghiệp vụ (surrogate key ổn định hơn)
    con.execute("""
    CREATE OR REPLACE TABLE dim_symbol AS
    SELECT
        DENSE_RANK() OVER (ORDER BY symbol, exchange) AS symbol_key,
        symbol,
        exchange
    FROM (
        SELECT DISTINCT symbol, exchange
        FROM v_gold_stock_price_daily
    )
    """)
    # Ghi chú PK (logic, không enforced)
    # symbol_key là khóa chính của dim_symbol

def create_facts(con: duckdb.DuckDBPyConnection) -> None:
    # FACT: chỉ chứa measure + FK
    # -- Grain: 1 row per symbol_key per trading day
    con.execute("""
    CREATE OR REPLACE TABLE fact_stock_price AS
    SELECT
        g.trading_date AS date_key,
        s.symbol_key,
        g.close_price,
        g.volume,
        g.daily_return,
        g.ma_5,
        g.ma_20
    FROM v_gold_stock_price_daily g
    JOIN dim_symbol s
        ON g.symbol = s.symbol
       AND g.exchange = s.exchange
    """)

def create_monthly_facts(con: duckdb.DuckDBPyConnection) -> None:
    # -- Grain: 1 row per symbol_key per month
    con.execute("""
    CREATE OR REPLACE TABLE fact_stock_price_monthly AS
    SELECT
        f.symbol_key,
        d.year,
        d.month,
        (d.year * 100 + d.month) AS month_key,
        ROUND(AVG(f.close_price), 2)        AS avg_close_price,
        ROUND(AVG(f.daily_return), 4)       AS avg_daily_return,
        ROUND(AVG(f.ma_5), 2)               AS avg_ma_5,
        ROUND(AVG(f.ma_20), 2)              AS avg_ma_20,
        SUM(f.volume)                       AS total_volume,
        COUNT(*)                            AS trading_days
    FROM fact_stock_price f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY
        f.symbol_key,
        d.year,
        d.month
    """)

def create_yearly_facts(con: duckdb.DuckDBPyConnection) -> None:
    # -- Grain: 1 row per symbol_key per year
    con.execute("""
    CREATE OR REPLACE TABLE fact_stock_price_yearly AS
    SELECT
        symbol_key,
        year,
        -- Weighted average theo số ngày giao dịch, làm tròn 2 số thập phân
        ROUND(
            SUM(avg_close_price * trading_days) / NULLIF(SUM(trading_days), 0),
            2
        ) AS avg_close_price,
        ROUND(AVG(avg_daily_return), 4)     AS avg_daily_return,
        ROUND(AVG(avg_ma_5), 2)             AS avg_ma_5,
        ROUND(AVG(avg_ma_20), 2)            AS avg_ma_20,
        SUM(total_volume)                   AS total_volume,
        SUM(trading_days)                   AS trading_days
    FROM fact_stock_price_monthly
    GROUP BY
        symbol_key,
        year
    """)

def sanity_checks(con: duckdb.DuckDBPyConnection) -> None:
    # Sanity checks cơ bản
    print("fact_stock_price rows:", con.execute("SELECT COUNT(*) FROM fact_stock_price").fetchone()[0])
    print("dim_date rows:", con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0])
    print("dim_symbol rows:", con.execute("SELECT COUNT(*) FROM dim_symbol").fetchone()[0])

    monthly_rows = con.execute("""
    SELECT COUNT(*) FROM fact_stock_price_monthly
    """).fetchone()[0]
    print("fact_stock_price_monthly rows:", monthly_rows)

    dup_monthly = con.execute("""
    SELECT COUNT(*)
    FROM (
        SELECT symbol_key, year, month, COUNT(*) AS cnt
        FROM fact_stock_price_monthly
        GROUP BY symbol_key, year, month
        HAVING COUNT(*) > 1
    )
    """).fetchone()[0]
    print("fact_stock_price_monthly duplicate grain:", dup_monthly)

    yearly_rows = con.execute("""
    SELECT COUNT(*) FROM fact_stock_price_yearly
    """).fetchone()[0]
    print("fact_stock_price_yearly rows:", yearly_rows)

    dup_yearly = con.execute("""
    SELECT COUNT(*)
    FROM (
        SELECT symbol_key, year, COUNT(*) AS cnt
        FROM fact_stock_price_yearly
        GROUP BY symbol_key, year
        HAVING COUNT(*) > 1
    )
    """).fetchone()[0]
    print("fact_stock_price_yearly duplicate grain:", dup_yearly)

    # Kiểm tra NULL key (chỉ kiểm tra FK, không kiểm tra business attribute)
    null_keys = con.execute("""
    SELECT COUNT(*)
    FROM fact_stock_price
    WHERE date_key IS NULL OR symbol_key IS NULL
    """).fetchone()[0]
    print("fact_stock_price null keys:", null_keys)

    # Kiểm tra orphan keys (date_key)
    orphan_dates = con.execute("""
    SELECT COUNT(*)
    FROM fact_stock_price f
    LEFT JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.date_key IS NULL
    """).fetchone()[0]
    print("fact_stock_price orphan date_key:", orphan_dates)

    # Kiểm tra orphan keys (symbol_key)
    orphan_symbols = con.execute("""
    SELECT COUNT(*)
    FROM fact_stock_price f
    LEFT JOIN dim_symbol s ON f.symbol_key = s.symbol_key
    WHERE s.symbol_key IS NULL
    """).fetchone()[0]
    print("fact_stock_price orphan symbol_key:", orphan_symbols)

    # Kiểm tra trading_days > 0 ở monthly
    zero_trading_days = con.execute("""
    SELECT COUNT(*)
    FROM fact_stock_price_monthly
    WHERE trading_days <= 0
    """).fetchone()[0]
    print("fact_stock_price_monthly trading_days <= 0:", zero_trading_days)

def demo_bi_queries(con: duckdb.DuckDBPyConnection) -> None:
    # Demo BI: tổng volume theo exchange & năm
    top_volume = con.execute("""
    SELECT
        d.year,
        s.exchange,
        SUM(f.volume) AS total_volume
    FROM fact_stock_price f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_symbol s ON f.symbol_key = s.symbol_key
    GROUP BY d.year, s.exchange
    ORDER BY total_volume DESC
    """).fetchall()
    print("Top volume by exchange/year:", top_volume[:5])

    # Demo BI: top 10 symbol theo avg_close 30 trading days gần nhất (chuẩn DW, làm tròn 2 số thập phân)
    top_symbols = con.execute("""
    WITH last_30_dates AS (
        SELECT date_key
        FROM dim_date
        WHERE date_key <= (
            SELECT MAX(date_key)
            FROM fact_stock_price
        )
        ORDER BY date_key DESC
        LIMIT 30
    )
    SELECT
        s.exchange,
        s.symbol,
        ROUND(AVG(f.close_price), 2) AS avg_close_30d
    FROM fact_stock_price f
    JOIN last_30_dates d
        ON f.date_key = d.date_key
    JOIN dim_symbol s ON f.symbol_key = s.symbol_key
    GROUP BY s.exchange, s.symbol
    ORDER BY avg_close_30d DESC
    LIMIT 10
    """).fetchall()
    print("Top symbols (30 trading days avg_close):", top_symbols)

def main() -> None:
    # 1. GOLD
    create_gold_views(con)

    # 2. DIMENSIONS
    create_dimensions(con)

    # 3. FACTS (base)
    create_facts(con)

    # 4. FACTS (aggregate monthly)
    create_monthly_facts(con)

    # 5. FACTS (aggregate yearly)
    create_yearly_facts(con)

    # 6. CHECKS
    sanity_checks(con)

    # 7. DEMO BI
    demo_bi_queries(con)

if __name__ == "__main__":
    logger.info("DuckDB ready 🚀")
    main()
