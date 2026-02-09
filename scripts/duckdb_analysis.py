import duckdb
import os
from dotenv import load_dotenv

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
    SELECT trading_date,
    symbol,
    open,
    high,
    low,
    close_price,
    volume,
    exchange
    FROM read_parquet(
      's3://vnstock-project/gold/stock_price_daily/trading_date=*/part-*.parquet'
    )
    WHERE exchange IN ('HOSE', 'HNX')
    """)

def create_dimensions(con: duckdb.DuckDBPyConnection) -> None:
    # DIM_DATE: chuẩn DW cho scope dự án
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
            STRFTIME(d, '%w') IN ('0','6') AS is_weekend
        FROM generate_series(
            DATE '2015-01-01',
            DATE '2035-12-31',
            INTERVAL 1 DAY
        ) t(d)
    )
    """)
    # DIM_SYMBOL: dimension nghiệp vụ (surrogate key)
    con.execute("""
    CREATE OR REPLACE TABLE dim_symbol AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY symbol, exchange) AS symbol_key,
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
    con.execute("""
    CREATE OR REPLACE TABLE fact_stock_price AS
    SELECT
        g.trading_date AS date_key,
        s.symbol_key,
        g.exchange,
        g.open,
        g.high,
        g.low,
        g.close_price,
        g.volume
    FROM v_gold_stock_price_daily g
    JOIN dim_symbol s
        ON g.symbol = s.symbol
       AND g.exchange = s.exchange
    """)

def create_monthly_facts(con: duckdb.DuckDBPyConnection) -> None:
    # Grain: 1 row per symbol_key per month
    con.execute("""
    CREATE OR REPLACE TABLE fact_stock_price_monthly AS
    SELECT
        f.symbol_key,
        f.exchange,
        d.year,
        d.month,
        AVG(f.close_price)        AS avg_close_price,
        MAX(f.high)               AS max_high,
        MIN(f.low)                AS min_low,
        SUM(f.volume)             AS total_volume,
        COUNT(*)                  AS trading_days
    FROM fact_stock_price f
    JOIN dim_date d
        ON f.date_key = d.date_key
    GROUP BY
        f.symbol_key,
        f.exchange,
        d.year,
        d.month
    """)

def create_yearly_facts(con: duckdb.DuckDBPyConnection) -> None:
    # Grain: 1 row per symbol_key per year
    con.execute("""
    CREATE OR REPLACE TABLE fact_stock_price_yearly AS
    SELECT
        symbol_key,
        exchange,
        year,
        SUM(avg_close_price * trading_days)
            / SUM(trading_days)            AS avg_close_price,
        MAX(max_high)                      AS max_high,
        MIN(min_low)                       AS min_low,
        SUM(total_volume)                  AS total_volume,
        SUM(trading_days)                  AS trading_days
    FROM fact_stock_price_monthly
    GROUP BY
        symbol_key,
        exchange,
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
        SELECT symbol_key, exchange, year, month, COUNT(*) AS cnt
        FROM fact_stock_price_monthly
        GROUP BY symbol_key, exchange, year, month
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
        SELECT symbol_key, exchange, year, COUNT(*) AS cnt
        FROM fact_stock_price_yearly
        GROUP BY symbol_key, exchange, year
        HAVING COUNT(*) > 1
    )
    """).fetchone()[0]
    print("fact_stock_price_yearly duplicate grain:", dup_yearly)

    # Kiểm tra NULL key
    null_keys = con.execute("""
    SELECT COUNT(*)
    FROM fact_stock_price
    WHERE date_key IS NULL OR symbol_key IS NULL OR exchange IS NULL
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

    # Demo BI: top 10 symbol theo avg_close 30 ngày gần nhất
    top_symbols = con.execute("""
    SELECT
        s.symbol,
        AVG(f.close_price) AS avg_close
    FROM fact_stock_price f
    JOIN dim_symbol s ON f.symbol_key = s.symbol_key
    WHERE f.date_key >= CURRENT_DATE - INTERVAL 30 DAY
    GROUP BY s.symbol
    ORDER BY avg_close DESC
    LIMIT 10
    """).fetchall()
    print("Top symbols (30d avg_close):", top_symbols)

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
    print("DuckDB ready 🚀")
    main()
