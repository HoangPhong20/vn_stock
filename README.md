# VN_stock – Vietnam Stock Data Pipeline

## 🎯 Project Overview

End-to-end **Data Engineering pipeline** cho dữ liệu chứng khoán Việt Nam, áp dụng mô hình **Bronze → Silver → Gold**, dùng **Python + DuckDB** làm analytical warehouse.

Mục tiêu:

* Xây dựng pipeline rõ ràng, tách extract / transform / load
* Thực hành data modeling (fact / dimension)
* Viết analytics & BI queries trực tiếp trên DuckDB

Phù hợp cho **Data Engineer / Analytics Engineer portfolio**.

---

## 🏗️ Project Structure

```text
VN_stock/
├── config/
│   └── config.yaml                  # Global configs
│
├── duckdb/
│   └── vnstock.duckdb               # DuckDB warehouse (local, demo)
│
├── schemas/
│   ├── stock_price_silver.yaml      # Silver schema definition
│   └── stock_price_gold.yaml        # Gold schema definition
│
├── scripts/
│   ├── run_pipeline.py              # Pipeline entrypoint
│   └── duckdb_analysis.py           # Analytics & BI queries
│
├── src/
│   ├── extract/
│   │   └── fetch_stock_price.py     # Ingest raw stock data
│   │
│   ├── transform/
│   │   ├── clean_stock_price.py     # Clean & standardize (Silver)
│   │   ├── build_stock_price_daily.py # Build fact table (Gold)
│   │   ├── validate_silver_schema.py
│   │   └── validate_gold_schema.py
│   │
│   ├── load/
│   │   ├── save_raw_to_s3.py         # Bronze
│   │   ├── save_silver_to_s3.py      # Silver
│   │   └── save_gold_to_s3.py        # Gold
│   │
│   └── utils/
│       ├── logger.py
│       └── s3_utils.py
│
├── requirements.txt
├── .env
└── README.md
```

---

## 🧱 Data Modeling

### Fact tables

* **fact_stock_price** – daily grain `(trading_date, symbol)`
* **fact_stock_price_monthly** – `(year_month, symbol)`
* **fact_stock_price_yearly** – `(year, symbol)`

### Dimension tables

* **dim_date**
* **dim_symbol**

---

## 🔄 Pipeline Flow

```text
Raw API
  ↓
Bronze (raw parquet on S3)
  ↓
Silver (cleaned, validated)
  ↓
Gold (fact / dim tables)
  ↓
DuckDB analytics & BI queries
```

---

## 🚀 How to Run

### 1️⃣ Environment setup

```bash
conda create -n spark python=3.10
conda activate spark
pip install -r requirements.txt
```

Create `.env`:

```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=your_region
```

---

### 2️⃣ Run full pipeline

```bash
python -m scripts.run_pipeline
```

---

### 3️⃣ Run analytics & checks

```bash
python -m scripts.duckdb_analysis
```

Example output:

```text
DuckDB ready 🚀
fact_stock_price rows: 158124
fact_stock_price orphan date_key: 0
Top volume by exchange/year:
(2025, 'HOSE', 226540745985)
```

---

## 📊 Example BI Query

```sql
SELECT
  symbol,
  ROUND(AVG(close), 2) AS avg_close_30d
FROM v_gold_stock_price_daily
WHERE trading_date >= CURRENT_DATE - INTERVAL 30 DAY
GROUP BY symbol
ORDER BY avg_close_30d DESC
LIMIT 10;
```

---

## ✅ Data Quality Checks

* Duplicate grain validation
* Null surrogate keys
* Orphan foreign keys
* Schema validation via YAML

---

## 🧠 Tech Stack

* Python
* DuckDB
* SQL Analytics
* Parquet
* AWS S3 (optional)

---

