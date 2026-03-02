# GreenTrail Insights

## Project Type
dbt + Python analytics project (PostgreSQL backend)

## Stack
- **Database:** PostgreSQL localhost:5432, user: portfolio, db: portfolio, schema: greentrail
- **Pipeline:** dbt-core 1.11 + dbt-postgres 1.10 (Python 3.13 venv in `.venv/`)
- **Analysis:** Python — pandas, scipy, matplotlib, seaborn

## Commands
```bash
source .venv/Scripts/activate    # Activate venv first
dbt build --full-refresh         # Rebuild all models + tests
dbt test                         # Run tests only
python analyses/store_performance_investigation.py  # Generate charts
```

## Key Files
- `scripts/generate_synthetic_data.py` — Creates all source data and loads to PostgreSQL
- `analyses/store_performance_investigation.py` — Full analytical script (7 charts)
- `docs/insights_brief.md` — CEO-facing executive brief (star deliverable)
- `models/marts/fct_daily_store_sales.sql` — Core fact table (store × date × category)

## Data Architecture
- Source schema: `greentrail` (stores, products, customers, transactions, daily_store_sales)
- Staging schema: `greentrail_staging` (views)
- Marts schema: `greentrail_marts` (tables: dim_stores, dim_products, dim_date, fct_daily_store_sales)
