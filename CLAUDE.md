# GreenTrail Insights

## Project Type
dbt + Python analytics project (PostgreSQL backend). Retail store performance analysis for a fictional NZ outdoor equipment chain — identifies declining stores, isolates competitive displacement, and delivers a CEO brief.

## Stack
- **Database:** PostgreSQL — localhost:5432, user: portfolio, password: portfolio_dev, db: portfolio, schema: `greentrail`
- **Pipeline:** dbt-core 1.11.6 + dbt-postgres 1.10.0 (Python 3.13 venv in `.venv/`)
- **Analysis:** Python — pandas, scipy, matplotlib, seaborn, psycopg2-binary
- **Packages:** dbt-utils (>=1.0.0, <2.0.0)

## Commands
```bash
# Activate venv
source .venv/Scripts/activate    # Windows bash

# Data generation
python scripts/generate_synthetic_data.py

# dbt pipeline
dbt deps
dbt build --full-refresh
dbt test

# Analysis (generates 7 charts)
python analyses/store_performance_investigation.py
```

## Key Files
| File | Purpose |
|------|---------|
| `scripts/generate_synthetic_data.py` | Generates 12 stores, 500 SKUs, 15K customers, 120K transactions |
| `analyses/store_performance_investigation.py` | Full analytical script (7 publication-quality charts + statistical tests) |
| `docs/insights_brief.md` | CEO-facing executive brief (star deliverable) |
| `outputs/figures/01_*.png` through `07_*.png` | Generated chart outputs |

## Data Architecture

### Schemas (3 separate)
| Schema | Purpose | Contents |
|--------|---------|----------|
| `greentrail` | Source data | stores (12), products (500), customers (15K), transactions (120K), daily_store_sales (78K) |
| `greentrail_staging` | dbt views | `stg_pos__stores`, `stg_pos__products`, `stg_pos__transactions`, `stg_pos__customers` |
| `greentrail_marts` | dbt tables | `dim_stores`, `dim_products`, `dim_date`, `fct_daily_store_sales` |

### Layers
| Layer | Materialization | Models |
|-------|----------------|--------|
| Staging | VIEW | 4 models: `stg_pos__*` (1:1 source mapping) |
| Marts | TABLE | 4 models: `dim_stores`, `dim_products`, `dim_date`, `fct_daily_store_sales` |

### Star Schema
- **Fact:** `fct_daily_store_sales` (grain: store x date x category, ~78K rows) with surrogate key via `dbt_utils.generate_surrogate_key`
- **Dimensions:** `dim_stores` (12 locations + demographics), `dim_products` (500 SKUs, 6 categories), `dim_date` (2023-09 to 2025-02)
- No intermediate layer (staging feeds marts directly)

## Business Logic

### Store Performance Analysis
- **12 stores** across NZ towns, 18 months of data
- **3 declining stores** (Milltown, Creekside, Hilltop): -4% to -7% population decline → proportional revenue decline
- **1 competitive displacement** (Riverside): Kathmandu competitor opened Oct 2023, 0.8km away
  - Camping: -31%, Hiking: -28% (direct competition)
  - Fishing: +2%, Hunting: +3% (no competition, slight gain)
  - Impact ramps over 90 days
- **8 growing stores:** +2% to +5% annual growth matching population trends

### Statistical Findings
- Pearson correlation: population change vs revenue growth r=0.90
- t-test: declining vs growing stores statistically significant
- Riverside isolated as competitive displacement (not population-driven)

### Product Categories (6)
camping, hiking, fishing, hunting, cycling, general_outdoor

## Analysis Outputs
| Chart | Description |
|-------|-------------|
| `01_chain_revenue_monthly.png` | Monthly total chain revenue |
| `02_store_indexed_revenue.png` | Individual store revenue trajectories |
| `03_store_growth_ranking.png` | Store growth rate rankings |
| `04_category_trends.png` | Revenue by product category over time |
| `05_external_factors.png` | Scatter: population change vs revenue growth |
| `06_riverside_deep_dive.png` | Category-specific competitive impact |
| `07_riverside_waterfall.png` | Revenue waterfall for Riverside |

## Testing
- **YAML tests:** unique, not_null on PKs; accepted_values on categories; `dbt_utils.accepted_range` on population
- **3 custom SQL tests:**
  - `assert_all_12_stores_present.sql` — fact table has all 12 stores
  - `assert_no_future_dates.sql` — no future-dated sales
  - `assert_revenue_reconciles.sql` — fact vs source revenue within 1% tolerance

## Synthetic Data
- Seed: 42 (deterministic)
- Date range: 2023-09-01 to 2025-02-28 (18 months)
- 12 stores with embedded analytical insights (declining, competitive, growing)
- 500 SKUs across 6 categories, lognormal pricing ($5-$500)
- Seasonal patterns: spring/summer peak (1.05-1.30x), weekend boost (+35% Sat, +20% Sun)
- Target revenue: ~$8M annual (~$22K/day across 12 stores)
- DB URL hardcoded: `postgresql://portfolio:portfolio_dev@localhost:5432/portfolio`

## Conventions
- Staging: `stg_pos__<entity>`
- Marts: `dim_<entity>`, `fct_<entity>`
- No dashboard — static PNG charts + markdown CEO brief
- Analysis script queries both source (`greentrail`) and marts (`greentrail_marts`) schemas
