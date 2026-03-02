# GreenTrail Insights

**Analytics engagement for GreenTrail Outdoor Co.** — a 12-store regional outdoor recreation retailer with $8M annual revenue.

> **[Read the Executive Insights Brief](docs/insights_brief.md)** — the 3-page deliverable with root cause analysis and recommendations

## Engagement Context

GreenTrail's CEO observed that 4 of 12 stores were declining while the remaining 8 continued to grow. With no existing analytics capability, she commissioned a 2-week investigation to identify root causes and recommend corrective action. The deliverable: a 3-page executive brief she can act on immediately.

## Analytical Approach

1. **Data pipeline** — dbt models (staging → marts) transforming 18 months of POS data into an analytical fact table at store × date × category grain
2. **Exploratory analysis** — Revenue decomposition, store benchmarking, category-level trend analysis, external factor correlation
3. **Statistical validation** — Pearson correlation (r=0.90, p<0.001) confirming population-revenue relationship; t-tests confirming significance of declining vs growing store differences
4. **Root cause identification** — Two distinct drivers isolated: demographic decline (3 stores) and competitive displacement (1 store)

## Key Findings

- **3 stores** (Milltown, Creekside, Hilltop) are declining because their towns are losing 4–7% population. Revenue decline tracks population decline across all categories — this is structural, not operational
- **1 store** (Riverside) lost ~35% of camping and hiking revenue after a Kathmandu competitor opened 800m away in October 2023. Fishing and hunting revenue was unaffected (no competitor overlap)
- Population change explains 81% of revenue growth variance (r=0.90) across 11 of 12 stores; Riverside is the clear outlier

## Business Impact

Identified that 3 of 4 declining stores face structural demographic decline (non-addressable), but Riverside's ~$290K annual decline is partially recoverable through category rebalancing. Executive brief delivered to CEO with 3 specific recommendations and projected $180K revenue recovery path.

## Tech Stack

- **Database:** PostgreSQL (schema: `greentrail`)
- **Pipeline:** dbt-core + dbt-postgres
- **Analysis:** Python (pandas, scipy, matplotlib, seaborn)
- **Data:** 180K+ transaction records, 12 stores, 500 SKUs, 15K customers

## Project Structure

```
greentrail-insights/
├── models/
│   ├── staging/pos/          # Source-conformed views
│   └── marts/                # Dimensional models
├── tests/                    # Data quality assertions
├── scripts/                  # Synthetic data generation
├── analyses/                 # Analytical scripts
├── outputs/figures/          # Publication-quality charts
└── docs/                     # Executive insights brief
```

## Running

```bash
# Activate environment
source .venv/Scripts/activate

# Generate data (requires PostgreSQL running)
python scripts/generate_synthetic_data.py

# Build dbt models and run tests
dbt deps && dbt build --full-refresh

# Run analysis
python analyses/store_performance_investigation.py
```
