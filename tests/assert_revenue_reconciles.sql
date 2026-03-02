-- Fact table revenue should reconcile with source transactions (within 1% tolerance)
with fact_total as (
    select sum(revenue) as fct_revenue from {{ ref('fct_daily_store_sales') }}
),
source_total as (
    select sum(line_total) as src_revenue from {{ source('pos', 'transactions') }}
)
select 1
from fact_total, source_total
where abs(fct_revenue - src_revenue) / nullif(src_revenue, 0) > 0.01
