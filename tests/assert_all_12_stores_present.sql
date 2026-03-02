-- All 12 stores must be present in the fact table
select 1
from (
    select count(distinct store_id) as store_count
    from {{ ref('fct_daily_store_sales') }}
) t
where t.store_count != 12
