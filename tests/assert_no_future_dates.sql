-- No sale dates should be in the future
select 1
from {{ ref('fct_daily_store_sales') }}
where sale_date > current_date
limit 1
