with stores as (
    select * from {{ ref('stg_pos__stores') }}
)

select
    store_id,
    store_name,
    town_name,
    population as town_population,
    population_change_pct,
    store_size_sqm,
    nearest_competitor_km,
    opened_date,
    current_date - opened_date as days_open
from stores
