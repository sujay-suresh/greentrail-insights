with source as (
    select * from {{ source('pos', 'stores') }}
),

renamed as (
    select
        store_id,
        store_name,
        town_name,
        population,
        population_change_pct,
        store_size_sqm,
        nearest_competitor_km,
        opened_date
    from source
)

select * from renamed
