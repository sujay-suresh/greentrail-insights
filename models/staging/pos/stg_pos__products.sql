with source as (
    select * from {{ source('pos', 'products') }}
),

renamed as (
    select
        product_id,
        sku,
        product_name,
        category,
        unit_price
    from source
)

select * from renamed
