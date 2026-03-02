with source as (
    select * from {{ source('pos', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        store_id,
        customer_id,
        product_id,
        transaction_date,
        quantity,
        unit_price,
        line_total
    from source
)

select * from renamed
