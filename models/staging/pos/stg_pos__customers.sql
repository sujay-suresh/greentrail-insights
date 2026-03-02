with source as (
    select * from {{ source('pos', 'customers') }}
),

renamed as (
    select
        customer_id,
        age_group,
        gender,
        postcode,
        primary_store_id
    from source
)

select * from renamed
