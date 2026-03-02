with products as (
    select * from {{ ref('stg_pos__products') }}
)

select
    product_id,
    sku,
    product_name,
    category,
    unit_price
from products
