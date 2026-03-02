with transactions as (
    select * from {{ ref('stg_pos__transactions') }}
),

products as (
    select * from {{ ref('stg_pos__products') }}
),

daily_agg as (
    select
        t.store_id,
        t.transaction_date as sale_date,
        p.category as product_category,
        sum(t.line_total) as revenue,
        count(distinct t.transaction_id) as transaction_count,
        count(distinct t.customer_id) as unique_customers,
        case
            when count(distinct t.transaction_id) > 0
            then sum(t.line_total) / count(distinct t.transaction_id)
            else 0
        end as avg_basket
    from transactions t
    left join products p on t.product_id = p.product_id
    group by t.store_id, t.transaction_date, p.category
)

select
    {{ dbt_utils.generate_surrogate_key(['store_id', 'sale_date', 'product_category']) }} as daily_sale_id,
    store_id,
    sale_date,
    product_category,
    revenue,
    transaction_count,
    unique_customers,
    round(avg_basket::numeric, 2) as avg_basket
from daily_agg
