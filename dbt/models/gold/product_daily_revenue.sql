{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='date',
        on_schema_change='fail',
    )
}}

-- Revenue per product per day. Same watermark as silver, against silver.cart_items: a date is
-- rebuilt when its silver lines were ingested after the ingested_at stored here.
-- product_title comes from the catalog at processing time; already processed dates keep it until
-- they're reprocessed (see DECISIONS.md).
with pending as (
    {{ pending_dates(ref('cart_items'), 'snapshot_date', 'ingested_at', 'date') }}
),

items as (
    select i.*
    from {{ ref('cart_items') }} as i
    inner join pending as p on i.snapshot_date = p.pending_date
)

select
    i.product_id,
    coalesce(p.title, max(i.product_title))  as product_title,
    i.snapshot_date                          as date,
    sum(i.quantity)::integer                 as units_sold,
    sum(i.line_discounted_total)             as revenue,
    sum(i.line_total)                        as gross_revenue,
    count(distinct i.cart_id)::integer       as carts_count,
    max(i.ingested_at)                       as ingested_at
from items as i
left join {{ ref('products') }} as p on p.product_id = i.product_id
group by i.product_id, i.snapshot_date, p.title
