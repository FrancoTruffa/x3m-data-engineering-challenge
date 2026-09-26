{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='date',
        on_schema_change='fail',
        indexes=[{'columns': ['ingested_at']}],
    )
}}

-- Revenue per product per day.
-- Incremental by ingestion watermark against silver.cart_items: only lines ingested after the
-- latest ingested_at already here are read. All lines of a day share one ingested_at (they come
-- from a single bronze load), so those are complete days, replaced with delete+insert on date.
-- product_title comes from the catalog at processing time; already processed dates keep it until
-- they're reprocessed (see DECISIONS.md).
-- With the dbt var force_date, only that day is rebuilt instead (macros/force_date.sql).
with items as (
    select *
    from {{ ref('cart_items') }}
    {% if is_incremental() %}
    where {{ incremental_filter('snapshot_date', 'ingested_at') }}
    {% endif %}
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
