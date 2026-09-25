{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='snapshot_date',
        on_schema_change='fail',
        indexes=[{'columns': ['ingested_at']}],
    )
}}

-- One row per line of a cart. line_number is the position in the cart's products array:
-- (snapshot_date, cart_id, product_id) is not unique, the same product can appear in two lines.
-- Incremental by ingestion watermark, same as silver.carts.
select
    c.audit_logical_date                                  as snapshot_date,
    c.id                                                  as cart_id,
    item.line_number::integer                             as line_number,
    (item.line ->> 'id')::integer                         as product_id,
    item.line ->> 'title'                                 as product_title,
    (item.line ->> 'price')::numeric(12, 2)               as unit_price,
    (item.line ->> 'quantity')::integer                   as quantity,
    (item.line ->> 'discountPercentage')::numeric(5, 2)   as discount_percentage,
    (item.line ->> 'total')::numeric(12, 2)               as line_total,
    (item.line ->> 'discountedTotal')::numeric(12, 2)     as line_discounted_total,
    c.audit_ingestion_timestamp                           as ingested_at
from {{ source('bronze', 'carts') }} as c
cross join lateral jsonb_array_elements(c.data -> 'products')
    with ordinality as item (line, line_number)
{% if is_incremental() %}
where c.audit_ingestion_timestamp > (
    select coalesce(max(ingested_at), '-infinity'::timestamptz) from {{ this }}
)
{% endif %}
