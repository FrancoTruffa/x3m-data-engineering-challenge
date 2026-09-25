{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='snapshot_date',
        on_schema_change='fail',
    )
}}

-- One row per line of a cart. line_number is the position in the cart's products array:
-- (snapshot_date, cart_id, product_id) is not unique, the same product can appear in two lines.
with pending as (
    {{ pending_dates(source('bronze', 'carts'), 'audit_logical_date', 'audit_ingestion_timestamp', 'snapshot_date') }}
)

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
inner join pending as p on c.audit_logical_date = p.pending_date
cross join lateral jsonb_array_elements(c.data -> 'products')
    with ordinality as item (line, line_number)
