{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='snapshot_date',
        on_schema_change='fail',
    )
}}

-- One row per cart per daily snapshot. Only dates with a newer bronze load are (re)built.
with pending as (
    {{ pending_dates(source('bronze', 'carts'), 'audit_logical_date', 'audit_ingestion_timestamp', 'snapshot_date') }}
)

select
    c.audit_logical_date                          as snapshot_date,
    c.id                                          as cart_id,
    (c.data ->> 'userId')::integer                as user_id,
    (c.data ->> 'total')::numeric(12, 2)          as total,
    (c.data ->> 'discountedTotal')::numeric(12, 2) as discounted_total,
    (c.data ->> 'totalProducts')::integer         as total_products,
    (c.data ->> 'totalQuantity')::integer         as total_quantity,
    c.audit_ingestion_timestamp                   as ingested_at
from {{ source('bronze', 'carts') }} as c
inner join pending as p on c.audit_logical_date = p.pending_date
