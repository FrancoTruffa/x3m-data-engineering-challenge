{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='snapshot_date',
        on_schema_change='fail',
        indexes=[{'columns': ['ingested_at']}],
    )
}}

-- One row per cart per daily snapshot.
-- Incremental by ingestion watermark: only bronze rows loaded after the latest ingested_at already
-- here are read. Each bronze load replaces a whole day with a single timestamp, so those rows are
-- complete snapshots of the changed days, and delete+insert on snapshot_date replaces them.
select
    audit_logical_date                          as snapshot_date,
    id                                          as cart_id,
    (data ->> 'userId')::integer                as user_id,
    (data ->> 'total')::numeric(12, 2)          as total,
    (data ->> 'discountedTotal')::numeric(12, 2) as discounted_total,
    (data ->> 'totalProducts')::integer         as total_products,
    (data ->> 'totalQuantity')::integer         as total_quantity,
    audit_ingestion_timestamp                   as ingested_at
from {{ source('bronze', 'carts') }}
{% if is_incremental() %}
where audit_ingestion_timestamp > (
    select coalesce(max(ingested_at), '-infinity'::timestamptz) from {{ this }}
)
{% endif %}
