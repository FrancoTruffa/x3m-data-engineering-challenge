{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='product_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['product_id'], 'unique': True},
            {'columns': ['ingested_at']},
        ],
    )
}}

-- SCD1: current state of each product.
-- Incremental by ingestion watermark, like silver.carts: only bronze rows loaded after the latest
-- ingested_at here are read, and those products are replaced (delete+insert on product_id).
-- Products missing from new loads keep their last known state.
-- Bronze only receives loads of the day that just closed (enforced by the extraction guard), so
-- a newer load is always a newer snapshot. With several pending days in one batch (recovery),
-- the most recent snapshot per product wins.
with batch as (
    select
        id                                             as product_id,
        data ->> 'title'                               as title,
        data ->> 'category'                            as category,
        data ->> 'brand'                               as brand,
        data ->> 'sku'                                 as sku,
        (data ->> 'price')::numeric(12, 2)             as price,
        (data ->> 'discountPercentage')::numeric(5, 2) as discount_percentage,
        (data ->> 'rating')::numeric(4, 2)             as rating,
        (data ->> 'stock')::integer                    as stock,
        data ->> 'availabilityStatus'                  as availability_status,
        audit_event_timestamp                          as source_updated_at,
        audit_logical_date                             as snapshot_date,
        audit_ingestion_timestamp                      as ingested_at,
        row_number() over (
            partition by id order by audit_logical_date desc, audit_ingestion_timestamp desc
        )                                              as recency_rank
    from {{ source('bronze', 'products') }}
    {% if is_incremental() %}
    where audit_ingestion_timestamp > (
        select coalesce(max(ingested_at), '-infinity'::timestamptz) from {{ this }}
    )
    {% endif %}
)

select
    product_id, title, category, brand, sku, price, discount_percentage, rating, stock,
    availability_status, source_updated_at, snapshot_date, ingested_at
from batch
where recency_rank = 1
