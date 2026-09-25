{{ config(materialized='table') }}

-- SCD1: current state of each product, taken from its most recent snapshot in bronze.
select distinct on (id)
    id                                            as product_id,
    data ->> 'title'                              as title,
    data ->> 'category'                           as category,
    data ->> 'brand'                              as brand,
    data ->> 'sku'                                as sku,
    (data ->> 'price')::numeric(12, 2)            as price,
    (data ->> 'discountPercentage')::numeric(5, 2) as discount_percentage,
    (data ->> 'rating')::numeric(4, 2)            as rating,
    (data ->> 'stock')::integer                   as stock,
    data ->> 'availabilityStatus'                 as availability_status,
    audit_event_timestamp                         as source_updated_at,
    audit_logical_date                            as snapshot_date,
    audit_ingestion_timestamp                     as ingested_at
from {{ source('bronze', 'products') }}
order by id, audit_logical_date desc, audit_ingestion_timestamp desc
