-- Volume check between bronze and silver: rows lost or added by the transformation.
-- Complements the value reconciliations (silver_cart_lines_reconcile_with_cart_totals,
-- gold_revenue_reconciles_with_silver_carts), which catch corrupted content with the same number
-- of rows. Returns one row per mismatch; any row fails the build.
--   carts:      rows per day in bronze.carts vs rows per snapshot_date in silver.carts.
--   cart_items: per cart and day, elements of the JSON products array vs rows in cart_items.
-- A day or cart present on only one side is a mismatch too.
--
-- Window: only the latest load, not the whole history. The test runs after the models, when
-- silver's watermark already includes the new load, so the window is the bronze load(s) at or
-- after max(ingested_at) of silver.carts: the day just processed in a daily run. If the model
-- lost that whole day, silver's max stays behind and the window widens to include it.
-- With force_date (targeted reprocessing), the window is that day.
-- Older days are not re-audited on every run (see DECISIONS.md 1.7).

with window_days as (
    {% if force_date() %}
    select '{{ force_date() }}'::date as snapshot_date
    {% else %}
    select distinct audit_logical_date as snapshot_date
    from {{ source('bronze', 'carts') }}
    where audit_ingestion_timestamp >= (
        select coalesce(max(ingested_at), '-infinity'::timestamptz) from {{ ref('carts') }}
    )
    {% endif %}
),

bronze_carts as (
    select
        audit_logical_date                     as snapshot_date,
        id                                     as cart_id,
        jsonb_array_length(data -> 'products') as lines
    from {{ source('bronze', 'carts') }}
    where audit_logical_date in (select snapshot_date from window_days)
),

bronze_days as (
    select snapshot_date, count(*) as carts
    from bronze_carts
    group by snapshot_date
),

silver_days as (
    select snapshot_date, count(*) as carts
    from {{ ref('carts') }}
    where snapshot_date in (select snapshot_date from window_days)
    group by snapshot_date
),

silver_lines as (
    select snapshot_date, cart_id, count(*) as lines
    from {{ ref('cart_items') }}
    where snapshot_date in (select snapshot_date from window_days)
    group by snapshot_date, cart_id
)

select
    'carts'                                    as check_name,
    coalesce(b.snapshot_date, s.snapshot_date) as snapshot_date,
    null::integer                              as cart_id,
    b.carts                                    as bronze_count,
    s.carts                                    as silver_count
from bronze_days as b
full outer join silver_days as s on s.snapshot_date = b.snapshot_date
where b.carts is distinct from s.carts

union all

select
    'cart_items'                               as check_name,
    coalesce(b.snapshot_date, s.snapshot_date) as snapshot_date,
    coalesce(b.cart_id, s.cart_id)             as cart_id,
    b.lines                                    as bronze_count,
    s.lines                                    as silver_count
from bronze_carts as b
full outer join silver_lines as s
    on s.snapshot_date = b.snapshot_date and s.cart_id = b.cart_id
-- A cart with an empty products array has no rows in cart_items: 0 lines on both sides.
where coalesce(b.lines, 0) is distinct from coalesce(s.lines, 0)
    or b.cart_id is null
