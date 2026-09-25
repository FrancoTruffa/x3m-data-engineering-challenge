-- Daily revenue in gold must match the sum of discounted_total of silver.carts for the same day,
-- within one cent. A day present in only one of the two layers also fails.
with gold as (
    select date, sum(revenue) as revenue
    from {{ ref('product_daily_revenue') }}
    group by date
),

silver as (
    select snapshot_date as date, sum(discounted_total) as revenue
    from {{ ref('carts') }}
    group by snapshot_date
)

select
    coalesce(g.date, s.date) as date,
    g.revenue                as gold_revenue,
    s.revenue                as silver_revenue
from gold as g
full outer join silver as s on g.date = s.date
where g.date is null
    or s.date is null
    or abs(g.revenue - s.revenue) > 0.01
