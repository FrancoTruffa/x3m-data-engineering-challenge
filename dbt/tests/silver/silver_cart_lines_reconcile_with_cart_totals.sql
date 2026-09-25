-- Sum of line totals (gross and net) must match the cart's totals, within one cent.
-- Returns the carts that don't reconcile.
select
    c.snapshot_date,
    c.cart_id,
    c.total,
    sum(i.line_total)            as sum_line_total,
    c.discounted_total,
    sum(i.line_discounted_total) as sum_line_discounted_total
from {{ ref('carts') }} as c
left join {{ ref('cart_items') }} as i
    on i.snapshot_date = c.snapshot_date and i.cart_id = c.cart_id
group by c.snapshot_date, c.cart_id, c.total, c.discounted_total
having abs(c.total - coalesce(sum(i.line_total), 0)) > 0.01
    or abs(c.discounted_total - coalesce(sum(i.line_discounted_total), 0)) > 0.01
