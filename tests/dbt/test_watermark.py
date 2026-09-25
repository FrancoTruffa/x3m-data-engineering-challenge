"""dbt incremental processing by ingestion watermark, end to end on fixtures."""

import copy
from datetime import UTC, date, datetime

import pytest
from dbt_harness import INCREMENTAL_MODELS, load_fixture, reprocessed_dates

pytestmark = pytest.mark.dbt

DAY_A, DAY_B, DAY_C = date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)
PRODUCTS = load_fixture("products")
CARTS = load_fixture("carts")


def ts(hour: int) -> datetime:
    return datetime(2026, 9, 25, hour, tzinfo=UTC)


def load_day(warehouse, day, ingested_at, carts=CARTS, products=PRODUCTS):
    warehouse.load("products", products, day, ingested_at)
    warehouse.load("carts", carts, day, ingested_at)


def silver_cart_counts(warehouse):
    return dict(warehouse.rows("select snapshot_date, count(*) from silver.carts group by 1"))


def test_reprocesses_exactly_the_days_with_new_loads(warehouse):
    load_day(warehouse, DAY_A, ts(1))
    warehouse.build()
    load_day(warehouse, DAY_B, ts(2))
    warehouse.build()
    first = warehouse.row_versions()
    assert silver_cart_counts(warehouse) == {DAY_A: 3, DAY_B: 3}

    # Day B retried (the only legitimate reload: same day), now with one cart less; then day C.
    # Day A gets no new load.
    load_day(warehouse, DAY_B, ts(3), carts=CARTS[:2])
    load_day(warehouse, DAY_C, ts(4))
    warehouse.build()
    second = warehouse.row_versions()

    for table, _ in INCREMENTAL_MODELS:
        assert reprocessed_dates(first, second)[table] == {DAY_B, DAY_C}, table
    assert silver_cart_counts(warehouse) == {DAY_A: 3, DAY_B: 2, DAY_C: 3}
    assert dict(
        warehouse.rows("select snapshot_date, max(ingested_at) from silver.carts group by 1")
    ) == {
        DAY_A: ts(1),
        DAY_B: ts(3),
        DAY_C: ts(4),
    }

    # Nothing new in bronze: nothing is reprocessed.
    warehouse.build()
    assert warehouse.row_versions() == second


def test_recovers_a_day_that_silver_missed(warehouse):
    load_day(warehouse, DAY_A, ts(1))
    warehouse.build()
    first = warehouse.row_versions()

    # Day B is loaded but its dbt build never happens (e.g. it failed); then day C arrives.
    load_day(warehouse, DAY_B, ts(2))
    load_day(warehouse, DAY_C, ts(3))
    warehouse.build()

    for table, _ in INCREMENTAL_MODELS:
        assert reprocessed_dates(first, warehouse.row_versions())[table] == {DAY_B, DAY_C}, table
    assert silver_cart_counts(warehouse) == {DAY_A: 3, DAY_B: 3, DAY_C: 3}


def snapshot_contents(warehouse):
    return {
        table: warehouse.rows(f"select * from {table} order by 1, 2, 3")
        for table in (
            "silver.products",
            "silver.carts",
            "silver.cart_items",
            "gold.product_daily_revenue",
        )
    }


def test_full_refresh_rebuilds_everything_and_matches_incremental(warehouse):
    load_day(warehouse, DAY_A, ts(1))
    warehouse.build()
    load_day(warehouse, DAY_B, ts(2))
    load_day(warehouse, DAY_B, ts(3), carts=CARTS[:2])  # retry of the same day
    warehouse.build()
    load_day(warehouse, DAY_C, ts(4))
    warehouse.build()
    incremental = snapshot_contents(warehouse)
    before = warehouse.row_versions()

    warehouse.build("--full-refresh")

    for table, _ in INCREMENTAL_MODELS:
        assert reprocessed_dates(before, warehouse.row_versions())[table] == {DAY_A, DAY_B, DAY_C}
    assert snapshot_contents(warehouse) == incremental


def test_gold_consolidates_duplicate_lines_and_falls_back_to_cart_title(warehouse):
    load_day(warehouse, DAY_A, ts(1))
    result = warehouse.dbt("build", "--selector", "silver")
    warehouse.build()

    gold = {
        row[0]: row[1:]
        for row in warehouse.rows(
            "select product_id, product_title, units_sold, revenue, gross_revenue, carts_count"
            " from gold.product_daily_revenue"
        )
    }
    lines_110 = [line for line in CARTS[2]["products"] if line["id"] == 110]
    assert len(lines_110) == 2  # fixture edge case: product 110 twice in cart 38
    title, units, revenue, gross, carts_count = gold[110]
    assert units == sum(line["quantity"] for line in lines_110)
    assert float(gross) == pytest.approx(sum(line["total"] for line in lines_110))
    assert float(revenue) == pytest.approx(sum(line["discountedTotal"] for line in lines_110))
    assert carts_count == 1

    # Product 161 is sold but missing from the catalog fixture: title from the cart, and the
    # relationships test only warns.
    [line_161] = [line for line in CARTS[2]["products"] if line["id"] == 161]
    assert gold[161][0] == line_161["title"]
    assert "WARN" in result.stdout and "relationships_cart_items_product_id" in result.stdout

    # Product 1 is in the catalog but has no sales: sparse table, no row.
    assert 1 not in gold


def test_gold_keeps_catalog_title_of_processing_time_until_full_refresh(warehouse):
    load_day(warehouse, DAY_A, ts(1))
    warehouse.build()

    renamed = copy.deepcopy(PRODUCTS)
    for product in renamed:
        if product["id"] == 86:
            product["title"] = "Renamed product"
    load_day(warehouse, DAY_B, ts(2), products=renamed)
    warehouse.build()

    def titles():
        return dict(
            warehouse.rows(
                "select date, product_title from gold.product_daily_revenue where product_id = 86"
            )
        )

    original = next(p["title"] for p in PRODUCTS if p["id"] == 86)
    assert titles() == {DAY_A: original, DAY_B: "Renamed product"}

    warehouse.build("--full-refresh")
    assert titles() == {DAY_A: "Renamed product", DAY_B: "Renamed product"}


def test_failing_data_test_stops_the_build(warehouse):
    broken = copy.deepcopy(CARTS)
    broken[0]["total"] += 100  # cart total no longer matches the sum of its lines
    load_day(warehouse, DAY_A, ts(1), carts=broken)

    result = warehouse.dbt("build", "--selector", "silver", check=False)

    assert result.returncode != 0
    assert (
        "FAIL" in result.stdout and "silver_cart_lines_reconcile_with_cart_totals" in result.stdout
    )
