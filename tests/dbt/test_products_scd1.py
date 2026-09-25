"""silver.products: SCD1 current state, incremental by ingestion watermark.

Loads follow the order the extraction guard enforces: each day is loaded after the previous one
(a retry reloads the same day). A reload of an older day can't happen, so it isn't tested here.
"""

import copy
from datetime import UTC, date, datetime

import pytest
from dbt_harness import load_fixture

pytestmark = pytest.mark.dbt

DAY_A, DAY_B, DAY_C = date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)
PRODUCTS = load_fixture("products")
PRODUCT_ID = 86


def ts(hour: int) -> datetime:
    return datetime(2026, 9, 25, hour, tzinfo=UTC)


def renamed(title: str) -> list[dict]:
    products = copy.deepcopy(PRODUCTS)
    for product in products:
        if product["id"] == PRODUCT_ID:
            product["title"] = title
    return products


def load_products(warehouse, day, ingested_at, products=PRODUCTS):
    warehouse.load("products", products, day, ingested_at)


def build_silver(warehouse, *extra):
    warehouse.dbt("build", "--selector", "silver", *extra)


def product_state(warehouse, product_id=PRODUCT_ID):
    [row] = warehouse.rows(
        "select title, snapshot_date, ingested_at from silver.products"
        f" where product_id = {product_id}"
    )
    return row


def product_versions(warehouse):
    return dict(warehouse.rows("select product_id, xmin::text from silver.products"))


def test_newer_snapshot_updates_the_product(warehouse):
    load_products(warehouse, DAY_A, ts(1))
    build_silver(warehouse)
    load_products(warehouse, DAY_B, ts(2), renamed("New title"))
    build_silver(warehouse)

    assert product_state(warehouse) == ("New title", DAY_B, ts(2))


def test_latest_day_wins_with_several_pending_days(warehouse):
    # Recovery: days A and B loaded, silver built only afterwards.
    load_products(warehouse, DAY_A, ts(1))
    load_products(warehouse, DAY_B, ts(2), renamed("New title"))
    build_silver(warehouse)

    assert product_state(warehouse) == ("New title", DAY_B, ts(2))


def test_product_missing_from_new_loads_keeps_its_last_state(warehouse):
    load_products(warehouse, DAY_A, ts(1))
    build_silver(warehouse)
    without_product_1 = [p for p in PRODUCTS if p["id"] != 1]
    load_products(warehouse, DAY_B, ts(2), without_product_1)
    build_silver(warehouse)

    title, snapshot_date, ingested_at = product_state(warehouse, product_id=1)
    assert (snapshot_date, ingested_at) == (DAY_A, ts(1))
    assert title == next(p["title"] for p in PRODUCTS if p["id"] == 1)
    assert warehouse.rows("select count(*) from silver.products") == [(len(PRODUCTS),)]


def test_only_products_in_the_new_batch_are_rewritten(warehouse):
    load_products(warehouse, DAY_A, ts(1))
    build_silver(warehouse)
    before = product_versions(warehouse)

    only_86 = [p for p in PRODUCTS if p["id"] == PRODUCT_ID]
    load_products(warehouse, DAY_B, ts(2), only_86)
    build_silver(warehouse)
    after = product_versions(warehouse)

    assert {pid for pid in after if after[pid] != before[pid]} == {PRODUCT_ID}


def test_full_refresh_matches_incremental(warehouse):
    load_products(warehouse, DAY_A, ts(1))
    build_silver(warehouse)
    load_products(warehouse, DAY_B, ts(2), renamed("New title"))
    load_products(warehouse, DAY_B, ts(3), renamed("Retried title"))  # retry of the same day
    build_silver(warehouse)
    load_products(warehouse, DAY_C, ts(4), [p for p in PRODUCTS if p["id"] != 1])
    build_silver(warehouse)
    incremental = warehouse.rows("select * from silver.products order by product_id")

    build_silver(warehouse, "--full-refresh")

    assert warehouse.rows("select * from silver.products order by product_id") == incremental
