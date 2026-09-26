"""Targeted reprocessing with the dbt var force_date (the dummyjson_reprocess DAG's command)."""

import json
from datetime import UTC, date, datetime

import pytest
from dbt_harness import INCREMENTAL_MODELS, load_fixture, reprocessed_dates

pytestmark = pytest.mark.dbt

DAY_A, DAY_B, DAY_C = date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)
PRODUCTS = load_fixture("products")
CARTS = load_fixture("carts")
# Same selection as dags/dummyjson_reprocess.py.
REPROCESS_SELECT = ["--selector", "reprocess"]


def ts(hour: int) -> datetime:
    return datetime(2026, 9, 25, hour, tzinfo=UTC)


@pytest.fixture
def three_days(warehouse):
    """Days A, B and C loaded and built by the daily flow."""
    for hour, day in enumerate((DAY_A, DAY_B, DAY_C), start=1):
        warehouse.load("products", PRODUCTS, day, ts(hour))
        warehouse.load("carts", CARTS, day, ts(hour))
        warehouse.build()
    return warehouse


def reprocess(warehouse, force_date: str, check: bool = True):
    return warehouse.dbt(
        "build", *REPROCESS_SELECT, "--vars", json.dumps({"force_date": force_date}), check=check
    )


def contents(warehouse):
    return {
        table: warehouse.rows(f"select * from {table} order by 1, 2, 3")
        for table in (
            "silver.products",
            "silver.carts",
            "silver.cart_items",
            "gold.product_daily_revenue",
        )
    }


def test_reprocessing_a_day_replaces_only_that_day(three_days):
    before = three_days.row_versions()

    reprocess(three_days, DAY_B.isoformat())

    changed = reprocessed_dates(before, three_days.row_versions())
    for table, _ in INCREMENTAL_MODELS:
        assert changed[table] == {DAY_B}, table


def test_reprocessing_without_logic_changes_gives_the_same_result(three_days):
    before = contents(three_days)

    reprocess(three_days, DAY_B.isoformat())

    # Same rows, including ingested_at (it comes from bronze, not from the reprocess run).
    assert contents(three_days) == before


@pytest.mark.parametrize(
    ("force_date", "error"),
    [
        ("2026-09-01", "has no data in bronze.carts"),  # well formed, not in bronze
        ("23/09/2026", "must be YYYY-MM-DD"),
        ("2026-02-30", "out of range"),  # well formed, impossible date
    ],
)
def test_invalid_force_date_fails_before_touching_any_table(three_days, force_date, error):
    before = three_days.row_versions()

    result = reprocess(three_days, force_date, check=False)

    assert result.returncode != 0
    assert error in result.stdout
    assert "OK created" not in result.stdout  # no model ran
    assert three_days.row_versions() == before


def test_force_date_cannot_be_combined_with_full_refresh(three_days):
    before = three_days.row_versions()

    result = three_days.dbt(
        "build",
        *REPROCESS_SELECT,
        "--full-refresh",
        "--vars",
        json.dumps({"force_date": DAY_B.isoformat()}),
        check=False,
    )

    assert result.returncode != 0
    assert "can't be combined with --full-refresh" in result.stdout
    assert three_days.row_versions() == before


def test_daily_build_after_a_reprocess_processes_nothing(three_days):
    reprocess(three_days, DAY_B.isoformat())
    after_reprocess = three_days.row_versions()

    three_days.build()  # daily flow, no new loads

    assert three_days.row_versions() == after_reprocess


def test_reprocess_selector_covers_the_day_models_and_checks_but_not_products(warehouse):
    result = warehouse.dbt("ls", *REPROCESS_SELECT, "--resource-type", "model", "--output", "name")
    models = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    assert {"carts", "cart_items", "product_daily_revenue"} <= models
    assert "products" not in models

    result = warehouse.dbt("ls", *REPROCESS_SELECT, "--resource-type", "test", "--output", "name")
    assert "silver_row_counts_match_bronze" in result.stdout
    assert "silver_cart_lines_reconcile_with_cart_totals" in result.stdout
    assert "gold_revenue_reconciles_with_silver_carts" in result.stdout
