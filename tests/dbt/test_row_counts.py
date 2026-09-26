"""silver_row_counts_match_bronze: rows lost or added between bronze and silver fail the build.

A transformation bug is simulated by altering silver after a correct build, then running the
check. The check only looks at the latest load (the day just processed) or at force_date. The
value reconciliations can miss these cases (a whole cart missing, or an extra zero-value line),
which is why the volume check exists.
"""

from datetime import UTC, date, datetime

import pytest
from dbt_harness import load_fixture

pytestmark = pytest.mark.dbt

DAY = date(2026, 9, 24)
TEST_NAME = "silver_row_counts_match_bronze"


@pytest.fixture
def built(warehouse):
    ingested_at = datetime(2026, 9, 25, 1, tzinfo=UTC)
    warehouse.load("products", load_fixture("products"), DAY, ingested_at)
    warehouse.load("carts", load_fixture("carts"), DAY, ingested_at)
    warehouse.build()
    return warehouse


VALUE_RECONCILIATION = "silver_cart_lines_reconcile_with_cart_totals"


def run_check(warehouse, test_name=TEST_NAME):
    return warehouse.dbt("test", "--select", test_name, check=False)


def test_passes_when_counts_match(built):
    result = run_check(built)

    assert result.returncode == 0, result.stdout
    assert f"PASS {TEST_NAME}" in result.stdout


def test_detects_a_cart_lost_in_silver(built):
    # Lines of cart 2 sum to its totals, so dropping the whole cart (and its lines) leaves the
    # line-vs-total reconciliation passing: only the volume check sees it.
    built.conn.execute("delete from silver.cart_items where cart_id = 2")
    built.conn.execute("delete from silver.carts where cart_id = 2")

    result = run_check(built)

    assert result.returncode != 0
    assert f"FAIL 2 {TEST_NAME}" in result.stdout  # carts per day + lines of cart 2
    assert run_check(built, VALUE_RECONCILIATION).returncode == 0  # blind to it


def test_detects_a_line_lost_in_silver(built):
    built.conn.execute("delete from silver.cart_items where cart_id = 38 and line_number = 3")

    result = run_check(built)

    assert result.returncode != 0
    assert f"FAIL 1 {TEST_NAME}" in result.stdout
    # A lost line with value also breaks the value reconciliation: both checks overlap here.
    assert run_check(built, VALUE_RECONCILIATION).returncode != 0


def test_detects_an_extra_line_in_silver(built):
    # An extra line with zero amounts keeps every sum intact.
    built.conn.execute(
        "insert into silver.cart_items"
        " select snapshot_date, cart_id, 99, product_id, product_title, unit_price, 1,"
        " discount_percentage, 0, 0, ingested_at"
        " from silver.cart_items where cart_id = 4 and line_number = 1"
    )

    result = run_check(built)

    assert result.returncode != 0
    assert f"FAIL 1 {TEST_NAME}" in result.stdout
    assert run_check(built, VALUE_RECONCILIATION).returncode == 0  # blind to it


# --- Window: the latest load only (or force_date) ---------------------------------------------

OLD_DAY, NEW_DAY = date(2026, 9, 23), date(2026, 9, 24)


@pytest.fixture
def two_days(warehouse):
    """Daily flow: OLD_DAY loaded and built, then NEW_DAY loaded and built."""
    for hour, day in ((1, OLD_DAY), (2, NEW_DAY)):
        ingested_at = datetime(2026, 9, 25, hour, tzinfo=UTC)
        warehouse.load("products", load_fixture("products"), day, ingested_at)
        warehouse.load("carts", load_fixture("carts"), day, ingested_at)
        warehouse.build()
    return warehouse


def lose_a_line(warehouse, day):
    warehouse.conn.execute(
        f"delete from silver.cart_items where snapshot_date = '{day}' and cart_id = 38"
        " and line_number = 3"
    )


def test_checks_the_day_just_processed(two_days):
    lose_a_line(two_days, NEW_DAY)

    result = run_check(two_days)

    assert result.returncode != 0
    assert f"FAIL 1 {TEST_NAME}" in result.stdout


def test_older_days_are_not_reaudited(two_days):
    # Accepted trade-off: a mismatch left in an older day isn't detected again by later runs.
    lose_a_line(two_days, OLD_DAY)

    result = run_check(two_days)

    assert result.returncode == 0, result.stdout


def test_force_date_checks_that_day(two_days):
    lose_a_line(two_days, OLD_DAY)

    result = two_days.dbt(
        "test", "--select", TEST_NAME, "--vars", f'{{"force_date": "{OLD_DAY}"}}', check=False
    )

    assert result.returncode != 0
    assert f"FAIL 1 {TEST_NAME}" in result.stdout
