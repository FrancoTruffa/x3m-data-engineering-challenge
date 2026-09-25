import uuid
from datetime import UTC, date, datetime

import pytest
from psycopg import sql

from ingestion.config import get_entity
from ingestion.loader import connect, event_timestamp, load_snapshot

PRODUCTS = get_entity("products")
CARTS = get_entity("carts")
DAY = date(2026, 9, 24)


# --- event_timestamp (pure) -------------------------------------------------------------------


def test_event_timestamp_from_configured_path():
    record = {"id": 1, "meta": {"updatedAt": "2026-05-23T11:27:41.868Z"}}
    assert event_timestamp(record, PRODUCTS) == datetime(2026, 5, 23, 11, 27, 41, 868000, UTC)


def test_event_timestamp_is_none_for_entities_without_it():
    assert event_timestamp({"id": 1, "meta": {"updatedAt": "2026-05-23T11:27:41Z"}}, CARTS) is None


@pytest.mark.parametrize(
    "record",
    [{"id": 1}, {"id": 1, "meta": None}, {"id": 1, "meta": {"updatedAt": "not a date"}}],
)
def test_event_timestamp_is_none_when_missing_or_invalid(record):
    assert event_timestamp(record, PRODUCTS) is None


# --- load_snapshot (against Postgres) --------------------------------------------------------


@pytest.fixture
def conn():
    """Connection with a throwaway schema holding copies of the bronze tables."""
    schema = f"test_{uuid.uuid4().hex[:8]}"
    with connect() as connection:
        connection.execute(sql.SQL("create schema {}").format(sql.Identifier(schema)))
        for table in ("products", "carts"):
            connection.execute(
                sql.SQL("create table {} (like {} including all)").format(
                    sql.Identifier(schema, table), sql.Identifier("bronze", table)
                )
            )
        connection.schema = schema
        try:
            yield connection
        finally:
            connection.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(schema)))


def fetch_rows(conn, table="products"):
    query = sql.SQL(
        "select id, data, audit_event_timestamp, audit_ingestion_timestamp, audit_logical_date,"
        " audit_process_name from {} order by audit_logical_date, id"
    ).format(sql.Identifier(conn.schema, table))
    return conn.execute(query).fetchall()


def load(conn, records, logical_date=DAY, entity=PRODUCTS, **kwargs):
    return load_snapshot(
        conn,
        entity,
        records,
        logical_date=logical_date,
        process_name="dag.task",
        schema=conn.schema,
        **kwargs,
    )


@pytest.mark.db
def test_loads_raw_records_with_audit_columns(conn):
    record = {"id": 7, "title": "x", "meta": {"updatedAt": "2026-05-23T11:27:41.868Z"}}
    ts = datetime(2026, 9, 25, 0, 31, tzinfo=UTC)

    assert load(conn, [record], ingestion_ts=ts) == 1

    [row] = fetch_rows(conn)
    assert row == (
        7,
        record,
        datetime(2026, 5, 23, 11, 27, 41, 868000, UTC),
        ts,
        DAY,
        "dag.task",
    )


@pytest.mark.db
def test_all_rows_share_one_ingestion_timestamp(conn):
    load(conn, [{"id": i} for i in range(1, 51)])

    assert len({row[3] for row in fetch_rows(conn)}) == 1


@pytest.mark.db
def test_reloading_same_day_replaces_instead_of_duplicating(conn):
    load(conn, [{"id": 1, "v": "old"}, {"id": 2, "v": "old"}, {"id": 3, "v": "old"}])
    load(conn, [{"id": 1, "v": "new"}, {"id": 2, "v": "new"}])

    rows = fetch_rows(conn)
    assert [(r[0], r[1]["v"]) for r in rows] == [(1, "new"), (2, "new")]


@pytest.mark.db
def test_reload_does_not_touch_other_days(conn):
    load(conn, [{"id": 1}], logical_date=date(2026, 9, 23))
    load(conn, [{"id": 1}], logical_date=DAY)
    load(conn, [{"id": 1}, {"id": 2}], logical_date=DAY)

    assert [(r[4], r[0]) for r in fetch_rows(conn)] == [
        (date(2026, 9, 23), 1),
        (DAY, 1),
        (DAY, 2),
    ]


@pytest.mark.db
def test_failed_load_keeps_previous_snapshot(conn):
    load(conn, [{"id": 1}, {"id": 2}])

    with pytest.raises(KeyError):
        load(conn, [{"id": 3}, {"no_id": True}])  # fails after the delete, inside the transaction

    assert [r[0] for r in fetch_rows(conn)] == [1, 2]


@pytest.mark.db
def test_carts_have_no_event_timestamp(conn):
    load(conn, [{"id": 1, "products": []}], entity=CARTS)

    [row] = fetch_rows(conn, "carts")
    assert row[2] is None
