"""Idempotent load of a daily snapshot into bronze."""

from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from ingestion.config import EntityConfig, warehouse_conninfo

BRONZE_SCHEMA = "bronze"


def connect(conninfo: str | None = None) -> psycopg.Connection:
    """Autocommit connection: transactions are opened explicitly with `conn.transaction()`."""
    return psycopg.connect(conninfo or warehouse_conninfo(), autocommit=True)


def load_snapshot(
    conn: psycopg.Connection,
    entity: EntityConfig,
    records: Sequence[dict[str, Any]],
    *,
    logical_date: date,
    process_name: str,
    ingestion_ts: datetime | None = None,
    schema: str = BRONZE_SCHEMA,
) -> int:
    """Replace the snapshot of `logical_date` with `records` in a single transaction.

    Re-running the same day deletes the previous load first, so retries never duplicate rows.
    All rows share one `audit_ingestion_timestamp`.
    """
    ingestion_ts = ingestion_ts or datetime.now(UTC)
    rows = [
        (
            record["id"],
            Jsonb(record),
            event_timestamp(record, entity),
            ingestion_ts,
            logical_date,
            process_name,
        )
        for record in records
    ]
    table = sql.Identifier(schema, entity.name)
    delete = sql.SQL("delete from {} where audit_logical_date = %s").format(table)
    insert = sql.SQL(
        "insert into {} (id, data, audit_event_timestamp, audit_ingestion_timestamp,"
        " audit_logical_date, audit_process_name) values (%s, %s, %s, %s, %s, %s)"
    ).format(table)

    with conn.transaction(), conn.cursor() as cur:
        cur.execute(delete, (logical_date,))
        cur.executemany(insert, rows)
    return len(rows)


def event_timestamp(record: dict[str, Any], entity: EntityConfig) -> datetime | None:
    """Source last-modified timestamp, or None when the entity (or this record) doesn't have it."""
    if entity.event_timestamp_path is None:
        return None
    value: Any = record
    for key in entity.event_timestamp_path:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
