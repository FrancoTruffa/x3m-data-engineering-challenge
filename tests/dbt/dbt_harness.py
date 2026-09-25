"""Helpers for dbt integration tests: fixtures loaded into bronze with the real loader, and dbt
run with the same selectors as the DAG."""

import json
import os
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import psycopg
import pytest

from ingestion.config import get_entity
from ingestion.loader import load_snapshot

ROOT = Path(__file__).resolve().parents[2]
DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = ROOT / "dbt"
INIT_SQL = sorted((ROOT / "docker" / "warehouse" / "init").glob("*.sql"))
FIXTURES = ROOT / "tests" / "fixtures"

# (table, date column) of every incremental model, as queried by the tests.
INCREMENTAL_MODELS = [
    ("silver.carts", "snapshot_date"),
    ("silver.cart_items", "snapshot_date"),
    ("gold.product_daily_revenue", "date"),
]


def load_fixture(name: str) -> list[dict]:
    return json.loads((FIXTURES / f"{name}.json").read_text())


@dataclass
class Warehouse:
    conn: psycopg.Connection

    def load(self, entity: str, records: list[dict], day: date, ingested_at: datetime) -> None:
        load_snapshot(
            self.conn,
            get_entity(entity),
            records,
            logical_date=day,
            process_name="tests.fixture",
            ingestion_ts=ingested_at,
        )

    def dbt(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        result = subprocess.run(
            [
                DBT_BIN,
                *args,
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
            ],
            env=os.environ.copy(),
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            pytest.fail(f"dbt {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}")
        return result

    def build(self, *extra: str) -> None:
        """Same as the DAG: silver, then gold."""
        for layer in ("silver", "gold"):
            self.dbt("build", "--selector", layer, *extra)

    def rows(self, query: str) -> list[tuple]:
        return self.conn.execute(query).fetchall()

    def row_versions(self) -> dict[str, dict[date, list[str]]]:
        """Per model and date, the sorted xmin of its rows: it changes when rows are re-inserted,
        so comparing two snapshots tells which dates were (re)processed."""
        versions: dict[str, dict[date, list[str]]] = {}
        for table, date_column in INCREMENTAL_MODELS:
            by_date: dict[date, list[str]] = defaultdict(list)
            for day, xmin in self.rows(f"select {date_column}, xmin::text from {table}"):
                by_date[day].append(xmin)
            versions[table] = {day: sorted(x) for day, x in by_date.items()}
        return versions


def reprocessed_dates(before: dict, after: dict) -> dict[str, set[date]]:
    return {
        table: {day for day in after[table] if after[table][day] != before[table].get(day)}
        for table in after
    }
