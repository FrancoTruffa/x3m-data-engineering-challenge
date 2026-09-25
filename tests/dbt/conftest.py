"""A throwaway warehouse database per dbt integration test."""

import uuid

import psycopg
import pytest
from dbt_harness import INIT_SQL, Warehouse
from psycopg import sql

from ingestion.config import warehouse_conninfo
from ingestion.loader import connect


@pytest.fixture
def warehouse(monkeypatch):
    """A fresh database with the bronze/silver/gold schemas, dropped afterwards."""
    name = f"dbt_test_{uuid.uuid4().hex[:8]}"
    admin_conninfo = warehouse_conninfo()
    with psycopg.connect(admin_conninfo, autocommit=True) as admin:
        admin.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
    # The loader and dbt's profile both read WAREHOUSE_DB.
    monkeypatch.setenv("WAREHOUSE_DB", name)
    try:
        with connect() as conn:
            for script in INIT_SQL:
                conn.execute(script.read_text())
            yield Warehouse(conn)
    finally:
        with psycopg.connect(admin_conninfo, autocommit=True) as admin:
            admin.execute(
                sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name))
            )
