"""Per-entity extraction config and warehouse connection settings."""

import os
from dataclasses import dataclass

BASE_URL = os.environ.get("DUMMYJSON_BASE_URL", "https://dummyjson.com")
PAGE_SIZE = 50


@dataclass(frozen=True)
class EntityConfig:
    name: str
    """Entity name; also the bronze table name."""
    endpoint: str
    """Path relative to BASE_URL."""
    records_key: str
    """Key of the records array in the paginated response."""
    event_timestamp_path: tuple[str, ...] | None
    """Path to the source's last-modified timestamp inside each record, if the source has one."""


ENTITIES: dict[str, EntityConfig] = {
    "products": EntityConfig(
        name="products",
        endpoint="products",
        records_key="products",
        event_timestamp_path=("meta", "updatedAt"),
    ),
    "carts": EntityConfig(
        name="carts",
        endpoint="carts",
        records_key="carts",
        event_timestamp_path=None,
    ),
}


def get_entity(name: str) -> EntityConfig:
    try:
        return ENTITIES[name]
    except KeyError:
        raise ValueError(f"Unknown entity {name!r}; expected one of {sorted(ENTITIES)}") from None


def warehouse_conninfo() -> str:
    """libpq connection string built from WAREHOUSE_* environment variables."""
    return (
        f"host={os.environ.get('WAREHOUSE_HOST', 'localhost')} "
        f"port={os.environ.get('WAREHOUSE_PORT', '5432')} "
        f"dbname={os.environ.get('WAREHOUSE_DB', 'warehouse')} "
        f"user={os.environ.get('WAREHOUSE_USER', 'warehouse')} "
        f"password={os.environ.get('WAREHOUSE_PASSWORD', 'warehouse')}"
    )
