"""Paginated DummyJSON client with retries, backoff, timeouts and completeness checks."""

from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.config import BASE_URL, PAGE_SIZE, EntityConfig

RETRY_STATUSES = (429, 500, 502, 503, 504)
DEFAULT_TIMEOUT = (5, 30)  # (connect, read) seconds


class ExtractionError(Exception):
    """The API response is malformed or incomplete."""


@dataclass(frozen=True)
class ExtractionResult:
    records: list[dict[str, Any]]
    total: int
    pages: int


def build_session(retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    """Session that retries GETs on connection errors and 429/5xx with exponential backoff."""
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRY_STATUSES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def fetch_all(
    entity: EntityConfig,
    *,
    session: requests.Session | None = None,
    base_url: str = BASE_URL,
    page_size: int = PAGE_SIZE,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
) -> ExtractionResult:
    """Walk every page of an entity and validate the result against the API's `total`."""
    session = session or build_session()
    url = f"{base_url.rstrip('/')}/{entity.endpoint}"
    records: list[dict[str, Any]] = []
    expected_total: int | None = None
    pages = 0

    while True:
        response = session.get(
            url, params={"limit": page_size, "skip": len(records)}, timeout=timeout
        )
        response.raise_for_status()
        page, total = _parse_page(response.json(), entity)
        pages += 1

        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise ExtractionError(
                f"{entity.name}: total changed during extraction ({expected_total} -> {total})"
            )

        records.extend(page)
        if not page or len(records) >= expected_total:
            break

    _validate(records, expected_total, entity)
    return ExtractionResult(records=records, total=expected_total, pages=pages)


def _parse_page(payload: Any, entity: EntityConfig) -> tuple[list[dict[str, Any]], int]:
    if not isinstance(payload, dict):
        raise ExtractionError(
            f"{entity.name}: expected a JSON object, got {type(payload).__name__}"
        )
    page = payload.get(entity.records_key)
    total = payload.get("total")
    if not isinstance(page, list) or not isinstance(total, int):
        raise ExtractionError(
            f"{entity.name}: response missing {entity.records_key!r} list or integer 'total'"
        )
    return page, total


def _validate(records: list[dict[str, Any]], expected_total: int, entity: EntityConfig) -> None:
    if len(records) != expected_total:
        raise ExtractionError(
            f"{entity.name}: fetched {len(records)} records, API reported total={expected_total}"
        )
    ids = [record.get("id") if isinstance(record, dict) else None for record in records]
    missing = sum(1 for id_ in ids if not isinstance(id_, int))
    if missing:
        raise ExtractionError(f"{entity.name}: {missing} records without an integer 'id'")
    if len(set(ids)) != len(ids):
        raise ExtractionError(f"{entity.name}: duplicate ids in extraction")
