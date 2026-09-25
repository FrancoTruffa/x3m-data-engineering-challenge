import pytest
import requests
import responses
from responses import matchers

from ingestion.client import ExtractionError, build_session, fetch_all
from ingestion.config import get_entity

BASE_URL = "https://api.test"
PRODUCTS = get_entity("products")
URL = f"{BASE_URL}/products"


def page(ids, total):
    return {"products": [{"id": i, "title": f"p{i}"} for i in ids], "total": total}


def add_page(skip, body, limit=2, status=200):
    responses.add(
        responses.GET,
        URL,
        json=body,
        status=status,
        match=[matchers.query_param_matcher({"limit": str(limit), "skip": str(skip)})],
    )


def fetch(**kwargs):
    session = build_session(retries=3, backoff_factor=0)
    return fetch_all(PRODUCTS, session=session, base_url=BASE_URL, page_size=2, **kwargs)


@responses.activate
def test_walks_every_page_until_total():
    add_page(0, page([1, 2], total=5))
    add_page(2, page([3, 4], total=5))
    add_page(4, page([5], total=5))

    result = fetch()

    assert [r["id"] for r in result.records] == [1, 2, 3, 4, 5]
    assert result.total == 5
    assert result.pages == 3


@responses.activate
def test_single_page_when_total_fits():
    add_page(0, page([1, 2], total=2))

    result = fetch()

    assert result.pages == 1
    assert len(responses.calls) == 1


@responses.activate
def test_fails_when_api_returns_fewer_records_than_total():
    add_page(0, page([1, 2], total=5))
    add_page(2, page([], total=5))  # API runs out early

    with pytest.raises(ExtractionError, match="fetched 2 records, API reported total=5"):
        fetch()


@responses.activate
def test_fails_when_total_changes_mid_extraction():
    add_page(0, page([1, 2], total=4))
    add_page(2, page([3, 4], total=5))

    with pytest.raises(ExtractionError, match="total changed"):
        fetch()


@responses.activate
def test_fails_on_duplicate_ids():
    add_page(0, page([1, 2], total=4))
    add_page(2, page([2, 3], total=4))

    with pytest.raises(ExtractionError, match="duplicate ids"):
        fetch()


@responses.activate
def test_fails_on_records_without_id():
    add_page(0, {"products": [{"id": 1}, {"title": "no id"}], "total": 2})

    with pytest.raises(ExtractionError, match="1 records without an integer 'id'"):
        fetch()


@responses.activate
def test_fails_on_malformed_payload():
    add_page(0, {"items": [], "total": 0})

    with pytest.raises(ExtractionError, match="missing 'products' list"):
        fetch()


@responses.activate
@pytest.mark.parametrize("status", [429, 500, 503])
def test_retries_transient_errors(status):
    add_page(0, {"message": "try again"}, status=status)
    add_page(0, page([1], total=1))

    result = fetch()

    assert [r["id"] for r in result.records] == [1]
    assert len(responses.calls) == 2


@responses.activate
def test_gives_up_after_max_retries():
    for _ in range(4):  # first attempt + 3 retries
        add_page(0, {"message": "down"}, status=503)

    with pytest.raises(requests.exceptions.RetryError):
        fetch()
    assert len(responses.calls) == 4


@responses.activate
def test_does_not_retry_client_errors():
    add_page(0, {"message": "not found"}, status=404)

    with pytest.raises(requests.exceptions.HTTPError):
        fetch()
    assert len(responses.calls) == 1


@responses.activate
def test_requests_use_timeout():
    add_page(0, page([1], total=1))

    fetch(timeout=(1, 2))

    assert responses.calls[0].request.req_kwargs["timeout"] == (1, 2)
