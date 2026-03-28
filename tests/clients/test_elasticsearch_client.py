"""Tests for :mod:`app.clients.elasticsearch_client`, with a stubbed transport."""

# An explicit comparison to an empty collection is the assertion these
# tests mean to make, and reads better here than a truthiness check.
# pylint: disable=use-implicit-booleaness-not-comparison
# The bulk timeout is asserted against the constant that sets it, rather
# than against a number repeated here.
# pyright: reportPrivateUsage=false

from collections.abc import Callable, Generator
from typing import Any, cast

import pytest
from elasticsearch import ApiError, TransportError

from app.clients.elasticsearch_client import _BULK_TIMEOUT, ElasticsearchClient
from app.core.errors import ExternalServiceError


# One attribute per call this stub records, and it stands in for a
# whole client.
# pylint: disable-next=too-many-instance-attributes
class _StubTransport:
    """Stands in for the official Elasticsearch client.

    Serves ``search``/``scroll`` from a queue of prepared pages and records
    every ``clear_scroll`` call, so the tests can assert that a scroll
    context is always released.
    """

    def __init__(
        self,
        pages: list[dict[str, Any]],
        *,
        clear_scroll_fails: bool = False,
        bulk_response: dict[str, Any] | None = None,
    ) -> None:
        """Store the pages to serve, in order."""
        self.pages = list(pages)
        self.cleared: list[str | None] = []
        self.indexed: list[dict[str, Any]] = []
        self.search_kwargs: dict[str, Any] = {}
        self.scroll_ids: list[str | None] = []
        self.closed = False
        self.clear_scroll_fails = clear_scroll_fails
        self.bulk_response = bulk_response
        self.bulk_kwargs: dict[str, Any] = {}
        self.bulk_calls = 0
        self.options_kwargs: dict[str, Any] = {}

    def search(self, **kwargs: Any) -> dict[str, Any]:
        """Serve the first prepared page."""
        self.search_kwargs = kwargs
        return self.pages.pop(0)

    def scroll(self, **kwargs: Any) -> dict[str, Any]:
        """Serve the next prepared page, or an empty one when exhausted."""
        self.scroll_ids.append(kwargs.get("scroll_id"))
        if not self.pages:
            return {"hits": {"hits": []}}
        return self.pages.pop(0)

    def clear_scroll(self, *, scroll_id: str | None = None) -> dict[str, Any]:
        """Record the released scroll id, failing if so configured."""
        self.cleared.append(scroll_id)
        if self.clear_scroll_fails:
            raise RuntimeError("clear failed")
        return {"succeeded": True}

    def index(self, **kwargs: Any) -> dict[str, Any]:
        """Record an indexed document."""
        self.indexed.append(kwargs)
        return {"result": "created"}

    def options(self, **kwargs: Any) -> "_StubTransport":
        """Record per-request options and keep serving from this stub."""
        self.options_kwargs = kwargs
        return self

    def bulk(self, **kwargs: Any) -> dict[str, Any]:
        """Serve a prepared response, or take every operation as created."""
        self.bulk_kwargs = kwargs
        self.bulk_calls += 1
        if self.bulk_response is not None:
            return self.bulk_response
        operations = cast(list[dict[str, Any]], kwargs.get("operations", []))
        items = [{"index": {"status": 201, "result": "created"}} for _ in operations[::2]]
        return {"errors": False, "items": items}

    def close(self) -> None:
        """Record that the transport was closed."""
        self.closed = True


class _StubResponseMeta:  # pylint: disable=too-few-public-methods
    """The only part of a response that an ``ApiError`` reads to format itself.

    Standing this up here keeps the tests off ``elastic_transport``,
    which this project does not declare as a dependency of its own.
    """

    status = 400


class _FailingSecurity:
    """Stands in for the ``security`` namespace, failing every request."""

    def __init__(self, error: Exception) -> None:
        """Store the error every request raises."""
        self._error = error

    def create_api_key(self, **_: Any) -> dict[str, Any]:
        """Fail the way Elasticsearch would."""
        raise self._error

    def invalidate_api_key(self, **_: Any) -> dict[str, Any]:
        """Fail the way Elasticsearch would."""
        raise self._error

    def get_api_key(self, **_: Any) -> dict[str, Any]:
        """Fail the way Elasticsearch would."""
        raise self._error


class _FailingTransport:
    """Fails every request with one prepared Elasticsearch error.

    Any *pages* are served first, so a test can let the opening search
    succeed and fail the scroll request that follows it.
    """

    def __init__(
        self,
        error: Exception,
        pages: list[dict[str, Any]] | None = None,
    ) -> None:
        """Store the error to raise, and any pages to serve before it."""
        self.error = error
        self.pages = list(pages or [])
        self.cleared: list[str | None] = []
        self.security = _FailingSecurity(error)

    def search(self, **_: Any) -> dict[str, Any]:
        """Serve the first prepared page, or fail."""
        return self._next()

    def scroll(self, **_: Any) -> dict[str, Any]:
        """Serve the next prepared page, or fail."""
        return self._next()

    def index(self, **_: Any) -> dict[str, Any]:
        """Reject the document."""
        raise self.error

    def options(self, **_: Any) -> "_FailingTransport":
        """Keep serving from this stub, so the request can fail instead."""
        return self

    def bulk(self, **_: Any) -> dict[str, Any]:
        """Reject the whole request."""
        raise self.error

    def clear_scroll(self, *, scroll_id: str | None = None) -> dict[str, Any]:
        """Record the released scroll id."""
        self.cleared.append(scroll_id)
        return {"succeeded": True}

    def _next(self) -> dict[str, Any]:
        """Pop the next prepared page, or raise once they run out."""
        if self.pages:
            return self.pages.pop(0)
        raise self.error


_API_KEY_OPERATIONS: list[tuple[Callable[[ElasticsearchClient], object], str]] = [
    (lambda client: client.create_api_key(), "Creating API key failed"),
    (lambda client: client.delete_api_key(key_id="abc"), "Invalidating API key failed"),
    (lambda client: client.list_api_keys(), "Listing API keys failed"),
]


def test_exactly_one_credential_is_required() -> None:
    """Passing both or neither is a programming error, not a fallback."""
    with pytest.raises(ValueError, match="Exactly one of api_key or basic_auth"):
        ElasticsearchClient(endpoint="https://es.example.com:9200")
    with pytest.raises(ValueError, match="Exactly one of api_key or basic_auth"):
        ElasticsearchClient(
            endpoint="https://es.example.com:9200",
            api_key="k",
            basic_auth=("u", "p"),
        )


def test_query_yields_sources_across_scroll_pages() -> None:
    """Results are streamed page by page, unwrapped to their ``_source``."""
    client, transport = _client([_page(["a", "b"]), _page(["c"])])
    assert [d["id"] for d in client.query(index="i")] == ["a", "b", "c"]
    assert transport.cleared == ["scroll-1"]


def test_query_scrolls_in_large_pages_by_default() -> None:
    """An unbounded scan must not pay a round trip per hundred documents."""
    client, transport = _client([_page(["a"])])
    list(client.query(index="i"))
    assert transport.search_kwargs["size"] == 5000


def test_query_stops_at_the_limit() -> None:
    """A limit caps the results and the page size, then releases the scroll."""
    client, transport = _client([_page(["a", "b", "c"])])
    assert [d["id"] for d in client.query(index="i", limit=2)] == ["a", "b"]
    assert transport.search_kwargs["size"] == 2
    assert transport.cleared == ["scroll-1"]


def test_query_caps_the_page_size_at_the_requested_size() -> None:
    """A limit above the page size does not enlarge the pages."""
    client, transport = _client([_page(["a"])])
    list(client.query(index="i", limit=10_000, size=100))
    assert transport.search_kwargs["size"] == 100


@pytest.mark.parametrize("limit", [0, -1])
def test_query_with_a_non_positive_limit_yields_nothing(limit: int) -> None:
    """Asking for no documents must not return one, nor open a scroll."""
    client, transport = _client([_page(["a"])])
    assert list(client.query(index="i", limit=limit)) == []
    assert transport.search_kwargs == {}
    assert transport.cleared == []


def test_query_releases_the_scroll_when_a_page_omits_its_id() -> None:
    """Elasticsearch may reuse the id and omit it from a scroll response.

    Overwriting the live id with *None* would both break the next scroll
    request and skip the cleanup that releases the context.
    """
    client, transport = _client([_page(["a"]), _page(["b"], scroll_id=None)])
    assert [d["id"] for d in client.query(index="i")] == ["a", "b"]
    assert transport.scroll_ids == ["scroll-1", "scroll-1"]
    assert transport.cleared == ["scroll-1"]


def test_query_releases_the_scroll_when_the_caller_stops_early() -> None:
    """Abandoning the generator still releases the server-side context."""
    client, transport = _client([_page(["a", "b"])])
    documents = cast(Generator[dict[str, Any], None, None], client.query(index="i"))
    next(documents)
    documents.close()
    assert transport.cleared == ["scroll-1"]


def test_query_survives_a_failing_clear_scroll(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cleanup must not replace the caller's result with an exception."""
    client, _ = _client([_page(["a"])], clear_scroll_fails=True)
    assert [d["id"] for d in client.query(index="i")] == ["a"]
    assert "failed to clear Elasticsearch scroll" in capsys.readouterr().err


def test_query_defaults_to_match_all() -> None:
    """With neither form of query, everything in the index matches."""
    client, transport = _client([_page([])])
    list(client.query(index="i"))
    assert transport.search_kwargs["query"] == {"match_all": {}}


def test_query_wraps_a_query_string() -> None:
    """Kibana-style syntax is wrapped in a ``query_string`` clause."""
    client, transport = _client([_page([])])
    list(client.query(index="i", query_string="status:200"))
    assert transport.search_kwargs["query"] == {"query_string": {"query": "status:200"}}


def test_query_passes_query_dsl_through() -> None:
    """Query DSL is sent as given, and takes precedence."""
    client, transport = _client([_page([])])
    dsl: dict[str, Any] = {"term": {"a": 1}}
    list(client.query(index="i", query=dsl, query_string="ignored"))
    assert transport.search_kwargs["query"] == dsl


def test_upload_documents_sends_one_bulk_request() -> None:
    """A whole batch costs one round trip, which is the point of the method."""
    client, transport = _client([])
    client.upload_documents(index="i", documents=[({"a": 1}, "x"), ({"a": 2}, "y")])
    assert transport.bulk_calls == 1
    assert transport.bulk_kwargs["index"] == "i"
    assert transport.bulk_kwargs["operations"] == [
        {"index": {"_id": "x"}},
        {"a": 1},
        {"index": {"_id": "y"}},
        {"a": 2},
    ]


def test_upload_documents_waits_longer_than_a_single_upload_would() -> None:
    """A batch is not one document, and the transport does not retry a timeout."""
    client, transport = _client([])
    client.upload_documents(index="i", documents=[({"a": 1}, "x")])
    assert transport.options_kwargs["request_timeout"] == _BULK_TIMEOUT


def test_upload_documents_omits_an_absent_document_id() -> None:
    """No id is not the same as an empty one; Elasticsearch assigns its own."""
    client, transport = _client([])
    client.upload_documents(index="i", documents=[({"a": 1}, None)])
    assert transport.bulk_kwargs["operations"] == [{"index": {}}, {"a": 1}]


def test_upload_documents_returns_one_result_per_document() -> None:
    """Callers report on each document, so results must line up with them."""
    client, _ = _client([])
    results = client.upload_documents(index="i", documents=[({"a": 1}, "x"), ({"a": 2}, "y")])
    assert results == [
        {"status": 201, "result": "created"},
        {"status": 201, "result": "created"},
    ]


def test_upload_documents_reports_a_rejected_document_without_raising() -> None:
    """``push-servers`` counts a rejection, so it must not lose the batch to one.

    This is the whole reason the method returns per-item results rather
    than a single verdict for the request.
    """
    rejection = {"status": 400, "error": {"type": "mapper_parsing_exception"}}
    client, _ = _client(
        [],
        bulk_response={
            "errors": True,
            "items": [{"index": rejection}, {"index": {"result": "created"}}],
        },
    )
    results = client.upload_documents(index="i", documents=[({"a": 1}, "x"), ({"a": 2}, "y")])
    assert results == [rejection, {"result": "created"}]


def test_upload_documents_rejects_a_response_that_skips_a_document() -> None:
    """Callers pair results with documents by position, so a gap is unusable."""
    client, _ = _client([], bulk_response={"errors": False, "items": [{"index": {}}]})
    with pytest.raises(
        ExternalServiceError,
        match="Bulk-uploading documents to Elasticsearch failed: 1 result",
    ):
        client.upload_documents(index="i", documents=[({"a": 1}, "x"), ({"a": 2}, "y")])


def test_upload_documents_with_nothing_to_send_makes_no_request() -> None:
    """An idle hub should cost nothing, and an empty bulk body is a 400."""
    client, transport = _client([])
    assert client.upload_documents(index="i", documents=[]) == []
    assert transport.bulk_calls == 0


def test_upload_documents_reports_a_failed_request_as_a_service_error() -> None:
    """A failure of the request as a whole is the one thing that still raises."""
    client, _ = _failing_client(_api_error())
    with pytest.raises(
        ExternalServiceError,
        match="Bulk-uploading documents to Elasticsearch failed",
    ):
        client.upload_documents(index="i", documents=[({"a": 1}, "x")])


def test_query_reports_a_failed_search_as_a_service_error() -> None:
    """The client speaks the application's error vocabulary, not the library's.

    Translating here is what lets every command drop the ``except
    AppError: raise`` / ``except Exception`` pair it used to need.
    """
    client, _ = _failing_client(TransportError("connection refused"))
    with pytest.raises(ExternalServiceError, match="Querying Elasticsearch failed"):
        list(client.query(index="i"))


def test_query_reports_a_failed_scroll_where_it_is_iterated() -> None:
    """A lazily consumed generator must still translate, and still clean up."""
    client, transport = _failing_client(TransportError("connection reset"), [_page(["a"])])
    documents = client.query(index="i")
    assert next(documents)["id"] == "a"
    with pytest.raises(ExternalServiceError, match="Querying Elasticsearch failed"):
        next(documents)
    assert transport.cleared == ["scroll-1"]


def test_query_reports_an_api_error_too() -> None:
    """``ApiError`` and ``TransportError`` are both roots of the hierarchy."""
    client, _ = _failing_client(_api_error())
    with pytest.raises(ExternalServiceError, match="Querying Elasticsearch failed"):
        list(client.query(index="i"))


def test_upload_document_reports_a_rejection_as_a_service_error() -> None:
    """The single-document path has no per-item result to report through."""
    client, _ = _failing_client(_api_error())
    with pytest.raises(
        ExternalServiceError,
        match="Uploading document to Elasticsearch failed",
    ):
        client.upload_document(index="i", document={"a": 1})


@pytest.mark.parametrize(("call", "message"), _API_KEY_OPERATIONS)
def test_each_api_key_operation_names_itself(
    call: Callable[[ElasticsearchClient], object],
    message: str,
) -> None:
    """A shared "request failed" message would be less useful than these."""
    client, _ = _failing_client(_api_error())
    with pytest.raises(ExternalServiceError, match=message):
        call(client)


def test_the_context_manager_closes_the_transport() -> None:
    """Leaving the block releases the connection."""
    client, transport = _client([])
    with client as entered:
        assert entered is client
    assert transport.closed is True


def _page(
    ids: list[str],
    *,
    scroll_id: str | None = "scroll-1",
) -> dict[str, Any]:
    """Build one search or scroll response carrying documents named *ids*."""
    page: dict[str, Any] = {"hits": {"hits": [{"_source": {"id": i}} for i in ids]}}
    if scroll_id is not None:
        page["_scroll_id"] = scroll_id
    return page


def _client(
    pages: list[dict[str, Any]],
    *,
    clear_scroll_fails: bool = False,
    bulk_response: dict[str, Any] | None = None,
) -> tuple[ElasticsearchClient, _StubTransport]:
    """Build a client whose transport serves *pages*."""
    client = ElasticsearchClient(endpoint="https://es.example.com:9200", api_key="k")
    transport = _StubTransport(
        pages,
        clear_scroll_fails=clear_scroll_fails,
        bulk_response=bulk_response,
    )
    client._client = transport  # type: ignore[assignment] # pylint: disable=protected-access
    return client, transport


def _failing_client(
    error: Exception,
    pages: list[dict[str, Any]] | None = None,
) -> tuple[ElasticsearchClient, _FailingTransport]:
    """Build a client whose transport fails with *error* once *pages* run out."""
    client = ElasticsearchClient(endpoint="https://es.example.com:9200", api_key="k")
    transport = _FailingTransport(error, pages)
    client._client = transport  # type: ignore[assignment] # pylint: disable=protected-access
    return client, transport


def _api_error() -> ApiError:
    """Build the kind of error Elasticsearch raises for a rejected request."""
    meta = cast(Any, _StubResponseMeta())
    return ApiError("rejected", meta, {"error": {"root_cause": [{"reason": "nope"}]}})
