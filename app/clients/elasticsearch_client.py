"""Elasticsearch API client wrapper."""

import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Any, Self, cast

from elasticsearch import ApiError, Elasticsearch, TransportError

from app.core.errors import ExternalServiceError

_SCROLL_TIMEOUT = "2m"

# Documents per scroll page.
# Snapshot documents are small, so a large page keeps a full-index export
# to a few hundred round trips.
_DEFAULT_PAGE_SIZE = 5000

# Client-side wait, in seconds, for a bulk request.
# The transport's ten-second default is tight for a whole batch,
# and a request that times out has probably been applied anyway,
# so its documents would be miscounted as failures.
_BULK_TIMEOUT = 30


@contextmanager
def _translating(operation: str) -> Iterator[None]:
    """Re-raise an Elasticsearch failure as an :class:`ExternalServiceError`.

    *operation* names what was being attempted, so each method reports itself
    and callers do not have to translate at their own boundary.
    ``ApiError`` and ``TransportError``
    are the two roots of the client library's hierarchy,
    so between them they cover every remote failure.

    Raises:
        ExternalServiceError: If the wrapped block fails.
    """
    try:
        yield
    except (ApiError, TransportError) as e:
        raise ExternalServiceError(f"{operation} failed: {e}") from e


class ElasticsearchClient:
    """A wrapper around the official Python client for Elasticsearch."""

    def __init__(
        self,
        endpoint: str,
        api_key: str | None = None,
        ca_cert: str | Path | None = None,
        basic_auth: tuple[str, str] | None = None,
    ) -> None:
        """Exactly one of *api_key* or *basic_auth* must be provided.

        Raises:
            ValueError: If both or neither are provided.
        """
        if (api_key is None) == (basic_auth is None):
            raise ValueError("Exactly one of api_key or basic_auth must be provided")

        connection_params: dict[str, Any] = {"hosts": [endpoint]}

        if api_key is not None:
            connection_params["api_key"] = api_key
        else:
            connection_params["basic_auth"] = basic_auth

        if ca_cert is not None:
            connection_params["ca_certs"] = str(ca_cert)
            connection_params["verify_certs"] = True

        self._client = Elasticsearch(**connection_params)

    def upload_document(
        self,
        index: str,
        document: dict[str, Any],
        doc_id: str | None = None,
    ) -> dict[str, Any]:
        """Upload a single document to an Elasticsearch index.

        Provided for a caller with one document to store;
        ``push-servers`` sends whole batches through :meth:`upload_documents`.

        Raises:
            ExternalServiceError: If Elasticsearch rejects the document,
                or a connection-level error occurs.
        """
        with _translating("Uploading document to Elasticsearch"):
            return cast(
                dict[str, Any],
                self._client.index(index=index, id=doc_id, document=document),
            )

    def upload_documents(
        self,
        index: str,
        documents: Sequence[tuple[dict[str, Any], str | None]],
    ) -> list[dict[str, Any]]:
        """Upload documents to an Elasticsearch index in one bulk request.

        Each pair is a document and the id to store it under;
        a *None* id lets Elasticsearch assign one.
        The operation is ``index``,
        so a repeated id overwrites the document already there
        instead of conflicting with it.

        Returns one result per document, in the order they were given,
        each carrying that item's ``status``
        and either a ``result`` or an ``error``.
        A document Elasticsearch rejects is described in its own result
        rather than raised, which is what lets a caller count it
        and still keep the rest of the batch.
        Only a failure of the request as a whole raises,
        and a request that times out may have been applied regardless,
        so a caller's count of what it stored is a lower bound.

        Raises:
            ExternalServiceError: If the request fails,
                or the response does not describe every document.
        """
        if not documents:
            return []

        operations: list[dict[str, Any]] = []
        for document, doc_id in documents:
            action: dict[str, Any] = {} if doc_id is None else {"_id": doc_id}
            operations.append({"index": action})
            operations.append(document)

        with _translating("Bulk-uploading documents to Elasticsearch"):
            response = self._client.options(request_timeout=_BULK_TIMEOUT).bulk(
                index=index,
                operations=operations,
            )

        items = cast(list[dict[str, Any]], response.get("items", []))
        if len(items) != len(documents):
            counted = f"{len(items)} result(s) for {len(documents)} document(s)"
            raise ExternalServiceError(
                f"Bulk-uploading documents to Elasticsearch failed: {counted}"
            )

        # Every item is keyed by the operation it answers,
        # and the only operation this method sends is ``index``.
        return [cast(dict[str, Any], item.get("index", {})) for item in items]

    def query(
        self,
        index: str,
        query: dict[str, Any] | None = None,
        query_string: str | None = None,
        size: int = _DEFAULT_PAGE_SIZE,
        sort: list[dict[str, Any]] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query an index and yield ``_source`` documents.

        Accepts either Query DSL (*query*) or query-string syntax
        (*query_string*); with neither, the query is ``match_all``.
        Results are streamed via scroll pagination in pages of *size*.
        *limit* caps both the results yielded and the page size;
        a *limit* of zero or less yields nothing.

        Because this is a generator, no request is made
        until it is first iterated, and failures surface there
        rather than at the call that created it.

        Raises:
            ExternalServiceError: If a search or scroll request fails.
        """
        if limit is not None and limit < 1:
            return

        if query is not None:
            es_query: dict[str, Any] = query
        elif query_string is not None:
            es_query = {
                "query_string": {
                    "query": query_string,
                }
            }
        else:
            es_query = {"match_all": {}}

        page_size = max(1, min(size, limit)) if limit is not None else size

        with _translating("Querying Elasticsearch"):
            response = self._client.search(
                index=index,
                query=es_query,
                sort=sort,
                scroll=_SCROLL_TIMEOUT,
                size=page_size,
            )

        scroll_id = response.get("_scroll_id")

        try:
            count = 0
            hits = response["hits"]["hits"]

            while hits:
                for hit in hits:
                    yield hit["_source"]
                    count += 1
                    if limit is not None and count >= limit:
                        return
                with _translating("Querying Elasticsearch"):
                    response = self._client.scroll(
                        scroll_id=scroll_id,
                        scroll=_SCROLL_TIMEOUT,
                    )
                # Elasticsearch may reuse the id and omit it from the response.
                # Overwriting it with None would both break the next scroll
                # and skip the cleanup below.
                scroll_id = response.get("_scroll_id") or scroll_id
                hits = response["hits"]["hits"]

        finally:
            if scroll_id:
                try:
                    self._client.clear_scroll(scroll_id=scroll_id)
                # Never let cleanup replace the caller's control flow,
                # including an exception already on its way out.
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    print(
                        f"Warning: failed to clear Elasticsearch scroll: {exc}",
                        file=sys.stderr,
                    )

    def close(self) -> None:
        """Close the underlying transport.

        Prefer using the client as a context manager.
        """
        self._client.close()

    def __enter__(self) -> Self:
        """Support for context manager protocol."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Support for context manager protocol."""
        self.close()

    def create_api_key(
        self,
        key_name: str | None = None,
        expiration: str | None = None,
        username: str | None = None,
    ) -> dict[str, Any]:
        """Create an Elasticsearch API key.

        Without *key_name* the key is named ``api-key-{username}``,
        or ``api-key`` when no *username* is given.
        A *username* is also recorded as ``created_by`` metadata on the key.

        No ``role_descriptors`` are sent, deliberately,
        so the key carries every privilege of the authenticated user.
        Narrow such a key by creating it as an account
        that already holds only the privileges it needs,
        rather than as a superuser.

        Raises:
            ExternalServiceError: If the request fails.
        """
        name = key_name or (f"api-key-{username}" if username else "api-key")
        metadata: dict[str, Any] | None = {"created_by": username} if username else None
        with _translating("Creating API key"):
            response = self._client.security.create_api_key(
                name=name,
                expiration=expiration,
                metadata=metadata,
            )
        return cast(dict[str, Any], response)

    def delete_api_key(
        self,
        *,
        key_id: str | None = None,
        key_name: str | None = None,
    ) -> dict[str, Any]:
        """Invalidate an Elasticsearch API key.

        Exactly one of *key_id* or *key_name* must be provided.
        The request is scoped to the authenticated user's own keys
        (``owner=True``).

        Raises:
            ValueError: If neither or both of *key_id* / *key_name*
                are provided.
            ExternalServiceError: If the request fails.
        """
        if key_id is None and key_name is None:
            raise ValueError("Either key_id or key_name must be provided")
        if key_id is not None and key_name is not None:
            raise ValueError("Only one of key_id or key_name may be provided")
        with _translating("Invalidating API key"):
            response = self._client.security.invalidate_api_key(
                id=key_id,
                name=key_name,
                owner=True,
            )
        return cast(dict[str, Any], response)

    def list_api_keys(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        """List API keys owned by the authenticated user.

        Raises:
            ExternalServiceError: If the request fails.
        """
        with _translating("Listing API keys"):
            response = self._client.security.get_api_key(
                owner=True,
                active_only=active_only,
            )
        return cast(list[dict[str, Any]], response.get("api_keys", []))
