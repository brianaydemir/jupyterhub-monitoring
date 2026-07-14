"""Elasticsearch API client wrapper."""

import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

from elasticsearch import ApiError, Elasticsearch, TransportError

_SCROLL_TIMEOUT = "2m"


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

        Raises:
            elasticsearch.ApiError: If Elasticsearch rejects the document.
            elasticsearch.TransportError: If a connection-level error occurs.
        """
        return cast(
            dict[str, Any],
            self._client.index(index=index, id=doc_id, document=document),
        )

    def upload_documents(
        self,
        index: str,
        documents: Iterator[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Upload documents to an Elasticsearch index.

        Raises:
            elasticsearch.ApiError: If Elasticsearch
                rejects a document upload.
            elasticsearch.TransportError: If a
                connection-level error occurs.
        """
        results: list[dict[str, Any]] = []
        for document in documents:
            result = self.upload_document(index=index, document=document)
            results.append(result)
        return results

    def query(
        self,
        index: str,
        query: dict[str, Any] | None = None,
        query_string: str | None = None,
        size: int = 100,
        sort: list[dict[str, Any]] | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Query an index and yield ``_source`` documents.

        Accepts either Query DSL (*query*) or query-string
        syntax (*query_string*).  When both are ``None``,
        the query defaults to ``match_all``.  Results are
        streamed via scroll pagination.  When *limit* is
        set, yielded results are capped and page size is
        also capped to avoid over-fetching.

        Raises:
            elasticsearch.ApiError: If Elasticsearch
                rejects a search or scroll request.
            elasticsearch.TransportError: If a
                transport-level failure occurs.
        """
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

        page_size = min(size, limit) if limit is not None else size

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
                response = self._client.scroll(
                    scroll_id=scroll_id,
                    scroll=_SCROLL_TIMEOUT,
                )
                scroll_id = response.get("_scroll_id")
                hits = response["hits"]["hits"]

        finally:
            if scroll_id:
                try:
                    self._client.clear_scroll(scroll_id=scroll_id)
                except (ApiError, TransportError) as exc:
                    print(
                        f"Warning: failed to clear Elasticsearch scroll: {exc}",
                        file=sys.stderr,
                    )

    def close(self) -> None:
        """Close the underlying transport.

        Prefer using the client as a context manager.
        """
        self._client.close()

    def __enter__(self) -> "ElasticsearchClient":
        """Support for context manager protocol."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Support for context manager protocol."""
        self.close()

    def create_api_key(
        self,
        key_name: str | None = None,
        expiration: str | None = None,
        username: str | None = None,
    ) -> dict[str, Any]:
        """Create an Elasticsearch API key.

        Raises:
            elasticsearch.ApiError: If Elasticsearch
                rejects the request.
        """
        name = key_name or (f"api-key-{username}" if username else "api-key")
        metadata: dict[str, Any] | None = {"created_by": username} if username else None
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

        Exactly one of *key_id* or *key_name* must be
        provided.  The request is scoped to the
        authenticated user's own keys (``owner=True``).

        Raises:
            ValueError: If neither or both of
                *key_id* / *key_name* are provided.
            elasticsearch.ApiError: If Elasticsearch
                rejects the request.
        """
        if key_id is None and key_name is None:
            raise ValueError("Either key_id or key_name must be provided")
        if key_id is not None and key_name is not None:
            raise ValueError("Only one of key_id or key_name may be provided")
        response = self._client.security.invalidate_api_key(
            id=key_id,
            name=key_name,
            owner=True,
        )
        return cast(dict[str, Any], response)

    def list_api_keys(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        """List API keys owned by the authenticated user.

        Raises:
            elasticsearch.ApiError: If Elasticsearch
                rejects the request.
        """
        response = self._client.security.get_api_key(
            owner=True,
            active_only=active_only,
        )
        return cast(list[dict[str, Any]], response.get("api_keys", []))
