"""Elasticsearch API client wrapper."""

import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

from elasticsearch import ApiError, Elasticsearch, TransportError


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

        Args:
            endpoint: The Elasticsearch API endpoint URL (e.g., "https://localhost:9200")
            api_key: The API key for authentication
            ca_cert: Optional path to the CA certificate file for TLS verification
            basic_auth: A ``(username, password)`` tuple for basic authentication

        Raises:
            ValueError: If both or neither of *api_key* and *basic_auth* are provided.
        """
        if (api_key is None) == (basic_auth is None):
            raise ValueError("Exactly one of api_key or basic_auth must be provided")

        # Build connection parameters
        connection_params: dict[str, Any] = {"hosts": [endpoint]}

        if api_key is not None:
            connection_params["api_key"] = api_key
        else:
            connection_params["basic_auth"] = basic_auth

        # Add CA certificate if provided
        if ca_cert is not None:
            connection_params["ca_certs"] = str(ca_cert)
            connection_params["verify_certs"] = True

        # Initialize the Elasticsearch client
        self._client = Elasticsearch(**connection_params)

    def upload_document(
        self,
        index: str,
        document: dict[str, Any],
        doc_id: str | None = None,
    ) -> dict[str, Any]:
        """Upload a single document to an Elasticsearch index.

        Args:
            index: The name of the index to upload to
            document: The document to upload (as a dictionary)
            doc_id: Optional document ID. If not provided, Elasticsearch generates one

        Returns:
            The response from Elasticsearch containing the upload result

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
        """Upload multiple documents to an Elasticsearch index from an iterator.

        Args:
            index: The name of the index to upload to
            documents: An iterator yielding documents (as dictionaries)

        Returns:
            A list of responses from Elasticsearch for each uploaded document

        Raises:
            elasticsearch.ApiError: If Elasticsearch rejects a document upload
            elasticsearch.TransportError: If a connection-level error occurs
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
        """Query an index and yield matching ``_source`` documents.

        Accepts either Query DSL (*query*) or query-string syntax
        (*query_string*). When both are ``None``, the query defaults to
        ``match_all``. Results are streamed via Elasticsearch scroll pagination.
        When *limit* is set, yielded results are capped and page size is also
        capped to avoid over-fetching.

        Args:
            index: Name of the index to query.
            query: Optional Query DSL dictionary.
            query_string: Optional Kibana-style query string.
            size: Number of documents to request per scroll page.
            sort: Optional Elasticsearch sort definition list.
            limit: Optional maximum number of documents to yield.

        Yields:
            Matching document ``_source`` dictionaries.

        Raises:
            elasticsearch.ApiError: If Elasticsearch rejects a search or scroll request.
            elasticsearch.TransportError: If a transport-level failure occurs.
        """
        # Build the query body
        query_body: dict[str, Any] = {}

        if query is not None:
            # Use the provided Query DSL query
            query_body["query"] = query
        elif query_string is not None:
            # Convert query string to Query DSL
            query_body["query"] = {
                "query_string": {
                    "query": query_string,
                }
            }
        else:
            # If no query provided, match all documents
            query_body["query"] = {"match_all": {}}

        if sort is not None:
            query_body["sort"] = sort

        # Cap the page size at the limit to avoid fetching more than needed
        page_size = min(size, limit) if limit is not None else size

        # Initialize scroll
        scroll_timeout = "2m"
        response = self._client.search(
            index=index,
            body=query_body,
            scroll=scroll_timeout,
            size=page_size,
        )

        # Get the scroll ID
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
                    scroll=scroll_timeout,
                )
                scroll_id = response.get("_scroll_id")
                hits = response["hits"]["hits"]

        finally:
            # Clean up the scroll context
            if scroll_id:
                try:
                    self._client.clear_scroll(scroll_id=scroll_id)
                except (ApiError, TransportError) as exc:
                    print(
                        f"Warning: failed to clear Elasticsearch scroll: {exc}", file=sys.stderr
                    )

    def close(self) -> None:
        """Prefer using the client as a context manager; call explicitly when
        that is not possible.
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
        """Create an Elasticsearch API key using the client's current credentials.

        Args:
            key_name: Name for the API key. Defaults to ``"api-key-{username}"``
                when *username* is provided, otherwise ``"api-key"``.
            expiration: Expiration time in Elasticsearch duration format (e.g.,
                ``"7d"``, ``"30d"``). Omit for a key that never expires.
            username: Username to record in the key's metadata as ``created_by``.
                Typically provided when authenticating with basic auth.

        Returns:
            A dictionary containing the API key information (id, name, api_key,
            encoded, and optionally expiration).

        Raises:
            elasticsearch.ApiError: If Elasticsearch rejects the request
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
        """Invalidate an Elasticsearch API key owned by the authenticated user.

        Exactly one of *key_id* or *key_name* must be provided. The request is
        scoped to the authenticated user's own keys (``owner=True``).

        Args:
            key_id: The ID of the API key to invalidate.
            key_name: The name of the API key(s) to invalidate.

        Returns:
            A dictionary with ``invalidated_api_keys``,
            ``previously_invalidated_api_keys``, and ``error_count``.

        Raises:
            ValueError: If neither or both of *key_id* / *key_name* are provided.
            elasticsearch.ApiError: If Elasticsearch rejects the request.
        """
        if not key_id and not key_name:
            raise ValueError("Either key_id or key_name must be provided")
        if key_id and key_name:
            raise ValueError("Only one of key_id or key_name may be provided")
        response = self._client.security.invalidate_api_key(
            id=key_id,
            name=key_name,
            owner=True,
        )
        return cast(dict[str, Any], response)

    def list_api_keys(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        """List API keys owned by the authenticated user.

        Args:
            active_only: When ``True`` (default), return only non-expired and
                non-invalidated keys. When ``False``, return all keys.

        Returns:
            A list of dictionaries describing each API key (id, name, creation,
            expiration, invalidated, username, realm, etc.).

        Raises:
            elasticsearch.ApiError: If Elasticsearch rejects the request.
        """
        response = self._client.security.get_api_key(
            owner=True,
            active_only=active_only,
        )
        return cast(list[dict[str, Any]], response.get("api_keys", []))
