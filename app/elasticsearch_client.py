"""
Elasticsearch client wrapper for querying and uploading documents.
"""

from typing import Any, Dict, Iterator, Optional, cast

from elasticsearch import Elasticsearch


class ElasticsearchClient:
    """
    A wrapper around the official Python client for Elasticsearch.

    This class provides a high-level interface for connecting to Elasticsearch,
    uploading documents, and querying indices with automatic pagination support.
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        ca_cert: Optional[str] = None,
    ) -> None:
        """
        Initialize the Elasticsearch client and validate the connection.

        Args:
            endpoint: The Elasticsearch API endpoint URL (e.g., "https://localhost:9200")
            api_key: The API key for authentication
            ca_cert: Optional path to the CA certificate file for TLS verification

        Raises:
            ConnectionError: If unable to connect to the Elasticsearch endpoint
            ValueError: If authentication fails or endpoint is invalid
        """
        # Build connection parameters
        connection_params: Dict[str, Any] = {
            "hosts": [endpoint],
            "api_key": api_key,
        }

        # Add CA certificate if provided
        if ca_cert is not None:
            connection_params["ca_certs"] = ca_cert
            connection_params["verify_certs"] = True

        # Initialize the Elasticsearch client
        self._client = Elasticsearch(**connection_params)

        # Validate the connection
        try:
            if not self._client.ping():
                raise ConnectionError(f"Unable to connect to Elasticsearch at {endpoint}")
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Elasticsearch at {endpoint}: {str(e)}"
            ) from e

    def upload_document(
        self,
        index: str,
        document: Dict[str, Any],
        doc_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Upload a single document to an Elasticsearch index.

        Args:
            index: The name of the index to upload to
            document: The document to upload (as a dictionary)
            doc_id: Optional document ID. If not provided, Elasticsearch generates one

        Returns:
            The response from Elasticsearch containing the upload result

        Raises:
            Exception: If the upload fails
        """
        return cast(
            Dict[str, Any], self._client.index(index=index, id=doc_id, document=document)
        )

    def upload_documents(
        self,
        index: str,
        documents: Iterator[Dict[str, Any]],
    ) -> list[Dict[str, Any]]:
        """
        Upload multiple documents to an Elasticsearch index from an iterator.

        This method accepts any iterator (e.g., list, generator) and uploads
        all documents returned by it.

        Args:
            index: The name of the index to upload to
            documents: An iterator yielding documents (as dictionaries)

        Returns:
            A list of responses from Elasticsearch for each uploaded document

        Raises:
            Exception: If any upload fails
        """
        results = []
        for document in documents:
            result = self.upload_document(index=index, document=document)
            results.append(result)
        return results

    def query(
        self,
        index: str,
        query: Optional[Dict[str, Any]] = None,
        query_string: Optional[str] = None,
        size: int = 100,
    ) -> Iterator[Dict[str, Any]]:
        """
        Query an Elasticsearch index and return an iterator over the results.

        This method automatically handles pagination using the scroll API,
        so the caller doesn't need to worry about large result sets.

        Supports both structured Query DSL queries and Kibana-style query strings.

        Args:
            index: The name of the index to query
            query: Optional Query DSL query (as a dictionary)
            query_string: Optional Kibana-style query string (e.g., "status:200 AND user:john")
            size: Number of documents to retrieve per scroll request (default: 100)

        Returns:
            An iterator yielding documents matching the query

        Raises:
            ValueError: If neither query nor query_string is provided
            Exception: If the query fails

        Example:
            # Using Query DSL
            client.query(
                index="logs",
                query={"match": {"status": "error"}}
            )

            # Using query string
            client.query(
                index="logs",
                query_string="status:error AND level:critical"
            )
        """
        # Build the query body
        query_body: Dict[str, Any] = {}

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

        # Initialize scroll
        scroll_timeout = "2m"
        response = self._client.search(
            index=index,
            body=query_body,
            scroll=scroll_timeout,
            size=size,
        )

        # Get the scroll ID
        scroll_id = response.get("_scroll_id")

        try:
            # Yield documents from the first batch
            hits = response["hits"]["hits"]
            for hit in hits:
                yield hit["_source"]

            # Continue scrolling until no more results
            while hits:
                response = self._client.scroll(
                    scroll_id=scroll_id,
                    scroll=scroll_timeout,
                )
                scroll_id = response.get("_scroll_id")
                hits = response["hits"]["hits"]

                for hit in hits:
                    yield hit["_source"]

        finally:
            # Clean up the scroll context
            if scroll_id:
                try:
                    self._client.clear_scroll(scroll_id=scroll_id)
                except Exception:  # pylint: disable=broad-exception-caught
                    # Ignore errors when clearing scroll - cleanup failures
                    # should not mask results
                    pass

    def close(self) -> None:
        """
        Close the Elasticsearch client connection.

        It's recommended to call this method when done using the client,
        or use the client as a context manager.
        """
        self._client.close()

    def __enter__(self) -> "ElasticsearchClient":
        """Support for context manager protocol."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Support for context manager protocol."""
        self.close()
