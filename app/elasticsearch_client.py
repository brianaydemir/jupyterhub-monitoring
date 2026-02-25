"""
Elasticsearch API client wrapper.
"""

from typing import Any, Iterator, cast

import requests
from elasticsearch import Elasticsearch
from requests.auth import HTTPBasicAuth


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
        ca_cert: str | None = None,
    ) -> None:
        """
        Initialize the Elasticsearch client.

        Args:
            endpoint: The Elasticsearch API endpoint URL (e.g., "https://localhost:9200")
            api_key: The API key for authentication
            ca_cert: Optional path to the CA certificate file for TLS verification
        """
        # Build connection parameters
        connection_params: dict[str, Any] = {
            "hosts": [endpoint],
            "api_key": api_key,
        }

        # Add CA certificate if provided
        if ca_cert is not None:
            connection_params["ca_certs"] = ca_cert
            connection_params["verify_certs"] = True

        # Initialize the Elasticsearch client
        self._client = Elasticsearch(**connection_params)

    def upload_document(
        self,
        index: str,
        document: dict[str, Any],
        doc_id: str | None = None,
    ) -> dict[str, Any]:
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
            dict[str, Any],
            self._client.index(index=index, id=doc_id, document=document),
        )

    def upload_documents(
        self,
        index: str,
        documents: Iterator[dict[str, Any]],
    ) -> list[dict[str, Any]]:
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
            elasticsearch.ApiError: If Elasticsearch rejects a document upload
            elasticsearch.TransportError: If a connection-level error occurs
        """
        results = []
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
    ) -> Iterator[dict[str, Any]]:
        """
        Query an Elasticsearch index and return an iterator over the results.

        This method automatically handles pagination using the scroll API,
        so the caller doesn't need to worry about large result sets.

        Supports both structured Query DSL queries and Kibana-style query strings.

        Args:
            index: The name of the index to query
            query: Optional Query DSL query (as a dictionary)
            query_string: Optional Kibana-style query string (e.g., "status:200 AND user:alice")
            size: Number of documents to retrieve per scroll request (default: 100)

        Returns:
            An iterator yielding documents matching the query

        Raises:
            elasticsearch.ApiError: If Elasticsearch rejects the query or scroll request
            elasticsearch.TransportError: If a connection-level error occurs

        Example:
            # Using Query DSL
            client.query(
                index="logs",
                query={"match": {"status": "error"}}
            )

            # Using Kibana-style query string
            client.query(
                index="logs",
                query_string="status:error AND level:critical"
            )
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
                except Exception:  # nosec  # pylint: disable=broad-exception-caught
                    # Ignore errors when clearing scroll - cleanup failures
                    # should not mask results
                    pass

    def close(self) -> None:
        """
        Close the Elasticsearch client connection.

        It's recommended to call this method when done using the client,
        or else use the client as a context manager.
        """
        self._client.close()

    def __enter__(self) -> "ElasticsearchClient":
        """Support for context manager protocol."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Support for context manager protocol."""
        self.close()

    @classmethod
    def create_api_key_with_basic_auth(
        cls,
        endpoint: str,
        username: str,
        password: str,
        ca_cert: str | None = None,
        key_name: str | None = None,
        expiration: str | None = None,
    ) -> dict[str, Any]:
        """
        Create an Elasticsearch API key using basic authentication.

        This method uses the user's credentials to create an API key with the same
        permissions as the user (based on their assigned roles).

        Args:
            endpoint: The Elasticsearch API endpoint URL (e.g., "https://localhost:9200")
            username: The username for basic authentication
            password: The password for basic authentication
            ca_cert: Optional path to the CA certificate file for TLS verification
            key_name: Optional name for the API key (defaults to "api-key-{username}")
            expiration: Optional expiration time (e.g., "1d", "7d", "30d").
                        If not provided, the key never expires.

        Returns:
            A dictionary containing the API key information with these keys:
            - id: The API key ID
            - name: The name of the API key
            - api_key: The API key secret (base64 encoded)
            - encoded: The complete encoded API key (id:api_key)
            - expiration: The expiration timestamp (if set)

        Raises:
            requests.exceptions.HTTPError: If the API request fails
            requests.exceptions.ConnectionError: If unable to connect to Elasticsearch
            ValueError: If authentication fails or the response is invalid

        Example:
            result = ElasticsearchClient.create_api_key_with_basic_auth(
                endpoint="https://localhost:9200",
                username="myuser",
                password="mypassword",
                key_name="my-api-key",
                expiration="7d"
            )
            print(result["encoded"])  # Use this for authentication
        """
        # Build the API key request body
        # Note: By not specifying role_descriptors, the API key will
        # automatically inherit all the user's permissions
        api_key_body: dict[str, Any] = {
            "metadata": {
                "created_by": username,
            },
        }

        # Set the API key name
        if key_name:
            api_key_body["name"] = key_name
        else:
            api_key_body["name"] = f"api-key-{username}"

        # Set the expiration if provided
        if expiration:
            api_key_body["expiration"] = expiration

        # Build the URL for the API key creation endpoint
        url = f"{endpoint.rstrip('/')}/_security/api_key"

        # Set up TLS verification
        verify: bool | str = True
        if ca_cert:
            verify = ca_cert

        # Make the request to create the API key
        try:
            response = requests.post(
                url,
                json=api_key_body,
                auth=HTTPBasicAuth(username, password),
                verify=verify,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Failed to create API key: {str(e)}") from e

        # Parse the response
        try:
            result = response.json()
        except ValueError as e:
            raise ValueError(
                f"Invalid JSON response from Elasticsearch: {str(e)}"
            ) from e

        # Validate that we got the expected fields
        if "id" not in result or "api_key" not in result:
            raise ValueError(
                "Unexpected response from Elasticsearch: missing required fields"
            )

        return cast(dict[str, Any], result)

    @classmethod
    def delete_api_key_with_basic_auth(
        cls,
        endpoint: str,
        username: str,
        password: str,
        ca_cert: str | None = None,
        *,
        key_id: str | None = None,
        key_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Invalidate an Elasticsearch API key using basic authentication.

        Exactly one of `key_id` or `key_name` must be provided.

        Args:
            endpoint: The Elasticsearch API endpoint URL (e.g., "https://localhost:9200")
            username: The username for basic authentication
            password: The password for basic authentication
            ca_cert: Optional path to the CA certificate file for TLS verification
            key_id: The ID of the API key to invalidate
            key_name: The name of the API key(s) to invalidate

        Returns:
            A dictionary containing:
            - invalidated_api_keys: list of key IDs just invalidated
            - previously_invalidated_api_keys: keys that were already invalid
            - error_count: number of errors encountered

        Raises:
            ValueError: If neither or both of key_id/key_name are provided,
                        if authentication fails, or if the response is invalid
        """
        if not key_id and not key_name:
            raise ValueError("Either key_id or key_name must be provided")
        if key_id and key_name:
            raise ValueError("Only one of key_id or key_name may be provided")

        body: dict[str, Any] = {}
        if key_id:
            body["id"] = key_id
        else:
            body["name"] = key_name

        url = f"{endpoint.rstrip('/')}/_security/api_key"

        verify: bool | str = True
        if ca_cert:
            verify = ca_cert

        try:
            response = requests.delete(
                url,
                json=body,
                auth=HTTPBasicAuth(username, password),
                verify=verify,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Failed to invalidate API key: {str(e)}") from e

        try:
            result = response.json()
        except ValueError as e:
            raise ValueError(
                f"Invalid JSON response from Elasticsearch: {str(e)}"
            ) from e

        return cast(dict[str, Any], result)

    @classmethod
    def list_api_keys_with_basic_auth(
        cls,
        endpoint: str,
        username: str,
        password: str,
        ca_cert: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        List the active API keys owned by the authenticated user.

        Uses GET /_security/api_key?active_only=true&owner=true to return
        only non-expired, non-invalidated keys belonging to the user.

        Args:
            endpoint: The Elasticsearch API endpoint URL (e.g., "https://localhost:9200")
            username: The username for basic authentication
            password: The password for basic authentication
            ca_cert: Optional path to the CA certificate file for TLS verification

        Returns:
            A list of dictionaries, each describing one API key with fields
            such as id, name, creation, expiration, invalidated, username,
            and realm.

        Raises:
            requests.exceptions.HTTPError: If the API request fails
            requests.exceptions.ConnectionError: If unable to connect to Elasticsearch
            ValueError: If authentication fails or the response is invalid
        """
        url = f"{endpoint.rstrip('/')}/_security/api_key"

        verify: bool | str = True
        if ca_cert:
            verify = ca_cert

        try:
            response = requests.get(
                url,
                params={"active_only": "true", "owner": "true"},
                auth=HTTPBasicAuth(username, password),
                verify=verify,
                timeout=30,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Failed to list API keys: {str(e)}") from e

        try:
            result = response.json()
        except ValueError as e:
            raise ValueError(
                f"Invalid JSON response from Elasticsearch: {str(e)}"
            ) from e

        return cast(list[dict[str, Any]], result.get("api_keys", []))
