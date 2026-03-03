"""
JupyterHub API client wrapper.
"""

from collections.abc import Iterator
from typing import Any, cast

import requests


class JupyterHubClient:
    """
    A wrapper around the JupyterHub REST API.

    This class provides a high-level interface for connecting to JupyterHub,
    authenticating with an API key, and querying user information.
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        ca_cert: str | None = None,
    ) -> None:
        """
        Initialize the JupyterHub client.

        Args:
            endpoint: The JupyterHub API endpoint URL (e.g., "https://localhost:8000/hub/api")
            api_key: The API key for authentication (used as a bearer token)
            ca_cert: Optional path to the CA certificate file for TLS verification

        Raises:
            ConnectionError: If unable to connect to or validate the JupyterHub endpoint
        """
        self._endpoint = endpoint.rstrip("/")
        self._api_key = api_key
        self._ca_cert = ca_cert

        # Build headers with bearer token
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        # Validate the connection  # fmt: off
        try:
            # Test connection with a simple GET request to the root API endpoint
            response = requests.get(
                self._endpoint,
                headers=self._headers,
                verify=ca_cert if ca_cert is not None else True,
                timeout=10,
            )

            # Check if the request was successful
            if response.status_code == 401:
                raise ConnectionError(f"Authentication failed for endpoint {endpoint}")
            if response.status_code == 403:
                raise ConnectionError("Access forbidden - API key might not have sufficient permissions")

            # Verify we got a valid response (handles any other error status codes)
            response.raise_for_status()

        except requests.exceptions.SSLError as e:
            raise ConnectionError(f"SSL verification failed for {endpoint}. Consider providing a CA certificate.") from e
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Unable to connect to JupyterHub at {endpoint}: {str(e)}") from e
        except requests.exceptions.Timeout as e:
            raise ConnectionError(f"Connection timeout when connecting to {endpoint}") from e
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to connect to JupyterHub at {endpoint}: {str(e)}") from e
        # fmt: on

    def list_users(
        self, state: str | None = None, limit: int = 100
    ) -> Iterator[dict[str, Any]]:
        """
        Get the list of users from JupyterHub using offset-based pagination.

        Args:
            state: Optional state filter for users (e.g., "active", "inactive").
                   If not provided, returns all users.
            limit: Number of users to fetch per page (default: 100).

        Yields:
            User dictionaries containing user information, one per user.

        Raises:
            ConnectionError: If the API request fails

        Example:
            .. code-block:: python

                # Get all users
                for user in client.list_users():
                    print(user["name"])

                # Get only active users
                for user in client.list_users(state="active"):
                    print(user["name"])
        """
        url = f"{self._endpoint}/users"
        offset = 0

        while True:
            params: dict[str, Any] = {"offset": offset, "limit": limit}
            if state is not None:
                params["state"] = state

            try:
                response = requests.get(
                    url,
                    headers=self._headers,
                    params=params,
                    verify=self._ca_cert if self._ca_cert is not None else True,
                    timeout=30,
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                raise ConnectionError(f"Failed to list users: {str(e)}") from e

            page = cast(list[dict[str, Any]], response.json())
            yield from page

            if len(page) < limit:
                break

            offset += limit

    def list_servers(self) -> list[dict[str, Any]]:
        """
        Get the list of servers from JupyterHub.

        This method retrieves all users and extracts their servers,
        flattening nested dictionaries by concatenating keys with ".".

        Returns:
            A list of dictionaries containing server information.
            Each dictionary has flattened keys (e.g., "user.name", "server.state").

        Raises:
            ConnectionError: If the API request fails

        Example:
            .. code-block:: python

                servers = client.list_servers()
                for server in servers:
                    print(f"User: {server['user.name']}, State: {server.get('server.state')}")
        """
        users = self.list_users()
        result = []

        for user in users:
            # Check if this user has any servers
            servers = user.get("servers", {})
            if not servers:
                continue

            # Process each server for this user
            for server_name, server_info in servers.items():
                if server_info:
                    server_data = self._flatten_dict(
                        {
                            "user": {
                                "name": user.get("name"),
                            },
                            "server": {
                                "name": server_name,
                                **server_info,
                            },
                        }
                    )
                    result.append(server_data)

        return result

    def _flatten_dict(
        self, d: dict[str, Any], parent_key: str = "", sep: str = "."
    ) -> dict[str, Any]:
        """
        Flatten a nested dictionary by concatenating keys with a separator.

        Args:
            d: Dictionary to flatten
            parent_key: Parent key for recursion
            sep: Separator to use between keys (default: ".")

        Returns:
            Flattened dictionary with concatenated keys
        """
        items: list[tuple] = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
