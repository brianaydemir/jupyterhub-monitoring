"""
JupyterHub REST API client wrapper.
"""

from typing import Any, Dict, List, Optional

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
        ca_cert: Optional[str] = None,
    ) -> None:
        """
        Initialize the JupyterHub client and validate the connection.

        Args:
            endpoint: The JupyterHub API endpoint URL (e.g., "https://localhost:8000/hub/api")
            api_key: The API key for authentication (used as bearer token)
            ca_cert: Optional path to the CA certificate file for TLS verification

        Raises:
            ConnectionError: If unable to connect to the JupyterHub endpoint
            ValueError: If authentication fails or endpoint is invalid
        """
        self._endpoint = endpoint.rstrip("/")
        self._api_key = api_key
        self._ca_cert = ca_cert

        # Build headers with bearer token
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        # Validate the connection
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
                raise ValueError(f"Authentication failed for endpoint {endpoint}")
            elif response.status_code == 403:
                raise ValueError(
                    f"Access forbidden - API key may not have sufficient permissions"
                )
            elif response.status_code >= 400:
                raise ConnectionError(
                    f"Failed to connect to JupyterHub at {endpoint}: "
                    f"HTTP {response.status_code}"
                )

            # Verify we got a valid response
            response.raise_for_status()

        except requests.exceptions.SSLError as e:
            raise ConnectionError(
                f"SSL verification failed for {endpoint}. "
                "Consider providing a CA certificate."
            ) from e
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(
                f"Unable to connect to JupyterHub at {endpoint}: {str(e)}"
            ) from e
        except requests.exceptions.Timeout as e:
            raise ConnectionError(
                f"Connection timeout when connecting to {endpoint}"
            ) from e
        except requests.exceptions.RequestException as e:
            raise ConnectionError(
                f"Failed to connect to JupyterHub at {endpoint}: {str(e)}"
            ) from e

    def list_users(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get the list of users from JupyterHub.

        Args:
            state: Optional state filter for users (e.g., "active", "inactive").
                   If not provided, returns all users.

        Returns:
            A list of user dictionaries containing user information

        Raises:
            Exception: If the API request fails

        Example:
            # Get all users
            all_users = client.list_users()

            # Get only active users
            active_users = client.list_users(state="active")
        """
        url = f"{self._endpoint}/users"

        # Build query parameters
        params: Dict[str, Any] = {}
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

            # Check for authentication/authorization errors
            if response.status_code == 401:
                raise ValueError("Authentication failed - invalid API key")
            elif response.status_code == 403:
                raise ValueError("Access forbidden - insufficient permissions")

            # Raise exception for other error status codes
            response.raise_for_status()

            # Parse and return the JSON response
            return response.json()

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to list users: {str(e)}") from e
