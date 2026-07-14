"""JupyterHub API client wrapper."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import requests


class JupyterHubClient:
    """A wrapper around the JupyterHub REST API."""

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        ca_cert: str | Path | None = None,
    ) -> None:
        """Store endpoint and auth settings for later API requests."""
        self._endpoint = endpoint.rstrip("/")
        self._api_key = api_key
        self._ca_cert = str(ca_cert) if ca_cert is not None else None

        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def list_users(
        self, state: str | None = None, limit: int = 100
    ) -> Iterator[dict[str, Any]]:
        """Get the list of users from JupyterHub using offset-based pagination.

        Raises:
            ConnectionError: If the API request fails.
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
                raise ConnectionError(f"Failed to list users: {e}") from e

            page = cast(list[dict[str, Any]], response.json())
            yield from page

            if len(page) < limit:
                break

            offset += limit

    def list_servers(self) -> list[dict[str, Any]]:
        """Get the list of servers from JupyterHub.

        Retrieves all users and extracts their servers,
        flattening nested dictionaries into dotted-key paths.

        Raises:
            ConnectionError: If the API request fails.
        """
        users = self.list_users()
        result: list[dict[str, Any]] = []

        for user in users:
            servers = user.get("servers", {})
            if not servers:
                continue

            for server_name, server_info in servers.items():
                if server_info:
                    server_data = JupyterHubClient._flatten_dict(
                        {
                            "user": {"name": user.get("name")},
                            "server": {"name": server_name, **server_info},
                        }
                    )
                    result.append(server_data)

        return result

    @staticmethod
    def _flatten_dict(
        d: dict[str, Any], parent_key: str = "", sep: str = "."
    ) -> dict[str, Any]:
        """Flatten a nested dictionary using dotted-key paths."""
        items: list[tuple[str, Any]] = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(
                    JupyterHubClient._flatten_dict(
                        cast(dict[str, Any], v), new_key, sep=sep
                    ).items()
                )
            else:
                items.append((new_key, v))
        return dict(items)
