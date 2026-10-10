"""JupyterHub API client wrapper."""

import sys
from collections.abc import Generator
from contextlib import closing
from pathlib import Path
from typing import Any, cast

import requests

from app.core.dict_utils import flatten_dict
from app.core.errors import ExternalServiceError


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
        self._ca_cert = str(ca_cert) if ca_cert is not None else None

        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def list_users(
        self, state: str | None = None, page_size: int = 100
    ) -> Generator[dict[str, Any]]:
        """Get the list of users from JupyterHub using offset-based pagination.

        *page_size* is how many users each request asks for, not a cap
        on how many are yielded: the walk pages through every user.
        A short page ends the walk. A hub that ignores *offset* would
        otherwise page forever, so a repeat of the previous page also
        ends it.

        *state* is passed through to the API as a server-state filter
        (e.g. ``"active"``); no caller uses it, because
        :meth:`list_servers` needs users whose servers are still spawning
        as well as those already running. A single session spans the walk,
        so paging reuses one connection.

        Because this is a generator, the request is not made until it is
        first iterated, and failures surface there. It is returned as a
        generator rather than a plain iterator so that a caller that
        stops early, such as :meth:`list_servers` under a limit, can
        close it and end the paging session at once.

        Raises:
            ExternalServiceError: If an API request fails.
        """
        url = f"{self._endpoint}/users"
        offset = 0
        previous_names: list[Any] | None = None

        with requests.Session() as session:
            session.headers.update(self._headers)
            session.verify = self._ca_cert if self._ca_cert is not None else True

            while True:
                params: dict[str, Any] = {"offset": offset, "limit": page_size}
                if state is not None:
                    params["state"] = state

                # A body that is not JSON, such as a login page answered
                # with 200, raises a RequestException too.
                try:
                    response = session.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    page = cast(list[dict[str, Any]], response.json())
                except requests.exceptions.RequestException as e:
                    raise ExternalServiceError(f"Listing users failed: {e}") from e

                names = [user.get("name") for user in page]

                if names == previous_names:
                    print(
                        "Warning: JupyterHub repeated a page of users; stopping pagination.",
                        file=sys.stderr,
                    )
                    break

                yield from page

                if len(page) < page_size:
                    break

                previous_names = names
                offset += len(page)

    def list_servers(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Get the list of servers from JupyterHub.

        Retrieves users and extracts their servers,
        flattening nested dictionaries into dotted-key paths
        (:func:`~app.core.dict_utils.flatten_dict`). An empty nested
        dictionary contributes no key, so a server whose ``user_options``
        is ``{}`` is indistinguishable from one that has none.

        *limit* caps how many servers are returned. Because
        :meth:`list_users` is lazy, the cap also ends the walk: the pages
        after the one that completes it are never requested, so a small
        *limit* costs a few API calls rather than one per hundred
        accounts on the hub. A *limit* of zero or less returns nothing
        and contacts the API not at all.

        Raises:
            ExternalServiceError: If an API request fails.
        """
        if limit is not None and limit <= 0:
            return []

        result: list[dict[str, Any]] = []

        # Closing the generator ends its paging session as soon as the
        # cap is met, instead of leaving it to garbage collection.
        with closing(self.list_users()) as users:
            for user in users:
                servers = user.get("servers", {})
                if not servers:
                    continue

                for server_name, server_info in servers.items():
                    if server_info:
                        server_data = flatten_dict(
                            {
                                "user": {"name": user.get("name")},
                                "server": {"name": server_name, **server_info},
                            }
                        )
                        result.append(server_data)

                        if limit is not None and len(result) >= limit:
                            return result

        return result
