"""Prometheus HTTP API client wrapper."""

from pathlib import Path
from types import TracebackType
from typing import Any, Self, cast

import requests

from app.core.errors import ExternalServiceError

# Client-side wait, in seconds, for one query.
_QUERY_TIMEOUT = 30


class PrometheusClient:
    """A wrapper around the Prometheus HTTP API.

    One session spans the client's life,
    so a caller issuing a query per session reuses one connection.
    """

    def __init__(
        self,
        endpoint: str,
        token: str | None = None,
        ca_cert: str | Path | None = None,
    ) -> None:
        """Prepare a session for *endpoint*, the server's base URL.

        Without a *token*, requests carry no credentials,
        as a Prometheus reached inside its own cluster usually needs none.
        """
        self._endpoint = endpoint.rstrip("/")
        self._verify: str | bool = str(ca_cert) if ca_cert is not None else True
        self._session = requests.Session()
        if token is not None:
            self._session.headers["Authorization"] = f"Bearer {token}"

    def query(self, promql: str, time: int) -> list[dict[str, Any]]:
        """Evaluate an instant query at *time*, in epoch seconds.

        Returns:
            The result vector, one entry per series,
            each carrying its ``metric`` labels and its ``value`` pair.

        Raises:
            ExternalServiceError: If the request fails,
                or Prometheus reports an error or a result other than a vector.
        """
        try:
            response = self._session.get(
                f"{self._endpoint}/api/v1/query",
                params={"query": promql, "time": time},
                timeout=_QUERY_TIMEOUT,
                verify=self._verify,
            )
        except requests.exceptions.RequestException as e:
            raise ExternalServiceError(f"Querying Prometheus failed: {e}") from e

        # Prometheus describes a rejected query in a JSON body
        # sent with an error status, and that body says more than the status.
        try:
            body = cast(object, response.json())
        except requests.exceptions.JSONDecodeError:
            body = None
        payload = cast(dict[str, Any], body) if isinstance(body, dict) else {}

        if payload.get("status") == "error":
            reason = payload.get("error") or payload.get("errorType") or "unknown error"
            raise ExternalServiceError(f"Querying Prometheus failed: {reason}")
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise ExternalServiceError(f"Querying Prometheus failed: {e}") from e

        data = payload.get("data")
        if payload.get("status") != "success" or not isinstance(data, dict):
            raise ExternalServiceError("Querying Prometheus failed: the response holds no data")
        data = cast(dict[str, Any], data)
        if data.get("resultType") != "vector":
            raise ExternalServiceError(
                f"Querying Prometheus failed: expected a vector, got {data.get('resultType')!r}"
            )
        return cast(list[dict[str, Any]], data.get("result", []))

    def close(self) -> None:
        """Close the underlying session.

        Prefer using the client as a context manager.
        """
        self._session.close()

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
