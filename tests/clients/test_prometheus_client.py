"""Tests for :mod:`app.clients.prometheus_client`, with a stubbed session."""

from typing import Any

import pytest
import requests

from app.clients import prometheus_client
from app.clients.prometheus_client import PrometheusClient
from app.core.errors import ExternalServiceError


class _StubResponse:
    """A minimal stand-in for :class:`requests.Response`."""

    def __init__(self, payload: object, *, error: Exception | None = None) -> None:
        """Store the body to return, and any HTTP error to raise."""
        self._payload = payload
        self._error = error

    def raise_for_status(self) -> None:
        """Raise the configured HTTP error, if any."""
        if self._error is not None:
            raise self._error

    def json(self) -> object:
        """Return the body, or fail as a body that is not JSON does."""
        if self._payload is None:
            raise requests.exceptions.JSONDecodeError("Expecting value", "<html>", 0)
        return self._payload


class _StubSession:
    """Serves one prepared response and records each request."""

    def __init__(self, response: _StubResponse | Exception) -> None:
        """Store the response to serve, or the transport error to raise."""
        self.response = response
        self.calls: list[dict[str, Any]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, **kwargs: Any) -> _StubResponse:
        """Record the request, then serve the response or raise the error."""
        self.calls.append({"url": url, **kwargs})
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    def close(self) -> None:
        """Stand in for closing the session."""


def _install(
    monkeypatch: pytest.MonkeyPatch, response: _StubResponse | Exception
) -> _StubSession:
    """Replace ``requests.Session`` with a stub serving *response*."""
    session = _StubSession(response)
    monkeypatch.setattr(prometheus_client.requests, "Session", lambda: session)
    return session


@pytest.mark.parametrize(
    ("token", "authorization"),
    [(None, None), ("secret", "Bearer secret")],
    ids=["anonymous", "bearer"],
)
def test_a_query_is_evaluated_as_an_instant_query(
    monkeypatch: pytest.MonkeyPatch, token: str | None, authorization: str | None
) -> None:
    """The result vector comes back, and a token is sent only when given one.

    A Prometheus reached inside its own cluster usually accepts anonymous
    requests, which is why the token is optional.
    """
    series: dict[str, Any] = {"metric": {}, "value": [1782000000, "1073741824"]}
    session = _install(
        monkeypatch,
        _StubResponse(
            {"status": "success", "data": {"resultType": "vector", "result": [series]}}
        ),
    )

    with PrometheusClient("https://prometheus.example.edu/", token=token) as client:
        assert client.query("up", time=1782000000) == [series]

    assert session.calls[0]["url"] == "https://prometheus.example.edu/api/v1/query"
    assert session.calls[0]["params"] == {"query": "up", "time": 1782000000}
    assert session.headers.get("Authorization") == authorization


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (requests.exceptions.ConnectionError("connection refused"), "connection refused"),
        (_StubResponse(None, error=requests.exceptions.HTTPError("503 Unavailable")), "503"),
        (
            _StubResponse(
                {"status": "error", "errorType": "bad_data", "error": "parse error"},
                error=requests.exceptions.HTTPError("400 Bad Request"),
            ),
            "parse error",
        ),
    ],
    ids=["transport", "http-status", "prometheus-error"],
)
def test_a_failed_query_is_reported_in_the_applications_vocabulary(
    monkeypatch: pytest.MonkeyPatch, response: _StubResponse | Exception, reason: str
) -> None:
    """Naming the operation lets a command report it without translating it.

    Prometheus explains a rejected query in its body,
    which says more than the status line, so that is what is reported.
    """
    _install(monkeypatch, response)
    with pytest.raises(ExternalServiceError, match=f"Querying Prometheus failed: .*{reason}"):
        PrometheusClient("https://prometheus.example.edu").query("up", time=0)
