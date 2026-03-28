"""Tests for :mod:`app.clients.jupyterhub_client`, with a stubbed session."""

# An explicit comparison to an empty collection is the assertion these
# tests mean to make, and reads better here than a truthiness check.
# pylint: disable=use-implicit-booleaness-not-comparison

from typing import Any

import pytest
import requests

from app.clients import jupyterhub_client
from app.clients.jupyterhub_client import JupyterHubClient
from app.core.errors import ExternalServiceError


class _StubResponse:
    """A minimal stand-in for :class:`requests.Response`."""

    def __init__(self, payload: object, *, error: Exception | None = None) -> None:
        """Store the payload to return, and any error to raise first."""
        self._payload = payload
        self._error = error

    def raise_for_status(self) -> None:
        """Raise the configured error, if any."""
        if self._error is not None:
            raise self._error

    def json(self) -> object:
        """Return the configured payload."""
        return self._payload


class _StubSession:
    """Serves prepared pages and records the query parameters of each request."""

    def __init__(self, pages: list[_StubResponse]) -> None:
        """Store the responses to serve, in order."""
        self.pages = list(pages)
        self.calls: list[dict[str, Any]] = []
        self.headers: dict[str, str] = {}
        self.verify: object = None
        self.closed = False

    def __enter__(self) -> "_StubSession":
        """Support use as a context manager, like a real session."""
        return self

    def __exit__(self, *_: object) -> None:
        """Record that the session was closed."""
        self.closed = True

    def get(self, url: str, **kwargs: Any) -> _StubResponse:
        """Serve the next prepared response."""
        self.calls.append({"url": url, **kwargs})
        return self.pages.pop(0)


def test_list_users_walks_every_page(monkeypatch: pytest.MonkeyPatch) -> None:
    """Paging continues until a short page ends the walk."""
    session = _install(
        monkeypatch,
        [_users_page(100), _users_page(100, offset=100), _users_page(3, offset=200)],
    )
    users = list(_client().list_users())
    assert len(users) == 203
    assert [call["params"]["offset"] for call in session.calls] == [0, 100, 200]


def test_list_users_stops_on_a_short_first_page(monkeypatch: pytest.MonkeyPatch) -> None:
    """One short page is the whole list; there is no second request."""
    session = _install(monkeypatch, [_users_page(5)])
    assert len(list(_client().list_users())) == 5
    assert len(session.calls) == 1


def test_list_users_stops_on_an_empty_page(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hub with no users yields nothing."""
    _install(monkeypatch, [_users_page(0)])
    assert list(_client().list_users()) == []


def test_list_users_stops_when_a_hub_repeats_a_page(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A hub that ignores ``offset`` would otherwise page forever."""
    page = _users_page(100)
    _install(monkeypatch, [page, _users_page(100), _users_page(100)])
    users = list(_client().list_users())
    assert len(users) == 100
    assert "repeated a page of users" in capsys.readouterr().err


def test_list_users_passes_the_state_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    """The filter reaches the API when a caller supplies one."""
    session = _install(monkeypatch, [_users_page(1)])
    list(_client().list_users(state="active"))
    assert session.calls[0]["params"]["state"] == "active"


def test_list_users_omits_the_state_filter_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without it, users whose servers are still spawning are included."""
    session = _install(monkeypatch, [_users_page(1)])
    list(_client().list_users())
    assert "state" not in session.calls[0]["params"]


def test_list_users_sends_the_bearer_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """The token is sent once, on the session, not rebuilt per request."""
    session = _install(monkeypatch, [_users_page(1)])
    list(_client().list_users())
    assert session.headers["Authorization"] == "Bearer token"


def test_list_users_verifies_tls_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verification is on whether or not a CA bundle was supplied."""
    session = _install(monkeypatch, [_users_page(1)])
    list(_client().list_users())
    assert session.verify is True

    session = _install(monkeypatch, [_users_page(1)])
    list(_client(ca_cert="/etc/ssl/ca.pem").list_users())
    assert session.verify == "/etc/ssl/ca.pem"


def test_list_users_reports_a_request_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A transport error surfaces in the application's own vocabulary.

    Naming the operation here is what lets a command print a specific
    message without translating the failure at its own boundary.
    """
    _install(
        monkeypatch,
        [_StubResponse(None, error=requests.exceptions.HTTPError("403 Forbidden"))],
    )
    with pytest.raises(ExternalServiceError, match="Listing users failed: 403 Forbidden"):
        list(_client().list_users())


def test_the_endpoint_trailing_slash_is_normalized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The request URL is built the same either way."""
    session = _install(monkeypatch, [_users_page(1)])
    list(JupyterHubClient(endpoint="https://hub/hub/api/", api_key="t").list_users())
    assert session.calls[0]["url"] == "https://hub/hub/api/users"


def test_list_servers_flattens_each_server(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each server becomes one document of dotted-key paths."""
    _install(
        monkeypatch,
        [
            _StubResponse(
                [
                    {
                        "name": "a@x.edu",
                        "servers": {
                            "": {"ready": True, "state": {"pod_name": "jupyter-a"}},
                            "gpu": {"ready": False},
                        },
                    }
                ]
            )
        ],
    )
    servers = _client().list_servers()
    assert servers == [
        {
            "user.name": "a@x.edu",
            "server.name": "",
            "server.ready": True,
            "server.state.pod_name": "jupyter-a",
        },
        {"user.name": "a@x.edu", "server.name": "gpu", "server.ready": False},
    ]


def test_list_servers_skips_users_without_servers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A user with no servers contributes no documents."""
    page: list[dict[str, Any]] = [
        {"name": "a@x.edu", "servers": {}},
        {"name": "b@x.edu"},
        {"name": "c@x.edu", "servers": {"": None}},
    ]
    _install(monkeypatch, [_StubResponse(page)])
    assert _client().list_servers() == []


def test_list_servers_stops_paging_once_the_limit_is_met(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cap ends the walk, so later pages are never requested."""
    session = _install(monkeypatch, [_servers_page(100), _servers_page(100, offset=100)])
    servers = _client().list_servers(limit=5)
    assert [server["user.name"] for server in servers] == [f"user{i}@x.edu" for i in range(5)]
    assert len(session.calls) == 1


def test_list_servers_keeps_paging_below_the_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cap the first page cannot fill still reaches the next one."""
    session = _install(monkeypatch, [_servers_page(100), _servers_page(3, offset=100)])
    assert len(_client().list_servers(limit=102)) == 102
    assert len(session.calls) == 2


def test_list_servers_without_a_limit_returns_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default is the whole hub, as before the cap existed."""
    _install(monkeypatch, [_servers_page(100), _servers_page(3, offset=100)])
    assert len(_client().list_servers()) == 103


def test_list_servers_with_a_zero_limit_asks_for_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing was asked for, so no request is made."""
    session = _install(monkeypatch, [_servers_page(100)])
    assert _client().list_servers(limit=0) == []
    assert session.calls == []


def _client(*, ca_cert: str | None = None) -> JupyterHubClient:
    """Build a client against a stand-in endpoint."""
    return JupyterHubClient(
        endpoint="https://hub.example.com/hub/api",
        api_key="token",
        ca_cert=ca_cert,
    )


def _users_page(count: int, *, offset: int = 0) -> _StubResponse:
    """Build a page of *count* distinct users."""
    return _StubResponse([{"name": f"user{offset + i}@x.edu"} for i in range(count)])


def _servers_page(count: int, *, offset: int = 0) -> _StubResponse:
    """Build a page of *count* distinct users, each running one server."""
    return _StubResponse(
        [
            {"name": f"user{offset + i}@x.edu", "servers": {"": {"ready": True}}}
            for i in range(count)
        ]
    )


def _install(monkeypatch: pytest.MonkeyPatch, pages: list[_StubResponse]) -> _StubSession:
    """Replace ``requests.Session`` with a stub serving *pages*."""
    session = _StubSession(pages)
    monkeypatch.setattr(jupyterhub_client.requests, "Session", lambda: session)
    return session
