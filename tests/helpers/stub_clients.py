"""Stand-ins for the Elasticsearch client, for the command tests.

A command opens its client as a context manager and calls one method on
it, so a stand-in only has to answer that call. The commands are tested
without connecting to anything, which is why each one's
``make_es_client`` is replaced by :func:`install_client`.
"""

# A stub client answers one call, so it has just one public method.
# pylint: disable=too-few-public-methods

import argparse
from types import ModuleType
from typing import Any, Self

import pytest


class StubClient:
    """A client that can be entered and left, and does nothing else."""

    def __enter__(self) -> Self:
        """Stand in for the client's context manager."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Leave the context manager, suppressing nothing."""
        return None


class QueryStubClient(StubClient):
    """Answers ``query`` with prepared documents, recording the request."""

    def __init__(self, documents: list[dict[str, Any]]) -> None:
        """Store the documents to yield."""
        self.documents = documents
        self.request: dict[str, Any] | None = None

    def query(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Record the request and return the prepared documents."""
        self.request = kwargs
        return list(self.documents)


def install_client(
    monkeypatch: pytest.MonkeyPatch,
    command: ModuleType,
    client: StubClient,
) -> None:
    """Make *command* build *client* in place of its Elasticsearch client."""

    def _make_client(_args: argparse.Namespace) -> StubClient:
        return client

    monkeypatch.setattr(command, "make_es_client", _make_client)
