"""Server-snapshot documents: selecting them and reading their fields.

``push-servers`` writes one flattened document per running server per snapshot,
and the report commands read those documents back.
This module holds the query that selects the active ones,
plus the accessors for the hub and the timestamps the reports are built from.
"""

from collections.abc import Iterable, Iterator
from typing import Any

from app.core.time_utils import parse_iso_utc


def build_query(
    cutoff: int,
    end: int,
    hub: str | None = None,
) -> dict[str, Any]:
    """Build Elasticsearch Query DSL for active server documents.

    The window is half-open, ``[cutoff, end)``,
    so that back-to-back reports do not both count the snapshot
    on their shared boundary.

    *hub* only narrows the search;
    pass the results through :func:`select_hub`
    to keep that hub's documents alone.
    """
    filters: list[dict[str, Any]] = [
        {
            "range": {
                "meta.snapshot-time": {
                    "gte": cutoff,
                    "lt": end,
                }
            }
        },
        {
            "bool": {
                "should": [
                    {"term": {"server.ready": True}},
                    {"term": {"server.pending": "spawn"}},
                ],
                "minimum_should_match": 1,
            }
        },
    ]

    if hub is not None:
        filters.append(
            {
                "bool": {
                    "should": [
                        {"term": {"meta.hub.keyword": hub}},
                        {"term": {"meta.hub": hub}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        )

    return {
        "bool": {
            "filter": filters,
            "must_not": [
                {
                    "bool": {
                        "should": [
                            {"term": {"meta.testing": "true"}},
                            {"term": {"meta.testing": True}},
                        ],
                        "minimum_should_match": 1,
                    }
                }
            ],
        }
    }


def select_hub(
    documents: Iterable[dict[str, Any]], hub: str | None
) -> Iterator[dict[str, Any]]:
    """Yield the documents whose ``meta.hub`` is exactly *hub*, lazily.

    :func:`build_query` matches the field
    whether it is mapped as a keyword or as text,
    but on a text mapping its term query also matches any hub
    whose name contains that word, as ``osg`` does ``osg-dev``.
    Without a *hub*, every document is kept.
    """
    return (doc for doc in documents if hub is None or snapshot_hub(doc) == hub)


def snapshot_hub(doc: dict[str, Any]) -> str | None:
    """Return the hub a document names, or ``None`` when it names none.

    ``push-servers`` writes the hub as a string,
    but another writer's mapping need not,
    so any other value is rendered with :func:`str`.
    """
    value = doc.get("meta.hub")
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def snapshot_seconds(doc: dict[str, Any]) -> int | None:
    """Return a document's ``meta.snapshot-time`` as epoch seconds.

    Accepts an integer or a float, and rejects ``bool``,
    which :func:`isinstance` would otherwise admit as an integer.
    """
    value = doc.get("meta.snapshot-time")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


def started_seconds(doc: dict[str, Any]) -> int | None:
    """Return a document's ``server.started`` as epoch seconds."""
    started = parse_iso_utc(doc.get("server.started"))
    if started is None:
        return None
    return int(started.timestamp())
