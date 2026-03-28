"""Server-snapshot documents: selecting them and reading their fields.

``push-servers`` writes one flattened document per running server per
snapshot, and the report commands read those documents back. This module
holds the query that selects the active ones, plus the accessors for the
timestamps the reports are built from.
"""

from typing import Any

from app.core.time_utils import parse_iso_utc


def build_query(
    cutoff: int,
    end: int,
    hub: str | None = None,
) -> dict[str, Any]:
    """Build Elasticsearch Query DSL for active server documents.

    The window is half-open, ``[cutoff, end)``, so that back-to-back
    reports do not both count the snapshot on their shared boundary.
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


def snapshot_seconds(doc: dict[str, Any]) -> int | None:
    """Return a document's ``meta.snapshot-time`` as epoch seconds.

    Accepts an integer or a float, and rejects ``bool``, which
    :func:`isinstance` would otherwise admit as an integer.
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
