"""The snapshot pipeline behind ``push-servers``.

Fetches the live server list, stamps each server with the metadata that
identifies its snapshot, and hands the result to a sink. The capabilities
it needs are declared here as protocols, so the pipeline does not depend
on the rest of the clients' surface.

Printing a document and uploading it are two sinks for one document
stream, not two modes: the choice belongs to the caller, which is why
``--debug`` lives in the command module and not in this pipeline.

A sink is handed whole batches rather than one document at a time, so
that a run's length does not grow with the size of the hub: a run that
outlasts its own ``--interval`` lets two runs overlap and count one real
interval twice.
"""

import json
import sys
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any, Protocol, cast

from app.core.errors import DataShapeError, ExternalServiceError
from app.core.time_utils import parse_duration_required


# pylint: disable=too-few-public-methods
class ServerLister(Protocol):
    """Supplies the current list of servers."""

    def list_servers(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Return one flattened document per running server.

        *limit*, when given, caps how many documents are returned and is
        expected to spare the implementation the work of finding the
        rest.

        Raises:
            ExternalServiceError: If the list cannot be fetched.
        """
        raise NotImplementedError


class DocumentUploader(Protocol):
    """Accepts documents in batches."""

    def upload_documents(
        self,
        index: str,
        documents: Sequence[tuple[dict[str, Any], str | None]],
    ) -> list[dict[str, Any]]:
        """Store each document under its id and return one result each.

        The results line up with *documents*, so a caller can report on
        every one of them. A document the service rejects is described
        in its own result rather than raised.
        """
        raise NotImplementedError


class DocumentSink(Protocol):
    """Consumes the stamped documents this pipeline produces."""

    def send_all(
        self,
        documents: Sequence[dict[str, Any]],
        first_index: int,
        total_servers: int,
    ) -> list[str | None]:
        """Handle *documents*, numbered from *first_index* of *total_servers*.

        *first_index* is one-based, so a sink numbers a document by its
        place in the whole run whichever batch happens to carry it.

        A sink reports its own successes, so the pipeline never has to
        ask which kind of sink it is holding. Returns one entry per
        document, in order: ``None`` where the document was handled, and
        the reason it was not otherwise.

        Raises:
            ExternalServiceError: If the batch as a whole could not be
                sent; the pipeline then counts each of its documents as
                failed and goes on to the next batch.
        """
        raise NotImplementedError


# pylint: enable=too-few-public-methods

# Metadata keys this pipeline sets itself, so --metadata cannot supply
# them. Kept next to the code that writes them in _snapshot_metadata.
_INTERVAL_KEY = "interval"
_SNAPSHOT_TIME_KEY = "snapshot-time"
_SNAPSHOT_TIME_ISO_KEY = "snapshot-time-iso"
_RESERVED_METADATA_KEYS = frozenset({_INTERVAL_KEY, _SNAPSHOT_TIME_KEY, _SNAPSHOT_TIME_ISO_KEY})

# Documents per bulk request. A snapshot document runs from a few
# hundred bytes to a few kilobytes, since a spawner puts what it likes
# in ``server.state`` and ``server.user_options`` and both are flattened
# into it, so a batch this size keeps a request body near a megabyte:
# under the limit a reverse proxy in front of Elasticsearch is likely to
# impose, and far under Elasticsearch's own. Batching here rather than
# in the client is what lets a failed request cost its own batch instead
# of the rest of the run.
_BATCH_SIZE = 500


def parse_metadata(raw_items: list[str] | None) -> dict[str, str]:
    """Parse ``--metadata KEY=VALUE`` items.

    Raises:
        DataShapeError: If an item is malformed, or uses a key in
            :data:`_RESERVED_METADATA_KEYS` that this pipeline sets itself.
    """
    metadata_dict: dict[str, str] = {}
    for item in raw_items or []:
        if "=" not in item:
            raise DataShapeError(f"Invalid metadata format '{item}': must be KEY=VALUE")
        key, value = item.split("=", 1)
        if not key:
            raise DataShapeError(f"Invalid metadata format '{item}': key cannot be empty")
        metadata_dict[key] = value

    reserved = sorted(_RESERVED_METADATA_KEYS & metadata_dict.keys())
    if reserved:
        joined = ", ".join(reserved)
        raise DataShapeError(
            f"these --metadata keys are reserved and set automatically: {joined}"
        )

    return metadata_dict


def _make_doc_id(server: dict[str, Any]) -> str:
    """Build a deterministic Elasticsearch document ID."""
    snapshot_time = server.get("meta.snapshot-time")
    parts = [
        str(server.get("meta.hub") or "unknown-hub"),
        str(server.get("user.name") or "unknown-user"),
        str(server.get("server.name") or "default"),
        str(server.get("server.started") or "unknown-started"),
        str(snapshot_time if snapshot_time is not None else "unknown-snapshot"),
    ]
    return "|".join(parts)


def _server_label(server: dict[str, Any]) -> str:
    """Identify a server in progress and error messages."""
    user_name = server.get("user.name", "unknown")
    server_name = server.get("server.name", "unknown")
    return f"{user_name}/{server_name}"


def _rejection_reason(result: dict[str, Any]) -> str | None:
    """Say why Elasticsearch rejected a bulk item, or None if it took it.

    An item carries an ``error`` if and only if its operation failed.
    """
    error = cast(object, result.get("error"))
    if not error:
        return None
    if isinstance(error, dict):
        details = cast(dict[str, Any], error)
        parts = [str(details[key]) for key in ("type", "reason") if details.get(key)]
        return ": ".join(parts) if parts else "unknown error"
    return str(error)


# pylint: disable=too-few-public-methods
class PrintingSink:
    """Prints each document to stdout instead of storing it.

    Backs ``push-servers --debug``.
    """

    def send_all(
        self,
        documents: Sequence[dict[str, Any]],
        first_index: int,
        total_servers: int,
    ) -> list[str | None]:
        """Print each document as indented JSON, under a numbered heading."""
        for offset, document in enumerate(documents):
            print(f"\n--- Document {first_index + offset}/{total_servers} ---")
            print(json.dumps(document, indent=2))
        return [None] * len(documents)


class ElasticsearchSink:
    """Uploads each document to one Elasticsearch index."""

    def __init__(self, client: DocumentUploader, index: str) -> None:
        """Bind *client* to the *index* every document is written to."""
        self._client = client
        self._index = index

    def send_all(
        self,
        documents: Sequence[dict[str, Any]],
        first_index: int,
        total_servers: int,
    ) -> list[str | None]:
        """Upload *documents* in one request, then report each outcome.

        Every document keeps the deterministic id that names its
        snapshot, so a re-run overwrites what it pushed before rather
        than duplicating it. A document Elasticsearch rejects is
        returned as its reason; only a failure of the request as a whole
        is raised.
        """
        results = self._client.upload_documents(
            index=self._index,
            documents=[(document, _make_doc_id(document)) for document in documents],
        )

        outcomes: list[str | None] = []
        for offset, (document, result) in enumerate(zip(documents, results)):
            reason = _rejection_reason(result)
            outcomes.append(reason)
            if reason is None:
                i = first_index + offset
                outcome = result.get("result", "unknown")
                label = _server_label(document)
                print(f"Pushed server {i}/{total_servers}: {label} (result: {outcome})")

        return outcomes


# pylint: enable=too-few-public-methods


def _validate_interval(interval: str) -> timedelta:
    """Reject an interval that ``get-new-activity`` cannot add up.

    Returns the parsed duration, which the run also measures itself
    against.

    Raises:
        DataShapeError: If *interval* is not a positive duration.
    """
    parsed = parse_duration_required(interval)
    if parsed.total_seconds() <= 0:
        raise DataShapeError(f"Interval must be positive, not {interval!r}")
    return parsed


def _warn_if_overrun(elapsed: float, interval: timedelta, interval_str: str) -> None:
    """Warn when a run outlasted the cadence it stamped on its documents.

    A run that overruns invites the next one to start before it has
    finished. The two stamp different snapshot times, so nothing
    downstream can tell them apart, and ``get-new-activity`` adds up a
    full interval for each: every duration it reports is then too large,
    and only this line says so.
    """
    if elapsed <= interval.total_seconds():
        return

    message = (
        f"Warning: this push took {elapsed:.1f}s, "
        + f"longer than its --interval of {interval_str}; "
        + "overlapping runs inflate get-new-activity figures."
    )
    print(message, file=sys.stderr)


def _snapshot_metadata(
    snapshot_time: datetime,
    interval: str,
    metadata: dict[str, str] | None,
) -> dict[str, Any]:
    """Build the ``meta.*`` fields that every document in one run shares."""
    stamp: dict[str, Any] = {f"meta.{key}": value for key, value in (metadata or {}).items()}

    stamp[f"meta.{_INTERVAL_KEY}"] = interval
    stamp[f"meta.{_SNAPSHOT_TIME_KEY}"] = int(snapshot_time.timestamp())
    stamp[f"meta.{_SNAPSHOT_TIME_ISO_KEY}"] = snapshot_time.replace(microsecond=0).isoformat()

    return stamp


def _process_batch(
    documents: Sequence[dict[str, Any]],
    sink: DocumentSink,
    first_index: int,
    total_servers: int,
) -> tuple[int, int]:
    """Hand *documents* to *sink*, then report on each one.

    Returns:
        The number of documents handled successfully and the number that
        failed, in that order. A failure is reported on stderr so that
        neither one rejected document nor one failed request costs the
        batches that follow it.
    """
    try:
        outcomes = sink.send_all(documents, first_index, total_servers)
    except ExternalServiceError as e:
        # The one request carrying the batch failed, so every document
        # in it did.
        outcomes = [str(e)] * len(documents)

    successful = 0
    errors = 0

    for offset, (document, reason) in enumerate(zip(documents, outcomes)):
        if reason is None:
            successful += 1
            continue

        errors += 1
        i = first_index + offset
        label = _server_label(document)
        print(f"Error pushing server {i}/{total_servers} ({label}): {reason}", file=sys.stderr)

    return (successful, errors)


def _process_batches(
    servers: list[dict[str, Any]],
    sink: DocumentSink,
) -> tuple[int, int]:
    """Hand *servers* to *sink* one batch at a time, and add up the outcomes.

    Returns:
        The number of documents handled successfully and the number that
        failed, in that order.
    """
    total_servers = len(servers)
    successful = 0
    errors = 0

    for start in range(0, total_servers, _BATCH_SIZE):
        batch = servers[start : start + _BATCH_SIZE]
        batch_successful, batch_errors = _process_batch(batch, sink, start + 1, total_servers)
        successful += batch_successful
        errors += batch_errors

    return (successful, errors)


def push_servers(
    jupyterhub_client: ServerLister,
    sink: DocumentSink,
    interval: str,
    limit: int | None = None,
    metadata: dict[str, str] | None = None,
) -> tuple[int, int]:
    """Fetch servers from JupyterHub and hand each one to *sink*.

    *interval* is the push cadence stamped on every document as
    ``meta.interval``. It is checked here, before anything is fetched,
    because a duration ``get-new-activity`` cannot add up is dropped
    only at report time, potentially days of pushes later.

    *limit* is handed to the client rather than applied to what it
    returns, so a capped run costs what was asked for instead of a full
    fetch that is then thrown away.

    Documents go out in batches of :data:`_BATCH_SIZE`, one request
    each, and a run that still outlasts its own *interval* says so on
    stderr without failing.

    Returns:
        The number of documents handled successfully and the number that
        failed, in that order.

    Raises:
        DataShapeError: If *interval* is not a positive duration.
        ExternalServiceError: If the server list cannot be fetched.
    """
    started = monotonic()
    parsed_interval = _validate_interval(interval)

    servers = jupyterhub_client.list_servers(limit)
    total_servers = len(servers)
    print(f"Found {total_servers} server(s) to process")

    # Stamp every document before any batch is sent, because the id
    # _make_doc_id builds includes the snapshot time.
    stamp = _snapshot_metadata(datetime.now(timezone.utc), interval, metadata)
    for server in servers:
        server.update(stamp)

    successful, errors = _process_batches(servers, sink)

    _warn_if_overrun(monotonic() - started, parsed_interval, interval)

    return (successful, errors)
