"""Tests for :mod:`app.usage.push`."""

# An explicit comparison to an empty collection is the assertion these
# tests mean to make, and reads better here than a truthiness check.
# pylint: disable=use-implicit-booleaness-not-comparison
# The stubs implement exactly the one method their protocol declares.
# pylint: disable=too-few-public-methods
# This module exercises the push pipeline's internal helpers directly.
# pyright: reportPrivateUsage=false

import json
from collections.abc import Sequence
from typing import Any

import pytest

from app.core.errors import DataShapeError, ExternalServiceError
from app.usage import push
from app.usage.push import (
    ElasticsearchSink,
    PrintingSink,
    _make_doc_id,
    parse_metadata,
    push_servers,
)

_INDEX = "servers"


class _StubHub:
    """Returns a prepared server list."""

    def __init__(self, servers: list[dict[str, Any]]) -> None:
        """Store the servers to return."""
        self.servers = servers
        self.limits: list[int | None] = []

    def list_servers(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Return the prepared servers, recording the cap it was asked for."""
        self.limits.append(limit)
        if limit is None:
            return list(self.servers)
        return list(self.servers[:limit])


# One attribute per thing a test asserts on, plus the two failure modes.
# pylint: disable-next=too-many-instance-attributes
class _StubElasticsearch:
    """Records bulk-uploaded documents, rejecting or failing some of them.

    *reject* names document positions, counted from zero across the
    whole run, that come back rejected the way a bulk item does.
    *error_on* names requests, counted from zero, that fail outright
    instead. The two are independent, because a rejected document and a
    failed request are what the pipeline must tell apart.
    """

    def __init__(
        self,
        *,
        reject: set[int] | None = None,
        error_on: set[int] | None = None,
        error: Exception | None = None,
    ) -> None:
        """Store which documents are rejected and which requests fail, and how."""
        self.uploaded: list[dict[str, Any]] = []
        self.indexes: list[str] = []
        self.doc_ids: list[str | None] = []
        self.batch_sizes: list[int] = []
        self.requests = 0
        self.reject = reject or set()
        self.error_on = error_on or set()
        self.error = error or ExternalServiceError("bulk request failed")

    def upload_documents(
        self,
        index: str,
        documents: Sequence[tuple[dict[str, Any], str | None]],
    ) -> list[dict[str, Any]]:
        """Record the batch, or raise if this request is configured to fail."""
        request = self.requests
        self.requests += 1
        self.batch_sizes.append(len(documents))
        if request in self.error_on:
            raise self.error

        results: list[dict[str, Any]] = []
        for document, doc_id in documents:
            position = len(self.uploaded)
            # A rejected document still rode the request, so record it.
            self.uploaded.append(document)
            self.indexes.append(index)
            self.doc_ids.append(doc_id)
            if position in self.reject:
                results.append(
                    {
                        "status": 400,
                        "error": {"type": "mapper_parsing_exception", "reason": "rejected"},
                    }
                )
            else:
                results.append({"status": 201, "result": "created"})
        return results


def test_metadata_items_become_key_value_pairs() -> None:
    """Each item is a key-value pair; the ``meta.`` prefix is added later."""
    assert parse_metadata(["hub=production", "site=wisc"]) == {
        "hub": "production",
        "site": "wisc",
    }


def test_metadata_values_may_contain_equals_signs() -> None:
    """Only the first ``=`` separates the key from the value."""
    assert parse_metadata(["note=a=b"]) == {"note": "a=b"}


def test_metadata_values_may_be_empty() -> None:
    """An empty value is a value; an empty key is not."""
    assert parse_metadata(["hub="]) == {"hub": ""}


def test_no_metadata_yields_an_empty_mapping() -> None:
    """The flag is optional."""
    assert parse_metadata(None) == {}
    assert parse_metadata([]) == {}


@pytest.mark.parametrize("item", ["hub", "=production", ""])
def test_malformed_metadata_is_rejected(item: str) -> None:
    """A missing separator or an empty key is a usage error."""
    with pytest.raises(DataShapeError, match="Invalid metadata format"):
        parse_metadata([item])


@pytest.mark.parametrize("key", ["interval", "snapshot-time", "snapshot-time-iso"])
def test_reserved_metadata_keys_are_rejected(key: str) -> None:
    """These are set by the command, so they cannot be overridden."""
    with pytest.raises(DataShapeError, match="reserved and set automatically"):
        parse_metadata([f"{key}=x"])


def test_a_document_id_identifies_one_server_at_one_snapshot() -> None:
    """The id makes a re-push of the same snapshot idempotent."""
    doc_id = _make_doc_id(
        {
            "meta.hub": "production",
            "user.name": "a@x.edu",
            "server.name": "gpu",
            "server.started": "2026-07-02T10:00:00Z",
            "meta.snapshot-time": 1782000000,
        }
    )
    assert doc_id == "production|a@x.edu|gpu|2026-07-02T10:00:00Z|1782000000"


def test_a_document_id_fills_in_missing_fields() -> None:
    """Every component is present, so ids stay comparable."""
    assert _make_doc_id({}) == (
        "unknown-hub|unknown-user|default|unknown-started|unknown-snapshot"
    )


def test_the_elasticsearch_sink_uploads_and_reports_the_outcome(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The document goes to the bound index under its deterministic id."""
    elasticsearch = _StubElasticsearch()
    document = {"user.name": "a@x.edu", "server.name": "gpu"}
    ElasticsearchSink(elasticsearch, _INDEX).send_all([document], 1, 1)
    assert elasticsearch.uploaded == [document]
    assert elasticsearch.indexes == [_INDEX]
    assert elasticsearch.doc_ids == [_make_doc_id(document)]
    assert capsys.readouterr().out.startswith(
        "Pushed server 1/1: a@x.edu/gpu (result: created)"
    )


def test_the_elasticsearch_sink_propagates_a_failed_request() -> None:
    """Counting the failure is the pipeline's job, not the sink's."""
    sink = ElasticsearchSink(_StubElasticsearch(error_on={0}), _INDEX)
    with pytest.raises(ExternalServiceError, match="bulk request failed"):
        sink.send_all([{"user.name": "a@x.edu"}], 1, 1)


def test_the_elasticsearch_sink_reports_a_rejected_document_without_raising(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A rejection arrives in the response, so it must not cost the batch."""
    sink = ElasticsearchSink(_StubElasticsearch(reject={0}), _INDEX)
    documents = [{"user.name": "a@x.edu"}, {"user.name": "b@x.edu"}]
    assert sink.send_all(documents, 1, 2) == ["mapper_parsing_exception: rejected", None]
    body = capsys.readouterr().out
    assert "Pushed server 1/2" not in body
    assert "Pushed server 2/2: b@x.edu/unknown (result: created)" in body


def test_the_printing_sink_numbers_a_whole_batch(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A document is numbered by its place in the run, not in its batch."""
    documents = [{"user.name": "a@x.edu"}, {"user.name": "b@x.edu"}]
    assert PrintingSink().send_all(documents, 2, 3) == [None, None]
    body = capsys.readouterr().out
    assert "--- Document 2/3 ---" in body
    assert "--- Document 3/3 ---" in body


def test_push_adds_metadata_and_timestamps_to_each_document() -> None:
    """Every pushed document carries the interval and both snapshot times."""
    elasticsearch = _StubElasticsearch()
    successful, errors = push_servers(
        jupyterhub_client=_StubHub([{"user.name": "a@x.edu", "server.name": ""}]),
        sink=ElasticsearchSink(elasticsearch, _INDEX),
        interval="5m",
        metadata={"hub": "production"},
    )
    assert (successful, errors) == (1, 0)
    document = elasticsearch.uploaded[0]
    assert document["meta.hub"] == "production"
    assert document["meta.interval"] == "5m"
    assert isinstance(document["meta.snapshot-time"], int)
    assert document["meta.snapshot-time-iso"].endswith("+00:00")
    assert elasticsearch.indexes == [_INDEX]
    assert elasticsearch.doc_ids == [_make_doc_id(document)]


def test_every_document_in_a_run_shares_one_snapshot_time() -> None:
    """Concurrency is computed by grouping on the snapshot time."""
    elasticsearch = _StubElasticsearch()
    push_servers(
        jupyterhub_client=_StubHub([{"user.name": f"u{i}@x.edu"} for i in range(3)]),
        sink=ElasticsearchSink(elasticsearch, _INDEX),
        interval="5m",
    )
    assert len({d["meta.snapshot-time"] for d in elasticsearch.uploaded}) == 1


def test_push_hands_the_limit_to_the_client() -> None:
    """The cap reaches the fetch, so a capped run is a small one."""
    hub = _StubHub([{"user.name": f"u{i}@x.edu"} for i in range(5)])
    push_servers(
        jupyterhub_client=hub,
        sink=PrintingSink(),
        interval="5m",
        limit=2,
    )
    assert hub.limits == [2]


def test_push_without_a_limit_asks_for_everything() -> None:
    """No cap is passed along as no cap, not as a number of its own."""
    hub = _StubHub([{"user.name": "a@x.edu"}])
    push_servers(jupyterhub_client=hub, sink=PrintingSink(), interval="5m")
    assert hub.limits == [None]


def test_the_printing_sink_pushes_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Nothing reaches Elasticsearch, and the document is valid JSON."""
    successful, errors = push_servers(
        jupyterhub_client=_StubHub([{"user.name": "a@x.edu"}]),
        sink=PrintingSink(),
        interval="5m",
    )
    assert (successful, errors) == (1, 0)
    body = capsys.readouterr().out
    assert "--- Document 1/1 ---" in body
    document = json.loads(body[body.index("{") : body.rindex("}") + 1])
    assert document["user.name"] == "a@x.edu"
    assert document["meta.interval"] == "5m"


def test_one_rejected_document_does_not_abort_the_batch(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A single bad document is counted and reported; the rest still go."""
    elasticsearch = _StubElasticsearch(reject={0})
    successful, errors = push_servers(
        jupyterhub_client=_StubHub([{"user.name": f"u{i}@x.edu"} for i in range(3)]),
        sink=ElasticsearchSink(elasticsearch, _INDEX),
        interval="5m",
    )
    assert (successful, errors) == (2, 1)
    assert (
        "Error pushing server 1/3 (u0@x.edu/unknown): mapper_parsing_exception: rejected"
        in capsys.readouterr().err
    )


def test_an_empty_server_list_is_not_an_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An idle hub is a normal outcome."""
    elasticsearch = _StubElasticsearch()
    assert push_servers(
        jupyterhub_client=_StubHub([]),
        sink=ElasticsearchSink(elasticsearch, _INDEX),
        interval="5m",
    ) == (0, 0)
    assert "Found 0 server(s) to process" in capsys.readouterr().out
    assert elasticsearch.uploaded == []
    assert elasticsearch.requests == 0


def test_documents_are_pushed_in_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hub larger than one batch is split, and the last batch is short."""
    monkeypatch.setattr(push, "_BATCH_SIZE", 2)
    elasticsearch = _StubElasticsearch()
    successful, errors = push_servers(
        jupyterhub_client=_StubHub([{"user.name": f"u{i}@x.edu"} for i in range(5)]),
        sink=ElasticsearchSink(elasticsearch, _INDEX),
        interval="5m",
    )
    assert (successful, errors) == (5, 0)
    assert elasticsearch.batch_sizes == [2, 2, 1]


def test_a_failed_request_does_not_abort_the_batches_that_follow(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Batching must not turn one bad round trip into a lost run."""
    monkeypatch.setattr(push, "_BATCH_SIZE", 2)
    elasticsearch = _StubElasticsearch(error_on={0})
    successful, errors = push_servers(
        jupyterhub_client=_StubHub([{"user.name": f"u{i}@x.edu"} for i in range(4)]),
        sink=ElasticsearchSink(elasticsearch, _INDEX),
        interval="5m",
    )
    assert (successful, errors) == (2, 2)
    assert elasticsearch.requests == 2
    assert [d["user.name"] for d in elasticsearch.uploaded] == ["u2@x.edu", "u3@x.edu"]
    err = capsys.readouterr().err
    assert "Error pushing server 1/4 (u0@x.edu/unknown): bulk request failed" in err
    assert "Error pushing server 2/4 (u1@x.edu/unknown): bulk request failed" in err


def test_a_document_keeps_its_number_across_batches(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The count is a place in the run, so it cannot restart at each batch."""
    monkeypatch.setattr(push, "_BATCH_SIZE", 2)
    push_servers(
        jupyterhub_client=_StubHub([{"user.name": f"u{i}@x.edu"} for i in range(3)]),
        sink=ElasticsearchSink(_StubElasticsearch(), _INDEX),
        interval="5m",
    )
    assert "Pushed server 3/3: u2@x.edu/unknown (result: created)" in capsys.readouterr().out


def test_every_document_in_a_batch_carries_its_own_id() -> None:
    """A batch built before stamping would collapse onto one id, silently."""
    elasticsearch = _StubElasticsearch()
    push_servers(
        jupyterhub_client=_StubHub([{"user.name": f"u{i}@x.edu"} for i in range(3)]),
        sink=ElasticsearchSink(elasticsearch, _INDEX),
        interval="5m",
    )
    assert elasticsearch.doc_ids == [_make_doc_id(d) for d in elasticsearch.uploaded]
    assert len(set(elasticsearch.doc_ids)) == 3
    assert not any(id_ and "unknown-snapshot" in id_ for id_ in elasticsearch.doc_ids)


def test_a_run_that_outlasts_its_interval_says_so(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Nothing downstream can see the overlap this invites, so the run reports it."""
    clock = iter([100.0, 500.0])
    monkeypatch.setattr(push, "monotonic", lambda: next(clock))
    successful, errors = push_servers(
        jupyterhub_client=_StubHub([{"user.name": "a@x.edu"}]),
        sink=ElasticsearchSink(_StubElasticsearch(), _INDEX),
        interval="5m",
    )
    # Slowness is not a failed push, so it must not reach the exit code.
    assert (successful, errors) == (1, 0)
    err = capsys.readouterr().err
    assert "Warning: this push took 400.0s" in err
    assert "longer than its --interval of 5m" in err


def test_a_run_inside_its_interval_stays_quiet(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The warning means something only if the ordinary run does not print it."""
    clock = iter([100.0, 101.0])
    monkeypatch.setattr(push, "monotonic", lambda: next(clock))
    push_servers(
        jupyterhub_client=_StubHub([{"user.name": "a@x.edu"}]),
        sink=ElasticsearchSink(_StubElasticsearch(), _INDEX),
        interval="5m",
    )
    assert capsys.readouterr().err == ""


def test_an_unparsable_interval_is_rejected() -> None:
    """get-new-activity could only drop such a document at report time."""
    with pytest.raises(DataShapeError, match="Invalid duration format"):
        push_servers(
            jupyterhub_client=_StubHub([]),
            sink=ElasticsearchSink(_StubElasticsearch(), _INDEX),
            interval="5 fortnights",
        )


@pytest.mark.parametrize("interval", ["0s", "-5m"])
def test_a_non_positive_interval_is_rejected(interval: str) -> None:
    """A zero or backwards cadence would make uptime meaningless."""
    with pytest.raises(DataShapeError, match="must be positive"):
        push_servers(
            jupyterhub_client=_StubHub([]),
            sink=ElasticsearchSink(_StubElasticsearch(), _INDEX),
            interval=interval,
        )


def test_the_interval_is_checked_before_the_hub_is_fetched() -> None:
    """A bad interval fails the run before the hub is contacted."""
    hub = _StubHub([])
    with pytest.raises(DataShapeError):
        push_servers(
            jupyterhub_client=hub,
            sink=ElasticsearchSink(_StubElasticsearch(), _INDEX),
            interval="5 fortnights",
        )
    assert hub.limits == []
