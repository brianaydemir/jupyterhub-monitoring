"""Tests for :mod:`app.cli.report_arguments`.

The module's job
is to keep a report command's argument groups and its validation in step,
so the tests check the pairing: what each source adds, what it then checks,
and that the shared groups follow the sources.

The last sections instead build the two report commands' own parsers,
which is where the ``include_*`` switches are set
and where the reporting window each command shares is driven end to end.
Each command's own test module covers its query and its stderr tally,
not its argument surface.
"""

# The last sections reach for each command's own parser builder and validator.
# pyright: reportPrivateUsage=false

import argparse
from collections.abc import Callable

import pytest

from app.cli.get_new_activity import _build_parser as build_activity_parser
from app.cli.get_new_activity import _validate_arguments as validate_activity_arguments
from app.cli.get_new_users import _build_parser as build_users_parser
from app.cli.get_new_users import _validate_arguments as validate_users_arguments
from app.cli.report_arguments import (
    ELASTICSEARCH_SOURCE,
    JUPYTERHUB_SOURCE,
    OPTIONAL_ELASTICSEARCH_SOURCE,
    ReportSource,
    configure_report_parser,
    validate_report_arguments,
)


@pytest.fixture(autouse=True)
def without_ambient_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the host's own API keys from deciding a case's outcome."""
    monkeypatch.delenv("JUPYTERHUB_API_KEY", raising=False)
    monkeypatch.delenv("ELASTICSEARCH_API_KEY", raising=False)


def _parser(
    *sources: ReportSource,
    include_anonymize: bool = True,
) -> argparse.ArgumentParser:
    """Build a report parser drawing on *sources*."""
    parser = argparse.ArgumentParser(prog="cmd")
    configure_report_parser(
        parser,
        sources=sources,
        subject_prefix="Subject",
        include_anonymize=include_anonymize,
    )
    return parser


def _reject(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    sources: tuple[ReportSource, ...],
    capsys: pytest.CaptureFixture[str],
) -> str:
    """Validate *args*, assert it exited 2, and return what it printed."""
    with pytest.raises(SystemExit) as exit_info:
        validate_report_arguments(args, parser, sources=sources)
    assert exit_info.value.code == 2
    return capsys.readouterr().err


def test_the_query_group_is_returned_for_the_caller_to_extend() -> None:
    """get-new-activity adds --hub to it."""
    parser = argparse.ArgumentParser(prog="cmd")
    query_group = configure_report_parser(
        parser,
        sources=(ELASTICSEARCH_SOURCE,),
        subject_prefix="Subject",
    )
    query_group.add_argument("--hub")
    args = parser.parse_args(
        ["--es-endpoint", "https://es.example.edu:9200", "--es-index", "i", "--hub", "prod"]
    )
    assert args.hub == "prod"


def test_each_source_adds_its_own_arguments() -> None:
    """Naming two sources produces the union of their groups."""
    parser = _parser(JUPYTERHUB_SOURCE, OPTIONAL_ELASTICSEARCH_SOURCE)
    args = parser.parse_args(["--jupyterhub-endpoint", "https://hub.example.edu/hub/api"])
    assert args.jupyterhub_endpoint == "https://hub.example.edu/hub/api"
    assert args.es_endpoint is None
    assert args.es_index is None


def test_a_source_validates_the_arguments_it_added(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A missing JupyterHub credential is reported by its own source."""
    sources = (JUPYTERHUB_SOURCE,)
    parser = _parser(*sources)
    args = parser.parse_args(
        ["--jupyterhub-endpoint", "https://hub.example.edu/hub/api", "--duration", "7 days"]
    )
    message = _reject(parser, args, sources, capsys)
    assert "JUPYTERHUB_API_KEY" in message


def test_the_window_is_validated_after_the_sources(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Every report command needs a window, whatever it reads from."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    sources = (JUPYTERHUB_SOURCE,)
    parser = _parser(*sources)
    args = parser.parse_args(["--jupyterhub-endpoint", "https://hub.example.edu/hub/api"])
    message = _reject(parser, args, sources, capsys)
    assert "one of --duration or --report-start / --report-end is required" in message


def test_email_arguments_are_validated_too(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--send-email is what makes the connection arguments mandatory."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    sources = (JUPYTERHUB_SOURCE,)
    parser = _parser(*sources)
    args = parser.parse_args(
        [
            "--jupyterhub-endpoint",
            "https://hub.example.edu/hub/api",
            "--duration",
            "7 days",
            "--send-email",
        ]
    )
    message = _reject(parser, args, sources, capsys)
    assert "--sender-email is required when --send-email is set" in message


def test_an_absent_optional_elasticsearch_group_is_not_checked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without --es-endpoint, Elasticsearch is simply not used."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    sources = (JUPYTERHUB_SOURCE, OPTIONAL_ELASTICSEARCH_SOURCE)
    parser = _parser(*sources)
    args = parser.parse_args(
        ["--jupyterhub-endpoint", "https://hub.example.edu/hub/api", "--duration", "7 days"]
    )
    validate_report_arguments(args, parser, sources=sources)


def test_a_half_specified_optional_elasticsearch_group_is_an_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An endpoint without an index would otherwise be silently skipped."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    sources = (JUPYTERHUB_SOURCE, OPTIONAL_ELASTICSEARCH_SOURCE)
    parser = _parser(*sources)
    args = parser.parse_args(
        [
            "--jupyterhub-endpoint",
            "https://hub.example.edu/hub/api",
            "--duration",
            "7 days",
            "--es-endpoint",
            "https://es.example.edu:9200",
        ]
    )
    message = _reject(parser, args, sources, capsys)
    assert "--es-index is required when --es-endpoint is given" in message


def test_an_optional_elasticsearch_flag_without_the_endpoint_is_an_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An index without an endpoint would otherwise be silently skipped."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    sources = (JUPYTERHUB_SOURCE, OPTIONAL_ELASTICSEARCH_SOURCE)
    parser = _parser(*sources)
    args = parser.parse_args(
        [
            "--jupyterhub-endpoint",
            "https://hub.example.edu/hub/api",
            "--duration",
            "7 days",
            "--es-index",
            "servers",
        ]
    )
    message = _reject(parser, args, sources, capsys)
    assert "--es-index requires --es-endpoint" in message


def test_a_complete_optional_elasticsearch_group_needs_a_credential(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Once it is in use, the group faces the ordinary credential rules."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    sources = (JUPYTERHUB_SOURCE, OPTIONAL_ELASTICSEARCH_SOURCE)
    parser = _parser(*sources)
    args = parser.parse_args(
        [
            "--jupyterhub-endpoint",
            "https://hub.example.edu/hub/api",
            "--duration",
            "7 days",
            "--es-endpoint",
            "https://es.example.edu:9200",
            "--es-index",
            "servers",
        ]
    )
    message = _reject(parser, args, sources, capsys)
    assert "one of --es-api-key, ELASTICSEARCH_API_KEY, or --es-username is required" in message


#
# --date-format, per command
#

ACTIVITY_ARGV = ["--es-endpoint", "https://es.example.edu:9200", "--es-index", "servers"]
USERS_ARGV = ["--jupyterhub-endpoint", "https://hub.example.edu/hub/api"]


def test_the_activity_report_takes_no_date_format(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Its ``Started`` column names a session, which a bare date does not.

    Two same-day runs of one user's one server would otherwise render
    as rows differing only in a duration.
    """
    parser = build_activity_parser()
    assert not hasattr(parser.parse_args(ACTIVITY_ARGV), "date_format")
    assert "--date-format" not in parser.format_help()
    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args([*ACTIVITY_ARGV, "--date-format", "datetime"])
    assert exit_info.value.code == 2
    assert "--date-format" in capsys.readouterr().err


@pytest.mark.parametrize("choice", ["date", "datetime"])
def test_the_new_users_report_takes_either_date_format(choice: str) -> None:
    """Its timestamps date a user rather than identify one, so both serve."""
    args = build_users_parser().parse_args([*USERS_ARGV, "--date-format", choice])
    assert args.date_format == choice


def test_the_new_users_report_defaults_to_a_bare_date() -> None:
    """Without the option, a user's timestamps are bare dates."""
    assert build_users_parser().parse_args(USERS_ARGV).date_format == "date"


#
# The reporting window, per command
#

Builder = Callable[[], argparse.ArgumentParser]
CommandValidator = Callable[[argparse.Namespace, argparse.ArgumentParser], None]

COMMANDS = [
    pytest.param(
        build_activity_parser, validate_activity_arguments, ACTIVITY_ARGV, id="activity"
    ),
    pytest.param(build_users_parser, validate_users_arguments, USERS_ARGV, id="users"),
]

PARSERS = [
    pytest.param(build_activity_parser, ACTIVITY_ARGV, id="activity"),
    pytest.param(build_users_parser, USERS_ARGV, id="users"),
]


@pytest.fixture
def with_ambient_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Satisfy both commands' credential checks, whichever is under test."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    monkeypatch.setenv("ELASTICSEARCH_API_KEY", "secret")


@pytest.mark.usefixtures("with_ambient_credentials")
@pytest.mark.parametrize(("build_parser", "validate", "argv"), COMMANDS)
def test_either_command_rejects_a_future_report_end(
    build_parser: Builder,
    validate: CommandValidator,
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No data can exist past the present, whichever report asks for it."""
    parser = build_parser()
    args = parser.parse_args(
        [*argv, "--report-start", "2 weeks ago", "--report-end", "tomorrow"]
    )
    with pytest.raises(SystemExit) as exit_info:
        validate(args, parser)
    assert exit_info.value.code == 2
    message = capsys.readouterr().err
    assert "--report-end resolves to" in message
    assert "which is in the future" in message


@pytest.mark.usefixtures("with_ambient_credentials")
@pytest.mark.parametrize(("build_parser", "validate", "argv"), COMMANDS)
def test_either_command_accepts_whole_days(
    build_parser: Builder,
    validate: CommandValidator,
    argv: list[str],
) -> None:
    """The flag is declared once, in the "Query" group both commands share."""
    parser = build_parser()
    args = parser.parse_args(
        [
            *argv,
            "--report-start",
            "2 weeks ago",
            "--report-end",
            "yesterday",
            "--whole-days",
        ]
    )
    assert args.whole_days is True
    validate(args, parser)
