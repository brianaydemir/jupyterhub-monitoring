"""Tests for :mod:`app.cli.arguments`.

The helpers only declare arguments, so the tests parse with a real parser
and assert on what came back.
That a flag exists, or is mandatory, is argparse's doing and is not tested;
what is covered is the conversion of values to the types later code relies on,
the one default that matters to a user, and how a recipient is read.
"""

import argparse
from pathlib import Path

import pytest

from app.cli.arguments import (
    add_email_argument_group,
    add_email_arguments,
    add_es_connection_argument_group,
    add_jupyterhub_argument_group,
    add_output_argument_group,
    add_query_argument_group,
)


def _parser() -> argparse.ArgumentParser:
    """Return a parser that reports failures instead of exiting silently."""
    return argparse.ArgumentParser(prog="cmd")


def _missing_argument(
    parser: argparse.ArgumentParser,
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> str:
    """Parse *argv*, assert it failed as a usage error, and return stderr."""
    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(argv)
    assert exit_info.value.code == 2
    return capsys.readouterr().err


#
# Types
#


def test_file_arguments_parse_as_paths_and_the_port_as_an_integer() -> None:
    """The validators and the delivery layer use these values as typed."""
    parser = _parser()
    add_jupyterhub_argument_group(parser)
    add_es_connection_argument_group(parser, required=False)
    add_output_argument_group(parser)
    add_email_argument_group(parser, subject_prefix="Subject")
    args = parser.parse_args(
        [
            "--jupyterhub-endpoint",
            "https://hub.example.edu/hub/api",
            "--jupyterhub-api-key",
            "/keys/hub.txt",
            "--jupyterhub-ca-cert",
            "/certs/hub.pem",
            "--es-api-key",
            "/keys/es.txt",
            "--es-ca-cert",
            "/certs/es.pem",
            "--text-file",
            "out.txt",
            "--html-file",
            "out.html",
            "--csv-file",
            "out.csv",
            "--xlsx-file",
            "out.xlsx",
            "--smtp-port",
            "465",
        ]
    )
    for name in [
        "jupyterhub_api_key",
        "jupyterhub_ca_cert",
        "es_api_key",
        "es_ca_cert",
        "text_file",
        "html_file",
        "csv_file",
        "xlsx_file",
    ]:
        assert isinstance(getattr(args, name), Path)
    assert args.smtp_port == 465


#
# Query
#


def test_the_query_group_defaults_to_central_time() -> None:
    """Every other window argument is unset until the command is run."""
    parser = _parser()
    add_query_argument_group(parser)
    args = parser.parse_args([])
    assert args.timezone == "America/Chicago"
    assert args.duration is None
    assert args.report_start is None
    assert args.report_end is None
    assert args.time is None


#
# Email
#


def test_recipients_can_be_repeated() -> None:
    """Each occurrence adds one entry, in order, and may itself be a list."""
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    args = parser.parse_args(
        [
            "--recipient-email",
            "a@example.edu, b@example.edu",
            "--recipient-email",
            "c@example.edu",
        ]
    )
    assert args.recipient_email == ["a@example.edu, b@example.edu", "c@example.edu"]


@pytest.mark.parametrize(
    "value",
    [
        "Ops Team <ops@example.edu>",
        '"Doe, Jane" <jane@example.edu>',
        "<ops@example.edu>",
        "ops@example.edu (Ops Team)",
        '"Ops: Team" <ops@example.edu>',
        '"Doe; Jane" <jane@example.edu>',
    ],
)
def test_a_recipient_may_be_written_as_a_to_header_writes_one(value: str) -> None:
    """The value reaches the namespace unchanged; app.reports renders it.

    The last two hold a group's punctuation inside a quoted display name,
    where it is ordinary text.
    They are accepted because the group check parses the value
    rather than scanning it for a ``:`` or a ``;``.
    """
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    args = parser.parse_args(["--recipient-email", value])
    assert args.recipient_email == [value]


def test_surrounding_whitespace_is_dropped() -> None:
    """Padding is an artifact of quoting, not part of the address."""
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    args = parser.parse_args(["--recipient-email", "  ops@example.edu  "])
    assert args.recipient_email == ["ops@example.edu"]


@pytest.mark.parametrize(
    "value",
    [
        "a@example.edu, b@example.edu",
        "Ops Team <ops@example.edu>, dean@example.edu",
        '"Doe, Jane" <jane@example.edu>, dean@example.edu',
    ],
)
def test_one_occurrence_may_name_several_recipients(value: str) -> None:
    """A list is one entry, unsplit; app.reports parses it out again.

    The last case is why the entry is not split here:
    the comma inside the quoted display name separates nothing.
    """
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    args = parser.parse_args(["--recipient-email", value])
    assert args.recipient_email == [value]


@pytest.mark.parametrize(
    "value",
    [
        "Team: a@example.edu, b@example.edu;",
        "a@example.edu, Team: b@example.edu;",
    ],
)
def test_group_syntax_is_refused(value: str, capsys: pytest.CaptureFixture[str]) -> None:
    """A group's name has nowhere to go, so the plain list is required.

    ``getaddresses`` reports a group as the mailboxes it holds
    and says nothing about its name, so accepting one would drop that name
    without saying so.
    """
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    message = _missing_argument(parser, ["--recipient-email", value], capsys)
    assert "group syntax is not supported" in message


@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
        ",",
        "ops@example.edu,",
        "a@example.edu b@example.edu",
        "Ops Team",
        "root",
        ",ops@example.edu",
        '"a@example.edu, b@example.edu"',
        "Ops Team, dean@example.edu",
        "Team:;",
        "undisclosed-recipients:;",
    ],
)
def test_a_recipient_without_a_readable_address_is_refused(
    value: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Each of these parses to a mailbox whose address is unusable.

    ``Ops Team`` would otherwise be truncated to the recipient ``Ops``,
    and a bare local name such as ``root`` is refused along with it.
    Every mailbox in a list has to be readable,
    so ``Ops Team, ...`` fails on its first one;
    a quoted ``"a@host, b@host"`` is a single address holding a comma
    rather than two mailboxes; and a group naming no address is unreadable
    rather than refused as a group, since the address checks run first.
    """
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    message = _missing_argument(parser, ["--recipient-email", value], capsys)
    assert "could not read a recipient" in message
