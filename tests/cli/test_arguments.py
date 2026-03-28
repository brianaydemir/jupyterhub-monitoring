"""Tests for :mod:`app.cli.arguments`.

The helpers only declare arguments, so the tests parse with a real parser
and assert on what came back: which flags exist, which are mandatory, what
they default to, and what their help text says.
"""

import argparse
from pathlib import Path

import pytest

from app.cli.arguments import (
    add_email_argument_group,
    add_email_arguments,
    add_es_argument_group,
    add_es_connection_argument_group,
    add_format_argument,
    add_hub_argument,
    add_jupyterhub_argument_group,
    add_output_argument_group,
    add_query_argument_group,
    configure_es_admin_parser,
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
# JupyterHub
#


def test_the_jupyterhub_endpoint_is_required(capsys: pytest.CaptureFixture[str]) -> None:
    """A command in this group always talks to a hub."""
    parser = _parser()
    add_jupyterhub_argument_group(parser)
    assert "--jupyterhub-endpoint" in _missing_argument(parser, [], capsys)


def test_jupyterhub_file_arguments_parse_as_paths() -> None:
    """The validators check these for existence, so they must be paths."""
    parser = _parser()
    add_jupyterhub_argument_group(parser)
    args = parser.parse_args(
        [
            "--jupyterhub-endpoint",
            "https://hub.example.edu/hub/api",
            "--jupyterhub-api-key",
            "/keys/hub.txt",
            "--jupyterhub-ca-cert",
            "/certs/ca.pem",
        ]
    )
    assert args.jupyterhub_api_key == Path("/keys/hub.txt")
    assert args.jupyterhub_ca_cert == Path("/certs/ca.pem")


def test_an_argument_group_can_be_extended_by_its_caller() -> None:
    """Every add_* helper returns its group, as the report commands rely on."""
    parser = _parser()
    group = add_jupyterhub_argument_group(parser)
    group.add_argument("--hub")
    args = parser.parse_args(
        ["--jupyterhub-endpoint", "https://hub.example.edu/hub/api", "--hub", "production"]
    )
    assert args.hub == "production"


#
# Elasticsearch
#


def test_the_elasticsearch_group_is_required_by_default(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Both the connection and the index must be named."""
    parser = _parser()
    add_es_argument_group(parser)
    message = _missing_argument(parser, [], capsys)
    assert "--es-endpoint" in message
    assert "--es-index" in message


def test_the_elasticsearch_group_can_be_made_optional() -> None:
    """get-new-users and push-servers use Elasticsearch conditionally."""
    parser = _parser()
    add_es_argument_group(parser, required=False)
    args = parser.parse_args([])
    assert args.es_endpoint is None
    assert args.es_index is None


def test_the_connection_group_omits_the_index() -> None:
    """The API-key commands connect without naming an index."""
    parser = _parser()
    add_es_connection_argument_group(parser)
    args = parser.parse_args(["--es-endpoint", "https://es.example.edu:9200"])
    assert not hasattr(args, "es_index")


def test_elasticsearch_credential_arguments_parse_as_paths() -> None:
    """The key and certificate are read from files."""
    parser = _parser()
    add_es_connection_argument_group(parser, required=False)
    args = parser.parse_args(
        ["--es-api-key", "/keys/es.txt", "--es-ca-cert", "/certs/ca.pem", "--es-username", "u"]
    )
    assert args.es_api_key == Path("/keys/es.txt")
    assert args.es_ca_cert == Path("/certs/ca.pem")
    assert args.es_username == "u"


def test_the_admin_parser_omits_the_index_by_default() -> None:
    """The API-key commands operate on the cluster, not on an index."""
    parser = _parser()
    configure_es_admin_parser(parser, include_index=False)
    args = parser.parse_args(["--es-endpoint", "https://es.example.edu:9200"])
    assert not hasattr(args, "es_index")


def test_the_admin_parser_adds_the_index_when_asked() -> None:
    """get-es-docs queries one index, so it opts the argument back in."""
    parser = _parser()
    configure_es_admin_parser(parser, include_index=True)
    args = parser.parse_args(
        ["--es-endpoint", "https://es.example.edu:9200", "--es-index", "servers"]
    )
    assert args.es_index == "servers"


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


def test_whole_days_is_an_opt_in_flag() -> None:
    """Off unless asked for, so an existing command line keeps its window."""
    parser = _parser()
    add_query_argument_group(parser)
    assert parser.parse_args([]).whole_days is False
    assert parser.parse_args(["--whole-days"]).whole_days is True


def test_the_hub_filter_is_optional_and_shared() -> None:
    """Both report commands add the one --hub, so it is spelled once."""
    parser = _parser()
    group = add_query_argument_group(parser)
    add_hub_argument(group)
    assert parser.parse_args([]).hub is None
    assert parser.parse_args(["--hub", "production"]).hub == "production"


#
# Output
#


def test_output_file_arguments_parse_as_paths() -> None:
    """Each output format writes to a path the delivery layer opens."""
    parser = _parser()
    add_output_argument_group(parser)
    args = parser.parse_args(
        [
            "--text-file",
            "out.txt",
            "--html-file",
            "out.html",
            "--csv-file",
            "out.csv",
            "--xlsx-file",
            "out.xlsx",
        ]
    )
    assert args.text_file == Path("out.txt")
    assert args.html_file == Path("out.html")
    assert args.csv_file == Path("out.csv")
    assert args.xlsx_file == Path("out.xlsx")


@pytest.mark.parametrize("choice", ["date", "datetime"])
def test_the_date_format_offers_both_choices_and_defaults_to_a_bare_date(
    choice: str,
) -> None:
    """The help text names both, so the default is legible from it alone."""
    parser = _parser()
    add_output_argument_group(parser)
    assert parser.parse_args([]).date_format == "date"
    assert parser.parse_args(["--date-format", choice]).date_format == choice
    help_text = parser.format_help()
    assert "'date' (default)" in help_text
    assert "'datetime'" in help_text


def test_the_date_format_argument_can_be_left_out(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A command that omits it must also stop reading args.date_format."""
    parser = _parser()
    add_output_argument_group(parser, include_date_format=False)
    assert not hasattr(parser.parse_args([]), "date_format")
    assert "--date-format" in _missing_argument(parser, ["--date-format", "date"], capsys)


@pytest.mark.parametrize("include_anonymize", [False, True])
def test_anonymize_is_added_only_when_asked(include_anonymize: bool) -> None:
    """It is off by default so a command opts in deliberately."""
    parser = _parser()
    add_output_argument_group(parser, include_anonymize=include_anonymize)
    assert hasattr(parser.parse_args([]), "anonymize") is include_anonymize


def test_detailed_usernames_defaults_to_off() -> None:
    """The login-method column otherwise appears only when it is needed."""
    parser = _parser()
    add_output_argument_group(parser)
    assert parser.parse_args([]).detailed_usernames is False


#
# Email
#


def test_email_arguments_are_optional_by_default() -> None:
    """The report commands gate them on --send-email instead."""
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    args = parser.parse_args([])
    assert args.sender_email is None
    assert args.recipient_email is None
    assert args.smtp_host is None
    assert args.smtp_port is None
    assert args.smtp_no_ssl is False
    assert "(required with --send-email)" in parser.format_help()


def test_email_arguments_can_be_made_mandatory(capsys: pytest.CaptureFixture[str]) -> None:
    """With *required* set, the conditional note drops out of the help."""
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject", required=True)
    message = _missing_argument(parser, [], capsys)
    for flag in ["--sender-email", "--recipient-email", "--smtp-host", "--smtp-port"]:
        assert flag in message
    assert "(required with --send-email)" not in parser.format_help()


def test_recipients_can_be_repeated() -> None:
    """Each occurrence adds one entry, in the order they were given."""
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    args = parser.parse_args(
        [
            "--recipient-email",
            "a@example.edu",
            "--recipient-email",
            "b@example.edu",
        ]
    )
    assert args.recipient_email == ["a@example.edu", "b@example.edu"]


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

    The last two hold a group's punctuation inside a quoted display
    name, where it is ordinary text. They are accepted because the
    group check parses the value rather than scanning it for a ``:``
    or a ``;``.
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

    The last case is why the entry is not split here: the comma inside
    the quoted display name separates nothing.
    """
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    args = parser.parse_args(["--recipient-email", value])
    assert args.recipient_email == [value]


def test_a_list_and_a_repeated_flag_can_be_mixed() -> None:
    """Both spellings say the same thing, so both may appear at once."""
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
        "Team: a@example.edu, b@example.edu;",
        "a@example.edu, Team: b@example.edu;",
    ],
)
def test_group_syntax_is_refused(value: str, capsys: pytest.CaptureFixture[str]) -> None:
    """A group's name has nowhere to go, so the plain list is required.

    ``getaddresses`` reports a group as the mailboxes it holds and says
    nothing about its name, so accepting one would drop that name
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
    Every mailbox in a list has to be readable, so ``Ops Team, ...``
    fails on its first one; a quoted ``"a@host, b@host"`` is a single
    address holding a comma rather than two mailboxes; and a group
    naming no address is unreadable rather than refused as a group,
    since the address checks run first.
    """
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    message = _missing_argument(parser, ["--recipient-email", value], capsys)
    assert "could not read a recipient" in message


def test_a_recipient_display_name_has_no_flag_of_its_own() -> None:
    """The name is written into --recipient-email, beside its address."""
    parser = _parser()
    add_email_arguments(parser, subject_prefix="Subject")
    assert not hasattr(parser.parse_args([]), "recipient_name")
    assert "--recipient-name" not in parser.format_help()


def test_the_subject_has_no_default_the_parser_can_supply() -> None:
    """The window it names is resolved long after the parser is built."""
    parser = _parser()
    add_email_arguments(parser, subject_prefix="JupyterHub Activity Report")
    assert parser.parse_args([]).subject is None
    assert parser.parse_args(["--subject", "Ours"]).subject == "Ours"
    assert '"JupyterHub Activity Report" followed by the reporting window' in " ".join(
        parser.format_help().split()
    )


def test_the_email_group_adds_the_send_email_switch() -> None:
    """--send-email is what turns the rest of the group on."""
    parser = _parser()
    add_email_argument_group(parser, subject_prefix="JupyterHub Activity Report")
    assert parser.parse_args([]).send_email is False
    assert parser.parse_args(["--send-email"]).send_email is True


def test_the_smtp_port_parses_as_an_integer() -> None:
    """The delivery layer hands it straight to smtplib."""
    parser = _parser()
    add_email_argument_group(parser, subject_prefix="Subject")
    assert parser.parse_args(["--smtp-port", "465"]).smtp_port == 465


#
# Format
#


def test_the_format_argument_splices_in_its_summary() -> None:
    """Only what 'key' prints differs between the two commands."""
    parser = _parser()
    add_format_argument(parser, key_format_summary="encoded key only")
    assert parser.parse_args([]).format == "key"
    assert parser.parse_args(["--format", "json"]).format == "json"
    assert "'key' (default, encoded key only)" in parser.format_help()


def test_the_format_argument_rejects_an_unknown_format(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The choices are fixed, so a typo is a usage error."""
    parser = _parser()
    add_format_argument(parser, key_format_summary="id and name per line")
    assert "--format" in _missing_argument(parser, ["--format", "yaml"], capsys)
