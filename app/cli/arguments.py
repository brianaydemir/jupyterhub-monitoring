"""The argument groups a command adds to its parser.

Each helper here only declares arguments.
Checking what was parsed is :mod:`app.cli.validation`'s job,
and reading values back out of the namespace is :mod:`app.cli.factories`'s.
"""

import argparse
from email.headerregistry import AddressHeader, HeaderRegistry
from email.utils import getaddresses
from pathlib import Path
from typing import cast

# argparse keeps its argument-group types private,
# but the helpers below need them in their signatures.
# Aliasing them here keeps the linter suppressions to these two lines.
# pylint: disable=protected-access
type ArgumentGroup = argparse._ArgumentGroup  # pyright: ignore[reportPrivateUsage]
type ActionsContainer = argparse._ActionsContainer  # pyright: ignore[reportPrivateUsage]
# pylint: enable=protected-access

# The parser behind ``To:``, which ``_recipient`` uses to detect group syntax.
_HEADERS = HeaderRegistry()


def add_jupyterhub_argument_group(
    parser: argparse.ArgumentParser,
) -> ArgumentGroup:
    """Add a "JupyterHub API" argument group and return it.

    Every ``add_*_argument_group`` helper returns its group
    so a command can add further arguments to it,
    as the report commands do with :func:`add_hub_argument`.
    Callers with nothing to add may ignore the return value.
    """
    hub_group = parser.add_argument_group("JupyterHub API")
    hub_group.add_argument(
        "--jupyterhub-endpoint",
        required=True,
        help="JupyterHub API endpoint URL (e.g., https://hub.example.com/hub/api)",
    )
    hub_group.add_argument(
        "--jupyterhub-api-key",
        type=Path,
        help=(
            "Path to file containing the JupyterHub API key for authentication "
            "(or set JUPYTERHUB_API_KEY); it needs the scopes list:users, "
            "read:users, and read:servers"
        ),
    )
    hub_group.add_argument(
        "--jupyterhub-ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )
    return hub_group


def add_es_argument_group(
    parser: argparse.ArgumentParser,
    *,
    required: bool = True,
) -> ArgumentGroup:
    """Add an "Elasticsearch" argument group and return it."""
    es_group = add_es_connection_argument_group(parser, required=required)
    es_group.add_argument(
        "--es-index",
        required=required,
        help="Name of the Elasticsearch index",
    )
    return es_group


def add_es_connection_argument_group(
    parser: argparse.ArgumentParser,
    *,
    required: bool = True,
) -> ArgumentGroup:
    """Add Elasticsearch connection arguments and return the group."""
    es_group = parser.add_argument_group("Elasticsearch")
    es_group.add_argument(
        "--es-endpoint",
        required=required,
        help="Elasticsearch API endpoint URL (e.g., https://localhost:9200)",
    )
    es_group.add_argument(
        "--es-ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )
    es_group.add_argument(
        "--es-api-key",
        type=Path,
        help=(
            "Path to file containing the Elasticsearch API key for authentication "
            "(or set ELASTICSEARCH_API_KEY)"
        ),
    )
    es_group.add_argument(
        "--es-username",
        metavar="USERNAME",
        help=(
            "Username for basic authentication (password will be prompted); "
            "takes precedence over ELASTICSEARCH_API_KEY"
        ),
    )
    return es_group


def add_prometheus_argument_group(
    parser: argparse.ArgumentParser,
) -> ArgumentGroup:
    """Add a "Prometheus" argument group, whose flags are all optional, and return it."""
    prometheus_group = parser.add_argument_group("Prometheus")
    prometheus_group.add_argument(
        "--prometheus-endpoint",
        help=(
            "Prometheus server base URL (e.g., https://prometheus.example.com); "
            "when given, the report adds each started session's peak memory"
        ),
    )
    prometheus_group.add_argument(
        "--prometheus-ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )
    prometheus_group.add_argument(
        "--prometheus-token",
        type=Path,
        help=(
            "Path to file containing a bearer token for Prometheus "
            "(or set PROMETHEUS_TOKEN); without either, requests are unauthenticated"
        ),
    )
    return prometheus_group


def add_query_argument_group(
    parser: argparse.ArgumentParser,
) -> ArgumentGroup:
    """Add query-window arguments and return the group."""
    query_group = parser.add_argument_group("Query")
    query_group.add_argument(
        "--duration",
        help=(
            'Time window to look back from now (e.g., "30 seconds", "15 min", '
            '"12h", "7 days", "3d 6h 12m"). '
            "Mutually exclusive with --report-start / --report-end"
        ),
    )
    query_group.add_argument(
        "--report-start",
        metavar="WHEN",
        help=(
            "Start of an explicit reporting window, as a human-readable "
            'datetime (e.g., "2 weeks ago", "July 1 2026", '
            '"2026-07-01 15:00"). Must be used together with --report-end '
            "and is mutually exclusive with --duration"
        ),
    )
    query_group.add_argument(
        "--report-end",
        metavar="WHEN",
        help=(
            "End of an explicit reporting window, as a human-readable "
            'datetime (e.g., "yesterday", "now"). Must be used together '
            "with --report-start and is mutually exclusive with --duration"
        ),
    )
    query_group.add_argument(
        "--whole-days",
        action="store_true",
        help=(
            "Widen an explicit reporting window to whole days in the given "
            "timezone: --report-start moves back to midnight and --report-end "
            "forward to the next one, or back to its own when that midnight "
            "has not yet arrived. A bound already at midnight does not move. "
            "Mutually exclusive with --duration"
        ),
    )
    query_group.add_argument(
        "--time",
        metavar="HH:MM",
        help=(
            "Interpret --duration as ending at the most recent occurrence of this "
            "wall-clock time (in the given timezone) within the past 24 hours"
        ),
    )
    query_group.add_argument(
        "--timezone",
        default="America/Chicago",
        metavar="TZ",
        help=(
            "Timezone for --time, for interpreting --report-start / "
            "--report-end values without an explicit offset, and for all "
            'output timestamps (e.g., "America/Chicago", "MST", "+04:00"); '
            "default: America/Chicago"
        ),
    )
    return query_group


def add_hub_argument(
    group: ActionsContainer,
) -> None:
    """Add the ``--hub`` filter that both report commands share."""
    group.add_argument(
        "--hub",
        help="Filter results to documents whose meta.hub is exactly this value",
    )


def add_output_argument_group(
    parser: argparse.ArgumentParser,
    *,
    include_date_format: bool = True,
) -> ArgumentGroup:
    """Add report output-format arguments and return the group.

    Clearing *include_date_format* leaves ``--date-format`` out,
    for a report whose timestamps identify a row rather than merely date it.
    Such a command must also stop reading ``args.date_format``,
    which is then absent from the namespace.
    """
    output_group = parser.add_argument_group("Output")
    output_group.add_argument(
        "--text-file",
        type=Path,
        help="Write output as plain text to the specified file",
    )
    output_group.add_argument(
        "--html-file",
        type=Path,
        help="Write output as HTML to the specified file (suitable for email body)",
    )
    output_group.add_argument(
        "--csv-file",
        type=Path,
        help=(
            "Write output as a single CSV file. For reports with multiple tables, "
            "all sections/blocks are appended with separator rows."
        ),
    )
    output_group.add_argument(
        "--xlsx-file",
        type=Path,
        help="Write output as an Excel workbook (.xlsx) with one sheet per table block",
    )
    if include_date_format:
        output_group.add_argument(
            "--date-format",
            choices=["date", "datetime"],
            default="date",
            help=(
                "Format for timestamps: 'date' (default) or 'datetime'; "
                "'date' is YYYY-MM-DD, 'datetime' is YYYY-MM-DD HH:MM"
            ),
        )
    output_group.add_argument(
        "--detailed-usernames",
        action="store_true",
        help='Always show the "Login method" column in the output',
    )
    output_group.add_argument(
        "--anonymize",
        action="store_true",
        help=(
            "Replace within-institution identifiers with generic "
            "pseudonyms; the institution is still shown. The pseudonyms "
            "are an unkeyed hash of the username, not a privacy guarantee"
        ),
    )
    return output_group


def _recipient(value: str) -> str:
    """Check that one ``--recipient-email`` value names usable mailboxes.

    The value is written as a ``To:`` header writes it,
    so it may list several mailboxes.
    It is returned as typed, stripped;
    rendering it is :func:`app.reports.email_utils.create_message`'s job.

    ``getaddresses`` is too forgiving to trust on its own:
    a malformed value yields an empty address,
    a quoted ``"a@host, b@host"`` yields one address holding a comma,
    and a bare display name yields its first word.
    Requiring an ``@`` and no comma in every address catches all three,
    and also refuses a local name such as ``root``.

    Group syntax (``Team: a@host, b@host;``) is refused,
    since the group name would be dropped.
    ``getaddresses`` cannot see a group,
    so the header registry parses the value a second time to find one.

    Raises:
        argparse.ArgumentTypeError: If the value names no mailbox,
            names one whose address is unreadable, or is written as a group.
    """
    mailboxes = getaddresses([value])
    unreadable = any("@" not in address or "," in address for _, address in mailboxes)
    if not mailboxes or unreadable:
        raise argparse.ArgumentTypeError(f"could not read a recipient from {value!r}")
    # ``HeaderRegistry`` builds its header classes at run time,
    # so it is typed as returning the base class;
    # ``To`` is an address header, and only those carry ``groups``.
    header = cast(AddressHeader, _HEADERS("To", value))
    if any(group.display_name is not None for group in header.groups):
        raise argparse.ArgumentTypeError(f"group syntax is not supported in {value!r}")
    return value.strip()


def add_email_arguments(
    group: ActionsContainer,
    *,
    subject_prefix: str,
    required: bool = False,
) -> None:
    """Add email-related arguments to a parser or argument group.

    *subject_prefix* is the fixed part of the default subject line,
    which :func:`app.cli.factories.make_output_targets` completes
    with the reporting window.
    ``--subject`` therefore defaults to ``None`` rather than to a string:
    a parser is built before the window is resolved,
    and a literal default could not be told apart from the same text typed out.

    With *required* set, the connection arguments become mandatory
    and their help text drops the "(required with --send-email)" note.
    The report commands gate them on ``--send-email`` instead
    (:func:`app.cli.validation.validate_email_arguments`),
    so they leave it unset.
    """
    group.add_argument(
        "--sender-email",
        required=required,
        help=(
            "Sender email address" + (" (required with --send-email)" if not required else "")
        ),
    )
    group.add_argument(
        "--recipient-email",
        action="append",
        type=_recipient,
        required=required,
        metavar="RECIPIENT",
        help=(
            "Recipient address, optionally with a display name; for several, "
            + "separate them with commas or repeat the flag"
            + (" (required with --send-email)" if not required else "")
        ),
    )
    group.add_argument(
        "--sender-name",
        help="Sender display name",
    )
    group.add_argument(
        "--subject",
        help=(
            f'Email subject line (default: "{subject_prefix}" followed by the '
            "reporting window's dates)"
        ),
    )
    group.add_argument(
        "--smtp-host",
        required=required,
        help=(
            "SMTP server hostname" + (" (required with --send-email)" if not required else "")
        ),
    )
    group.add_argument(
        "--smtp-port",
        type=int,
        required=required,
        help=("SMTP server port" + (" (required with --send-email)" if not required else "")),
    )
    group.add_argument(
        "--smtp-no-ssl",
        action="store_true",
        help="Disable SSL/TLS for the SMTP connection (SSL enabled by default)",
    )


def add_email_argument_group(
    parser: argparse.ArgumentParser,
    *,
    subject_prefix: str,
) -> ArgumentGroup:
    """Add an "Email" argument group with ``--send-email`` and return it."""
    email_group = parser.add_argument_group("Email")
    email_group.add_argument(
        "--send-email",
        action="store_true",
        help="Send the report via email in addition to any file output",
    )
    add_email_arguments(email_group, subject_prefix=subject_prefix)
    return email_group


def add_format_argument(
    parser: argparse.ArgumentParser,
    *,
    key_format_summary: str,
) -> None:
    """Add the ``--format`` argument the API-key commands share.

    Only what the ``key`` format prints differs between them,
    so that is all a caller supplies: *key_format_summary* is spliced
    into the help text (for example, ``"encoded key only"``).
    """
    parser.add_argument(
        "--format",
        choices=["key", "json", "full"],
        default="key",
        help=(
            f"Output format: 'key' (default, {key_format_summary}), "
            "'json', or 'full' (human-readable)"
        ),
    )


def configure_es_admin_parser(
    parser: argparse.ArgumentParser,
    *,
    include_index: bool = False,
) -> None:
    """Configure shared Elasticsearch admin command argument groups."""
    if include_index:
        add_es_argument_group(parser)
    else:
        add_es_connection_argument_group(parser)
