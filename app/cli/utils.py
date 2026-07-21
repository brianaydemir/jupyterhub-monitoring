"""Helper functions for building command-line interfaces."""

import argparse
import getpass
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from app.cli.runtime import parse_duration_required
from app.clients.elasticsearch_client import ElasticsearchClient
from app.clients.jupyterhub_client import JupyterHubClient
from app.core.errors import AuthCancelledError, DataShapeError
from app.core.time_utils import (
    compute_time_range,
    parse_datetime,
    parse_duration,
    parse_timezone,
)


def prompt_credentials(
    username_arg: str | None,
) -> tuple[str, str] | None:
    """Prompt for username and password interactively.

    Returns *None* if the user cancels (Ctrl-C / EOF) or if either value is
    empty after entry.
    """
    if username_arg:
        username = username_arg
    else:
        try:
            username = input("Username: ")
        except EOFError, KeyboardInterrupt:
            print("\nOperation cancelled.", file=sys.stderr)
            return None

    try:
        password = getpass.getpass("Password: ")
    except EOFError, KeyboardInterrupt:
        print("\nOperation cancelled.", file=sys.stderr)
        return None

    if not username or not password:
        print("Error: Username and password are required.", file=sys.stderr)
        return None

    return username, password


def add_jupyterhub_argument_group(
    parser: argparse.ArgumentParser,
) -> argparse._ArgumentGroup:  # pyright: ignore[reportPrivateUsage]
    """Add a "JupyterHub API" argument group and return it."""
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
            "(or set JUPYTERHUB_API_KEY)"
        ),
    )
    hub_group.add_argument(
        "--jupyterhub-ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )
    return hub_group


def validate_jupyterhub_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate JupyterHub credentials and CA certificate arguments."""
    if args.jupyterhub_ca_cert is not None and not args.jupyterhub_ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.jupyterhub_ca_cert}")

    if args.jupyterhub_api_key is not None:
        if not args.jupyterhub_api_key.exists():
            parser.error(f"API key file not found: {args.jupyterhub_api_key}")
    elif not os.environ.get("JUPYTERHUB_API_KEY"):
        parser.error(
            "--jupyterhub-api-key or the JUPYTERHUB_API_KEY environment variable is required"
        )


def add_es_argument_group(
    parser: argparse.ArgumentParser,
    *,
    required: bool = True,
) -> argparse._ArgumentGroup:  # pyright: ignore[reportPrivateUsage]
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
) -> argparse._ArgumentGroup:  # pyright: ignore[reportPrivateUsage]
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
        help="Username for basic authentication (password will be prompted)",
    )
    return es_group


def validate_es_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate Elasticsearch credentials and CA certificate after parsing.

    Calls ``parser.error`` if:

    - The CA certificate file does not exist (when provided).
    - Both ``--es-api-key`` / ``ELASTICSEARCH_API_KEY`` and
      ``--es-username`` are provided (mutually exclusive).
    - Neither is provided.
    - The ``--es-api-key`` file does not exist (when provided).

    """
    if args.es_ca_cert is not None and not args.es_ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.es_ca_cert}")

    has_api_key = args.es_api_key is not None or bool(os.environ.get("ELASTICSEARCH_API_KEY"))
    has_username = bool(getattr(args, "es_username", None))

    if has_api_key and has_username:
        parser.error(
            "--es-api-key / ELASTICSEARCH_API_KEY and --es-username are mutually exclusive"
        )
    if not has_api_key and not has_username:
        parser.error("one of --es-api-key, ELASTICSEARCH_API_KEY, or --es-username is required")
    if has_api_key and args.es_api_key is not None:
        if not args.es_api_key.exists():
            parser.error(f"API key file not found: {args.es_api_key}")


def add_query_argument_group(
    parser: argparse.ArgumentParser,
) -> argparse._ArgumentGroup:  # pyright: ignore[reportPrivateUsage]
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


def validate_query_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate the reporting window and ``--timezone``.

    Exactly one window mode must be selected: ``--duration`` (optionally
    anchored with ``--time``), or the ``--report-start`` / ``--report-end``
    pair.
    """
    try:
        parse_timezone(args.timezone)
    except ValueError as e:
        parser.error(str(e))

    has_range = args.report_start is not None or args.report_end is not None
    has_duration = args.duration is not None

    if has_range and has_duration:
        parser.error("--duration is mutually exclusive with --report-start / --report-end")
    if not has_range and not has_duration:
        parser.error("one of --duration or --report-start / --report-end is required")

    if has_range:
        _validate_report_range(args, parser)
        return

    if parse_duration(args.duration) is None:
        parser.error(f"Invalid duration format: {args.duration!r}")
    _validate_time(args, parser)


def _validate_report_range(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate the ``--report-start`` / ``--report-end`` window."""
    if args.report_start is None or args.report_end is None:
        parser.error("--report-start and --report-end must be used together")
    if args.time is not None:
        parser.error("--time cannot be combined with --report-start / --report-end")
    start = parse_datetime(args.report_start, args.timezone)
    if start is None:
        parser.error(f"Invalid --report-start datetime: {args.report_start!r}")
    end = parse_datetime(args.report_end, args.timezone)
    if end is None:
        parser.error(f"Invalid --report-end datetime: {args.report_end!r}")
    if start >= end:
        parser.error("--report-start must be before --report-end")


def _validate_time(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate the optional ``--time`` anchor for duration mode."""
    if args.time is None:
        return
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", args.time)
    if not m:
        parser.error("--time must be in HH:MM format")
    hh, mm = int(m.group(1)), int(m.group(2))
    if hh > 23 or mm > 59:
        parser.error("--time must be a valid time (HH: 0-23, MM: 0-59)")


def compute_report_time_range(
    args: argparse.Namespace,
) -> tuple[datetime, datetime]:
    """Return the ``(start, end)`` datetime pair for the reporting window.

    Handles both window modes. Assumes the arguments have already passed
    :func:`validate_query_arguments`.

    Raises:
        DataShapeError: If a duration-mode ``--duration`` cannot be parsed.
    """
    if args.report_start is not None:
        start = parse_datetime(args.report_start, args.timezone)
        end = parse_datetime(args.report_end, args.timezone)
        if start is None or end is None:
            raise DataShapeError("Invalid --report-start / --report-end datetime")
        return start, end

    duration_td = parse_duration_required(args.duration)
    return compute_time_range(duration_td, args.time, parse_timezone(args.timezone))


def add_output_argument_group(
    parser: argparse.ArgumentParser,
    *,
    include_date_format: bool = True,
    date_format_default: str = "date",
    include_anonymize: bool = False,
) -> argparse._ArgumentGroup:  # pyright: ignore[reportPrivateUsage]
    """Add report output-format arguments and return the group."""
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
        other = "datetime" if date_format_default == "date" else "date"
        output_group.add_argument(
            "--date-format",
            choices=["date", "datetime"],
            default=date_format_default,
            help=(
                f"Format for timestamps: '{date_format_default}' (default) or "
                f"'{other}'; 'date' is YYYY-MM-DD, 'datetime' is YYYY-MM-DD HH:MM"
            ),
        )
    output_group.add_argument(
        "--detailed-usernames",
        action="store_true",
        help='Always show the "Login method" column in the output',
    )
    if include_anonymize:
        output_group.add_argument(
            "--anonymize",
            action="store_true",
            help=(
                "Replace within-institution identifiers with generic "
                "pseudonyms; the institution is still shown"
            ),
        )
    return output_group


def add_email_arguments(
    group: argparse._ActionsContainer,  # pyright: ignore[reportPrivateUsage]
    *,
    default_subject: str,
    required: bool = False,
) -> None:
    """Add email-related arguments to a parser or argument group."""
    group.add_argument(
        "--sender-email",
        required=required,
        help=(
            "Sender email address" + (" (required with --send-email)" if not required else "")
        ),
    )
    group.add_argument(
        "--recipient-email",
        required=required,
        help=(
            "Recipient email address"
            + (" (required with --send-email)" if not required else "")
        ),
    )
    group.add_argument(
        "--sender-name",
        help="Sender display name",
    )
    group.add_argument(
        "--recipient-name",
        help="Recipient display name",
    )
    group.add_argument(
        "--subject",
        default=default_subject,
        help=f'Email subject line (default: "{default_subject}")',
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
    default_subject: str,
) -> argparse._ArgumentGroup:  # pyright: ignore[reportPrivateUsage]
    """Add an "Email" argument group with ``--send-email`` and return it."""
    email_group = parser.add_argument_group("Email")
    email_group.add_argument(
        "--send-email",
        action="store_true",
        help="Send the report via email in addition to any file output",
    )
    add_email_arguments(email_group, default_subject=default_subject)
    return email_group


def validate_email_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate required email arguments when ``--send-email`` is set."""
    if args.send_email:
        for flag, attr in [
            ("--sender-email", "sender_email"),
            ("--recipient-email", "recipient_email"),
            ("--smtp-host", "smtp_host"),
            ("--smtp-port", "smtp_port"),
        ]:
            if getattr(args, attr) is None:
                parser.error(f"{flag} is required when --send-email is set")


def read_api_key(file: Path | None, env_var: str) -> str:
    """Read an API key from a file, falling back to an environment variable."""
    if file is not None:
        return file.read_text().strip()
    return os.environ[env_var]


def make_es_client(args: argparse.Namespace) -> ElasticsearchClient:
    """Build an Elasticsearch client from parsed args.

    When ``args.es_username`` is set, prompts for a
    password via :func:`prompt_credentials`.

    Raises:
        AuthCancelledError: If interactive credential
            entry is cancelled.
    """
    if args.es_username:
        credentials = prompt_credentials(args.es_username)
        if credentials is None:
            raise AuthCancelledError("Credential entry cancelled")
        username, password = credentials
        return ElasticsearchClient(
            endpoint=args.es_endpoint,
            basic_auth=(username, password),
            ca_cert=args.es_ca_cert,
        )
    return ElasticsearchClient(
        endpoint=args.es_endpoint,
        api_key=read_api_key(args.es_api_key, "ELASTICSEARCH_API_KEY"),
        ca_cert=args.es_ca_cert,
    )


def make_jupyterhub_client(args: argparse.Namespace) -> JupyterHubClient:
    """Build a JupyterHub client from parsed args."""
    return JupyterHubClient(
        endpoint=args.jupyterhub_endpoint,
        api_key=read_api_key(args.jupyterhub_api_key, "JUPYTERHUB_API_KEY"),
        ca_cert=args.jupyterhub_ca_cert,
    )


def get_strftime_fmt(args: argparse.Namespace) -> str:
    """Return the strftime format string for ``--date-format``."""
    return "%Y-%m-%d" if args.date_format == "date" else "%Y-%m-%d %H:%M"


# Per-source dispatch for report commands.  A command's ``source`` is one
# or more of these tokens joined with ``"+"`` (e.g. ``"jupyterhub+es"``),
# so a single command can require several credential sets.
_SOURCE_ADDERS = {
    "jupyterhub": add_jupyterhub_argument_group,
    "es": add_es_argument_group,
}
_SOURCE_VALIDATORS = {
    "jupyterhub": validate_jupyterhub_arguments,
    "es": validate_es_arguments,
}


def configure_report_parser(
    parser: argparse.ArgumentParser,
    *,
    source: str,
    default_subject: str,
    include_date_format: bool = True,
    date_format_default: str = "date",
    include_anonymize: bool = False,
) -> argparse._ArgumentGroup:  # pyright: ignore[reportPrivateUsage]
    """Configure shared report command argument groups.

    *source* is one or more of ``"jupyterhub"`` and ``"es"`` joined with
    ``"+"`` (e.g. ``"jupyterhub+es"``).

    Raises:
        ValueError: If *source* contains an unsupported token.
    """
    for token in source.split("+"):
        adder = _SOURCE_ADDERS.get(token)
        if adder is None:
            raise ValueError(f"Unsupported report source: {token!r}")
        adder(parser)

    query_group = add_query_argument_group(parser)
    add_output_argument_group(
        parser,
        include_date_format=include_date_format,
        date_format_default=date_format_default,
        include_anonymize=include_anonymize,
    )
    add_email_argument_group(parser, default_subject=default_subject)
    return query_group


def validate_report_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    *,
    source: str,
) -> None:
    """Run shared validation for report commands.

    *source* is one or more of ``"jupyterhub"`` and ``"es"`` joined with
    ``"+"`` (e.g. ``"jupyterhub+es"``).

    Raises:
        ValueError: If *source* contains an unsupported token.
    """
    for token in source.split("+"):
        validator = _SOURCE_VALIDATORS.get(token)
        if validator is None:
            raise ValueError(f"Unsupported report source: {token!r}")
        validator(args, parser)
    validate_query_arguments(args, parser)
    validate_email_arguments(args, parser)


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


def validate_es_admin_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run shared validation for Elasticsearch admin commands."""
    validate_es_arguments(args, parser)
