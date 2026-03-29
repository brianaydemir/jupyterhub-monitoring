"""Helper functions for building command-line interfaces."""

import argparse
import getpass
import os
import re
import sys
from pathlib import Path

from app.clients.elasticsearch_client import ElasticsearchClient
from app.clients.jupyterhub_client import JupyterHubClient
from app.core.errors import AuthCancelledError
from app.core.time_utils import parse_duration, parse_timezone


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
        except (EOFError, KeyboardInterrupt):
            print("\nOperation cancelled.", file=sys.stderr)
            return None

    try:
        password = getpass.getpass("Password: ")
    except (EOFError, KeyboardInterrupt):
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
            "--es-api-key / ELASTICSEARCH_API_KEY and " + "--es-username are mutually exclusive"
        )
    if not has_api_key and not has_username:
        parser.error(
            "one of --es-api-key, ELASTICSEARCH_API_KEY, or " + "--es-username is required"
        )
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
        required=True,
        help=(
            'Time window to look back from now (e.g., "30 seconds", "15 min", '
            '"12h", "7 days", "3d 6h 12m")'
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
            "Timezone for --time and all output timestamps "
            '(e.g., "America/Chicago", "MST", "+04:00"); default: America/Chicago'
        ),
    )
    return query_group


def validate_query_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate ``--duration``, ``--time``, and ``--timezone``."""
    if parse_duration(args.duration) is None:
        parser.error(f"Invalid duration format: {args.duration!r}")

    if args.time is not None:
        if not re.fullmatch(r"\d{1,2}:\d{2}", args.time):
            parser.error("--time must be in HH:MM format")

    try:
        parse_timezone(args.timezone)
    except ValueError as e:
        parser.error(str(e))


def add_output_argument_group(
    parser: argparse.ArgumentParser,
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
    output_group.add_argument(
        "--date-format",
        choices=["date", "datetime"],
        default="date",
        help=(
            "Format for creation timestamps: 'date' (default, YYYY-MM-DD) or "
            "'datetime' (YYYY-MM-DD HH:MM)"
        ),
    )
    output_group.add_argument(
        "--detailed-usernames",
        action="store_true",
        help='Always show the "Login method" column in the output',
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
        help="Sender email address" + (" (required with --send-email)" if not required else ""),
    )
    group.add_argument(
        "--recipient-email",
        required=required,
        help="Recipient email address"
        + (" (required with --send-email)" if not required else ""),
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
        help="SMTP server hostname" + (" (required with --send-email)" if not required else ""),
    )
    group.add_argument(
        "--smtp-port",
        type=int,
        required=required,
        help="SMTP server port" + (" (required with --send-email)" if not required else ""),
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
    """Construct an :class:`~app.elasticsearch_client.ElasticsearchClient` from parsed args.

    When ``args.es_username`` is set, prompts for a password via
    :func:`prompt_credentials`.

    Raises:
        AuthCancelledError: If interactive credential entry is cancelled.
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
    """Construct a :class:`~app.jupyterhub_client.JupyterHubClient` from parsed args."""
    return JupyterHubClient(
        endpoint=args.jupyterhub_endpoint,
        api_key=read_api_key(args.jupyterhub_api_key, "JUPYTERHUB_API_KEY"),
        ca_cert=args.jupyterhub_ca_cert,
    )


def get_strftime_fmt(args: argparse.Namespace) -> str:
    """Return the strftime format string for ``--date-format``."""
    return "%Y-%m-%d" if args.date_format == "date" else "%Y-%m-%d %H:%M"


def configure_report_parser(
    parser: argparse.ArgumentParser,
    *,
    source: str,
    default_subject: str,
) -> argparse._ArgumentGroup:  # pyright: ignore[reportPrivateUsage]
    """Configure shared report command argument groups.

    Raises:
        ValueError: If *source* is not ``"jupyterhub"`` or ``"es"``.
    """
    if source == "jupyterhub":
        add_jupyterhub_argument_group(parser)
    elif source == "es":
        add_es_argument_group(parser)
    else:
        raise ValueError(f"Unsupported report source: {source!r}")

    query_group = add_query_argument_group(parser)
    add_output_argument_group(parser)
    add_email_argument_group(parser, default_subject=default_subject)
    return query_group


def validate_report_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    *,
    source: str,
) -> None:
    """Run shared validation for report commands.

    Raises:
        ValueError: If *source* is not ``"jupyterhub"`` or ``"es"``.
    """
    if source == "jupyterhub":
        validate_jupyterhub_arguments(args, parser)
    elif source == "es":
        validate_es_arguments(args, parser)
    else:
        raise ValueError(f"Unsupported report source: {source!r}")
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
