"""Helper functions for building command-line interfaces."""

import argparse
import os
import re
from pathlib import Path

from app.time_utils import parse_timezone


def add_jupyterhub_argument_group(
    parser: argparse.ArgumentParser,
) -> argparse._ArgumentGroup:
    """Add a "JupyterHub API" argument group to a parser and return it.

    Adds ``--jupyterhub-endpoint``, ``--jupyterhub-api-key``, and
    ``--jupyterhub-ca-cert``. The returned group can be used to append
    additional script-specific arguments.

    Use :func:`validate_jupyterhub_arguments` after parsing to validate the
    API key and CA certificate.

    Args:
        parser: The argument parser to add the group to

    Returns:
        The newly created argument group
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
    """Validate JupyterHub API key and CA certificate arguments after parsing.

    Calls ``parser.error`` if the CA certificate file does not exist (when
    provided), the API key file does not exist (when provided), or neither
    ``--jupyterhub-api-key`` nor the ``JUPYTERHUB_API_KEY`` environment
    variable is set.

    Args:
        args: Parsed command-line arguments
        parser: The argument parser (used to report errors)
    """
    if args.jupyterhub_ca_cert is not None and not args.jupyterhub_ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.jupyterhub_ca_cert}")

    if args.jupyterhub_api_key is not None:
        if not args.jupyterhub_api_key.exists():
            parser.error(f"API key file not found: {args.jupyterhub_api_key}")
    elif not os.environ.get("JUPYTERHUB_API_KEY"):
        parser.error(
            "--jupyterhub-api-key or the JUPYTERHUB_API_KEY environment variable is required"
        )


def add_query_argument_group(
    parser: argparse.ArgumentParser,
) -> argparse._ArgumentGroup:
    """Add a "Query" argument group to a parser and return it.

    Adds ``--duration``, ``--time``, and ``--timezone``. The returned group
    can be used to append additional script-specific arguments.

    Use :func:`validate_query_arguments` after parsing to validate ``--time``
    and ``--timezone``.

    Args:
        parser: The argument parser to add the group to

    Returns:
        The newly created argument group
    """
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
    """Validate ``--time`` format and ``--timezone`` after parsing.

    Calls ``parser.error`` if ``--time`` is not in HH:MM format or if
    ``--timezone`` is not a recognized timezone.

    Args:
        args: Parsed command-line arguments
        parser: The argument parser (used to report errors)
    """
    if args.time is not None:
        if not re.fullmatch(r"\d{1,2}:\d{2}", args.time):
            parser.error("--time must be in HH:MM format")

    try:
        parse_timezone(args.timezone)
    except ValueError as e:
        parser.error(str(e))


def add_output_argument_group(
    parser: argparse.ArgumentParser,
) -> None:
    """Add an "Output" argument group to a parser.

    Adds ``--text-file``, ``--html-file``, ``--csv-file``,
    ``--date-format``, and ``--detailed-usernames``.

    Args:
        parser: The argument parser to add the group to
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
        help="Write output as CSV to the specified file",
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


def add_email_arguments(
    group: argparse._ActionsContainer,
    *,
    default_subject: str,
    required: bool = False,
) -> None:
    """Add email-related arguments to an argument group or parser.

    When *required* is True, ``--sender-email``, ``--recipient-email``,
    ``--smtp-host``, and ``--smtp-port`` are marked as required by argparse.
    Use this when email sending is always intended (e.g., in ``send-email``).

    When *required* is False (the default), those four fields are optional at
    the argparse level and should be validated separately (e.g., via
    :func:`validate_email_arguments`).

    Args:
        group: The argument group or parser to add arguments to
        default_subject: Default value for the ``--subject`` argument
        required: Whether sender/recipient/SMTP fields are argparse-required
    """
    group.add_argument(
        "--sender-email",
        required=required,
        help="Sender email address"
        + (" (required with --send-email)" if not required else ""),
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
        help="SMTP server hostname"
        + (" (required with --send-email)" if not required else ""),
    )
    group.add_argument(
        "--smtp-port",
        type=int,
        required=required,
        help="SMTP server port"
        + (" (required with --send-email)" if not required else ""),
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
) -> None:
    """Add an "Email" argument group with ``--send-email`` to a parser.

    Adds ``--send-email`` followed by all core email arguments (via
    :func:`add_email_arguments`). Use :func:`validate_email_arguments` in
    ``parse_arguments`` to enforce the required fields when ``--send-email``
    is set.

    Args:
        parser: The argument parser to add the group to
        default_subject: Default value for the ``--subject`` argument
    """
    email_group = parser.add_argument_group("Email")
    email_group.add_argument(
        "--send-email",
        action="store_true",
        help="Send the report via email in addition to any file output",
    )
    add_email_arguments(email_group, default_subject=default_subject)


def validate_email_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate that required email arguments are present when ``--send-email`` is set.

    Calls ``parser.error`` for each missing required field.

    Args:
        args: Parsed command-line arguments
        parser: The argument parser (used to report errors)
    """
    if args.send_email:
        for flag, attr in [
            ("--sender-email", "sender_email"),
            ("--recipient-email", "recipient_email"),
            ("--smtp-host", "smtp_host"),
            ("--smtp-port", "smtp_port"),
        ]:
            if getattr(args, attr) is None:
                parser.error(f"{flag} is required when --send-email is set")
