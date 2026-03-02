"""Helper functions for building command-line interfaces."""

import argparse


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
