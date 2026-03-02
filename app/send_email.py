"""Command-line tool for sending emails via SMTP."""

import argparse
import mimetypes
import smtplib
import sys
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path

from app.cli_utils import add_email_arguments


def create_message(
    sender_name: str | None,
    sender_email: str,
    recipient_name: str | None,
    recipient_email: str,
    subject: str,
    text_content: str | None,
    html_content: str | None,
    attachments: list[Path] | None = None,
    attachment_data: list[tuple[str, bytes]] | None = None,
) -> MIMEMultipart:
    """Create an email message with the given parameters.

    Args:
        sender_name: The sender's display name (optional)
        sender_email: The sender's email address
        recipient_name: The recipient's display name (optional)
        recipient_email: The recipient's email address
        subject: The email subject line
        text_content: Plain text body content (optional)
        html_content: HTML body content (optional)
        attachments: Paths to files to attach (optional)
        attachment_data: In-memory attachments as (filename, bytes) pairs
            (optional)

    Returns:
        A MIMEMultipart message ready to send
    """
    body = MIMEMultipart("alternative")

    # Read and attach plain text content
    if text_content:
        body.attach(MIMEText(text_content, "plain"))

    # Read and attach HTML content
    if html_content:
        body.attach(MIMEText(html_content, "html"))

    if attachments or attachment_data:
        msg = MIMEMultipart("mixed")
        msg.attach(body)
        for path in attachments or []:
            mime_type, _ = mimetypes.guess_type(str(path))
            if mime_type is None:
                mime_type = "application/octet-stream"
            maintype, subtype = mime_type.split("/", 1)
            part = MIMEBase(maintype, subtype)
            part.set_payload(path.read_bytes())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment", filename=path.name)
            msg.attach(part)
        for filename, data in attachment_data or []:
            mime_type, _ = mimetypes.guess_type(filename)
            if mime_type is None:
                mime_type = "application/octet-stream"
            maintype, subtype = mime_type.split("/", 1)
            part = MIMEBase(maintype, subtype)
            part.set_payload(data)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment", filename=filename)
            msg.attach(part)
    else:
        msg = body

    # Format the From and To fields with names if provided
    if sender_name:
        msg["From"] = formataddr((sender_name, sender_email))
    else:
        msg["From"] = sender_email

    if recipient_name:
        msg["To"] = formataddr((recipient_name, recipient_email))
    else:
        msg["To"] = recipient_email

    msg["Subject"] = subject

    return msg


def send_email(
    smtp_host: str,
    smtp_port: int,
    use_ssl: bool,
    sender_email: str,
    recipient_email: str,
    message: MIMEMultipart,
) -> None:
    """Send an email message via SMTP.

    Args:
        smtp_host: SMTP server hostname
        smtp_port: SMTP server port
        use_ssl: Whether to use SSL/TLS
        sender_email: The sender's email address
        recipient_email: The recipient's email address
        message: The message to send
    """
    if use_ssl:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.send_message(message, sender_email, recipient_email)
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.send_message(message, sender_email, recipient_email)


def send_report_email(
    args: argparse.Namespace,
    text_content: str,
    html_content: str,
    csv_content: str,
    attachment_filename: str,
) -> int:
    """Build and send a report email with a CSV attachment.

    Args:
        args: Parsed command-line arguments (must include ``sender_name``,
            ``sender_email``, ``recipient_name``, ``recipient_email``,
            ``subject``, ``smtp_host``, ``smtp_port``, ``smtp_no_ssl``)
        text_content: Plain-text body for the email
        html_content: HTML body for the email
        csv_content: CSV data to attach
        attachment_filename: Filename for the CSV attachment

    Returns:
        0 on success, 1 on failure (error is printed to stderr)
    """
    try:
        message = create_message(
            sender_name=args.sender_name,
            sender_email=args.sender_email,
            recipient_name=args.recipient_name,
            recipient_email=args.recipient_email,
            subject=args.subject,
            text_content=text_content,
            html_content=html_content,
            attachment_data=[(attachment_filename, csv_content.encode("utf-8"))],
        )
        send_email(
            smtp_host=args.smtp_host,
            smtp_port=args.smtp_port,
            use_ssl=not args.smtp_no_ssl,
            sender_email=args.sender_email,
            recipient_email=args.recipient_email,
            message=message,
        )
        print("Email sent successfully", file=sys.stderr)
        return 0
    except (OSError, smtplib.SMTPException) as e:
        print(f"Error sending email: {e}", file=sys.stderr)
        return 1


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Send an email via SMTP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Email sender, recipient, subject, and SMTP settings
    add_email_arguments(
        parser,
        default_subject="JupyterHub Monitoring Report",
        required=True,
    )

    # Email content (at least one required)
    parser.add_argument(
        "--text-file",
        type=Path,
        help="Path to a plain text file containing the email body",
    )
    parser.add_argument(
        "--html-file",
        type=Path,
        help="Path to an HTML file containing the email body",
    )
    parser.add_argument(
        "--attachment",
        type=Path,
        action="append",
        metavar="PATH",
        help="Path to a file to attach (may be repeated)",
    )

    args = parser.parse_args()

    # Validate that at least one content file is provided
    if not args.text_file and not args.html_file:
        parser.error("At least one of --text-file or --html-file must be provided")

    # Validate that files exist
    if args.text_file and not args.text_file.exists():
        parser.error(f"Text file not found: {args.text_file}")
    if args.html_file and not args.html_file.exists():
        parser.error(f"HTML file not found: {args.html_file}")
    for path in args.attachment or []:
        if not path.exists():
            parser.error(f"Attachment file not found: {path}")

    return args


def main() -> int:
    """Main entry point for the send-email script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        # Create the email message
        message = create_message(
            sender_name=args.sender_name,
            sender_email=args.sender_email,
            recipient_name=args.recipient_name,
            recipient_email=args.recipient_email,
            subject=args.subject,
            text_content=(
                args.text_file.read_text(encoding="utf-8") if args.text_file else None
            ),
            html_content=(
                args.html_file.read_text(encoding="utf-8") if args.html_file else None
            ),
            attachments=args.attachment,
        )

        # Send the email
        send_email(
            smtp_host=args.smtp_host,
            smtp_port=args.smtp_port,
            use_ssl=not args.smtp_no_ssl,
            sender_email=args.sender_email,
            recipient_email=args.recipient_email,
            message=message,
        )

        print("Email sent successfully")
        return 0

    except (OSError, smtplib.SMTPException) as e:
        print(f"Error sending email: {e}", file=sys.stderr)
        return 1
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
