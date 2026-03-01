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


def create_message(
    sender_name: str | None,
    sender_email: str,
    recipient_name: str | None,
    recipient_email: str,
    subject: str,
    text_file: Path | None,
    html_file: Path | None,
    subject_heading: bool = True,
    attachments: list[Path] | None = None,
) -> MIMEMultipart:
    """Create an email message with the given parameters.

    Args:
        sender_name: The sender's display name (optional)
        sender_email: The sender's email address
        recipient_name: The recipient's display name (optional)
        recipient_email: The recipient's email address
        subject: The email subject line
        text_file: Path to a plain text file for the email body (optional)
        html_file: Path to an HTML file for the email body (optional)
        subject_heading: Whether to prepend the subject as a heading in the
            body (default: True)
        attachments: Paths to files to attach (optional)

    Returns:
        A MIMEMultipart message ready to send
    """
    body = MIMEMultipart("alternative")

    # Read and attach plain text content
    if text_file:
        text_content = text_file.read_text(encoding="utf-8")
        if subject_heading:
            text_content = f"{subject}\n{'-' * len(subject)}\n\n{text_content}"
        body.attach(MIMEText(text_content, "plain"))

    # Read and attach HTML content
    if html_file:
        html_content = html_file.read_text(encoding="utf-8")
        if subject_heading:
            html_content = f"<h1>{subject}</h1>\n{html_content}"
        body.attach(MIMEText(html_content, "html"))

    if attachments:
        msg = MIMEMultipart("mixed")
        msg.attach(body)
        for path in attachments:
            mime_type, _ = mimetypes.guess_type(str(path))
            if mime_type is None:
                mime_type = "application/octet-stream"
            maintype, subtype = mime_type.split("/", 1)
            part = MIMEBase(maintype, subtype)
            part.set_payload(path.read_bytes())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment", filename=path.name)
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


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Send an email via SMTP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Email addresses (required)
    parser.add_argument(
        "--sender-email",
        required=True,
        help="The sender's email address",
    )
    parser.add_argument(
        "--recipient-email",
        required=True,
        help="The recipient's email address",
    )

    # Names (optional)
    parser.add_argument(
        "--sender-name",
        help="The sender's display name",
    )
    parser.add_argument(
        "--recipient-name",
        help="The recipient's display name",
    )

    # Subject (optional with default)
    parser.add_argument(
        "--subject",
        default="JupyterHub Monitoring Report",
        help='Email subject line (default: "JupyterHub Monitoring Report")',
    )

    # SMTP server settings (required)
    parser.add_argument(
        "--smtp-host",
        required=True,
        help="SMTP server hostname",
    )
    parser.add_argument(
        "--smtp-port",
        type=int,
        required=True,
        help="SMTP server port",
    )

    # SSL/TLS (optional with default)
    parser.add_argument(
        "--no-ssl",
        action="store_true",
        help="Disable SSL/TLS (enabled by default)",
    )

    # Subject heading (optional with default)
    parser.add_argument(
        "--no-subject-heading",
        action="store_true",
        help="Do not prepend the subject as a heading in the message body",
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
            text_file=args.text_file,
            html_file=args.html_file,
            subject_heading=not args.no_subject_heading,
            attachments=args.attachment,
        )

        # Send the email
        send_email(
            smtp_host=args.smtp_host,
            smtp_port=args.smtp_port,
            use_ssl=not args.no_ssl,
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
