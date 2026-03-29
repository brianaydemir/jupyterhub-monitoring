"""Library helpers for composing and sending emails via SMTP."""

import mimetypes
import smtplib
import sys
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
from types import SimpleNamespace
from typing import Protocol, cast

from app.reports.model import ReportAttachment


# pylint: disable=too-few-public-methods
class _ReportEmailArgs(Protocol):
    """Typed view of the email-related CLI arguments used by report senders."""

    sender_name: str | None
    sender_email: str
    recipient_name: str | None
    recipient_email: str
    subject: str
    smtp_host: str
    smtp_port: int
    smtp_no_ssl: bool


def _make_attachment_part(mime_hint: str, payload: bytes, filename: str) -> MIMEBase:
    """Create a base64-encoded MIME attachment part."""
    mime_type, _ = mimetypes.guess_type(mime_hint)
    if mime_type is None:
        mime_type = "application/octet-stream"
    maintype, subtype = mime_type.split("/", 1)
    part = MIMEBase(maintype, subtype)
    part.set_payload(payload)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", "attachment", filename=filename)
    return part


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
    """Build an email message from text/HTML bodies and optional attachments."""
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
            msg.attach(_make_attachment_part(str(path), path.read_bytes(), path.name))
        for filename, data in attachment_data or []:
            msg.attach(_make_attachment_part(filename, data, filename))
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

    Raises:
        OSError: If the connection to the SMTP server fails.
        SMTPException: If an SMTP-level error occurs.
    """
    if use_ssl:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.send_message(message, sender_email, recipient_email)
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.send_message(message, sender_email, recipient_email)


def send_report_email(
    args: _ReportEmailArgs,
    text_content: str,
    html_content: str,
    attachments: list[ReportAttachment],
) -> int:
    """Build and send a report email with one or more attachments."""
    try:
        message = create_message(
            sender_name=args.sender_name,
            sender_email=args.sender_email,
            recipient_name=args.recipient_name,
            recipient_email=args.recipient_email,
            subject=args.subject,
            text_content=text_content,
            html_content=html_content,
            attachment_data=[
                (attachment.filename, attachment.payload) for attachment in attachments
            ],
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


def make_report_email_args(args: object) -> _ReportEmailArgs:
    """Extract only report-email attributes from an arbitrary args object."""
    return cast(
        _ReportEmailArgs,
        SimpleNamespace(
            sender_name=getattr(args, "sender_name"),
            sender_email=getattr(args, "sender_email"),
            recipient_name=getattr(args, "recipient_name"),
            recipient_email=getattr(args, "recipient_email"),
            subject=getattr(args, "subject"),
            smtp_host=getattr(args, "smtp_host"),
            smtp_port=getattr(args, "smtp_port"),
            smtp_no_ssl=getattr(args, "smtp_no_ssl"),
        ),
    )
