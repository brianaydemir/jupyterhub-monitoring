"""Library helpers for composing and sending emails via SMTP."""

import argparse
import mimetypes
import smtplib
import sys
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
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


def _make_attachment_part(
    filename: str,
    payload: bytes,
    mime_type: str | None = None,
) -> MIMEBase:
    """Create a base64-encoded MIME attachment part.

    When *mime_type* is ``None``, the type is guessed from
    *filename*, falling back to ``application/octet-stream``.
    """
    if mime_type is None:
        mime_type, _ = mimetypes.guess_type(filename)
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
    report_attachments: list[ReportAttachment] | None = None,
) -> MIMEMultipart:
    """Build an email message with text/HTML bodies and attachments."""
    body = MIMEMultipart("alternative")

    if text_content:
        body.attach(MIMEText(text_content, "plain"))

    if html_content:
        body.attach(MIMEText(html_content, "html"))

    if report_attachments:
        msg = MIMEMultipart("mixed")
        msg.attach(body)
        for att in report_attachments:
            msg.attach(
                _make_attachment_part(
                    att.filename,
                    att.payload,
                    att.mime_hint,
                )
            )
    else:
        msg = body

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
        OSError: If the SMTP connection fails.
        smtplib.SMTPException: If an SMTP-level error
            occurs.
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
            report_attachments=attachments,
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


def make_report_email_args(args: argparse.Namespace) -> _ReportEmailArgs:
    """Extract report-email attributes from a parsed args namespace."""
    return cast(
        _ReportEmailArgs,
        SimpleNamespace(
            sender_name=args.sender_name,
            sender_email=args.sender_email,
            recipient_name=args.recipient_name,
            recipient_email=args.recipient_email,
            subject=args.subject,
            smtp_host=args.smtp_host,
            smtp_port=args.smtp_port,
            smtp_no_ssl=args.smtp_no_ssl,
        ),
    )
