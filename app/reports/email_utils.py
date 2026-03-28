"""Library helpers for composing and sending emails via SMTP."""

import mimetypes
import smtplib
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, getaddresses, make_msgid

from app.reports.model import ReportAttachment


@dataclass(frozen=True)
class EmailConfig:
    """The settings a report email is composed and sent with.

    The CLI layer builds one from its parsed arguments, so nothing here
    has to know how those arguments are spelled. One field per email
    flag.

    *recipients* holds at least one entry, each of them one or more
    ``To:`` header mailboxes as they were typed — a bare address, an
    address with a display name, or a comma-separated list of either.
    An entry therefore need not be one recipient, and the count of
    entries is not the count of people the report reaches. Turning them
    into a header is :func:`_format_recipients`'s job, and into envelope
    addresses :func:`send_email`'s.
    """

    sender_name: str | None
    sender_email: str
    recipients: tuple[str, ...]
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
    recipients: Sequence[str],
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

    msg["To"] = _format_recipients(recipients)
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = _make_message_id(sender_email)

    return msg


def _mailboxes(recipients: Sequence[str]) -> list[tuple[str, str]]:
    """Parse each recipient into a display name and an address.

    An entry may name several mailboxes, so this returns a flat list
    rather than one pair per entry.

    Each entry is parsed on its own rather than the joined list, which
    is not the same thing: ``getaddresses`` given every entry at once
    parses their comma-joined text as a single header field, so one
    entry with an unbalanced quote swallows the entries after it and
    returns a single empty mailbox. Parsing one at a time keeps a bad
    entry from costing anybody else their copy, and matches how
    :func:`app.cli.arguments._recipient` checked the entry in the first
    place: that function requires every mailbox it parses out of an
    entry to carry a readable address, and refuses group syntax, so no
    group name reaches here to be dropped.
    """
    return [mailbox for recipient in recipients for mailbox in getaddresses([recipient])]


def _format_recipients(recipients: Sequence[str]) -> str:
    """Render the ``To`` header for one or more recipients.

    Each entry is parsed and rendered again rather than passed through.
    A display name is the reason: the legacy email policy encodes a
    header it cannot send as ASCII all in one piece, so a name with an
    accent in it would take the address into the encoded word with it
    and leave no address a reader could reply to. ``formataddr``
    encodes the name alone. A bare address is unchanged by the
    round trip, and a name holding a comma comes back quoted.
    """
    return ", ".join(formataddr(mailbox) for mailbox in _mailboxes(recipients))


def _make_message_id(sender_email: str) -> str:
    """Return a Message-ID scoped to the sender's domain.

    :func:`~email.utils.make_msgid` otherwise uses the local hostname,
    which puts an internal host name into every outgoing report.
    """
    _, at_sign, domain = sender_email.rpartition("@")
    return make_msgid(domain=domain if at_sign and domain else None)


def send_email(
    smtp_host: str,
    smtp_port: int,
    use_ssl: bool,
    sender_email: str,
    recipients: Sequence[str],
    message: MIMEMultipart,
) -> None:
    """Send an email message via SMTP.

    The envelope recipients are the bare addresses parsed out of
    *recipients*, since an entry may carry a display name that only the
    ``To`` header wants. A bare string passed here would be taken for a
    sequence of one-character recipients, which no type checker can
    catch: :class:`str` is itself a sequence of strings.

    Raises:
        OSError: If the SMTP connection fails.
        smtplib.SMTPException: If an SMTP-level error
            occurs.
    """
    to_addrs = [address for _, address in _mailboxes(recipients)]
    if use_ssl:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.send_message(message, sender_email, to_addrs)
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.send_message(message, sender_email, to_addrs)


def send_report_email(
    config: EmailConfig,
    text_content: str,
    html_content: str,
    attachments: list[ReportAttachment],
) -> int:
    """Build and send a report email with one or more attachments.

    Returns:
        An exit code: 0 if the message was sent, 1 if it was not. The
        failure is reported on stderr rather than raised, so that file
        output already written is not discarded.
    """
    try:
        message = create_message(
            sender_name=config.sender_name,
            sender_email=config.sender_email,
            recipients=config.recipients,
            subject=config.subject,
            text_content=text_content,
            html_content=html_content,
            report_attachments=attachments,
        )
        send_email(
            smtp_host=config.smtp_host,
            smtp_port=config.smtp_port,
            use_ssl=not config.smtp_no_ssl,
            sender_email=config.sender_email,
            recipients=config.recipients,
            message=message,
        )
        print("Email sent successfully", file=sys.stderr)
        return 0
    except (OSError, smtplib.SMTPException) as e:
        print(f"Error sending email: {e}", file=sys.stderr)
        return 1
