"""Report delivery helpers for stdout, files, and email."""

import sys
from dataclasses import dataclass
from pathlib import Path

from app.reports.email_utils import EmailConfig, send_report_email
from app.reports.model import Report, ReportAttachment
from app.reports.renderers import (
    render_csv_bytes,
    render_html_document,
    render_html_fragment,
    render_text,
    render_xlsx_bytes,
    report_attachments,
)


@dataclass(frozen=True)
class OutputTargets:
    """Where a report goes, besides stdout.

    A path is ``None`` when its flag was not given.
    *email* carries the settings for ``--send-email`` and is ``None``
    when the report is not being emailed,
    so the destination and its configuration cannot disagree.
    """

    text_file: Path | None
    html_file: Path | None
    csv_file: Path | None
    xlsx_file: Path | None
    email: EmailConfig | None


def deliver_report(targets: OutputTargets, report: Report) -> int:
    """Deliver a report to stdout and every configured destination.

    Returns:
        0 on success, or 1 if sending email failed.
        File output failures propagate instead,
        since there is nothing sensible to fall back to.

    Raises:
        OSError: If an output file cannot be written.
    """
    text = render_text(report)

    print(f"---\n{text}\n---")

    if targets.text_file:
        targets.text_file.write_text(text + "\n", encoding="utf-8")
        print(f"Plain text output written to: {targets.text_file}", file=sys.stderr)

    if targets.html_file:
        targets.html_file.write_text(render_html_document(report), encoding="utf-8")
        print(f"HTML output written to: {targets.html_file}", file=sys.stderr)

    if targets.csv_file:
        targets.csv_file.write_bytes(render_csv_bytes(report))
        print(f"CSV output written to: {targets.csv_file}", file=sys.stderr)

    xlsx_payload: bytes | None = None
    if targets.xlsx_file:
        xlsx_payload = render_xlsx_bytes(report)
        targets.xlsx_file.write_bytes(xlsx_payload)
        print(f"XLSX output written to: {targets.xlsx_file}", file=sys.stderr)

    if targets.email is not None:
        return send_report_email(
            targets.email,
            text_content=text,
            html_content=render_html_fragment(report),
            attachments=_email_attachments(report, targets.xlsx_file, xlsx_payload),
        )

    return 0


def _email_attachments(
    report: Report,
    xlsx_file: Path | None,
    xlsx_payload: bytes | None,
) -> list[ReportAttachment]:
    """Collect the attachments to send, adding the workbook if one was built.

    The workbook is attached only when ``--xlsx-file`` also wrote it,
    so the email carries exactly the artifacts the run produced.
    """
    attachments = report_attachments(report)
    if xlsx_file is not None and xlsx_payload is not None:
        attachments.append(
            ReportAttachment(
                filename=xlsx_file.name,
                payload=xlsx_payload,
                mime_hint="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        )
    return attachments
