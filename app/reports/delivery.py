"""Report delivery helpers for stdout, files, and email."""

import argparse
import sys
from pathlib import Path

from app.reports.email_utils import make_report_email_args, send_report_email
from app.reports.model import Report, ReportAttachment
from app.reports.renderers import (
    render_csv_bytes,
    render_html,
    render_text,
    render_xlsx_bytes,
    report_attachments,
)


class RenderedReport:
    """Cached rendered report content for selected destinations."""

    def __init__(self, report: Report) -> None:
        """Store report object and initialize lazy render cache."""
        self.report = report
        self._text: str | None = None
        self._html: str | None = None

    def text(self) -> str:
        """Return plain text rendering, computing it once lazily."""
        if self._text is None:
            self._text = render_text(self.report)
        return self._text

    def html(self) -> str:
        """Return HTML rendering, computing it once lazily."""
        if self._html is None:
            self._html = render_html(self.report)
        return self._html


def deliver_report(args: argparse.Namespace, report: Report) -> int:
    """Deliver a report to configured destinations."""
    rendered = RenderedReport(report)
    attachments = report_attachments(report)

    print(f"---\n{rendered.text()}\n---")

    if args.text_file:
        args.text_file.write_text(rendered.text() + "\n", encoding="utf-8")
        print(f"Plain text output written to: {args.text_file}", file=sys.stderr)

    if args.html_file:
        args.html_file.write_text(rendered.html() + "\n", encoding="utf-8")
        print(f"HTML output written to: {args.html_file}", file=sys.stderr)

    if args.csv_file:
        args.csv_file.write_bytes(render_csv_bytes(report))
        print(f"CSV output written to: {args.csv_file}", file=sys.stderr)

    xlsx_attachment: ReportAttachment | None = _xlsx_attachment(args.xlsx_file, report)
    if args.xlsx_file:
        print(f"XLSX output written to: {args.xlsx_file}", file=sys.stderr)

    if args.send_email:
        email_args = make_report_email_args(args)
        email_attachments = attachments + ([xlsx_attachment] if xlsx_attachment else [])
        if send_report_email(
            email_args,
            text_content=rendered.text(),
            html_content=rendered.html(),
            attachments=email_attachments,
        ):
            return 1

    return 0


def _xlsx_attachment(path: Path | None, report: Report) -> ReportAttachment | None:
    """Write XLSX output if requested and return attachment payload."""
    if path is None:
        return None

    payload = render_xlsx_bytes(report)
    path.write_bytes(payload)
    return ReportAttachment(
        filename=path.name,
        payload=payload,
        mime_hint="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
