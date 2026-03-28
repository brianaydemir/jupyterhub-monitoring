"""Report delivery helpers for stdout, files, and email."""

import argparse
import sys

from app.core.errors import DataShapeError
from app.reports.email_utils import make_report_email_args, send_report_email
from app.reports.model import Report
from app.reports.renderers import render_html, render_text, report_attachments


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
        csv_attachments = [a for a in attachments if (a.mime_hint or "") == "text/csv"]
        if len(csv_attachments) > 1:
            names = ", ".join(a.filename for a in csv_attachments)
            raise DataShapeError(
                "--csv-file supports exactly one CSV attachment; "
                + f"report generated multiple CSV attachments ({names}). "
                + "Use email delivery to receive all attachments."
            )
        if len(csv_attachments) == 1:
            args.csv_file.write_bytes(csv_attachments[0].payload)
            print(f"CSV output written to: {args.csv_file}", file=sys.stderr)
        else:
            args.csv_file.write_text("", encoding="utf-8")
            print(f"CSV output written to: {args.csv_file}", file=sys.stderr)

    if args.send_email:
        email_args = make_report_email_args(args)
        if send_report_email(
            email_args,
            text_content=rendered.text(),
            html_content=rendered.html(),
            attachments=attachments,
        ):
            return 1

    return 0
