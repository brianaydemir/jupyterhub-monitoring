"""Sample :mod:`app.reports.model` values for the report tests."""

from app.reports.model import Cell, Duration, Report, ReportSection, TableBlock, TextBlock


def table_block(
    *,
    headers: list[str] | None = None,
    rows: list[list[Cell]] | None = None,
    right_align_columns: set[int] | None = None,
    attachment_filename: str | None = None,
) -> TableBlock:
    """Build a table block, defaulting to a small two-column table."""
    return TableBlock(
        headers=headers if headers is not None else ["Metric", "Value"],
        rows=rows if rows is not None else [["Total active time", Duration(3600)]],
        right_align_columns=right_align_columns if right_align_columns is not None else {1},
        attachment_filename=attachment_filename,
    )


def report(
    *,
    sections: list[ReportSection] | None = None,
    footnotes: list[str] | None = None,
    title: str = "Test Report",
) -> Report:
    """Build a report, defaulting to one section with one table."""
    return Report(
        title=title,
        sections=(
            sections
            if sections is not None
            else [
                ReportSection(
                    heading="Summary",
                    description="Usage from 2026-07-01 00:00 to 2026-07-08 00:00.",
                    blocks=[table_block(attachment_filename="summary.csv")],
                    footnotes=["Timezone: America/Chicago"],
                )
            ]
        ),
        footnotes=footnotes if footnotes is not None else [],
    )


def report_with_text_block() -> Report:
    """Build a report exercising the text-block and report-footnote paths.

    No builder emits either today; they are extension points, so the
    renderers' handling of them is only covered here.
    """
    return report(
        sections=[
            ReportSection(
                heading="Narrative",
                description="A section mixing prose and a table.",
                blocks=[TextBlock(text="A free-form remark."), table_block()],
                footnotes=["A section note."],
            )
        ],
        footnotes=["A report-wide note."],
    )
