"""Tests for :mod:`app.reports.renderers`, including cross-format agreement."""

# An explicit comparison to an empty collection is the assertion
# these tests mean to make, and reads better here than a truthiness check.
# pylint: disable=use-implicit-booleaness-not-comparison

import csv
import io
from collections.abc import Callable
from datetime import timedelta

import pytest
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

from app.reports.model import (
    Cell,
    Duration,
    Fixed,
    Percent,
    Report,
    ReportAttachment,
    ReportSection,
)
from app.reports.renderers import (
    render_csv_bytes,
    render_html_document,
    render_html_fragment,
    render_text,
    render_xlsx_bytes,
    report_attachments,
)
from tests.helpers import sample_reports

Renderer = Callable[[Report], str]


def _csv_text(built: Report) -> str:
    """Decode the CSV rendering, which is the only entry point CSV has."""
    return render_csv_bytes(built).decode("utf-8")


_RENDERERS: list[Renderer] = [
    render_text,
    render_html_fragment,
    render_html_document,
    _csv_text,
]


@pytest.mark.parametrize("render", _RENDERERS)
def test_every_renderer_handles_a_report_with_no_rows(
    render: Renderer,
) -> None:
    """An empty report renders in every format rather than raising."""
    empty = sample_reports.report(
        sections=[
            ReportSection(
                heading="New Users",
                description="No new users created between A and B.",
                blocks=[sample_reports.table_block(rows=[])],
            )
        ]
    )
    assert render(empty)


def test_xlsx_handles_a_report_with_no_rows() -> None:
    """A table with no rows still gets a sheet, holding headers and notes."""
    empty = sample_reports.report(sections=[_section(rows=[])])
    assert _sheet_column(empty, "Summary") == [
        "Metric",
        None,
        "Notes",
        "Timezone: America/Chicago",
    ]


@pytest.mark.parametrize("render", _RENDERERS)
def test_every_renderer_includes_the_data(
    render: Renderer,
) -> None:
    """A cell's text reaches every text-based format."""
    rendered = render(sample_reports.report(sections=[_section(rows=[["Widgets", 42]])]))
    assert "Widgets" in rendered
    assert "42" in rendered


@pytest.mark.parametrize("render", _RENDERERS)
def test_every_renderer_shows_a_typed_cell_as_its_display_string(
    render: Renderer,
) -> None:
    """Only the workbook writes the value; the rest show the text of it."""
    rendered = render(
        sample_reports.report(
            sections=[_section(rows=[["Total active time", Duration(96000)]])]
        )
    )
    assert "26:40" in rendered
    assert "1.111" not in rendered


@pytest.mark.parametrize("render", _RENDERERS)
def test_every_renderer_labels_footnotes_identically(
    render: Renderer,
) -> None:
    """The four formats use one label for section notes and one for report notes."""
    rendered = render(sample_reports.report_with_text_block())
    assert "Notes" in rendered
    assert "Report notes" in rendered


def test_xlsx_labels_footnotes_identically() -> None:
    """The workbook's info sheet uses the same footnote labels."""
    cells = _info_sheet_column(sample_reports.report_with_text_block())
    assert "Notes" in cells
    assert "Report notes" in cells


@pytest.mark.parametrize("render", _RENDERERS)
def test_every_renderer_includes_a_text_block(
    render: Renderer,
) -> None:
    """Text blocks are an extension point, so no format may drop them."""
    assert "A free-form remark." in render(sample_reports.report_with_text_block())


def test_xlsx_includes_a_text_block() -> None:
    """The workbook keeps text blocks on its info sheet."""
    assert "A free-form remark." in _info_sheet_column(sample_reports.report_with_text_block())


def test_html_fragment_has_no_document_wrapper() -> None:
    """The fragment is meant for an email body, which supplies its own."""
    fragment = render_html_fragment(sample_reports.report())
    assert "<!doctype html>" not in fragment.lower()
    assert "<table" in fragment


def test_html_document_declares_a_doctype_and_charset() -> None:
    """A file a browser opens directly needs both, around unescaped markup."""
    document = render_html_document(sample_reports.report())
    assert document.startswith("<!doctype html>")
    assert '<meta charset="utf-8">' in document
    assert "<title>Test Report</title>" in document
    assert "<table" in document


def test_html_escapes_cell_text() -> None:
    """Report values come from usernames, so they must not become markup."""
    rendered = render_html_fragment(
        sample_reports.report(sections=[_section(rows=[["<b>x</b>", "1"]])])
    )
    assert "&lt;b&gt;x&lt;/b&gt;" in rendered
    assert "<b>x</b>" not in rendered


def test_csv_bytes_are_utf8_encoded_csv() -> None:
    """CSV is produced as bytes, so its text must survive the encoding."""
    built = sample_reports.report(sections=[_section(rows=[["Ünïcodé", "1"]])])
    payload = render_csv_bytes(built)
    assert "Ünïcodé".encode("utf-8") in payload
    assert payload.decode("utf-8").startswith("Test Report")


def test_csv_escapes_formula_like_text() -> None:
    """Report values come from usernames, so a spreadsheet must not run them.

    Every string the document holds is escaped, not only the table cells.
    """
    built = _formula_like_report()
    rows = _csv_rows(render_csv_bytes(built))
    assert ["'=1+1"] in rows
    assert ["'=SUM(A1:A2)", "'=cmd|' /c calc'!A1"] in rows
    assert ["'=SUM(B1:B2)"] in rows


@pytest.mark.parametrize("lead", ["=", "+", "-", "@", "\t", "\r"])
def test_csv_escapes_every_formula_lead_character(lead: str) -> None:
    """Excel and LibreOffice read each of these as the start of a formula."""
    built = sample_reports.report(sections=[_section(rows=[[f"{lead}x", "1"]])])
    assert [f"'{lead}x", "1"] in _csv_rows(render_csv_bytes(built))


def test_csv_leaves_a_negative_duration_unescaped() -> None:
    """A number is not text anyone chose, and the sign is the figure's own."""
    built = sample_reports.report(
        sections=[_section(rows=[["CPU active time", Duration(-1800)]])]
    )
    assert ["CPU active time", "-0:30"] in _csv_rows(render_csv_bytes(built))


def test_csv_attachment_escapes_formula_like_text() -> None:
    """The attachment path escapes too, so the two CSV entry points agree."""
    built = sample_reports.report(
        sections=[
            ReportSection(
                heading="Summary",
                description="d",
                blocks=[
                    sample_reports.table_block(
                        rows=[["=SUM(A1:A2)", "1"]], attachment_filename="summary.csv"
                    )
                ],
            )
        ]
    )
    attachments = report_attachments(built)
    assert len(attachments) == 1
    assert ["'=SUM(A1:A2)", "1"] in _csv_rows(attachments[0].payload)


def test_text_table_aligns_right_aligned_columns() -> None:
    """Duration columns are right-aligned so the digits line up."""
    rendered = render_text(
        sample_reports.report(
            sections=[_section(rows=[["a", Duration(3600)], ["b", Duration(360000)]])]
        )
    )
    rows = [line for line in rendered.splitlines() if line.startswith(("a ", "b "))]
    assert len(rows) == 2
    # Right-aligned values end in the same column and leave no trailing space.
    assert all(row.endswith(":00") for row in rows)
    assert len(rows[0]) == len(rows[1])


def test_xlsx_writes_one_sheet_per_table_after_the_info_sheet() -> None:
    """Sheet order is the info sheet, then each table in report order."""
    built = sample_reports.report(
        sections=[
            ReportSection(
                heading="Summary",
                description="d",
                blocks=[sample_reports.table_block()],
            ),
            ReportSection(
                heading="User Activity",
                description="d",
                blocks=[sample_reports.table_block()],
            ),
        ]
    )
    workbook = load_workbook(io.BytesIO(render_xlsx_bytes(built)))
    assert workbook.sheetnames == ["Report", "Summary", "User Activity"]


def test_xlsx_table_sheet_repeats_its_sections_footnotes() -> None:
    """A reader who clicks straight to the data still meets the notes."""
    built = sample_reports.report(sections=[_section(rows=[["Widgets", 42]])])
    assert _sheet_column(built, "Summary") == [
        "Metric",
        "Widgets",
        None,
        "Notes",
        "Timezone: America/Chicago",
    ]


def test_xlsx_table_sheet_of_a_section_without_footnotes_ends_at_the_data() -> None:
    """No notes means no trailing label and no trailing blank row."""
    built = sample_reports.report(
        sections=[
            ReportSection(
                heading="Summary",
                description="d",
                blocks=[sample_reports.table_block(rows=[["Widgets", 42]])],
            )
        ]
    )
    assert _table_sheet(built, "Summary").max_row == 2


def test_xlsx_info_sheet_keeps_the_notes_the_table_sheets_repeat() -> None:
    """The duplication is intended: the overview states them too."""
    built = sample_reports.report(sections=[_section(rows=[["Widgets", 42]])])
    assert _info_sheet_column(built) == [
        "Test Report",
        "Summary",
        "Usage from 2026-07-01 00:00 to 2026-07-08 00:00.",
        "Notes",
        "Timezone: America/Chicago",
    ]


def test_xlsx_repeats_the_notes_on_each_sheet_of_a_multi_table_section() -> None:
    """Footnotes qualify the section, so every one of its sheets gets them."""
    built = sample_reports.report(
        sections=[
            ReportSection(
                heading="Summary",
                description="d",
                blocks=[sample_reports.table_block(), sample_reports.table_block()],
                footnotes=["Timezone: America/Chicago"],
            )
        ]
    )
    for name in ("Summary 1", "Summary 2"):
        assert _sheet_column(built, name)[-2:] == ["Notes", "Timezone: America/Chicago"]


def test_xlsx_numbers_the_sheets_of_a_multi_table_section() -> None:
    """Two tables in one section cannot share a sheet name."""
    built = sample_reports.report(
        sections=[
            ReportSection(
                heading="Summary",
                description="d",
                blocks=[sample_reports.table_block(), sample_reports.table_block()],
            )
        ]
    )
    workbook = load_workbook(io.BytesIO(render_xlsx_bytes(built)))
    assert workbook.sheetnames == ["Report", "Summary 1", "Summary 2"]


def test_xlsx_disambiguates_a_section_named_like_the_info_sheet() -> None:
    """A section called "Report" must not collide with the info sheet."""
    built = sample_reports.report(
        sections=[
            ReportSection(
                heading="Report", description="d", blocks=[sample_reports.table_block()]
            )
        ]
    )
    workbook = load_workbook(io.BytesIO(render_xlsx_bytes(built)))
    assert workbook.sheetnames == ["Report", "Report (2)"]


@pytest.mark.parametrize(
    "heading",
    ["A/B: [c]*d?", "A very long section heading that Excel will not accept"],
)
def test_xlsx_sanitizes_and_truncates_sheet_names(heading: str) -> None:
    """Excel rejects some characters and any name over 31 characters."""
    built = sample_reports.report(
        sections=[
            ReportSection(
                heading=heading, description="d", blocks=[sample_reports.table_block()]
            )
        ]
    )
    workbook = load_workbook(io.BytesIO(render_xlsx_bytes(built)))
    name = workbook.sheetnames[1]
    assert len(name) <= 31
    assert not set(name) & set("[]:*?/\\")


def test_xlsx_keeps_formula_like_cells_as_text() -> None:
    """openpyxl infers a formula from a leading "=", and cells hold usernames."""
    built = _formula_like_report()
    workbook = load_workbook(io.BytesIO(render_xlsx_bytes(built)))
    for sheet in workbook.worksheets:
        for row in sheet.iter_rows():
            for cell in row:
                assert cell.data_type != "f"
    assert workbook["Summary"].cell(row=2, column=1).value == "=SUM(A1:A2)"
    assert workbook["Summary"].cell(row=2, column=1).data_type == "s"
    assert _info_sheet_column(built)[0] == "=1+1"


@pytest.mark.parametrize(
    ("value", "expected", "number_format"),
    [
        (28, 28, "General"),
        (Duration(3000), timedelta(seconds=3000), "[h]:mm"),
        (Duration(96000), timedelta(seconds=96000), "[h]:mm"),
        (Percent(0.42), 0.42, "0%"),
        (Fixed(1.11, digits=2), 1.11, "0.00"),
        (Fixed(1.3, digits=1), 1.3, "0.0"),
    ],
)
def test_xlsx_writes_a_typed_cell_as_a_number(
    value: Cell, expected: object, number_format: str
) -> None:
    """A workbook exists to be summed and charted, which text cannot be."""
    written, written_format, data_type = _value_cell(
        sample_reports.report(sections=[_section(rows=[["Metric", value]])])
    )
    assert written == expected
    assert written_format == number_format
    assert data_type != "s"


def test_xlsx_keeps_a_text_cell_as_text() -> None:
    """Only a number becomes a number; a label stays the string it is."""
    written, _, data_type = _value_cell(
        sample_reports.report(sections=[_section(rows=[["Metric", "n/a"]])])
    )
    assert written == "n/a"
    assert data_type == "s"


def test_report_attachments_render_flagged_tables_to_csv() -> None:
    """A table asking for an attachment becomes a CSV of its own."""
    attachments = report_attachments(sample_reports.report())
    assert [a.filename for a in attachments] == ["summary.csv"]
    assert attachments[0].mime_hint == "text/csv"
    assert attachments[0].payload.decode("utf-8").startswith("Metric,Value")


def test_report_attachments_skips_unflagged_tables() -> None:
    """Only tables naming a file are attached."""
    built = sample_reports.report(sections=[_section(rows=[["a", "1"]])])
    assert report_attachments(built) == []


def test_report_attachments_collects_prebuilt_section_attachments() -> None:
    """Pre-built section attachments are an extension point with no producer."""
    built = sample_reports.report(
        sections=[
            ReportSection(
                heading="Summary",
                description="d",
                blocks=[sample_reports.table_block()],
                attachments=[ReportAttachment(filename="extra.bin", payload=b"data")],
            )
        ]
    )
    assert [a.filename for a in report_attachments(built)] == ["extra.bin"]


def test_report_attachments_deduplicates_by_filename() -> None:
    """Two tables naming one file yield one attachment, the later winning."""
    built = sample_reports.report(
        sections=[
            ReportSection(
                heading="First",
                description="d",
                blocks=[
                    sample_reports.table_block(
                        rows=[["first", "1"]], attachment_filename="data.csv"
                    )
                ],
            ),
            ReportSection(
                heading="Second",
                description="d",
                blocks=[
                    sample_reports.table_block(
                        rows=[["second", "2"]], attachment_filename="data.csv"
                    )
                ],
            ),
        ]
    )
    attachments = report_attachments(built)
    assert len(attachments) == 1
    assert b"second" in attachments[0].payload


def _section(*, rows: list[list[Cell]]) -> ReportSection:
    """Build a "Summary" section wrapping a single table of *rows*."""
    return ReportSection(
        heading="Summary",
        description="Usage from 2026-07-01 00:00 to 2026-07-08 00:00.",
        blocks=[sample_reports.table_block(rows=rows)],
        footnotes=["Timezone: America/Chicago"],
    )


def _formula_like_report() -> Report:
    """Build a report whose title, cells, and footnote all read as formulas."""
    return sample_reports.report(
        title="=1+1",
        sections=[
            ReportSection(
                heading="Summary",
                description="d",
                blocks=[
                    sample_reports.table_block(rows=[["=SUM(A1:A2)", "=cmd|' /c calc'!A1"]])
                ],
                footnotes=["=SUM(B1:B2)"],
            )
        ],
    )


def _csv_rows(payload: bytes) -> list[list[str]]:
    """Parse CSV bytes back into rows, since an escaped cell may be quoted."""
    return list(csv.reader(io.StringIO(payload.decode("utf-8"), newline="")))


def _value_cell(built: Report) -> tuple[object, str, str]:
    """Return the first ``Value`` cell's value, number format, and type."""
    workbook = load_workbook(io.BytesIO(render_xlsx_bytes(built)))
    cell = workbook["Summary"].cell(row=2, column=2)
    return cell.value, cell.number_format, cell.data_type


def _table_sheet(built: Report, sheet_name: str) -> Worksheet:
    """Return one of the workbook's sheets, for the rows it ends on."""
    return load_workbook(io.BytesIO(render_xlsx_bytes(built)))[sheet_name]


def _sheet_column(built: Report, sheet_name: str) -> list[str | None]:
    """Return a sheet's first-column values, a blank row reading ``None``."""
    workbook = load_workbook(io.BytesIO(render_xlsx_bytes(built)))
    return [
        None if row[0] is None else str(row[0])
        for row in workbook[sheet_name].iter_rows(min_col=1, max_col=1, values_only=True)
    ]


def _info_sheet_column(built: Report) -> list[str]:
    """Return the non-empty first-column values of the workbook's info sheet."""
    return [value for value in _sheet_column(built, "Report") if value is not None]
