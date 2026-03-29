"""Render report-domain models into plain text and HTML."""

import csv
import io
import re

from jinja2 import BaseLoader, Environment
from openpyxl import Workbook
from tabulate import tabulate

from app.reports.model import Report, ReportAttachment, TableBlock, TextBlock

_ENV = Environment(loader=BaseLoader(), autoescape=True, trim_blocks=True, lstrip_blocks=True)

_HTML_TEMPLATE = _ENV.from_string("""
<style>
  @media (prefers-color-scheme: dark) {
    .th { background: #1d4060 !important; color: #d0e8f8 !important; border-color: #3a607a !important; }
    .td { background: #18232d !important; color: #d8e8f0 !important; border-color: #3a607a !important; }
    .td-alt { background: #1e2f3d !important; color: #d8e8f0 !important; border-color: #3a607a !important; }
  }
</style>
<h2>{{ report.title }}</h2>
{% for section in report.sections %}
  <h3>{{ section.heading }}</h3>
  <p>{{ section.description }}</p>
  {% for block in section.blocks %}
    {% if block.kind == "table" %}
      <table style="border-collapse:collapse">
        <thead>
          <tr>
          {% for header in block.headers %}
            <th class="th" style="{{ header.style }}">{{ header.text }}</th>
          {% endfor %}
          </tr>
        </thead>
        <tbody>
        {% for row in block.rows %}
          <tr>
          {% for cell in row %}
            <td class="{{ cell.cls }}" style="{{ cell.style }}">{{ cell.text }}</td>
          {% endfor %}
          </tr>
        {% endfor %}
        </tbody>
      </table>
    {% else %}
      <p>{{ block.text }}</p>
    {% endif %}
  {% endfor %}
  {% if section.footnotes %}
    <ul>
    {% for footnote in section.footnotes %}
      <li><em>{{ footnote }}</em></li>
    {% endfor %}
    </ul>
  {% endif %}
{% endfor %}
{% if report.footnotes %}
  <h4>Notes</h4>
  <ul>
  {% for footnote in report.footnotes %}
    <li><em>{{ footnote }}</em></li>
  {% endfor %}
  </ul>
{% endif %}
""")


def _render_text_table(block: TableBlock) -> list[str]:
    colalign = tuple(
        "right" if idx in block.right_align_columns else "left"
        for idx, _ in enumerate(block.headers)
    )
    table = tabulate(
        block.rows,
        headers=block.headers,
        tablefmt="simple",
        colalign=colalign,
        disable_numparse=True,
    )
    return table.splitlines()


def render_text(report: Report) -> str:
    """Render a report as plain text with section headings and tables."""
    lines = [report.title, ""]
    for section in report.sections:
        lines.append(section.heading)
        lines.append("=" * len(section.heading))
        lines.append(section.description)
        lines.append("")
        for block in section.blocks:
            if isinstance(block, TextBlock):
                lines.append(block.text)
                lines.append("")
            else:
                lines.extend(_render_text_table(block))
                lines.append("")
        if section.footnotes:
            lines.append("Notes:")
            lines.extend(f"- {note}" for note in section.footnotes)
            lines.append("")
    if report.footnotes:
        lines.append("Global notes:")
        lines.extend(f"- {note}" for note in report.footnotes)
    return "\n".join(lines).rstrip()


def _header_style(right_align: bool) -> str:
    align = _align_value(right_align)
    return (
        f"text-align:{align}; border:1px solid #9ab3c8; padding:2px 8px; "
        "background:#a6c9e8; color:#000000"
    )


def _align_value(right_align: bool) -> str:
    return "right" if right_align else "left"


def _cell_style(row_idx: int, right_align: bool) -> tuple[str, str]:
    cls = "td-alt" if row_idx % 2 else "td"
    bg = "#e6eff4" if row_idx % 2 else "#ffffff"
    align = f"text-align:{_align_value(right_align)}; "
    style = (
        f"{align}border:1px solid #9ab3c8; padding:2px 8px; " f"background:{bg}; color:#000000"
    )
    return cls, style


def _shape_table_block(block: TableBlock) -> dict[str, object]:
    header_cells = [
        {
            "text": header,
            "style": _header_style(idx in block.right_align_columns),
        }
        for idx, header in enumerate(block.headers)
    ]
    row_cells: list[list[dict[str, str]]] = []
    for row_idx, row in enumerate(block.rows):
        row_data: list[dict[str, str]] = []
        for col_idx, value in enumerate(row):
            cls, style = _cell_style(row_idx, col_idx in block.right_align_columns)
            row_data.append({"text": value, "cls": cls, "style": style})
        row_cells.append(row_data)
    return {"kind": "table", "headers": header_cells, "rows": row_cells}


def render_html(report: Report) -> str:
    """Render a report as HTML using the shared Jinja2 template."""
    shaped_sections: list[dict[str, object]] = []
    for section in report.sections:
        blocks: list[dict[str, object]] = []
        for block in section.blocks:
            if isinstance(block, TextBlock):
                blocks.append({"kind": "text", "text": block.text})
                continue

            blocks.append(_shape_table_block(block))

        shaped_sections.append(
            {
                "heading": section.heading,
                "description": section.description,
                "blocks": blocks,
                "footnotes": list(section.footnotes),
            }
        )

    return _HTML_TEMPLATE.render(
        report={
            "title": report.title,
            "sections": shaped_sections,
            "footnotes": list(report.footnotes),
        }
    )


def report_attachments(report: Report) -> list[ReportAttachment]:
    """Collect and de-duplicate generated report attachments."""
    attachments: list[ReportAttachment] = []
    for section in report.sections:
        attachments.extend(section.attachments)
        for block in section.blocks:
            if isinstance(block, TableBlock) and block.attachment_filename:
                attachments.append(
                    ReportAttachment(
                        filename=block.attachment_filename,
                        payload=_table_to_csv_bytes(block),
                        mime_hint="text/csv",
                    )
                )

    deduped: dict[tuple[str, bytes], ReportAttachment] = {}
    for attachment in attachments:
        deduped[(attachment.filename, attachment.payload)] = attachment
    return list(deduped.values())


def _table_to_csv_bytes(block: TableBlock) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(block.headers)
    for row in block.rows:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def render_csv(report: Report) -> str:
    """Render a report to a single human-readable CSV document."""
    buf = io.StringIO()
    writer = csv.writer(buf)

    writer.writerow([report.title])
    writer.writerow([])

    for section in report.sections:
        writer.writerow([section.heading])
        writer.writerow([section.description])
        writer.writerow([])

        for block in section.blocks:
            if isinstance(block, TextBlock):
                writer.writerow([block.text])
                writer.writerow([])
                continue

            writer.writerow(block.headers)
            writer.writerows(block.rows)
            writer.writerow([])

        if section.footnotes:
            writer.writerow(["Notes"])
            for note in section.footnotes:
                writer.writerow([note])
            writer.writerow([])

    if report.footnotes:
        writer.writerow(["Global notes"])
        for note in report.footnotes:
            writer.writerow([note])

    return buf.getvalue()


def render_csv_bytes(report: Report) -> bytes:
    """Render a report to UTF-8 CSV bytes."""
    return render_csv(report).encode("utf-8")


def render_xlsx_bytes(report: Report) -> bytes:
    """Render report tables to an XLSX workbook (one sheet per table block)."""
    workbook = Workbook()
    initial_sheet = workbook.active
    if initial_sheet is None:
        raise ValueError("Workbook did not create an initial worksheet")
    table_index = 0
    used_sheet_names: set[str] = set()

    for section in report.sections:
        section_table_index = 0
        for block in section.blocks:
            if isinstance(block, TextBlock):
                continue

            section_table_index += 1
            table_index += 1
            title = f"{section.heading} {section_table_index}"
            sheet_name = _unique_sheet_name(title, used_sheet_names)
            worksheet = workbook.create_sheet(title=sheet_name)
            worksheet.append(block.headers)
            for row in block.rows:
                worksheet.append(row)

    if table_index == 0:
        initial_sheet.title = "Report"
        initial_sheet.append([report.title])
        initial_sheet.append(["No table data available."])
    else:
        workbook.remove(initial_sheet)

    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()


def _sanitize_sheet_name(name: str) -> str:
    cleaned = re.sub(r"[\[\]\:\*\?\/\\\\]", "_", name).strip().strip("'")
    return cleaned or "Sheet"


def _unique_sheet_name(base: str, used_names: set[str]) -> str:
    base_clean = _sanitize_sheet_name(base)
    candidate = base_clean[:31]

    if candidate not in used_names:
        used_names.add(candidate)
        return candidate

    suffix = 2
    while True:
        suffix_text = f" ({suffix})"
        trimmed = base_clean[: 31 - len(suffix_text)]
        candidate = f"{trimmed}{suffix_text}"
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
        suffix += 1
