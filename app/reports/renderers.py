"""Render report-domain models into plain text and HTML."""

import csv
import io

from jinja2 import BaseLoader, Environment
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
