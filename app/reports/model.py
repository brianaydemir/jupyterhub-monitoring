"""Rendering models for reports, used by renderers and delivery helpers.

A table cell carries the value it means rather than the text of it,
so that the workbook can hold a number Excel sorts, sums, and charts.
:func:`cell_text` is the one place a cell becomes the string
the text, CSV, and HTML renderers show, so those three cannot drift apart.
"""

from dataclasses import dataclass, field

from app.core.dataclass_defaults import empty_int_set, empty_string_list


@dataclass(frozen=True)
class Duration:
    """A span of time, held as seconds and shown as hours and minutes."""

    seconds: float

    def display(self) -> str:
        """Return the duration as hours and minutes.

        Hours are neither zero-padded nor capped at 24
        (``"5:00"``, ``"123:45"``), and minutes are truncated
        rather than rounded.
        Because each value is truncated independently,
        the displayed GPU and CPU times need not add up to the displayed total.

        A negative span reads by sign and magnitude
        (``"-0:30"``, not ``"-1:30"``),
        and keeps its sign even below a minute (``"-0:00"``).
        """
        total_minutes = int(abs(self.seconds)) // 60
        hours, minutes = divmod(total_minutes, 60)
        sign = "-" if self.seconds < 0 else ""
        return f"{sign}{hours}:{minutes:02d}"


@dataclass(frozen=True)
class Percent:
    """A proportion, held as a 0..1 fraction and shown as a percentage.

    The fraction rather than the whole number is what the workbook wants:
    Excel's percent formats scale a 0..1 value themselves.
    """

    fraction: float

    def display(self) -> str:
        """Return the fraction as a whole percentage."""
        return f"{round(100 * self.fraction)}%"


@dataclass(frozen=True)
class Fixed:
    """A number shown to a fixed number of decimal places.

    The places travel with the value because they vary by row:
    ``Mean sessions per user`` reads to one place
    and the two concurrency means to two, which a bare ``float`` could not say.
    """

    value: float
    digits: int

    def __post_init__(self) -> None:
        """Validate that the value has decimal places to be shown to.

        Raises:
            ValueError: If *digits* is not positive.
        """
        if self.digits < 1:
            raise ValueError("fixed-point digits must be positive")

    def display(self) -> str:
        """Return the number to its own number of decimal places."""
        return f"{self.value:.{self.digits}f}"


#: A value a table cell holds.
#: Text stays text, and a quantity keeps the type that says what it is,
#: so that the XLSX renderer can write a real number
#: where the other formats write :func:`cell_text`'s string.
Cell = str | int | Duration | Percent | Fixed


def cell_text(value: Cell) -> str:
    """Return the string the text, CSV, and HTML renderers show for a cell."""
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    return value.display()


# A local counterpart to the factories in :mod:`app.core.dataclass_defaults`,
# which cannot hold this one without importing this module;
# see it for why these wrappers exist.
def _attachment_list() -> list["ReportAttachment"]:
    """Return an empty typed attachment list for dataclass defaults."""
    return []


@dataclass(frozen=True)
class ReportAttachment:
    """A binary attachment generated as part of a report."""

    filename: str
    payload: bytes
    mime_hint: str | None = None


@dataclass(frozen=True)
class TextBlock:
    """A free-form text block inside a report section.

    An extension point: no builder emits one yet,
    so every renderer's text-block branch is currently unexercised.
    Renderers must keep handling it for :data:`SectionBlock` to stay honest.
    """

    text: str


@dataclass(frozen=True)
class TableBlock:
    """A tabular block inside a report section.

    *right_align_columns* is stated rather than inferred from the cells' types:
    a ``Metric``/``Value`` table right-aligns one column
    whose cells are numbers and text by turns,
    so the alignment is the table's choice
    and not a property of any one value in it.
    """

    headers: list[str]
    rows: list[list[Cell]]
    right_align_columns: set[int] = field(default_factory=empty_int_set)
    attachment_filename: str | None = None

    def __post_init__(self) -> None:
        """Validate table invariants used by renderers and attachments."""
        if not self.headers:
            raise ValueError("table headers must not be empty")

        width = len(self.headers)
        invalid_rows = [idx for idx, row in enumerate(self.rows) if len(row) != width]
        if invalid_rows:
            joined = ", ".join(str(idx) for idx in invalid_rows)
            raise ValueError(f"table rows with invalid column count at indices: {joined}")

        bad_cols = sorted(idx for idx in self.right_align_columns if idx < 0 or idx >= width)
        if bad_cols:
            joined = ", ".join(str(idx) for idx in bad_cols)
            raise ValueError(f"right-aligned columns out of range: {joined}")

        if self.attachment_filename is not None and not self.attachment_filename:
            raise ValueError("attachment filename must not be empty")


SectionBlock = TextBlock | TableBlock


@dataclass(frozen=True)
class ReportSection:
    """A report section with title, narrative, blocks, and footnotes."""

    heading: str
    description: str
    blocks: list[SectionBlock]
    footnotes: list[str] = field(default_factory=empty_string_list)
    #: Pre-built attachments to send alongside the section.
    #: An extension point for payloads a renderer cannot derive
    #: from a table block; no builder populates it yet.
    attachments: list[ReportAttachment] = field(default_factory=_attachment_list)

    def __post_init__(self) -> None:
        """Validate section invariants."""
        if not self.heading:
            raise ValueError("section heading must not be empty")
        if not self.description:
            raise ValueError("section description must not be empty")
        if not self.blocks:
            raise ValueError("section must include at least one block")


@dataclass(frozen=True)
class Report:
    """A report composed of one or more sections."""

    title: str
    sections: list[ReportSection]
    #: Report-wide footnotes, rendered after the last section.
    #: An extension point; builders currently attach footnotes per section.
    footnotes: list[str] = field(default_factory=empty_string_list)

    def __post_init__(self) -> None:
        """Validate report invariants."""
        if not self.title:
            raise ValueError("report title must not be empty")
        if not self.sections:
            raise ValueError("report must include at least one section")
