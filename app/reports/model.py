"""Core report-domain models used by renderers and delivery helpers."""

from dataclasses import dataclass, field


def _int_set() -> set[int]:
    """Return an empty typed integer set for dataclass defaults."""
    return set()


def _string_list() -> list[str]:
    """Return an empty typed string list for dataclass defaults."""
    return []


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
    """A free-form text block inside a report section."""

    text: str


@dataclass(frozen=True)
class TableBlock:
    """A tabular block inside a report section."""

    headers: list[str]
    rows: list[list[str]]
    right_align_columns: set[int] = field(default_factory=_int_set)
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
    footnotes: list[str] = field(default_factory=_string_list)
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
    footnotes: list[str] = field(default_factory=_string_list)

    def __post_init__(self) -> None:
        """Validate report invariants."""
        if not self.title:
            raise ValueError("report title must not be empty")
        if not self.sections:
            raise ValueError("report must include at least one section")
