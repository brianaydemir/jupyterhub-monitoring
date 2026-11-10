"""Tests for :mod:`jupyterhub_monitoring.reports.model`."""

import pytest

from jupyterhub_monitoring.reports.model import (
    Cell,
    Duration,
    Fixed,
    Percent,
    TableBlock,
    cell_text,
)


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0.9, "0:00"),
        (59, "0:00"),
        (60, "0:01"),
        (3599, "0:59"),
        (3600, "1:00"),
        (172805, "48:00"),
    ],
)
def test_a_duration_reads_as_unpadded_hours_and_minutes(seconds: float, expected: str) -> None:
    """Hours are neither zero-padded nor capped at a day, and a partial minute truncates."""
    assert Duration(seconds).display() == expected


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(-30, "-0:00"), (-1800, "-0:30"), (-3600, "-1:00"), (-3661, "-1:01"), (-0.0, "0:00")],
)
def test_a_negative_duration_reads_by_sign_and_magnitude(
    seconds: float,
    expected: str,
) -> None:
    """Flooring the signed total would read half an hour as ``"-1:30"``."""
    assert Duration(seconds).display() == expected


@pytest.mark.parametrize(
    ("fraction", "expected"), [(0.0, "0%"), (0.25, "25%"), (0.424, "42%"), (1.0, "100%")]
)
def test_a_percent_reads_as_a_whole_percentage(fraction: float, expected: str) -> None:
    """The share a report states is rounded to the percentage point."""
    assert Percent(fraction).display() == expected


@pytest.mark.parametrize(
    ("value", "digits", "expected"),
    [(1.3333333, 1, "1.3"), (5.5, 2, "5.50"), (1 / 24, 2, "0.04"), (1.11, 2, "1.11")],
)
def test_a_fixed_number_reads_to_its_own_decimal_places(
    value: float, digits: int, expected: str
) -> None:
    """One row reads to one place and another to two, so the value says which."""
    assert Fixed(value, digits=digits).display() == expected


@pytest.mark.parametrize("digits", [0, -1])
def test_fixed_rejects_having_no_decimal_places(digits: int) -> None:
    """A number shown to no decimal places is an int, so it is rejected."""
    with pytest.raises(ValueError, match="digits must be positive"):
        Fixed(1.0, digits=digits)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("n/a", "n/a"),
        ("", ""),
        (28, "28"),
        (Duration(3000), "0:50"),
        (Percent(0.42), "42%"),
        (Fixed(1.1, digits=1), "1.1"),
    ],
)
def test_every_cell_type_has_a_display_string(value: Cell, expected: str) -> None:
    """Text, CSV, and HTML all read this one function, so it covers the union."""
    assert cell_text(value) == expected


def test_table_rejects_rows_of_the_wrong_width() -> None:
    """A short or long row would silently misalign every output format."""
    with pytest.raises(ValueError, match="invalid column count at indices: 1, 2"):
        TableBlock(headers=["a", "b"], rows=[["1", "2"], ["1"], ["1", "2", "3"]])


@pytest.mark.parametrize("column", [-1, 2, 99])
def test_table_rejects_out_of_range_alignment(column: int) -> None:
    """A right-aligned column must exist."""
    with pytest.raises(ValueError, match="right-aligned columns out of range"):
        TableBlock(headers=["a", "b"], rows=[], right_align_columns={column})
