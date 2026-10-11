"""Tests for :mod:`app.reports.anonymize`."""

from app.reports.anonymize import LABEL_WIDTH, NATO_ALPHABET, build_pseudonyms


def test_pseudonyms_are_fixed_width_nato_labels() -> None:
    """Every label is exactly :data:`LABEL_WIDTH` alphabet words."""
    labels = build_pseudonyms([("x.edu", "a"), ("x.edu", "b"), ("orcid.org", "1")])
    assert len(labels) == 3
    for label in labels.values():
        words = label.split(" ")
        assert len(words) == LABEL_WIDTH
        assert all(word in NATO_ALPHABET for word in words)


def test_pseudonyms_are_stable_across_calls() -> None:
    """The same identifier gets the same label whatever else is present."""
    alone = build_pseudonyms([("x.edu", "a")])
    crowded = build_pseudonyms([("x.edu", f"user{i}") for i in range(50)] + [("x.edu", "a")])
    assert alone[("x.edu", "a")] == crowded[("x.edu", "a")]


def test_pseudonyms_do_not_depend_on_input_order() -> None:
    """Labels are derived per identifier, not from position."""
    pairs = [("x.edu", "a"), ("x.edu", "b"), ("y.edu", "c")]
    assert build_pseudonyms(pairs) == build_pseudonyms(list(reversed(pairs)))


def test_the_same_uid_at_two_institutions_maps_independently() -> None:
    """The domain is part of the hashed identity."""
    labels = build_pseudonyms([("x.edu", "shared"), ("y.edu", "shared")])
    assert labels[("x.edu", "shared")] != labels[("y.edu", "shared")]


def test_a_pseudonym_does_not_change_between_runs() -> None:
    """Reports are compared over time, so a user's label is fixed.

    Pinning one label catches a change to the hash, the alphabet, or the width,
    and a switch to :func:`hash`, which varies per process.
    """
    assert build_pseudonyms([("x.edu", "a")]) == {("x.edu", "a"): "Lima Yankee Golf"}
