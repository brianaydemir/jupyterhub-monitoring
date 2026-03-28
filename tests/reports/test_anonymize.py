"""Tests for :mod:`app.reports.anonymize`."""

import subprocess
import sys

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


def test_pseudonyms_survive_a_fresh_interpreter() -> None:
    """Labels are independent of ``PYTHONHASHSEED``.

    Cross-run stability is the whole point of the pseudonyms, and Python
    randomizes :func:`hash` per process, so this has to be checked in a
    separate interpreter rather than in-process.
    """
    script = (
        "from app.reports.anonymize import build_pseudonyms;"
        " print(build_pseudonyms([('x.edu', 'a')])[('x.edu', 'a')])"
    )
    runs = {
        subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            check=True,
            env={"PYTHONHASHSEED": seed, "PATH": "/usr/bin:/bin"},
            text=True,
        ).stdout.strip()
        for seed in ("0", "1", "12345")
    }
    assert runs == {build_pseudonyms([("x.edu", "a")])[("x.edu", "a")]}


def test_the_label_space_is_the_documented_size() -> None:
    """The collision figures in the docstring assume 26 ** LABEL_WIDTH."""
    assert len(NATO_ALPHABET) == 26
    assert len(NATO_ALPHABET) ** LABEL_WIDTH == 17576
