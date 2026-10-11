"""Typed factories for the empty collections used as dataclass defaults.

These wrappers exist because pyright's strict mode reports
a bare ``default_factory=list`` (or ``set`` / ``dict``)
as a partially unknown type; do not inline them.
Each call returns a fresh object,
so every instance still gets a default of its own.

A field whose element type lives outside this package
keeps a one-line factory beside that type,
since a factory here would have to import it.
"""


def empty_int_set() -> set[int]:
    """Return an empty typed integer set."""
    return set()


def empty_string_list() -> list[str]:
    """Return an empty typed string list."""
    return []


def empty_string_set() -> set[str]:
    """Return an empty typed string set."""
    return set()
