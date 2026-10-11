"""Utilities for reshaping nested dictionaries."""

from typing import Any, cast


def flatten_dict(d: dict[str, Any], parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    """Flatten a nested dictionary using dotted-key paths.

    An empty nested dictionary contributes no key,
    so it is indistinguishable from an absent one.
    Non-dict values, lists included, are carried through as-is.
    """
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(cast(dict[str, Any], v), new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
