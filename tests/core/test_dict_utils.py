"""Tests for :mod:`app.core.dict_utils`."""

from app.core.dict_utils import flatten_dict


def test_flatten_dict_joins_nested_keys_with_dots() -> None:
    """Nesting becomes a dotted path, at any depth."""
    assert flatten_dict({"a": {"b": {"c": 1}}, "d": 2}) == {"a.b.c": 1, "d": 2}


def test_flatten_dict_keeps_lists_intact() -> None:
    """A list is a value, not a level of nesting."""
    assert flatten_dict({"a": {"b": [1, 2]}}) == {"a.b": [1, 2]}


def test_flatten_dict_drops_empty_nested_dicts() -> None:
    """An empty mapping contributes no key, so it looks like an absent one."""
    assert flatten_dict({"a": {}, "b": 1}) == {"b": 1}
