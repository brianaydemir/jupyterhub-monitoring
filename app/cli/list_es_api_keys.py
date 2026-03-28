"""Command-line tool for listing Elasticsearch API keys."""

import argparse
import datetime
import json
import sys
from typing import Any

from app.cli.runtime import run_command
from app.cli.utils import (
    configure_es_admin_parser,
    make_es_client,
    validate_es_admin_arguments,
)
from app.core.errors import ExternalServiceError
from app.core.time_utils import get_now_ms


def _ms_to_datetime(ms: int | None) -> str:
    """Convert a millisecond epoch timestamp to a human-readable UTC string.

    Args:
        ms: Millisecond epoch timestamp, or None for keys that have never expired

    Returns:
        Formatted UTC datetime string, or ``"Never"`` when *ms* is None
    """
    if ms is None:
        return "Never"
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def _key_status_tags(key: dict[str, Any], now_ms: int | None = None) -> list[str]:
    """Return status tags for a key that is not fully active.

    Args:
        key: API key dict as returned by the Elasticsearch list-API-keys endpoint
        now_ms: Current time as a millisecond epoch timestamp, or None to call
            :func:`~app.time_utils.get_now_ms` automatically.

    Returns:
        List of status tag strings (e.g. ``["invalidated"]``, ``["expired"]``)
    """
    tags: list[str] = []
    if key.get("invalidated"):
        tags.append("invalidated")
    expiration = key.get("expiration")
    if expiration is not None:
        if now_ms is None:
            now_ms = get_now_ms()
        if expiration <= now_ms:
            tags.append("expired")
    return tags


def format_output_key(keys: list[dict[str, Any]], *, active_only: bool = True) -> str:
    """Format API keys as a simple id/name listing.

    Args:
        keys: List of API key dictionaries from Elasticsearch
        active_only: Whether the list was filtered to active keys only

    Returns:
        One line per key in the form "id  name" with optional status tags
    """
    if not keys:
        return "(no active API keys)" if active_only else "(no API keys)"
    lines: list[str] = []
    for key in keys:
        line = f"{key.get('id', '')}  {key.get('name', '')}"
        tags = _key_status_tags(key)
        if tags:
            line += f"  [{', '.join(tags)}]"
        lines.append(line)
    return "\n".join(lines)


def format_output_json(keys: list[dict[str, Any]]) -> str:
    """Format API keys as a pretty-printed JSON array.

    Args:
        keys: List of API key dictionaries from Elasticsearch

    Returns:
        Pretty-printed JSON string
    """
    return json.dumps(keys, indent=2)


def format_output_full(keys: list[dict[str, Any]], *, active_only: bool = True) -> str:
    """Format API keys as a human-readable table.

    Args:
        keys: List of API key dictionaries from Elasticsearch
        active_only: Whether the list was filtered to active keys only

    Returns:
        Human-readable formatted string with id, name, creation, and expiration
    """
    label = "Active API Keys" if active_only else "API Keys"
    if not keys:
        return f"No {label.lower()} found."

    lines = [f"{label} ({len(keys)}):", ""]
    now_ms = get_now_ms()
    for key in keys:
        lines.append(f"  ID:          {key.get('id', 'N/A')}")
        lines.append(f"  Name:        {key.get('name', 'N/A')}")
        lines.append(f"  Created:     {_ms_to_datetime(key.get('creation'))}")
        expiration_str = _ms_to_datetime(key.get("expiration"))
        if "expired" in _key_status_tags(key, now_ms):
            expiration_str += "  (expired)"
        lines.append(f"  Expires:     {expiration_str}")
        if not active_only:
            invalidated = key.get("invalidated", False)
            lines.append(f"  Invalidated: {invalidated}")
        lines.append("")
    return "\n".join(lines).rstrip()


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description=("List Elasticsearch API keys owned by the authenticated user"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://elastic.example.com:9200
  %(prog)s --es-endpoint https://elastic.example.com:9200 --all
  %(prog)s --es-endpoint https://elastic.example.com:9200 --format json
  %(prog)s --es-endpoint https://elastic.example.com:9200 --format full
  %(prog)s --es-endpoint https://elastic.example.com:9200 --es-ca-cert /path/to/ca.crt
        """,
    )

    configure_es_admin_parser(parser, include_index=False)

    # Filter options
    parser.add_argument(
        "--all",
        dest="all_keys",
        action="store_true",
        help="Include expired and invalidated keys (default: active keys only)",
    )

    # Output format
    parser.add_argument(
        "--format",
        choices=["key", "json", "full"],
        default="key",
        help=(
            "Output format: 'key' (default, prints id and name per line), "
            "'json' (JSON array), 'full' (detailed human-readable output)"
        ),
    )

    return parser


def _validate_arguments(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Run all post-parse argument validation for this command."""
    validate_es_admin_arguments(args, parser)


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        app.errors.ExternalServiceError: If listing API keys fails.
    """
    try:
        with make_es_client(args) as client:
            keys = client.list_api_keys(active_only=not args.all_keys)

        active_only = not args.all_keys
        if args.format == "key":
            output = format_output_key(keys, active_only=active_only)
        elif args.format == "json":
            output = format_output_json(keys)
        else:  # full
            output = format_output_full(keys, active_only=active_only)

        print(output)
        return 0
    except Exception as e:
        raise ExternalServiceError(f"Listing API keys failed: {e}") from e


def main() -> int:
    """Main entry point for the list-es-api-keys script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
