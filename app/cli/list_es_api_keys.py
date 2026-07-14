"""Command-line tool for listing Elasticsearch API keys."""

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any

from app.cli.runtime import run_command
from app.cli.utils import configure_es_admin_parser, make_es_client, validate_es_admin_arguments
from app.core.errors import AppError, ExternalServiceError
from app.core.time_utils import get_now_ms


def _ms_to_datetime(ms: int | None) -> str:
    """Convert a millisecond epoch timestamp to a UTC string."""
    if ms is None:
        return "Never"
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _key_status_tags(
    key: dict[str, Any],
    now_ms: int | None = None,
) -> list[str]:
    """Return status tags (e.g., invalidated, expired) for an API key."""
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


def format_output_key(
    keys: list[dict[str, Any]],
    *,
    active_only: bool = True,
) -> str:
    """Format API keys as a simple id/name listing."""
    if not keys:
        if active_only:
            return "(no active API keys)"
        return "(no API keys)"
    now_ms = get_now_ms()
    lines: list[str] = []
    for key in keys:
        line = f"{key.get('id', '')}  {key.get('name', '')}"
        tags = _key_status_tags(key, now_ms)
        if tags:
            line += f"  [{', '.join(tags)}]"
        lines.append(line)
    return "\n".join(lines)


def format_output_json(keys: list[dict[str, Any]]) -> str:
    """Format API keys as a pretty-printed JSON array."""
    return json.dumps(keys, indent=2)


def format_output_full(
    keys: list[dict[str, Any]],
    *,
    active_only: bool = True,
) -> str:
    """Format API keys as a human-readable listing."""
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
    """Build the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="List Elasticsearch API keys owned by the authenticated user",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://elastic.example.com:9200
  %(prog)s --es-endpoint https://elastic.example.com:9200 \\
    --all
  %(prog)s --es-endpoint https://elastic.example.com:9200 \\
    --format json
        """,
    )

    configure_es_admin_parser(parser, include_index=False)

    parser.add_argument(
        "--all",
        dest="all_keys",
        action="store_true",
        help="Include expired and invalidated keys (default: active keys only)",
    )

    parser.add_argument(
        "--format",
        choices=["key", "json", "full"],
        default="key",
        help=(
            "Output format: 'key' (default, id and name "
            "per line), 'json', or 'full' (human-readable)"
        ),
    )

    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation."""
    validate_es_admin_arguments(args, parser)


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        ExternalServiceError: If listing API keys fails.
    """
    try:
        with make_es_client(args) as client:
            keys = client.list_api_keys(active_only=not args.all_keys)
    except AppError:
        raise
    except Exception as e:
        raise ExternalServiceError(f"Listing API keys failed: {e}") from e

    active_only = not args.all_keys
    if args.format == "key":
        output = format_output_key(keys, active_only=active_only)
    elif args.format == "json":
        output = format_output_json(keys)
    else:  # full
        output = format_output_full(keys, active_only=active_only)

    print(output)
    return 0


def main() -> int:
    """Main entry point for the list-es-api-keys script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
