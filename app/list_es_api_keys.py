"""Command-line tool for listing Elasticsearch API keys."""

import argparse
import datetime
import getpass
import json
import sys
from pathlib import Path
from typing import Any

from app.elasticsearch_client import ElasticsearchClient


def _ms_to_datetime(ms: int | None) -> str:
    """Convert a millisecond epoch timestamp to a human-readable UTC string."""
    if ms is None:
        return "Never"
    return datetime.datetime.fromtimestamp(
        ms / 1000, tz=datetime.timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")


def _key_status_tags(key: dict[str, Any]) -> list[str]:
    """Return status tags for a key that is not fully active."""
    tags = []
    if key.get("invalidated"):
        tags.append("invalidated")
    expiration = key.get("expiration")
    if expiration is not None:
        now_ms = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
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
    lines = []
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
    for key in keys:
        lines.append(f"  ID:          {key.get('id', 'N/A')}")
        lines.append(f"  Name:        {key.get('name', 'N/A')}")
        lines.append(f"  Created:     {_ms_to_datetime(key.get('creation'))}")
        expiration = key.get("expiration")
        expiration_str = _ms_to_datetime(expiration)
        if expiration is not None:
            now_ms = int(
                datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000
            )
            if expiration <= now_ms:
                expiration_str += "  (expired)"
        lines.append(f"  Expires:     {expiration_str}")
        if not active_only:
            invalidated = key.get("invalidated", False)
            lines.append(f"  Invalidated: {invalidated}")
        lines.append("")
    return "\n".join(lines).rstrip()


def _get_credentials(username_arg: str | None) -> tuple[str, str] | None:
    """Prompt for username and password interactively.

    Args:
        username_arg: Pre-supplied username, or None to prompt.

    Returns:
        A (username, password) tuple, or None if the user cancelled.
    """
    if username_arg:
        username = username_arg
    else:
        try:
            username = input("Username: ")
        except EOFError, KeyboardInterrupt:
            print("\nOperation cancelled.", file=sys.stderr)
            return None

    try:
        password = getpass.getpass("Password: ")
    except EOFError, KeyboardInterrupt:
        print("\nOperation cancelled.", file=sys.stderr)
        return None

    return username, password


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=("List Elasticsearch API keys owned by the authenticated user"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --endpoint https://elastic.example.com:9200
  %(prog)s --endpoint https://elastic.example.com:9200 --all
  %(prog)s --endpoint https://elastic.example.com:9200 --format json
  %(prog)s --endpoint https://elastic.example.com:9200 --format full
  %(prog)s --endpoint https://elastic.example.com:9200 --ca-cert /path/to/ca.crt
        """,
    )

    # Elasticsearch connection parameters
    parser.add_argument(
        "--endpoint",
        required=True,
        help="Elasticsearch API endpoint URL (e.g., https://elastic.example.com:9200)",
    )
    parser.add_argument(
        "--ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )

    # Authentication parameters
    parser.add_argument(
        "--username",
        help="Username for authentication (will prompt if not provided)",
    )

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

    args = parser.parse_args()

    # Validate CA certificate exists if provided
    if args.ca_cert and not args.ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.ca_cert}")

    return args


def main() -> int:
    """Main entry point for the list-es-api-keys script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        credentials = _get_credentials(args.username)
        if credentials is None:
            return 1
        username, password = credentials

        if not username or not password:
            print("Error: Username and password are required.", file=sys.stderr)
            return 1

        try:
            keys = ElasticsearchClient.list_api_keys_with_basic_auth(
                endpoint=args.endpoint,
                username=username,
                password=password,
                ca_cert=str(args.ca_cert) if args.ca_cert else None,
                active_only=not args.all_keys,
            )
        except ValueError as e:
            print(f"Error listing API keys: {e}", file=sys.stderr)
            return 1

        active_only = not args.all_keys
        if args.format == "key":
            output = format_output_key(keys, active_only=active_only)
        elif args.format == "json":
            output = format_output_json(keys)
        else:  # full
            output = format_output_full(keys, active_only=active_only)

        print(output)
        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
