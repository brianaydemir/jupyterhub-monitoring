"""Command-line tool for obtaining Elasticsearch API keys."""

import argparse
import json
import re
import sys
from typing import Any

from app.cli.arguments import add_format_argument, configure_es_admin_parser
from app.cli.factories import make_es_client
from app.cli.runtime import run_command
from app.cli.validation import validate_es_admin_arguments
from app.core.errors import DataShapeError


def _format_encoded_key(result: dict[str, Any]) -> str:
    """Format a created key as the encoded secret alone.

    Raises:
        DataShapeError: If the response carries no encoded key.
            Printing an empty string and exiting 0
            would hand a shell pipeline an empty secret
            while reporting success.
    """
    encoded = result.get("encoded")
    if not isinstance(encoded, str) or not encoded:
        raise DataShapeError("Elasticsearch returned no encoded API key")
    return encoded


def _format_created_key_json(result: dict[str, Any]) -> str:
    """Format a created key as JSON."""
    output_data = {
        "id": result.get("id"),
        "name": result.get("name"),
        "api_key": result.get("api_key"),
        "encoded": result.get("encoded"),
    }
    if "expiration" in result:
        output_data["expiration"] = result["expiration"]
    return json.dumps(output_data, indent=2)


def _format_created_key_details(result: dict[str, Any]) -> str:
    """Format a created key as human-readable detail."""
    lines = ["Elasticsearch API Key Created Successfully:", ""]
    lines.append(f"ID:       {result.get('id', 'N/A')}")
    lines.append(f"Name:     {result.get('name', 'N/A')}")
    lines.append(f"API Key:  {result.get('api_key', 'N/A')}")
    lines.append(f"Encoded:  {result.get('encoded', 'N/A')}")

    if "expiration" in result:
        lines.append(f"Expires:  {result['expiration']}")
    else:
        lines.append("Expires:  Never")

    lines.append("")
    lines.append("Use the 'Encoded' value for API authentication.")

    return "\n".join(lines)


_EXPIRATION_NEVER = "never"

# A whole number followed by one of Elasticsearch's time units.
_ES_TIME_VALUE = re.compile(r"(\d+)(nanos|micros|ms|d|h|m|s)", re.IGNORECASE)


def _expiration(value: str) -> str | None:
    """Parse ``--expiration`` into what the client sends.

    ``"never"`` becomes ``None``, which omits the field
    and leaves the key with no expiry.
    Anything else must be a positive Elasticsearch time value,
    checked here so that a typo fails before a key is minted
    rather than after its secret has been printed.

    Raises:
        argparse.ArgumentTypeError: If *value* is neither.
    """
    text = value.strip()
    if text.lower() == _EXPIRATION_NEVER:
        return None
    match = _ES_TIME_VALUE.fullmatch(text)
    if match is None or int(match.group(1)) == 0:
        raise argparse.ArgumentTypeError(
            f'expected "{_EXPIRATION_NEVER}" or a positive Elasticsearch time value '
            + f"(e.g., 7d, 12h), not {value!r}"
        )
    return text


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Create an Elasticsearch API key owned by the authenticated user",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://elastic.example.com:9200 \\
    --es-username elastic --expiration 30d
  %(prog)s --es-endpoint https://elastic.example.com:9200 \\
    --es-username elastic --name "my-api-key" --expiration "7d"
  %(prog)s --es-endpoint https://elastic.example.com:9200 \\
    --es-username elastic --expiration never --format json

Expiration format:
  Elasticsearch time units: "1d", "7d", "30d", "12h".
  Pass "never" for a key that never expires.

Authentication:
  Use --es-username. The new key carries the privileges of
  the user who creates it, and Elasticsearch refuses to
  create such a key for a request authenticated by another
  API key.
        """,
    )

    configure_es_admin_parser(parser, include_index=False)

    parser.add_argument(
        "--name",
        help=(
            "Name for the API key (defaults to 'api-key-{username}' with "
            "--es-username, otherwise 'api-key')"
        ),
    )
    parser.add_argument(
        "--expiration",
        required=True,
        type=_expiration,
        metavar="WHEN",
        help=(
            "When the API key expires, as an Elasticsearch time value "
            "(e.g., '1d', '7d', '30d'), or 'never' for a key that never "
            "expires; required, so that a permanent key is always asked for"
        ),
    )

    add_format_argument(parser, key_format_summary="encoded key only")

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
        ExternalServiceError: If API-key creation fails.
    """
    with make_es_client(args) as client:
        result = client.create_api_key(
            key_name=args.name,
            expiration=args.expiration,
            username=args.es_username,
        )

    if args.format == "json":
        print(_format_created_key_json(result))
    elif args.format == "full":
        print(_format_created_key_details(result))
    else:
        print(_format_encoded_key(result))
    return 0


def main() -> int:
    """Main entry point for the create-es-api-key script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
