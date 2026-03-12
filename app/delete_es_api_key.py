"""Command-line tool for deleting Elasticsearch API keys."""

import argparse
import sys

from app.cli_utils import (
    add_elasticsearch_basic_argument_group,
    prompt_credentials,
    read_api_key,
    validate_elasticsearch_basic_arguments,
)
from app.elasticsearch_client import ElasticsearchClient


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Invalidate an Elasticsearch API key using username and password",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --id abc123
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --name "my-api-key"
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --id abc123 --elasticsearch-ca-cert /path/to/ca.crt
        """,
    )

    # Elasticsearch connection parameters
    add_elasticsearch_basic_argument_group(parser)

    # API key selection (exactly one required)
    key_group = parser.add_mutually_exclusive_group(required=True)
    key_group.add_argument(
        "--id",
        dest="key_id",
        metavar="ID",
        help="ID of the API key to invalidate",
    )
    key_group.add_argument(
        "--name",
        dest="key_name",
        metavar="NAME",
        help="Name of the API key(s) to invalidate",
    )

    args = parser.parse_args()

    # Validate CA certificate exists if provided
    validate_elasticsearch_basic_arguments(args, parser)

    return args


def main() -> int:
    """Main entry point for the delete-es-api-key script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        ca_cert = str(args.elasticsearch_ca_cert) if args.elasticsearch_ca_cert else None
        if args.elasticsearch_username:
            credentials = prompt_credentials(args.elasticsearch_username)
            if credentials is None:
                return 1
            _, password = credentials
            client = ElasticsearchClient(
                endpoint=args.elasticsearch_endpoint,
                basic_auth=(args.elasticsearch_username, password),
                ca_cert=ca_cert,
            )
        else:
            client = ElasticsearchClient(
                endpoint=args.elasticsearch_endpoint,
                api_key=read_api_key(args.elasticsearch_api_key, "ELASTICSEARCH_API_KEY"),
                ca_cert=ca_cert,
            )

        # Invalidate the API key
        try:
            result = client.delete_api_key(
                key_id=args.key_id,
                key_name=args.key_name,
            )
        except (ValueError, Exception) as e:  # pylint: disable=broad-exception-caught
            print(f"Error invalidating API key: {e}", file=sys.stderr)
            return 1

        invalidated = result.get("invalidated_api_keys", [])
        previously = result.get("previously_invalidated_api_keys", [])
        error_count = result.get("error_count", 0)

        if invalidated:
            print(f"Invalidated {len(invalidated)} API key(s): {', '.join(invalidated)}")
        if previously:
            print(f"Already invalidated: {len(previously)} key(s): " f"{', '.join(previously)}")
        if not invalidated and not previously:
            print("No matching API keys found.")
        if error_count:
            print(f"Errors: {error_count}", file=sys.stderr)
            return 1

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
