"""Command-line tool for querying Elasticsearch documents."""

import argparse
import json
import os
import sys
from pathlib import Path

from app.elasticsearch_client import ElasticsearchClient


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=(
            "Query Elasticsearch and retrieve documents using "
            "a Kibana-style query string"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --endpoint https://es.example.com:9200 --api-key /path/to/api-key --index logs --query "status:200"
  %(prog)s --endpoint https://es.example.com:9200 --api-key /path/to/api-key --index logs --query "level:error AND timestamp:[now-1d TO now]"
  %(prog)s --endpoint https://es.example.com:9200 --api-key /path/to/api-key --index logs --query "*" --ca-cert /path/to/ca.crt

Environment variables:
  ELASTICSEARCH_API_KEY  Elasticsearch API key (used when --api-key is not provided)
        """,
    )

    # Elasticsearch connection parameters
    parser.add_argument(
        "--endpoint",
        required=True,
        help="Elasticsearch API endpoint URL (e.g., https://localhost:9200)",
    )
    parser.add_argument(
        "--api-key",
        type=Path,
        help=(
            "Path to file containing the Elasticsearch API key for authentication "
            "(or set ELASTICSEARCH_API_KEY)"
        ),
    )
    parser.add_argument(
        "--ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )

    # Query parameters
    parser.add_argument(
        "--index",
        required=True,
        help="Name of the Elasticsearch index to query",
    )
    parser.add_argument(
        "--query",
        required=True,
        help='Kibana-style query string (e.g., "status:200", "field:value AND other:*")',
    )

    args = parser.parse_args()

    # Validate CA certificate exists if provided
    if args.ca_cert and not args.ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.ca_cert}")

    # Validate API key: file arg takes precedence over env var
    if args.api_key is not None:
        if not args.api_key.exists():
            parser.error(f"API key file not found: {args.api_key}")
    elif not os.environ.get("ELASTICSEARCH_API_KEY"):
        parser.error(
            "--api-key or the ELASTICSEARCH_API_KEY environment variable is required"
        )

    return args


def main() -> int:
    """Main entry point for the get-es-docs script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        # Initialize the Elasticsearch client
        api_key = (
            args.api_key.read_text().strip()
            if args.api_key
            else os.environ["ELASTICSEARCH_API_KEY"]
        )
        client = ElasticsearchClient(
            endpoint=args.endpoint,
            api_key=api_key,
            ca_cert=str(args.ca_cert) if args.ca_cert else None,
        )

        # Query Elasticsearch and print results
        try:
            for document in client.query(index=args.index, query_string=args.query):
                print(json.dumps(document, indent=2))
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"Error querying Elasticsearch: {e}", file=sys.stderr)
            return 1

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
