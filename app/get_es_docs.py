"""Command-line tool for querying Elasticsearch documents."""

import argparse
import json
import sys

from app.cli_utils import (
    add_elasticsearch_argument_group,
    read_api_key,
    validate_elasticsearch_arguments,
)
from app.elasticsearch_client import ElasticsearchClient


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=(
            "Query Elasticsearch and retrieve documents using " "a Kibana-style query string"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --elasticsearch-endpoint https://es.example.com:9200 --elasticsearch-api-key /path/to/api-key --elasticsearch-index logs --query "status:200"
  %(prog)s --elasticsearch-endpoint https://es.example.com:9200 --elasticsearch-api-key /path/to/api-key --elasticsearch-index logs --query "level:error AND timestamp:[now-1d TO now]"
  %(prog)s --elasticsearch-endpoint https://es.example.com:9200 --elasticsearch-api-key /path/to/api-key --elasticsearch-index logs --query "*" --elasticsearch-ca-cert /path/to/ca.crt

Environment variables:
  ELASTICSEARCH_API_KEY  Elasticsearch API key (used when --elasticsearch-api-key is not provided)
        """,
    )

    # Elasticsearch connection parameters
    add_elasticsearch_argument_group(parser)

    # Query parameters
    parser.add_argument(
        "--query",
        required=True,
        help='Kibana-style query string (e.g., "status:200", "field:value AND other:*")',
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help=(
            "Maximum number of documents to retrieve and display. "
            "Results are sorted in descending order by meta.snapshot-time."
        ),
    )

    args = parser.parse_args()

    # Validate Elasticsearch API key and CA certificate
    validate_elasticsearch_arguments(args, parser)

    return args


def main() -> int:
    """Main entry point for the get-es-docs script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        # Initialize the Elasticsearch client
        client = ElasticsearchClient(
            endpoint=args.elasticsearch_endpoint,
            api_key=read_api_key(args.elasticsearch_api_key, "ELASTICSEARCH_API_KEY"),
            ca_cert=(str(args.elasticsearch_ca_cert) if args.elasticsearch_ca_cert else None),
        )

        # Query Elasticsearch and print results
        try:
            sort = (
                [{"meta.snapshot-time": {"order": "desc"}}] if args.limit is not None else None
            )
            for document in client.query(
                index=args.elasticsearch_index,
                query_string=args.query,
                sort=sort,
                limit=args.limit,
            ):
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
