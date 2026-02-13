"""Command-line tool for pushing JupyterHub active servers to Elasticsearch."""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from app.elasticsearch_client import ElasticsearchClient
from app.jupyterhub_client import JupyterHubClient


def push_active_servers(
    jupyterhub_client: JupyterHubClient,
    elasticsearch_client: Optional[ElasticsearchClient],
    elasticsearch_index: str,
    limit: Optional[int] = None,
    debug: bool = False,
) -> tuple[int, int]:
    """
    Fetch active servers from JupyterHub and push them to Elasticsearch.

    Args:
        jupyterhub_client: The JupyterHub API client
        elasticsearch_client: The Elasticsearch API client (None if debug mode)
        elasticsearch_index: The Elasticsearch index name
        limit: Maximum number of servers to process (None for unlimited)
        debug: If True, print documents without pushing to Elasticsearch

    Returns:
        A tuple of (successful_count, error_count)
    """
    # Fetch active servers from JupyterHub
    try:
        active_servers = jupyterhub_client.list_active_servers()
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error fetching active servers from JupyterHub: {e}", file=sys.stderr)
        return (0, 1)

    # Apply limit if specified
    if limit is not None:
        active_servers = active_servers[:limit]

    total_servers = len(active_servers)
    print(f"Found {total_servers} active server(s) to process")

    if total_servers == 0:
        return (0, 0)

    # Process each server
    successful = 0
    errors = 0

    for i, server in enumerate(active_servers, 1):
        try:
            if debug:
                # In debug mode, just print the document
                print(f"\n--- Document {i}/{total_servers} ---")
                print(json.dumps(server, indent=2))
                successful += 1
            elif elasticsearch_client is not None:
                # Push to Elasticsearch
                result = elasticsearch_client.upload_document(
                    index=elasticsearch_index,
                    document=server,
                )
                print(
                    f"Pushed server {i}/{total_servers}: "
                    f"{server.get('user.name', 'unknown')}/"
                    f"{server.get('server.name', 'unknown')} "
                    f"(result: {result.get('result', 'unknown')})"
                )
                successful += 1
        except Exception as e:  # pylint: disable=broad-exception-caught
            # Continue processing other servers even if one fails
            print(
                f"Error pushing server {i}/{total_servers} "
                f"({server.get('user.name', 'unknown')}/"
                f"{server.get('server.name', 'unknown')}): {e}",
                file=sys.stderr,
            )
            errors += 1

    return (successful, errors)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Push JupyterHub active servers to Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # JupyterHub settings (required)
    parser.add_argument(
        "--jupyterhub-endpoint",
        required=True,
        help='JupyterHub API endpoint URL (e.g., "https://localhost:8000/hub/api")',
    )
    parser.add_argument(
        "--jupyterhub-api-key",
        required=True,
        help="JupyterHub API key for authentication",
    )
    parser.add_argument(
        "--jupyterhub-ca-cert",
        type=Path,
        help="Path to CA certificate file for JupyterHub TLS verification",
    )

    # Elasticsearch settings (required unless --debug)
    parser.add_argument(
        "--elasticsearch-endpoint",
        help='Elasticsearch API endpoint URL (e.g., "https://localhost:9200")',
    )
    parser.add_argument(
        "--elasticsearch-api-key",
        help="Elasticsearch API key for authentication",
    )
    parser.add_argument(
        "--elasticsearch-index",
        help="Elasticsearch index name to push documents to",
    )
    parser.add_argument(
        "--elasticsearch-ca-cert",
        type=Path,
        help="Path to CA certificate file for Elasticsearch TLS verification",
    )

    # Optional flags
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print documents without pushing to Elasticsearch",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Process only N servers (for testing or limiting scope)",
    )

    args = parser.parse_args()

    # Validate that Elasticsearch settings are provided unless in debug mode
    if not args.debug:
        if not args.elasticsearch_endpoint:
            parser.error("--elasticsearch-endpoint is required unless --debug is used")
        if not args.elasticsearch_api_key:
            parser.error("--elasticsearch-api-key is required unless --debug is used")
        if not args.elasticsearch_index:
            parser.error("--elasticsearch-index is required unless --debug is used")

    # Validate limit is positive if provided
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be a positive integer")

    # Validate CA cert files exist if provided
    if args.jupyterhub_ca_cert and not args.jupyterhub_ca_cert.exists():
        parser.error(f"JupyterHub CA certificate file not found: {args.jupyterhub_ca_cert}")
    if args.elasticsearch_ca_cert and not args.elasticsearch_ca_cert.exists():
        parser.error(
            f"Elasticsearch CA certificate file not found: {args.elasticsearch_ca_cert}"
        )

    return args


def main() -> int:
    """
    Main entry point for the push_active_servers script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        # Initialize JupyterHub client
        print("Connecting to JupyterHub...")
        jupyterhub_client = JupyterHubClient(
            endpoint=args.jupyterhub_endpoint,
            api_key=args.jupyterhub_api_key,
            ca_cert=str(args.jupyterhub_ca_cert) if args.jupyterhub_ca_cert else None,
        )
        print("Connected to JupyterHub")

        # Initialize Elasticsearch client (unless in debug mode)
        elasticsearch_client = None
        if not args.debug:
            print("Connecting to Elasticsearch...")
            elasticsearch_client = ElasticsearchClient(
                endpoint=args.elasticsearch_endpoint,
                api_key=args.elasticsearch_api_key,
                ca_cert=str(args.elasticsearch_ca_cert) if args.elasticsearch_ca_cert else None,
            )
            print("Connected to Elasticsearch")
        else:
            print("Debug mode: Documents will be printed, not pushed to Elasticsearch")

        # Push active servers
        successful, errors = push_active_servers(
            jupyterhub_client=jupyterhub_client,
            elasticsearch_client=elasticsearch_client,
            elasticsearch_index=args.elasticsearch_index if not args.debug else "",
            limit=args.limit,
            debug=args.debug,
        )

        # Clean up
        if elasticsearch_client:
            elasticsearch_client.close()

        # Report results
        print(f"\nProcessing complete: {successful} successful, {errors} errors")

        # Return exit code based on results
        if errors > 0:
            return 1
        return 0

    except (ConnectionError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
