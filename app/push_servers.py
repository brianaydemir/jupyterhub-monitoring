"""Command-line tool for pushing JupyterHub servers to Elasticsearch."""

import argparse
import json
import os
import sys
from pathlib import Path

from app.elasticsearch_client import ElasticsearchClient
from app.jupyterhub_client import JupyterHubClient


def push_servers(
    jupyterhub_client: JupyterHubClient,
    elasticsearch_client: ElasticsearchClient | None,
    elasticsearch_index: str,
    limit: int | None = None,
    debug: bool = False,
    metadata: dict[str, str] | None = None,
) -> tuple[int, int]:
    """
    Fetch servers from JupyterHub and push them to Elasticsearch.

    Args:
        jupyterhub_client: The JupyterHub API client
        elasticsearch_client: The Elasticsearch API client (None if debug mode)
        elasticsearch_index: The Elasticsearch index name
        limit: Maximum number of servers to process (None for unlimited)
        debug: If True, print documents without pushing to Elasticsearch
        metadata: Optional metadata key-value pairs to add to documents

    Returns:
        A tuple of (successful_count, error_count)
    """
    # Fetch servers from JupyterHub
    try:
        servers = jupyterhub_client.list_servers()
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error fetching servers from JupyterHub: {e}", file=sys.stderr)
        return (0, 1)

    # Apply limit if specified
    if limit is not None:
        servers = servers[:limit]

    total_servers = len(servers)
    print(f"Found {total_servers} server(s) to process")

    if total_servers == 0:
        return (0, 0)

    # Process each server
    successful = 0
    errors = 0

    for i, server in enumerate(servers, 1):
        try:
            # Add metadata fields to the document
            if metadata:
                for key, value in metadata.items():
                    server[f"meta.{key}"] = value

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


def parse_arguments() -> tuple[argparse.Namespace, dict[str, str]]:
    """
    Parse command-line arguments.

    Returns:
        A tuple of (parsed arguments, metadata dictionary)
    """
    parser = argparse.ArgumentParser(
        description="Push JupyterHub servers to Elasticsearch",
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
        type=Path,
        help=(
            "Path to file containing the JupyterHub API key for authentication "
            "(or set JUPYTERHUB_API_KEY)"
        ),
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
        type=Path,
        help=(
            "Path to file containing the Elasticsearch API key for authentication "
            "(or set ELASTICSEARCH_API_KEY)"
        ),
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
    parser.add_argument(
        "--metadata",
        action="append",
        metavar="KEY=VALUE",
        help="Additional metadata to add to documents (can be specified multiple times)",
    )

    args = parser.parse_args()

    # Validate that the JupyterHub API key is available
    if not args.jupyterhub_api_key and not os.environ.get("JUPYTERHUB_API_KEY"):
        parser.error(
            "--jupyterhub-api-key or the JUPYTERHUB_API_KEY environment variable is required"
        )

    # Validate that Elasticsearch settings are provided unless in debug mode
    if not args.debug:
        if not args.elasticsearch_endpoint:
            parser.error("--elasticsearch-endpoint is required unless --debug is used")
        if not args.elasticsearch_api_key and not os.environ.get(
            "ELASTICSEARCH_API_KEY"
        ):
            parser.error(
                "--elasticsearch-api-key or ELASTICSEARCH_API_KEY is required "
                "unless --debug is used"
            )
        if not args.elasticsearch_index:
            parser.error("--elasticsearch-index is required unless --debug is used")

    # Validate limit is positive if provided
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be a positive integer")

    # Parse and validate metadata key-value pairs
    metadata_dict = {}
    if args.metadata:
        for item in args.metadata:
            if "=" not in item:
                parser.error(f"Invalid metadata format '{item}': must be KEY=VALUE")
            key, value = item.split("=", 1)
            if not key:
                parser.error(f"Invalid metadata format '{item}': key cannot be empty")
            metadata_dict[key] = value

    # Validate that path-type arguments point to existing files
    path_args = [
        (args.jupyterhub_api_key, "JupyterHub API key file"),
        (args.jupyterhub_ca_cert, "JupyterHub CA certificate file"),
        (args.elasticsearch_api_key, "Elasticsearch API key file"),
        (args.elasticsearch_ca_cert, "Elasticsearch CA certificate file"),
    ]
    for path, label in path_args:
        if path is not None and not path.exists():
            parser.error(f"{label} not found: {path}")

    return args, metadata_dict


def main() -> int:
    """
    Main entry point for the push_servers script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args, metadata = parse_arguments()

        # Initialize JupyterHub client
        print("Connecting to JupyterHub...")
        jupyterhub_api_key = (
            args.jupyterhub_api_key.read_text().strip()
            if args.jupyterhub_api_key
            else os.environ["JUPYTERHUB_API_KEY"]
        )
        jupyterhub_client = JupyterHubClient(
            endpoint=args.jupyterhub_endpoint,
            api_key=jupyterhub_api_key,
            ca_cert=str(args.jupyterhub_ca_cert) if args.jupyterhub_ca_cert else None,
        )
        print("Connected to JupyterHub")

        # Initialize Elasticsearch client (unless in debug mode)
        elasticsearch_client = None
        if not args.debug:
            print("Connecting to Elasticsearch...")
            elasticsearch_api_key = (
                args.elasticsearch_api_key.read_text().strip()
                if args.elasticsearch_api_key
                else os.environ["ELASTICSEARCH_API_KEY"]
            )
            elasticsearch_client = ElasticsearchClient(
                endpoint=args.elasticsearch_endpoint,
                api_key=elasticsearch_api_key,
                ca_cert=(
                    str(args.elasticsearch_ca_cert)
                    if args.elasticsearch_ca_cert
                    else None
                ),
            )
            print("Connected to Elasticsearch")
        else:
            print("Debug mode: Documents will be printed, not pushed to Elasticsearch")

        # Push servers
        successful, errors = push_servers(
            jupyterhub_client=jupyterhub_client,
            elasticsearch_client=elasticsearch_client,
            elasticsearch_index=args.elasticsearch_index if not args.debug else "",
            limit=args.limit,
            debug=args.debug,
            metadata=metadata,
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

    except ConnectionError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
