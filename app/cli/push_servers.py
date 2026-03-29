"""Command-line tool for pushing JupyterHub servers to Elasticsearch."""

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any

from app.cli.runtime import positive_int_or_error, run_command
from app.cli.utils import (
    add_es_argument_group,
    add_jupyterhub_argument_group,
    make_es_client,
    make_jupyterhub_client,
    validate_es_arguments,
    validate_jupyterhub_arguments,
)
from app.clients.elasticsearch_client import ElasticsearchClient
from app.clients.jupyterhub_client import JupyterHubClient
from app.core.errors import ExternalServiceError


def _make_doc_id(server: dict[str, Any]) -> str:
    """Build a deterministic, human-readable Elasticsearch document ID."""
    snapshot_time = server.get("meta.snapshot-time")
    parts = [
        str(server.get("meta.hub") or "unknown-hub"),
        str(server.get("user.name") or "unknown-user"),
        str(server.get("server.name") or "default"),
        str(server.get("server.started") or "unknown-started"),
        str(snapshot_time if snapshot_time is not None else "unknown-snapshot"),
    ]
    return "|".join(parts)


def _process_single_server(
    server: dict[str, Any],
    metadata: dict[str, str] | None,
    debug: bool,
    elasticsearch_client: ElasticsearchClient | None,
    elasticsearch_index: str | None,
    i: int,
    total_servers: int,
) -> bool:
    """Add metadata to a server document and push or print it."""
    user_name = server.get("user.name", "unknown")
    server_name = server.get("server.name", "unknown")
    try:
        if metadata:
            for key, value in metadata.items():
                server[f"meta.{key}"] = value

        now = datetime.now(timezone.utc)
        server["meta.snapshot-time"] = int(now.timestamp())
        server["meta.snapshot-time-iso"] = now.replace(microsecond=0).isoformat()

        if debug:
            print(f"\n--- Document {i}/{total_servers} ---")
            print(json.dumps(server, indent=2))
        elif elasticsearch_client is not None and elasticsearch_index is not None:
            result = elasticsearch_client.upload_document(
                index=elasticsearch_index,
                document=server,
                doc_id=_make_doc_id(server),
            )
            result_str = result.get("result", "unknown")
            print(
                f"Pushed server {i}/{total_servers}: "
                + f"{user_name}/{server_name} (result: {result_str})"
            )
        return True
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(
            f"Error pushing server {i}/{total_servers} ({user_name}/{server_name}): {e}",
            file=sys.stderr,
        )
        return False


def push_servers(
    jupyterhub_client: JupyterHubClient,
    elasticsearch_client: ElasticsearchClient | None,
    elasticsearch_index: str | None,
    limit: int | None = None,
    debug: bool = False,
    metadata: dict[str, str] | None = None,
) -> tuple[int, int]:
    """Fetch servers from JupyterHub and push them to Elasticsearch."""
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
        if _process_single_server(
            server, metadata, debug, elasticsearch_client, elasticsearch_index, i, total_servers
        ):
            successful += 1
        else:
            errors += 1

    return (successful, errors)


def _parse_metadata(
    raw_items: list[str] | None, parser: argparse.ArgumentParser
) -> dict[str, str]:
    """Parse ``--metadata KEY=VALUE`` items, dropping reserved keys."""
    metadata_dict: dict[str, str] = {}
    for item in raw_items or []:
        if "=" not in item:
            parser.error(f"Invalid metadata format '{item}': must be KEY=VALUE")
        key, value = item.split("=", 1)
        if not key:
            parser.error(f"Invalid metadata format '{item}': key cannot be empty")
        metadata_dict[key] = value

    reserved_keys = {"snapshot-time", "snapshot-time-iso"}
    for key in reserved_keys & metadata_dict.keys():
        print(
            f"Warning: --metadata {key}=... is reserved and will be ignored.",
            file=sys.stderr,
        )
        del metadata_dict[key]

    return metadata_dict


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Push JupyterHub servers to Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # JupyterHub settings (required)
    add_jupyterhub_argument_group(parser)

    # Elasticsearch settings (required unless --debug)
    add_es_argument_group(parser, required=False)

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
    return parser


def _validate_arguments(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Run all post-parse argument validation for this command."""
    validate_jupyterhub_arguments(args, parser)

    if not args.debug:
        if not args.es_endpoint:
            parser.error("--es-endpoint is required unless --debug is used")
        if not args.es_index:
            parser.error("--es-index is required unless --debug is used")
        validate_es_arguments(args, parser)

    positive_int_or_error(args.limit, parser=parser, flag="--limit")
    args.metadata_dict = _parse_metadata(args.metadata, parser)


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        ExternalServiceError: If client creation or push execution fails.
    """
    try:
        jupyterhub_client = make_jupyterhub_client(args)
    except ConnectionError as e:
        raise ExternalServiceError(str(e)) from e

    elasticsearch_client: ElasticsearchClient | None = None
    try:
        if not args.debug:
            print("Connecting to Elasticsearch...")
            elasticsearch_client = make_es_client(args)
            print("Connected to Elasticsearch")
        else:
            print("Debug mode: Documents will be printed, not pushed to Elasticsearch")

        successful, errors = push_servers(
            jupyterhub_client=jupyterhub_client,
            elasticsearch_client=elasticsearch_client,
            elasticsearch_index=args.es_index,
            limit=args.limit,
            debug=args.debug,
            metadata=args.metadata_dict,
        )

        if elasticsearch_client:
            elasticsearch_client.close()
    except Exception as e:
        raise ExternalServiceError(f"Pushing servers failed: {e}") from e

    print(f"\nProcessing complete: {successful} successful, {errors} errors")

    if errors > 0:
        return 1
    return 0


def main() -> int:
    """Main entry point for the push_servers script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
