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
from app.core.time_utils import parse_duration


def _make_doc_id(server: dict[str, Any]) -> str:
    """Build a deterministic Elasticsearch document ID."""
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
    interval: str | None,
    metadata: dict[str, str] | None,
    debug: bool,
    elasticsearch_client: ElasticsearchClient | None,
    elasticsearch_index: str | None,
    i: int,
    total_servers: int,
    snapshot_time: datetime,
) -> bool:
    """Add metadata to a server document and push or print it."""
    user_name = server.get("user.name", "unknown")
    server_name = server.get("server.name", "unknown")
    try:
        if metadata:
            for key, value in metadata.items():
                server[f"meta.{key}"] = value

        if interval is not None:
            server["meta.interval"] = interval

        server["meta.snapshot-time"] = int(snapshot_time.timestamp())
        server["meta.snapshot-time-iso"] = snapshot_time.replace(microsecond=0).isoformat()

        if debug:
            print(f"\n--- Document {i}/{total_servers} ---")
            print(json.dumps(server, indent=2))
        elif elasticsearch_client is not None and elasticsearch_index is not None:
            result = elasticsearch_client.upload_document(
                index=elasticsearch_index,
                document=server,
                doc_id=_make_doc_id(server),
            )
            print(
                f"Pushed server {i}/{total_servers}: {user_name}/{server_name} (result: {result.get('result', 'unknown')})"
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
    interval: str | None = None,
    limit: int | None = None,
    debug: bool = False,
    metadata: dict[str, str] | None = None,
) -> tuple[int, int]:
    """Fetch servers from JupyterHub and push to Elasticsearch.

    Raises:
        ExternalServiceError: If the server list cannot be fetched.
    """
    try:
        servers = jupyterhub_client.list_servers()
    except Exception as e:
        raise ExternalServiceError(f"Fetching servers from JupyterHub failed: {e}") from e

    if limit is not None:
        servers = servers[:limit]

    total_servers = len(servers)
    print(f"Found {total_servers} server(s) to process")

    if total_servers == 0:
        return (0, 0)

    snapshot_time = datetime.now(timezone.utc)
    successful = 0
    errors = 0

    for i, server in enumerate(servers, 1):
        if _process_single_server(
            server,
            interval,
            metadata,
            debug,
            elasticsearch_client,
            elasticsearch_index,
            i,
            total_servers,
            snapshot_time,
        ):
            successful += 1
        else:
            errors += 1

    return (successful, errors)


def _parse_metadata(
    raw_items: list[str] | None,
    parser: argparse.ArgumentParser,
) -> dict[str, str]:
    """Parse ``--metadata KEY=VALUE`` items.

    Reserved keys (``snapshot-time``, ``snapshot-time-iso``,
    ``interval``) cause a parse error.
    """
    metadata_dict: dict[str, str] = {}
    for item in raw_items or []:
        if "=" not in item:
            parser.error(f"Invalid metadata format '{item}': must be KEY=VALUE")
        key, value = item.split("=", 1)
        if not key:
            parser.error(f"Invalid metadata format '{item}': key cannot be empty")
        metadata_dict[key] = value

    reserved = {"snapshot-time", "snapshot-time-iso", "interval"}
    for key in reserved & metadata_dict.keys():
        parser.error(
            f"--metadata {key}=... is reserved;"
            + " it is set automatically and cannot be overridden"
        )

    return metadata_dict


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Push JupyterHub servers to Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    add_jupyterhub_argument_group(parser)
    add_es_argument_group(parser, required=False)

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print documents instead of pushing to Elasticsearch",
    )
    parser.add_argument(
        "--interval",
        metavar="DURATION",
        help=(
            "Push interval (e.g. 5m, 1h). "
            "Stored as meta.interval so get-new-activity can compute uptime. "
            "Required unless --debug is used."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Process only N servers",
    )
    parser.add_argument(
        "--metadata",
        action="append",
        metavar="KEY=VALUE",
        help="Additional metadata to add to documents (can be specified multiple times)",
    )
    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation."""
    validate_jupyterhub_arguments(args, parser)

    if not args.debug:
        if not args.es_endpoint:
            parser.error("--es-endpoint is required unless --debug is used")
        if not args.es_index:
            parser.error("--es-index is required unless --debug is used")
        validate_es_arguments(args, parser)
        if not args.interval:
            parser.error("--interval is required unless --debug is used")
    elif args.interval is None:
        print(
            "Note: --interval not set; meta.interval will be absent from debug output.",
            file=sys.stderr,
        )

    if args.interval is not None:
        if parse_duration(args.interval) is None:
            parser.error(f"--interval: invalid duration {args.interval!r}")

    positive_int_or_error(args.limit, parser=parser, flag="--limit")
    args.metadata_dict = _parse_metadata(args.metadata, parser)


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        ExternalServiceError: If client creation or
            server push fails.
    """
    jupyterhub_client = make_jupyterhub_client(args)

    if args.debug:
        print("Debug mode: documents will be printed, not pushed to Elasticsearch")
        successful, errors = push_servers(
            jupyterhub_client=jupyterhub_client,
            elasticsearch_client=None,
            elasticsearch_index=None,
            interval=args.interval,
            limit=args.limit,
            debug=True,
            metadata=args.metadata_dict,
        )
    else:
        with make_es_client(args) as es_client:
            successful, errors = push_servers(
                jupyterhub_client=jupyterhub_client,
                elasticsearch_client=es_client,
                elasticsearch_index=args.es_index,
                interval=args.interval,
                limit=args.limit,
                debug=False,
                metadata=args.metadata_dict,
            )

    print(f"\nProcessing complete: {successful} successful, {errors} errors")

    if errors > 0:
        return 1
    return 0


def main() -> int:
    """Main entry point for the push-servers script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
