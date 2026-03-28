"""Post-parse validation of a command's arguments.

Every helper here reports a problem through ``parser.error``, which
prints usage and exits 2; none of them returns a value.
"""

import argparse
import os
import re
from datetime import datetime, timedelta, tzinfo

from app.core.time_utils import (
    DATETIME_FMT,
    parse_datetime,
    parse_duration,
    parse_timezone,
    snap_to_whole_days,
)

# How far past the present a resolved ``--report-end`` may fall before it
# is rejected. ``--report-end "now"`` is legitimate, and the value is
# resolved once here and again in
# :func:`app.cli.factories.compute_report_time_range`, so the two answers
# sit a fraction of a second apart; one minute is orders of magnitude
# more slack than that, and it is also the finest unit any of this
# project's timestamps display, so a window this check admits can never
# show a clock time the user would read as still to come. A real typo is
# further out by a wide margin: ``"tomorrow"`` lands a day ahead and
# ``"next week"`` seven.
_FUTURE_END_TOLERANCE = timedelta(minutes=1)


def positive_int_or_error(
    value: int | None,
    *,
    parser: argparse.ArgumentParser,
    flag: str,
) -> None:
    """Validate that an optional integer is positive when provided."""
    if value is not None and value < 1:
        parser.error(f"{flag} must be a positive integer")


def validate_jupyterhub_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate JupyterHub credentials and CA certificate arguments."""
    if args.jupyterhub_ca_cert is not None and not args.jupyterhub_ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.jupyterhub_ca_cert}")

    if args.jupyterhub_api_key is not None:
        if not args.jupyterhub_api_key.exists():
            parser.error(f"API key file not found: {args.jupyterhub_api_key}")
    elif not os.environ.get("JUPYTERHUB_API_KEY"):
        parser.error(
            "--jupyterhub-api-key or the JUPYTERHUB_API_KEY environment variable is required"
        )


def validate_es_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate Elasticsearch credentials and CA certificate after parsing.

    Calls ``parser.error`` if:

    - The CA certificate file does not exist (when provided).
    - Both ``--es-api-key`` and ``--es-username`` are given
      (mutually exclusive).
    - No credential is available from any source.
    - The ``--es-api-key`` file does not exist (when provided).

    Only an explicitly passed ``--es-api-key`` conflicts with
    ``--es-username``. ``ELASTICSEARCH_API_KEY`` does not, because it is
    typically injected into the environment by a container or secret
    mount, where it would otherwise make ``--es-username`` unusable;
    an explicit ``--es-username`` takes precedence over it.
    """
    if args.es_ca_cert is not None and not args.es_ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.es_ca_cert}")

    has_api_key_file = args.es_api_key is not None
    has_api_key_env = bool(os.environ.get("ELASTICSEARCH_API_KEY"))
    has_username = bool(args.es_username)

    if has_api_key_file and has_username:
        parser.error("--es-api-key and --es-username are mutually exclusive")
    if not has_api_key_file and not has_api_key_env and not has_username:
        parser.error("one of --es-api-key, ELASTICSEARCH_API_KEY, or --es-username is required")
    if args.es_api_key is not None and not args.es_api_key.exists():
        parser.error(f"API key file not found: {args.es_api_key}")


def validate_query_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate the reporting window and ``--timezone``.

    Exactly one window mode must be selected: ``--duration`` (optionally
    anchored with ``--time``), or the ``--report-start`` / ``--report-end``
    pair (optionally widened with ``--whole-days``). Each mode rejects the
    other's modifier, so a flag that would do nothing is an error rather
    than a silent no-op.
    """
    try:
        tz = parse_timezone(args.timezone)
    except ValueError as e:
        parser.error(str(e))

    has_range = args.report_start is not None or args.report_end is not None
    has_duration = args.duration is not None

    if has_range and has_duration:
        parser.error("--duration is mutually exclusive with --report-start / --report-end")
    if not has_range and not has_duration:
        parser.error("one of --duration or --report-start / --report-end is required")

    if has_range:
        _validate_report_range(args, parser, tz)
        return

    if args.whole_days:
        parser.error("--whole-days cannot be combined with --duration")
    duration_td = parse_duration(args.duration)
    if duration_td is None:
        parser.error(f"Invalid duration format: {args.duration!r}")
    if duration_td.total_seconds() <= 0:
        parser.error(f"--duration must be positive, not {args.duration!r}")
    _validate_time(args, parser)


def _validate_report_range(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    tz: tzinfo,
) -> None:
    """Validate the ``--report-start`` / ``--report-end`` window.

    *tz* is the already-parsed ``--timezone``, so the window's bounds are
    read in the same zone the caller validated.

    An end past ``_FUTURE_END_TOLERANCE`` is rejected, because no data can
    exist for a window reaching beyond the present. Only the end is
    checked: a future start is either later than the end, which the
    ordering check catches, or earlier than an end that is further into
    the future still. Duration mode needs no such check, since
    :func:`app.core.time_utils.compute_time_range` ends its window at the
    current moment or at a wall-clock time it has already stepped back
    into the past.

    ``--whole-days`` widens the window before those last two checks, so
    that they judge the window the report will actually cover, which is
    the one :func:`app.cli.factories.compute_report_time_range` echoes.
    The ordering check runs both before and after the snap: the first
    names the bounds the user wrote, and the second catches the one way a
    forward-running window can still come out empty, a start and an end
    that land on the same midnight.
    """
    if args.report_start is None or args.report_end is None:
        parser.error("--report-start and --report-end must be used together")
    if args.time is not None:
        parser.error("--time cannot be combined with --report-start / --report-end")
    start = parse_datetime(args.report_start, tz)
    if start is None:
        parser.error(f"Invalid --report-start datetime: {args.report_start!r}")
    end = parse_datetime(args.report_end, tz)
    if end is None:
        parser.error(f"Invalid --report-end datetime: {args.report_end!r}")
    if start >= end:
        parser.error("--report-start must be before --report-end")
    if args.whole_days:
        start, end = snap_to_whole_days(start, end, tz)
        if start >= end:
            parser.error(
                "--whole-days leaves no whole day between --report-start and --report-end"
            )
    if end - datetime.now(tz) > _FUTURE_END_TOLERANCE:
        parser.error(
            f"--report-end resolves to {end.strftime(DATETIME_FMT)}, which is in the future"
        )


def _validate_time(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate the optional ``--time`` anchor for duration mode."""
    if args.time is None:
        return
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", args.time)
    if not m:
        parser.error("--time must be in HH:MM format")
    hh, mm = int(m.group(1)), int(m.group(2))
    if hh > 23 or mm > 59:
        parser.error("--time must be a valid time (HH: 0-23, MM: 0-59)")


def validate_email_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate required email arguments when ``--send-email`` is set.

    ``--recipient-email`` collects a list, so it is missing when that
    list is absent rather than when it is empty: the parser refuses an
    occurrence naming no recipient, which leaves nothing between the
    two.
    """
    if args.send_email:
        for flag, attr in [
            ("--sender-email", "sender_email"),
            ("--recipient-email", "recipient_email"),
            ("--smtp-host", "smtp_host"),
            ("--smtp-port", "smtp_port"),
        ]:
            if getattr(args, attr) is None:
                parser.error(f"{flag} is required when --send-email is set")


def validate_es_admin_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate the arguments the Elasticsearch admin commands share.

    The admin commands need nothing beyond the credential and CA
    certificate checks in :func:`validate_es_arguments`, so this
    function only delegates for now. It is the validation counterpart to
    :func:`app.cli.arguments.configure_es_admin_parser`, and exists so
    that a rule applying to the admin commands alone has one home where
    all four of them pick it up, rather than being added to the shared
    function the report and push commands also call.
    """
    validate_es_arguments(args, parser)
