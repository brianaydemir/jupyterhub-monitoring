"""Clients, configuration, and derived values built from parsed args.

Each helper here reads an ``argparse.Namespace``
so that nothing outside :mod:`app.cli` has to.
All of them assume the arguments have already passed :mod:`app.cli.validation`.
"""

import argparse
import sys
from datetime import datetime, timedelta, tzinfo

from app.cli.credentials import prompt_password, read_api_key
from app.clients.elasticsearch_client import ElasticsearchClient
from app.clients.jupyterhub_client import JupyterHubClient
from app.core.errors import AuthCancelledError, DataShapeError
from app.core.time_utils import (
    DATE_FMT,
    DATETIME_FMT,
    compute_time_range,
    elapsed,
    format_timezone,
    last_instant,
    parse_datetime,
    parse_duration_required,
    parse_timezone,
    snap_to_whole_days,
)
from app.reports.delivery import OutputTargets
from app.reports.email_utils import EmailConfig
from app.reports.model import Duration


def make_es_client(args: argparse.Namespace) -> ElasticsearchClient:
    """Build an Elasticsearch client from parsed args.

    When ``args.es_username`` is set, prompts for its password
    via :func:`app.cli.credentials.prompt_password`.

    Raises:
        AppError: If the API key file cannot be read.
        AuthCancelledError: If interactive credential entry is cancelled.
        DataShapeError: If no API key is available.
    """
    if args.es_username:
        password = prompt_password()
        if password is None:
            raise AuthCancelledError("Credential entry cancelled")
        return ElasticsearchClient(
            endpoint=args.es_endpoint,
            basic_auth=(args.es_username, password),
            ca_cert=args.es_ca_cert,
        )
    return ElasticsearchClient(
        endpoint=args.es_endpoint,
        api_key=read_api_key(args.es_api_key, "ELASTICSEARCH_API_KEY"),
        ca_cert=args.es_ca_cert,
    )


def make_jupyterhub_client(args: argparse.Namespace) -> JupyterHubClient:
    """Build a JupyterHub client from parsed args.

    Raises:
        AppError: If the API key file cannot be read.
        DataShapeError: If no API key is available.
    """
    return JupyterHubClient(
        endpoint=args.jupyterhub_endpoint,
        api_key=read_api_key(args.jupyterhub_api_key, "JUPYTERHUB_API_KEY"),
        ca_cert=args.jupyterhub_ca_cert,
    )


def make_output_targets(
    args: argparse.Namespace,
    *,
    subject_prefix: str,
    window: tuple[datetime, datetime],
) -> OutputTargets:
    """Build a report's delivery targets from parsed args.

    The email settings are collected only when ``--send-email`` is set,
    which is also what makes their flags required
    (:func:`app.cli.validation.validate_email_arguments`).

    *subject_prefix* and *window* are what a default ``--subject`` is made of.
    They arrive here rather than through the parser
    because the parser is built before the window is resolved,
    so a caller passes both once it holds the window
    :func:`compute_report_time_range` returned.
    """
    return OutputTargets(
        text_file=args.text_file,
        html_file=args.html_file,
        csv_file=args.csv_file,
        xlsx_file=args.xlsx_file,
        email=_make_email_config(args, subject_prefix, window) if args.send_email else None,
    )


def _make_email_config(
    args: argparse.Namespace,
    subject_prefix: str,
    window: tuple[datetime, datetime],
) -> EmailConfig:
    """Collect the email settings from parsed args.

    ``--subject`` has no string default, so an explicit subject is sent
    exactly as given, even one that reads like a generated default.
    """
    subject = args.subject
    if subject is None:
        subject = _default_subject(subject_prefix, *window)
    return EmailConfig(
        sender_name=args.sender_name,
        sender_email=args.sender_email,
        recipients=tuple(args.recipient_email),
        subject=subject,
        smtp_host=args.smtp_host,
        smtp_port=args.smtp_port,
        smtp_no_ssl=args.smtp_no_ssl,
    )


def _default_subject(prefix: str, start: datetime, end: datetime) -> str:
    """Render the subject line a run gets when ``--subject`` is not given.

    The stderr echo names the window to the minute,
    with its zone and its length; an inbox list has room for far less,
    so this names only the days the report covers,
    and only once when they are the same day.
    The window is half-open, so its last day is the one
    holding the instant before *end*: a window closing at midnight is named
    by the day it reported on rather than by the day it stops at.
    """
    first = start.strftime(DATE_FMT)
    last = last_instant(end).strftime(DATE_FMT)
    return f"{prefix}: {first}" if first == last else f"{prefix}: {first} to {last}"


def compute_report_time_range(
    args: argparse.Namespace,
) -> tuple[datetime, datetime]:
    """Return the ``(start, end)`` datetime pair for the reporting window.

    Assumes the arguments have already passed
    :func:`app.cli.validation.validate_query_arguments`,
    which resolves the window the same way.

    Echoes the resolved window to stderr,
    which is what makes a misparse visible:
    ``dateparser`` reads a bare ``"a"`` as a date rather than refusing it.

    Raises:
        DataShapeError: If the window's ``--duration`` or bounds
            cannot be parsed.
    """
    tz = parse_timezone(args.timezone)

    if args.report_start is not None:
        start = parse_datetime(args.report_start, tz)
        end = parse_datetime(args.report_end, tz)
        if start is None or end is None:
            raise DataShapeError("Invalid --report-start / --report-end datetime")
        if args.whole_days:
            start, end = snap_to_whole_days(start, end, tz)
    else:
        duration_td = parse_duration_required(args.duration)
        start, end = compute_time_range(duration_td, args.time, tz)

    print(_format_reporting_window(start, end, tz), file=sys.stderr)
    return start, end


def _format_reporting_window(start: datetime, end: datetime, tz: tzinfo) -> str:
    """Render the resolved reporting window for the echo."""
    return (
        f"Reporting window: {start.strftime(DATETIME_FMT)}"
        f" to {end.strftime(DATETIME_FMT)}"
        f" ({format_timezone(tz)}, {_format_window_length(elapsed(start, end))})"
    )


def _format_window_length(length: timedelta) -> str:
    """Render a window's length in whole days, or in hours below a day.

    Days alone would report a half-day window as ``0 days``, so a shorter one
    borrows the ``H:MM`` rendering of :class:`app.reports.model.Duration`
    rather than growing a duration formatter of its own.
    """
    if length < timedelta(days=1):
        return f"{Duration(length.total_seconds()).display()} hours"
    days = round(length / timedelta(days=1))
    return f"{days} day" if days == 1 else f"{days} days"


def get_strftime_fmt(args: argparse.Namespace) -> str:
    """Return the strftime format string for ``--date-format``.

    Only a command whose parser adds that argument may call this.
    ``get-new-activity`` omits it and passes :data:`DATETIME_FMT` itself,
    so ``get-new-users`` is the one caller.
    """
    return DATE_FMT if args.date_format == "date" else DATETIME_FMT
