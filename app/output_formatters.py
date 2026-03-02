"""Shared output formatting helpers for report scripts."""

import csv
import html
import io
from datetime import datetime
from typing import TYPE_CHECKING, Any

from app.name_utils import _trailing_domain_key, parse_name

if TYPE_CHECKING:
    from datetime import tzinfo

# ---------------------------------------------------------------------------
# Shared private helpers
# ---------------------------------------------------------------------------

_DATETIME_FMT = "%Y-%m-%d %H:%M"

# Inline CSS used by HTML table headers (left-aligned)
_TH_STYLE = (
    "text-align:left; border:1px solid #9ab3c8; padding:2px 8px;"
    " background:#a6c9e8; color:#000000"
)

# Inline CSS used by HTML table headers (right-aligned, e.g. numeric columns)
_TH_STYLE_R = (
    "text-align:right; border:1px solid #9ab3c8; padding:2px 8px;"
    " background:#a6c9e8; color:#000000"
)


def _show_method(
    domain_id_pairs: list[tuple[str, str]],
    detailed_usernames: bool,
) -> bool:
    """Return True if the "Login method" column should be shown.

    The column is shown when *detailed_usernames* is True, or when the
    (domain, id) pairs are not all unique (meaning disambiguation is needed).

    Args:
        domain_id_pairs: List of (domain, id) tuples derived from parsed names
        detailed_usernames: Always show the column when True

    Returns:
        True if the "Login method" column should be included
    """
    return detailed_usernames or len(domain_id_pairs) != len(set(domain_id_pairs))


def _write_csv_footer(writer: Any, summary: str, tz_name: str) -> None:
    """Write the standard CSV report footer.

    Writes an empty row, a summary row, and a timezone row.

    Args:
        writer: csv.writer to write to
        summary: Summary sentence (e.g. "3 users were active from … to …")
        tz_name: Timezone name string
    """
    writer.writerow([])
    writer.writerow([summary])
    writer.writerow([f"Timezone: {tz_name}"])


def _html_table_footer(tz_name: str) -> str:
    """Return the standard HTML timezone footnote paragraph.

    Args:
        tz_name: Timezone name string

    Returns:
        HTML string
    """
    return f"<p><em>Timezone: {html.escape(tz_name)}</em></p>"


# ---------------------------------------------------------------------------
# New-user formatters (moved from get_new_users.py)
# ---------------------------------------------------------------------------


def _format_created(created_str: str, strftime_fmt: str, tz: "tzinfo") -> str:
    """Reformat a JupyterHub ISO 8601 creation timestamp in a given timezone."""
    try:
        dt = datetime.fromisoformat(created_str.replace("Z", "+00:00")).astimezone(tz)
        return dt.strftime(strftime_fmt)
    except ValueError, AttributeError:
        return created_str


def format_users_text(
    users: list[dict],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    tz: "tzinfo",
    strftime_fmt: str,
    detailed_usernames: bool = False,
) -> str:
    """Format new JupyterHub users as plain text.

    Args:
        users: List of user dicts with 'name' and 'created' keys
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footnote
        tz: Timezone for converting creation timestamps
        strftime_fmt: strftime format string for creation timestamps
        detailed_usernames: Always show the "Login method" column

    Returns:
        Plain text formatted string
    """
    range_str = (
        f"from {start_time.strftime(_DATETIME_FMT)}"
        f" to {end_time.strftime(_DATETIME_FMT)}"
    )
    n = len(users)
    if n == 0:
        lines = [
            f"No new users created between "
            f"{start_time.strftime(_DATETIME_FMT)} and {end_time.strftime(_DATETIME_FMT)}."
        ]
    else:
        noun = "user" if n == 1 else "users"
        lines = [f"{n} new {noun} created {range_str}:", ""]
        parsed = [
            (
                _format_created(user.get("created", ""), strftime_fmt, tz),
                *parse_name(user.get("name", "")),
            )
            for user in users
        ]
        domain_id_pairs = [(r[2], r[3]) for r in parsed]
        show_method = _show_method(domain_id_pairs, detailed_usernames)
        if show_method:
            rows = sorted(parsed, key=lambda r: (r[0], r[1], r[2], r[3]))
        else:
            rows = sorted(
                parsed,
                key=lambda r: (r[0], _trailing_domain_key(r[2]), r[2], r[3]),
            )
        created_width = max(len("Created"), max(len(r[0]) for r in rows))
        domain_width = max(len("Institution"), max(len(r[2]) for r in rows))
        id_width = max(len("ID"), max(len(r[3]) for r in rows))
        if show_method:
            method_width = max(len("Login method"), max(len(r[4]) for r in rows))
            lines.append(
                f"{'Created':<{created_width}}  {'Institution':<{domain_width}}  "
                f"{'ID':<{id_width}}  {'Login method':<{method_width}}"
            )
            lines.append(
                f"{'-' * created_width}  {'-' * domain_width}  "
                f"{'-' * id_width}  {'-' * method_width}"
            )
            for created, _priority, domain, uid, method in rows:
                lines.append(
                    f"{created:<{created_width}}  {domain:<{domain_width}}  "
                    f"{uid:<{id_width}}  {method:<{method_width}}"
                )
        else:
            lines.append(
                f"{'Created':<{created_width}}  {'Institution':<{domain_width}}  "
                f"{'ID':<{id_width}}"
            )
            lines.append(
                f"{'-' * created_width}  {'-' * domain_width}  {'-' * id_width}"
            )
            for created, _priority, domain, uid, _method in rows:
                lines.append(
                    f"{created:<{created_width}}  {domain:<{domain_width}}  "
                    f"{uid:<{id_width}}"
                )

    lines.append("")
    lines.append(f"Timezone: {tz_name}")
    return "\n".join(lines)


def format_users_html(
    users: list[dict],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    tz: "tzinfo",
    strftime_fmt: str,
    detailed_usernames: bool = False,
) -> str:
    """Format new JupyterHub users as HTML suitable for an email body.

    Args:
        users: List of user dicts with 'name' and 'created' keys
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footnote
        tz: Timezone for converting creation timestamps
        strftime_fmt: strftime format string for creation timestamps
        detailed_usernames: Always show the "Login method" column

    Returns:
        HTML formatted string (body content only)
    """
    range_str = (
        f"from {html.escape(start_time.strftime(_DATETIME_FMT))}"
        f" to {html.escape(end_time.strftime(_DATETIME_FMT))}"
    )
    n = len(users)

    if not users:
        html_lines = [
            f"<p>No new users created between "
            f"{html.escape(start_time.strftime(_DATETIME_FMT))} and "
            f"{html.escape(end_time.strftime(_DATETIME_FMT))}.</p>"
        ]
    else:
        noun = "user" if n == 1 else "users"
        html_lines = [f"<p>{n} new {noun} created {range_str}:</p>"]
        parsed = [parse_name(user.get("name", "")) for user in users]
        domain_id_pairs = [(p[1], p[2]) for p in parsed]
        show_method = _show_method(domain_id_pairs, detailed_usernames)

        html_lines.append('<table style="border-collapse:collapse">')
        html_lines.append("  <thead>")
        if show_method:
            html_lines.append(
                f"    <tr>"
                f'<th style="{_TH_STYLE}">Created</th>'
                f'<th style="{_TH_STYLE}">Institution</th>'
                f'<th style="{_TH_STYLE}">ID</th>'
                f'<th style="{_TH_STYLE}">Login method</th>'
                f"</tr>"
            )
        else:
            html_lines.append(
                f"    <tr>"
                f'<th style="{_TH_STYLE}">Created</th>'
                f'<th style="{_TH_STYLE}">Institution</th>'
                f'<th style="{_TH_STYLE}">ID</th>'
                f"</tr>"
            )
        html_lines.append("  </thead>")
        html_lines.append("  <tbody>")

        def sort_key(u: dict) -> tuple:
            created = _format_created(u.get("created", ""), strftime_fmt, tz)
            name = parse_name(u.get("name", ""))
            if show_method:
                return (created, *name[:3])
            return (created, _trailing_domain_key(name[1]), name[1], name[2])

        for i, user in enumerate(sorted(users, key=sort_key)):
            bg = "#e6eff4" if i % 2 else "#ffffff"
            td = f"border:1px solid #9ab3c8; padding:2px 8px; background:{bg}; color:#000000"
            created = html.escape(
                _format_created(user.get("created", ""), strftime_fmt, tz)
            )
            _priority, domain, uid, method = parse_name(user.get("name", ""))
            if show_method:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{td}">{created}</td>'
                    f'<td style="{td}">{html.escape(domain)}</td>'
                    f'<td style="{td}">{html.escape(uid)}</td>'
                    f'<td style="{td}">{html.escape(method)}</td>'
                    f"</tr>"
                )
            else:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{td}">{created}</td>'
                    f'<td style="{td}">{html.escape(domain)}</td>'
                    f'<td style="{td}">{html.escape(uid)}</td>'
                    f"</tr>"
                )
        html_lines.append("  </tbody>")
        html_lines.append("</table>")

    html_lines.append(_html_table_footer(tz_name))
    return "\n".join(html_lines)


def format_users_csv(
    users: list[dict],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    tz: "tzinfo",
    strftime_fmt: str,
    detailed_usernames: bool = False,
) -> str:
    """Format new JupyterHub users as CSV.

    The CSV starts with the data table (header row plus one row per user),
    followed by an empty row, a summary line, and a timezone line.

    Args:
        users: List of user dicts with 'name' and 'created' keys
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footer
        tz: Timezone for converting creation timestamps
        strftime_fmt: strftime format string for creation timestamps
        detailed_usernames: Always show the "Login method" column

    Returns:
        CSV formatted string
    """
    n = len(users)
    buf = io.StringIO()
    writer = csv.writer(buf)

    parsed = [
        (
            _format_created(user.get("created", ""), strftime_fmt, tz),
            *parse_name(user.get("name", "")),
        )
        for user in users
    ]
    domain_id_pairs = [(r[2], r[3]) for r in parsed]
    show_method = _show_method(domain_id_pairs, detailed_usernames)
    if show_method:
        writer.writerow(["Created", "Institution", "ID", "Login method"])
        rows = sorted(parsed, key=lambda r: (r[0], r[1], r[2], r[3]))
        for created, _priority, domain, uid, method in rows:
            writer.writerow([created, domain, uid, method])
    else:
        writer.writerow(["Created", "Institution", "ID"])
        rows = sorted(
            parsed,
            key=lambda r: (r[0], _trailing_domain_key(r[2]), r[2], r[3]),
        )
        for created, _priority, domain, uid, _method in rows:
            writer.writerow([created, domain, uid])

    if n == 0:
        summary = (
            f"No new users created between "
            f"{start_time.strftime(_DATETIME_FMT)} and {end_time.strftime(_DATETIME_FMT)}."
        )
    else:
        noun = "user" if n == 1 else "users"
        summary = (
            f"{n} new {noun} created "
            f"from {start_time.strftime(_DATETIME_FMT)} to {end_time.strftime(_DATETIME_FMT)}"
        )
    _write_csv_footer(writer, summary, tz_name)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Activity formatters (moved from get_new_activity.py)
# ---------------------------------------------------------------------------


def _format_duration(seconds: float) -> str:
    """Format a duration in seconds as HH:MM."""
    total_minutes = int(seconds) // 60
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours}:{minutes:02d}"


def _sorted_activity_rows(
    totals: dict[str, float], show_method: bool
) -> list[tuple[str, float]]:
    """Return (user, seconds) pairs sorted by time descending, then by parsed name."""

    def name_key(user: str) -> tuple:
        name = parse_name(user)
        if show_method:
            return name[:3]
        return (_trailing_domain_key(name[1]), name[1], name[2])

    return sorted(totals.items(), key=lambda item: (-item[1], name_key(item[0])))


def format_activity_text(
    totals: dict[str, float],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    detailed_usernames: bool = False,
) -> str:
    """Format per-user activity as plain text.

    Args:
        totals: Dictionary mapping user name to total active seconds
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footnote
        detailed_usernames: Always show the "Login method" column

    Returns:
        Plain text formatted string with a table
    """
    range_str = (
        f"from {start_time.strftime(_DATETIME_FMT)}"
        f" to {end_time.strftime(_DATETIME_FMT)}"
    )
    n = len(totals)
    if n == 0:
        lines = [
            f"No users were active between "
            f"{start_time.strftime(_DATETIME_FMT)} and {end_time.strftime(_DATETIME_FMT)}."
        ]
    else:
        noun = "user" if n == 1 else "users"
        lines = [f"{n} {noun} were active {range_str}:", ""]
        parsed_names = {user: parse_name(user) for user in totals}
        domain_id_pairs = [(p[1], p[2]) for p in parsed_names.values()]
        show_method = _show_method(domain_id_pairs, detailed_usernames)
        rows = [
            (*parsed_names[user], _format_duration(seconds))
            for user, seconds in _sorted_activity_rows(totals, show_method)
        ]
        domain_width = max(len("Institution"), max(len(r[1]) for r in rows))
        id_width = max(len("ID"), max(len(r[2]) for r in rows))
        time_width = max(len("Time (HH:MM)"), max(len(r[4]) for r in rows))
        if show_method:
            method_width = max(len("Login method"), max(len(r[3]) for r in rows))
            lines.append(
                f"{'Time (HH:MM)':>{time_width}}  {'Institution':<{domain_width}}  "
                f"{'ID':<{id_width}}  {'Login method':<{method_width}}"
            )
            lines.append(
                f"{'-' * time_width}  {'-' * domain_width}  "
                f"{'-' * id_width}  {'-' * method_width}"
            )
            for _priority, domain, uid, method, active_time in rows:
                lines.append(
                    f"{active_time:>{time_width}}  {domain:<{domain_width}}  "
                    f"{uid:<{id_width}}  {method:<{method_width}}"
                )
        else:
            lines.append(
                f"{'Time (HH:MM)':>{time_width}}  {'Institution':<{domain_width}}  "
                f"{'ID':<{id_width}}"
            )
            lines.append(f"{'-' * time_width}  {'-' * domain_width}  {'-' * id_width}")
            for _priority, domain, uid, _method, active_time in rows:
                lines.append(
                    f"{active_time:>{time_width}}  {domain:<{domain_width}}  "
                    f"{uid:<{id_width}}"
                )

    lines.append("")
    lines.append(f"Timezone: {tz_name}")
    return "\n".join(lines)


def format_activity_html(
    totals: dict[str, float],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    detailed_usernames: bool = False,
) -> str:
    """Format per-user activity as HTML suitable for an email body.

    Args:
        totals: Dictionary mapping user name to total active seconds
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footnote
        detailed_usernames: Always show the "Login method" column

    Returns:
        HTML formatted string (body content only)
    """
    range_str = (
        f"from {html.escape(start_time.strftime(_DATETIME_FMT))}"
        f" to {html.escape(end_time.strftime(_DATETIME_FMT))}"
    )
    n = len(totals)

    if not totals:
        html_lines = [
            f"<p>No users were active between "
            f"{html.escape(start_time.strftime(_DATETIME_FMT))} and "
            f"{html.escape(end_time.strftime(_DATETIME_FMT))}.</p>"
        ]
    else:
        noun = "user" if n == 1 else "users"
        html_lines = [f"<p>{n} {noun} were active {range_str}:</p>"]
        parsed_names = {user: parse_name(user) for user in totals}
        domain_id_pairs = [(p[1], p[2]) for p in parsed_names.values()]
        show_method = _show_method(domain_id_pairs, detailed_usernames)

        html_lines.append('<table style="border-collapse:collapse">')
        html_lines.append("  <thead>")
        if show_method:
            html_lines.append(
                f"    <tr>"
                f'<th style="{_TH_STYLE_R}">Time (HH:MM)</th>'
                f'<th style="{_TH_STYLE}">Institution</th>'
                f'<th style="{_TH_STYLE}">ID</th>'
                f'<th style="{_TH_STYLE}">Login method</th>'
                f"</tr>"
            )
        else:
            html_lines.append(
                f"    <tr>"
                f'<th style="{_TH_STYLE_R}">Time (HH:MM)</th>'
                f'<th style="{_TH_STYLE}">Institution</th>'
                f'<th style="{_TH_STYLE}">ID</th>'
                f"</tr>"
            )
        html_lines.append("  </thead>")
        html_lines.append("  <tbody>")
        for i, (user, seconds) in enumerate(_sorted_activity_rows(totals, show_method)):
            bg = "#e6eff4" if i % 2 else "#ffffff"
            td = f"border:1px solid #9ab3c8; padding:2px 8px; background:{bg}; color:#000000"
            td_r = f"text-align:right; border:1px solid #9ab3c8; padding:2px 8px; background:{bg}; color:#000000"
            _priority, domain, uid, method = parsed_names[user]
            active_time = html.escape(_format_duration(seconds))
            if show_method:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{td_r}">{active_time}</td>'
                    f'<td style="{td}">{html.escape(domain)}</td>'
                    f'<td style="{td}">{html.escape(uid)}</td>'
                    f'<td style="{td}">{html.escape(method)}</td>'
                    f"</tr>"
                )
            else:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{td_r}">{active_time}</td>'
                    f'<td style="{td}">{html.escape(domain)}</td>'
                    f'<td style="{td}">{html.escape(uid)}</td>'
                    f"</tr>"
                )
        html_lines.append("  </tbody>")
        html_lines.append("</table>")

    html_lines.append(_html_table_footer(tz_name))
    return "\n".join(html_lines)


def format_activity_csv(
    totals: dict[str, float],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    detailed_usernames: bool = False,
) -> str:
    """Format per-user activity as CSV.

    The CSV starts with the data table (header row plus one row per user),
    followed by an empty row, a summary line, and a timezone line.

    Args:
        totals: Dictionary mapping user name to total active seconds
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footer
        detailed_usernames: Always show the "Login method" column

    Returns:
        CSV formatted string
    """
    n = len(totals)
    buf = io.StringIO()
    writer = csv.writer(buf)

    parsed_names = {user: parse_name(user) for user in totals}
    domain_id_pairs = [(p[1], p[2]) for p in parsed_names.values()]
    show_method = _show_method(domain_id_pairs, detailed_usernames)
    if show_method:
        writer.writerow(["Time (HH:MM)", "Institution", "ID", "Login method"])
        for user, seconds in _sorted_activity_rows(totals, show_method):
            _priority, domain, uid, method = parsed_names[user]
            writer.writerow([_format_duration(seconds), domain, uid, method])
    else:
        writer.writerow(["Time (HH:MM)", "Institution", "ID"])
        for user, seconds in _sorted_activity_rows(totals, show_method):
            _priority, domain, uid, _method = parsed_names[user]
            writer.writerow([_format_duration(seconds), domain, uid])

    if n == 0:
        summary = (
            f"No users were active between "
            f"{start_time.strftime(_DATETIME_FMT)} and {end_time.strftime(_DATETIME_FMT)}."
        )
    else:
        noun = "user" if n == 1 else "users"
        summary = (
            f"{n} {noun} were active "
            f"from {start_time.strftime(_DATETIME_FMT)} to {end_time.strftime(_DATETIME_FMT)}"
        )
    _write_csv_footer(writer, summary, tz_name)
    return buf.getvalue()
