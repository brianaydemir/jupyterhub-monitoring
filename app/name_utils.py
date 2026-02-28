"""Utilities for parsing and sorting JupyterHub usernames."""

import re

_EMAIL_RE = re.compile(r"^email:([^@]+)@(.+)$")
_EPPN_RE = re.compile(r"^([^@]+)@(.+)$")
_LEGACY_EPPN_RE = re.compile(r"^eppn:([^@]+)@(.+)$")
_ORCID_RE = re.compile(r"^orcid:(.+)$")

# Domains listed here sort after all other domains, in the order given.
TRAILING_SORT_DOMAINS: list[str] = ["orcid.org"]


def _trailing_domain_key(domain: str) -> tuple[int, int]:
    """Return a sort key that pushes listed domains to the end."""
    try:
        return (1, TRAILING_SORT_DOMAINS.index(domain))
    except ValueError:
        return (0, 0)


def parse_name(name: str) -> tuple[int, str, str, str]:
    """Parse a JupyterHub username into its components.

    Names are recognised in four forms:
      - ``{id}@{domain}``          — ePPN-style (priority 0)
      - ``eppn:{id}@{domain}``     — legacy ePPN (priority 1)
      - ``email:{id}@{domain}``    — explicit email (priority 2)
      - ``orcid:{id}``             — ORCID (priority 3)
    Unrecognised names are returned with an empty domain (priority 4).

    Args:
        name: The raw JupyterHub username string.

    Returns:
        A four-tuple ``(sort_priority, domain, id, login_method)`` where
        *sort_priority* establishes the ordering between name forms.
    """
    if m := _EMAIL_RE.match(name):
        return (2, m.group(2), m.group(1), "email")
    if m := _LEGACY_EPPN_RE.match(name):
        return (1, m.group(2), m.group(1), "legacy eppn")
    if m := _EPPN_RE.match(name):
        return (0, m.group(2), m.group(1), "local NetID")
    if m := _ORCID_RE.match(name):
        return (3, "orcid.org", m.group(1), "ORCID")
    return (4, "", name, "")
