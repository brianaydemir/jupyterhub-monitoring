"""Utilities for parsing and sorting JupyterHub usernames."""

import re
from dataclasses import dataclass

_EMAIL_RE = re.compile(r"^email:([^@]+)@(.+)$")
_EPPN_RE = re.compile(r"^([^@]+)@(.+)$")
_LEGACY_EPPN_RE = re.compile(r"^eppn:([^@]+)@(.+)$")
_ORCID_RE = re.compile(r"^orcid:(.+)$")

# Domains listed here sort after all other domains, in the order given.
TRAILING_SORT_DOMAINS: list[str] = ["orcid.org"]


@dataclass(frozen=True)
class ParsedName:
    """Parsed components of a JupyterHub username."""

    priority: int
    domain: str
    uid: str
    login_method: str


def trailing_domain_key(domain: str) -> tuple[int, int]:
    """Return a key that pushes configured trailing domains to the end."""
    try:
        return (1, TRAILING_SORT_DOMAINS.index(domain))
    except ValueError:
        return (0, 0)


def parse_name(name: str) -> ParsedName:
    """Parse a JupyterHub username into its components.

    Names are recognised in four forms:
      - ``{id}@{domain}``          — ePPN-style (priority 0)
      - ``eppn:{id}@{domain}``     — legacy ePPN (priority 1)
      - ``email:{id}@{domain}``    — explicit email (priority 2)
      - ``orcid:{id}``             — ORCID (priority 3)

    Unrecognised names are returned with an empty domain (priority 4).

    Returns a :class:`ParsedName` with ``priority``, ``domain``, ``uid``,
    and ``login_method``.
    """
    if m := _EMAIL_RE.match(name):
        return ParsedName(priority=2, domain=m.group(2), uid=m.group(1), login_method="email")
    if m := _LEGACY_EPPN_RE.match(name):
        return ParsedName(
            priority=1, domain=m.group(2), uid=m.group(1), login_method="legacy eppn"
        )
    if m := _EPPN_RE.match(name):
        return ParsedName(
            priority=0, domain=m.group(2), uid=m.group(1), login_method="local NetID"
        )
    if m := _ORCID_RE.match(name):
        return ParsedName(priority=3, domain="orcid.org", uid=m.group(1), login_method="ORCID")
    return ParsedName(priority=4, domain="", uid=name, login_method="")
