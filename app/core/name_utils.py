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
    """Return a key that pushes configured trailing domains to the end.

    A name carrying no domain sorts after even those,
    which is where :attr:`ParsedName.priority` already puts it:
    an unrecognized name takes the highest priority.
    Ordering on either value therefore leaves such a name in the same place,
    at the end.
    """
    if not domain:
        return (2, 0)
    try:
        return (1, TRAILING_SORT_DOMAINS.index(domain))
    except ValueError:
        return (0, 0)


def parse_name(name: str) -> ParsedName:
    """Parse a JupyterHub username into its components.

    Names are recognized in four forms:
      - ``{id}@{domain}``          — ePPN-style (priority 0)
      - ``eppn:{id}@{domain}``     — legacy ePPN (priority 1)
      - ``email:{id}@{domain}``    — explicit email (priority 2)
      - ``orcid:{id}``             — ORCID (priority 3)

    The prefixed forms are matched first,
    so a bare ``{id}@{domain}`` name is only assumed
    once the others have been ruled out.

    Returns:
        The parsed components.
        An unrecognized name yields priority 4,
        an empty *domain* and *login_method*, and the whole name as *uid*.
    """
    if m := _EMAIL_RE.match(name):
        return ParsedName(priority=2, domain=m.group(2), uid=m.group(1), login_method="email")
    if m := _LEGACY_EPPN_RE.match(name):
        return ParsedName(
            priority=1, domain=m.group(2), uid=m.group(1), login_method="legacy eppn"
        )
    # ORCID is matched before the bare ePPN form,
    # whose pattern would otherwise claim any ORCID value
    # that happens to contain an "@".
    if m := _ORCID_RE.match(name):
        return ParsedName(priority=3, domain="orcid.org", uid=m.group(1), login_method="ORCID")
    if m := _EPPN_RE.match(name):
        return ParsedName(
            priority=0, domain=m.group(2), uid=m.group(1), login_method="local NetID"
        )
    return ParsedName(priority=4, domain="", uid=name, login_method="")
