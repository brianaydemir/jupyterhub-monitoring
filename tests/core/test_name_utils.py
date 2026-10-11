"""Tests for :mod:`app.core.name_utils`."""

import pytest

from app.core.name_utils import parse_name, trailing_domain_key


@pytest.mark.parametrize(
    ("name", "domain", "uid", "login_method"),
    [
        # The four forms that carry an institution.
        ("user@example.edu", "example.edu", "user", "local NetID"),
        ("eppn:user@example.edu", "example.edu", "user", "legacy eppn"),
        ("email:user@example.edu", "example.edu", "user", "email"),
        (
            "orcid:0009-0008-3064-0494",
            "orcid.org",
            "0009-0008-3064-0494",
            "ORCID",
        ),
        # Anything else keeps the name as the identifier.
        ("service-account", "", "service-account", ""),
        ("", "", "", ""),
    ],
)
def test_parse_name_recognizes_the_documented_forms(
    name: str,
    domain: str,
    uid: str,
    login_method: str,
) -> None:
    """Each documented username form maps to its documented columns."""
    parsed = parse_name(name)
    assert (parsed.domain, parsed.uid, parsed.login_method) == (domain, uid, login_method)


def test_parse_name_prefers_orcid_over_the_bare_eppn_form() -> None:
    """An ORCID value containing "@" is still an ORCID, not a NetID.

    The bare ``{id}@{domain}`` pattern would otherwise claim it and report
    the prefix as part of the identifier.
    """
    parsed = parse_name("orcid:abc@example.org")
    assert (parsed.domain, parsed.uid, parsed.login_method) == (
        "orcid.org",
        "abc@example.org",
        "ORCID",
    )


def test_parse_name_splits_on_the_first_at_sign() -> None:
    """Only the leading label is the identifier; the rest is the domain."""
    parsed = parse_name("a@b@c.edu")
    assert (parsed.uid, parsed.domain) == ("a", "b@c.edu")


def test_parse_name_priorities_order_the_login_methods() -> None:
    """Priorities are distinct and ascend in the documented order."""
    priorities = [
        parse_name(name).priority
        for name in [
            "user@example.edu",
            "eppn:user@example.edu",
            "email:user@example.edu",
            "orcid:0009-0008-3064-0494",
            "service-account",
        ]
    ]
    assert priorities == [0, 1, 2, 3, 4]


def test_trailing_domain_key_sorts_configured_domains_last() -> None:
    """A configured domain sorts after every other domain."""
    domains = ["orcid.org", "example.edu", "zzz.edu"]
    assert sorted(domains, key=trailing_domain_key) == [
        "example.edu",
        "zzz.edu",
        "orcid.org",
    ]


def test_trailing_domain_key_sorts_a_missing_domain_after_even_those() -> None:
    """A name holding no domain is the least informative, so it goes last.

    That is where its :func:`parse_name` priority, the highest one,
    already puts it, so the two keys the reports order on agree.
    """
    domains = ["", "orcid.org", "example.edu"]
    assert sorted(domains, key=trailing_domain_key) == ["example.edu", "orcid.org", ""]
