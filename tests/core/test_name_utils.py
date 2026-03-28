"""Tests for :mod:`app.core.name_utils`."""

import pytest

from app.core.name_utils import TRAILING_SORT_DOMAINS, parse_name, trailing_domain_key


@pytest.mark.parametrize(
    ("name", "domain", "uid", "login_method"),
    [
        # The four forms documented in README's "Username Parsing" table.
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


def test_parse_name_leaves_an_unrecognized_names_domain_empty() -> None:
    """The empty string is the honest domain; the reports mark it as absent.

    :func:`~app.reports.anonymize.build_pseudonyms` keys a pseudonym on
    the ``(domain, uid)`` pair, so moving that display marker down here
    would relabel every unrecognized name.
    """
    assert parse_name("service-account").domain == ""


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


def test_parse_name_prefers_the_prefixed_forms() -> None:
    """A prefix wins over the bare form it would also match."""
    assert parse_name("email:user@example.edu").login_method == "email"
    assert parse_name("eppn:user@example.edu").login_method == "legacy eppn"


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


def test_trailing_domain_key_preserves_the_configured_order() -> None:
    """Among trailing domains, the configured order is the sort order."""
    keys = [trailing_domain_key(d) for d in TRAILING_SORT_DOMAINS]
    assert keys == sorted(keys)
    assert all(key[0] == 1 for key in keys)
