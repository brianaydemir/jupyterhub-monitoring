"""Stable, per-institution pseudonyms for de-identifying user IDs."""

import hashlib
from collections.abc import Iterable

# NATO phonetic alphabet, one word per base-26 digit.
NATO_ALPHABET: list[str] = [
    "Alpha",
    "Bravo",
    "Charlie",
    "Delta",
    "Echo",
    "Foxtrot",
    "Golf",
    "Hotel",
    "India",
    "Juliett",
    "Kilo",
    "Lima",
    "Mike",
    "November",
    "Oscar",
    "Papa",
    "Quebec",
    "Romeo",
    "Sierra",
    "Tango",
    "Uniform",
    "Victor",
    "Whiskey",
    "X-ray",
    "Yankee",
    "Zulu",
]

_BASE = len(NATO_ALPHABET)

# Extra label space beyond the number of users in an institution.  Each
# institution's word count is chosen so that its label space is at least
# this multiple of the squared user count; the squared term is the
# birthday bound, so a larger headroom makes a collision within the
# institution less likely.
COLLISION_HEADROOM = 20


def _nato_label(n: int, width: int) -> str:
    """Render *n* as *width* space-joined NATO words (fixed-width base 26).

    *n* is reduced modulo ``26 ** width``, so any non-negative integer
    yields exactly *width* words.
    """
    digits: list[str] = []
    for _ in range(width):
        n, rem = divmod(n, _BASE)
        digits.append(NATO_ALPHABET[rem])
    return " ".join(reversed(digits))


def _label_width(count: int) -> int:
    """Return how many NATO words an institution of *count* users needs.

    The width is the smallest positive integer whose base-26 label space
    covers *count* users with :data:`COLLISION_HEADROOM` to spare, per
    the birthday bound.
    """
    needed = COLLISION_HEADROOM * max(1, count) ** 2
    width = 1
    while _BASE**width < needed:
        width += 1
    return width


def build_pseudonyms(pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], str]:
    """Map ``(domain, uid)`` identifiers to per-institution pseudonyms.

    Each distinct *uid* within a *domain* is assigned a NATO-phonetic
    label derived from a stable hash of the identifier, so the same user
    always maps to the same label across runs (independent of
    ``PYTHONHASHSEED``) and the same *uid* at different institutions maps
    independently.  The label's word count grows with the number of users
    in the institution to keep collisions rare; collisions remain
    possible but unlikely.  Duplicate input pairs collapse to one entry.
    """
    uids_by_domain: dict[str, set[str]] = {}
    for domain, uid in pairs:
        uids_by_domain.setdefault(domain, set()).add(uid)

    pseudonyms: dict[tuple[str, str], str] = {}
    for domain, uids in uids_by_domain.items():
        width = _label_width(len(uids))
        modulus = _BASE**width
        for uid in uids:
            digest = hashlib.blake2b(f"{domain}\x00{uid}".encode(), digest_size=8)
            index = int.from_bytes(digest.digest(), "big") % modulus
            pseudonyms[(domain, uid)] = _nato_label(index, width)

    return pseudonyms
