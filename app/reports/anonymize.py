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

# Words per label. This is fixed rather than sized to the report,
# because a width that tracked the number of users in an institution
# would relabel the same person whenever that count crossed a power of
# 26, breaking the cross-run stability the pseudonyms exist to provide.
LABEL_WIDTH = 3


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


def build_pseudonyms(pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], str]:
    """Map ``(domain, uid)`` identifiers to per-institution pseudonyms.

    Each identifier is assigned a :data:`LABEL_WIDTH`-word NATO-phonetic
    label derived from a stable hash of the ``(domain, uid)`` pair.
    The same user therefore maps to the same label on every run,
    whatever else the report contains and independent of
    ``PYTHONHASHSEED``, and the same *uid* at two institutions maps
    independently.

    Labels are drawn from a fixed space of ``26 ** LABEL_WIDTH``, so two
    users at one institution can collide; by the birthday bound that is
    roughly 1% at 20 users, 7% at 50, and 24% at 100.
    """
    pseudonyms: dict[tuple[str, str], str] = {}
    modulus = _BASE**LABEL_WIDTH
    for domain, uid in pairs:
        digest = hashlib.blake2b(f"{domain}\x00{uid}".encode(), digest_size=8)
        index = int.from_bytes(digest.digest(), "big") % modulus
        pseudonyms[(domain, uid)] = _nato_label(index, LABEL_WIDTH)
    return pseudonyms
