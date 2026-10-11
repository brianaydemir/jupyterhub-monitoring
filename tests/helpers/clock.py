"""A frozen stand-in for :class:`datetime.datetime`, for time-dependent code."""

from dataclasses import dataclass
from datetime import datetime, tzinfo


@dataclass(frozen=True)
class FrozenClock:
    """Answers :meth:`now` with a fixed instant.

    Substituted for the ``datetime`` name inside a module under test,
    which is enough for code that only calls ``datetime.now(tz)``.
    """

    instant: datetime

    def now(self, tz: tzinfo | None = None) -> datetime:
        """Return the fixed instant, converted to *tz* when one is given."""
        if tz is None:
            return self.instant
        return self.instant.astimezone(tz)
