"""Shared exception types raised across the application."""


class AppError(Exception):
    """Base class for expected, user-facing application errors."""


class AuthCancelledError(AppError):
    """Raised when interactive credential entry is cancelled."""


class ExternalServiceError(AppError):
    """Raised when a remote service call fails."""


class DataShapeError(AppError):
    """Raised when data does not match the expected shape or invariants."""
