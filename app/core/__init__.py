"""Core shared utilities and errors.

The error types are re-exported here as the package's public entry point.
Modules inside this project import them from :mod:`app.core.errors` directly;
both paths are supported.
"""

from app.core.errors import AppError, AuthCancelledError, DataShapeError, ExternalServiceError

__all__ = ["AppError", "AuthCancelledError", "DataShapeError", "ExternalServiceError"]
