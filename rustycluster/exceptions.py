"""
Custom exception hierarchy for RustyCluster client.
All exceptions inherit from RustyClusterError for easy catch-all handling.
"""

from __future__ import annotations

import grpc


class RustyClusterError(Exception):
    """Base exception for all RustyCluster client errors."""

    def __init__(self, message: str, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.cause = cause

    def __str__(self) -> str:
        base = super().__str__()
        if self.cause:
            return f"{base} (caused by: {self.cause})"
        return base


class ConnectionError(RustyClusterError):
    """Raised when the client cannot connect to the RustyCluster server."""
    pass


class AuthenticationError(RustyClusterError):
    """Raised when authentication fails or the session token is invalid."""
    pass


class KeyNotFoundError(RustyClusterError):
    """Raised when a requested key does not exist in the store."""

    def __init__(self, key: str) -> None:
        super().__init__(f"Key not found: '{key}'")
        self.key = key


class OperationError(RustyClusterError):
    """Raised when a store operation fails."""
    pass


class TimeoutError(RustyClusterError):
    """Raised when a request exceeds the configured timeout."""
    pass


class BatchOperationError(RustyClusterError):
    """Raised when one or more operations in a batch write fail."""

    def __init__(self, message: str, results: list[bool] | None = None) -> None:
        super().__init__(message)
        self.results = results or []


class ConfigurationError(RustyClusterError):
    """Raised when the client configuration is invalid."""
    pass


class ScriptError(RustyClusterError):
    """Raised when a Lua script operation fails."""
    pass


# gRPC status code → RustyCluster exception mapping
_GRPC_STATUS_MAP: dict[grpc.StatusCode, type[RustyClusterError]] = {
    grpc.StatusCode.UNAUTHENTICATED: AuthenticationError,
    grpc.StatusCode.PERMISSION_DENIED: AuthenticationError,
    grpc.StatusCode.UNAVAILABLE: ConnectionError,
    grpc.StatusCode.DEADLINE_EXCEEDED: TimeoutError,
    grpc.StatusCode.NOT_FOUND: OperationError,
    grpc.StatusCode.INTERNAL: OperationError,
    grpc.StatusCode.UNKNOWN: OperationError,
    grpc.StatusCode.INVALID_ARGUMENT: OperationError,
    grpc.StatusCode.RESOURCE_EXHAUSTED: OperationError,
    grpc.StatusCode.FAILED_PRECONDITION: OperationError,
    grpc.StatusCode.ABORTED: OperationError,
    grpc.StatusCode.UNIMPLEMENTED: OperationError,
}

# Status codes that should NOT be retried
NON_RETRYABLE_CODES: frozenset[grpc.StatusCode] = frozenset({
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.UNIMPLEMENTED,
    grpc.StatusCode.ALREADY_EXISTS,
})

# Status codes that trigger re-authentication
REAUTH_CODES: frozenset[grpc.StatusCode] = frozenset({
    grpc.StatusCode.UNAUTHENTICATED,
})


def from_grpc_error(error: grpc.RpcError) -> RustyClusterError:
    """Convert a gRPC RpcError into the appropriate RustyClusterError subclass."""
    code = error.code() if hasattr(error, "code") else grpc.StatusCode.UNKNOWN
    details = error.details() if hasattr(error, "details") else str(error)
    exc_class = _GRPC_STATUS_MAP.get(code, OperationError)
    return exc_class(f"[{code.name}] {details}", cause=error)


def is_retryable(error: grpc.RpcError) -> bool:
    """Return True if the gRPC error is considered transient and retryable."""
    code = error.code() if hasattr(error, "code") else grpc.StatusCode.UNKNOWN
    return code not in NON_RETRYABLE_CODES


def is_reauth_required(error: grpc.RpcError) -> bool:
    """Return True if the error indicates the session token has expired."""
    code = error.code() if hasattr(error, "code") else grpc.StatusCode.UNKNOWN
    return code in REAUTH_CODES
