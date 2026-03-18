"""
Retry logic with exponential backoff for RustyCluster gRPC calls.

Retries on transient gRPC errors (UNAVAILABLE, DEADLINE_EXCEEDED).
Does NOT retry on: INVALID_ARGUMENT, UNAUTHENTICATED, NOT_FOUND, etc.
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

import grpc

from rustycluster.exceptions import (
    NON_RETRYABLE_CODES,
    REAUTH_CODES,
    from_grpc_error,
    is_retryable,
)

logger = logging.getLogger("rustycluster.retry")

F = TypeVar("F", bound=Callable[..., Any])


def with_retry(
    max_retries: int,
    backoff_base: float,
    backoff_max: float,
    on_reauth: Callable[[], None] | None = None,
) -> Callable[[F], F]:
    """
    Decorator factory that wraps a gRPC call with retry + exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        backoff_base: Base delay in seconds (doubles each attempt).
        backoff_max: Maximum delay cap in seconds.
        on_reauth: Optional callable invoked when UNAUTHENTICATED is received,
                   before retrying.

    Example:
        @with_retry(max_retries=3, backoff_base=0.5, backoff_max=30.0)
        def call():
            return stub.Get(request, timeout=10)
    """
    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Exception | None = None
            reauthed = False

            for attempt in range(max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except grpc.RpcError as exc:
                    code = exc.code() if hasattr(exc, "code") else grpc.StatusCode.UNKNOWN

                    # Handle re-authentication
                    if code in REAUTH_CODES and on_reauth and not reauthed:
                        logger.warning(
                            "Session token expired, re-authenticating (attempt %d/%d)",
                            attempt + 1,
                            max_retries + 1,
                        )
                        try:
                            on_reauth()
                            reauthed = True
                            continue  # retry immediately after reauth
                        except Exception as reauth_err:
                            logger.error("Re-authentication failed: %s", reauth_err)
                            raise from_grpc_error(exc) from exc

                    # Non-retryable errors: raise immediately
                    if code in NON_RETRYABLE_CODES or not is_retryable(exc):
                        raise from_grpc_error(exc) from exc

                    last_error = exc

                    if attempt < max_retries:
                        delay = min(backoff_base * (2 ** attempt), backoff_max)
                        logger.warning(
                            "Transient error [%s] on attempt %d/%d, retrying in %.2fs: %s",
                            code.name,
                            attempt + 1,
                            max_retries + 1,
                            delay,
                            exc.details() if hasattr(exc, "details") else str(exc),
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            "All %d retry attempts exhausted. Last error: [%s] %s",
                            max_retries + 1,
                            code.name,
                            exc.details() if hasattr(exc, "details") else str(exc),
                        )

            # All retries exhausted
            if last_error is not None:
                raise from_grpc_error(last_error) from last_error  # type: ignore[arg-type]
            raise RuntimeError("Retry loop exited unexpectedly")  # Should never reach here

        return wrapper  # type: ignore[return-value]
    return decorator


class RetryPolicy:
    """
    Encapsulates retry configuration and provides a call() helper.

    Example:
        policy = RetryPolicy(max_retries=3, backoff_base=0.5, backoff_max=30.0)
        result = policy.call(stub.Get, request, timeout=10)
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_base: float = 0.5,
        backoff_max: float = 30.0,
        on_reauth: Callable[[], None] | None = None,
    ) -> None:
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.on_reauth = on_reauth

    def call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute a callable with retry logic applied."""
        decorated = with_retry(
            max_retries=self.max_retries,
            backoff_base=self.backoff_base,
            backoff_max=self.backoff_max,
            on_reauth=self.on_reauth,
        )(fn)
        return decorated(*args, **kwargs)
