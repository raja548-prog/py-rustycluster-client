"""
Retry + failover logic for RustyCluster gRPC calls.

Per RPC, the policy:
  1. Tries the call on the current node.
  2. Retries on transient gRPC errors (UNAVAILABLE, DEADLINE_EXCEEDED).
  3. Re-authenticates once on UNAUTHENTICATED, then retries.
  4. When retries on the current node are exhausted *and* the error is
     transient, invokes `on_node_failure()` (which advances the
     NodeManager to the next node). The retry loop then restarts on the
     new node.
  5. When `on_node_failure()` raises (no more nodes), the last gRPC
     error is converted via `from_grpc_error` and re-raised.

Non-retryable codes (INVALID_ARGUMENT, NOT_FOUND, etc.) are raised
immediately without triggering failover.
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
    stub_provider: Callable[[], Any] | None = None,
    on_node_failure: Callable[[], None] | None = None,
) -> Callable[[F], F]:
    """
    Decorator factory that wraps a gRPC call with retry, reauth, and failover.

    The wrapped callable is expected to be a bound method on a gRPC stub
    (e.g. `stub.Set`). The decorator extracts the method name via
    `fn.__name__` and, when a `stub_provider` is given, rebinds the call
    to `getattr(stub_provider(), method_name)` on every attempt. This is
    what lets retries automatically target the new stub after failover.

    Args:
        max_retries: Maximum retry attempts on a single node.
        backoff_base: Base delay in seconds (doubles each attempt).
        backoff_max: Maximum delay cap in seconds.
        on_reauth: Optional callable invoked when UNAUTHENTICATED is received.
        stub_provider: Optional zero-arg callable returning the current stub.
        on_node_failure: Optional zero-arg callable invoked when retries are
            exhausted with a transient error. Must raise (e.g.
            `ConnectionError`) when no more nodes are available.
    """
    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            method_name = getattr(fn, "__name__", None)
            last_error: grpc.RpcError | None = None

            while True:
                reauthed = False

                for attempt in range(max_retries + 1):
                    if stub_provider is not None and method_name:
                        current_fn = getattr(stub_provider(), method_name)
                    else:
                        current_fn = fn

                    try:
                        return current_fn(*args, **kwargs)
                    except grpc.RpcError as exc:
                        code = exc.code() if hasattr(exc, "code") else grpc.StatusCode.UNKNOWN

                        # Re-auth on UNAUTHENTICATED (one shot per node)
                        if code in REAUTH_CODES and on_reauth and not reauthed:
                            logger.warning(
                                "Session token expired, re-authenticating (attempt %d/%d)",
                                attempt + 1,
                                max_retries + 1,
                            )
                            try:
                                on_reauth()
                                reauthed = True
                                continue
                            except Exception as reauth_err:
                                logger.error("Re-authentication failed: %s", reauth_err)
                                raise from_grpc_error(exc) from exc

                        # Non-retryable: raise immediately, no failover
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
                                "All %d retry attempts exhausted on this node. Last error: [%s] %s",
                                max_retries + 1,
                                code.name,
                                exc.details() if hasattr(exc, "details") else str(exc),
                            )

                # Inner loop exhausted on a transient error. Try failover.
                if on_node_failure is not None:
                    try:
                        on_node_failure()
                    except Exception as failover_err:
                        logger.error("Failover unavailable: %s", failover_err)
                        # Propagate the failover error (e.g. ConnectionError "All nodes exhausted")
                        if last_error is not None:
                            raise failover_err from last_error
                        raise
                    # Failover succeeded — restart retry loop on the new node
                    continue

                # No failover hook: just raise the converted last error
                if last_error is not None:
                    raise from_grpc_error(last_error) from last_error
                raise RuntimeError("Retry loop exited unexpectedly")

        return wrapper  # type: ignore[return-value]
    return decorator


class RetryPolicy:
    """
    Encapsulates retry + failover configuration with a `call()` helper.

    Example:
        policy = RetryPolicy(
            max_retries=3, backoff_base=0.5, backoff_max=30.0,
            on_reauth=auth.authenticate,
            stub_provider=lambda: manager.stub,
            on_node_failure=manager.failover,
        )
        result = policy.call(stub.Get, request, timeout=10)
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_base: float = 0.5,
        backoff_max: float = 30.0,
        on_reauth: Callable[[], None] | None = None,
        stub_provider: Callable[[], Any] | None = None,
        on_node_failure: Callable[[], None] | None = None,
    ) -> None:
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.on_reauth = on_reauth
        self.stub_provider = stub_provider
        self.on_node_failure = on_node_failure

    def call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute a callable with retry + failover logic applied."""
        decorated = with_retry(
            max_retries=self.max_retries,
            backoff_base=self.backoff_base,
            backoff_max=self.backoff_max,
            on_reauth=self.on_reauth,
            stub_provider=self.stub_provider,
            on_node_failure=self.on_node_failure,
        )(fn)
        return decorated(*args, **kwargs)
