"""
gRPC client interceptors for RustyCluster.

Provides:
- AuthInterceptor: Injects Bearer session token into every outgoing RPC call.
- LoggingInterceptor: Logs method names, latency, and errors (never values).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

import grpc

logger = logging.getLogger("rustycluster.interceptors")


class _AuthCallCredentials(grpc.AuthMetadataPlugin):
    """gRPC auth metadata plugin that injects the Bearer token."""

    def __init__(self, token_provider: Callable[[], str]) -> None:
        self._token_provider = token_provider

    def __call__(
        self,
        context: grpc.AuthMetadataContext,
        callback: grpc.AuthMetadataPluginCallback,
    ) -> None:
        token = self._token_provider()
        if token:
            callback([("authorization", f"Bearer {token}")], None)
        else:
            callback([], None)


class AuthInterceptor(grpc.UnaryUnaryClientInterceptor):
    """
    Injects the current session token as a Bearer Authorization header.

    The token is retrieved lazily via token_provider() on each call,
    so token rotation is automatically picked up.

    Args:
        token_provider: A zero-argument callable returning the current session token.
    """

    def __init__(self, token_provider: Callable[[], str]) -> None:
        self._token_provider = token_provider

    def intercept_unary_unary(
        self,
        continuation: Callable,
        client_call_details: grpc.ClientCallDetails,
        request: Any,
    ) -> Any:
        token = self._token_provider()
        updated_details = _ClientCallDetailsWithMetadata(
            client_call_details, token
        )
        return continuation(updated_details, request)


class LoggingInterceptor(grpc.UnaryUnaryClientInterceptor):
    """
    Logs every outgoing RPC call: method name, outcome, and latency.
    Never logs key values to avoid leaking sensitive data.
    """

    def intercept_unary_unary(
        self,
        continuation: Callable,
        client_call_details: grpc.ClientCallDetails,
        request: Any,
    ) -> Any:
        method = client_call_details.method or "unknown"
        start = time.perf_counter()

        try:
            response = continuation(client_call_details, request)
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.debug("RPC %s succeeded in %.2fms", method, elapsed_ms)
            return response
        except grpc.RpcError as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            code = exc.code() if hasattr(exc, "code") else "UNKNOWN"
            logger.error(
                "RPC %s failed [%s] in %.2fms: %s",
                method,
                code,
                elapsed_ms,
                exc.details() if hasattr(exc, "details") else str(exc),
            )
            raise


class _ClientCallDetailsWithMetadata(grpc.ClientCallDetails):
    """Helper that augments ClientCallDetails with additional metadata."""

    def __init__(
        self,
        base: grpc.ClientCallDetails,
        token: str,
    ) -> None:
        existing = list(base.metadata or [])
        if token:
            existing.append(("authorization", f"Bearer {token}"))
        self.method = base.method
        self.timeout = base.timeout
        self.metadata = existing
        self.credentials = base.credentials
        self.wait_for_ready = base.wait_for_ready
        self.compression = base.compression


def build_interceptors(token_provider: Callable[[], str]) -> list:
    """
    Build the default interceptor chain for a RustyCluster channel.

    Args:
        token_provider: Callable returning the current session token.

    Returns:
        List of interceptors to pass to grpc.intercept_channel().
    """
    return [
        LoggingInterceptor(),
        AuthInterceptor(token_provider),
    ]
