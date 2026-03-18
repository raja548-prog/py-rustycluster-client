"""Unit tests for the exception hierarchy and gRPC error mapping."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from rustycluster.exceptions import (
    AuthenticationError,
    BatchOperationError,
    ConnectionError,
    KeyNotFoundError,
    OperationError,
    RustyClusterError,
    TimeoutError,
    from_grpc_error,
    is_retryable,
    is_reauth_required,
)


class FakeRpcError(grpc.RpcError):
    """Minimal grpc.RpcError subclass suitable for raising in tests."""
    def __init__(self, code: grpc.StatusCode, details: str = "test error") -> None:
        self._code = code
        self._details = details
    def code(self) -> grpc.StatusCode:
        return self._code
    def details(self) -> str:
        return self._details


def _make_rpc_error(code: grpc.StatusCode, details: str = "test error") -> grpc.RpcError:
    return FakeRpcError(code, details)


class TestExceptionHierarchy:
    def test_all_inherit_from_base(self):
        for exc_class in [
            ConnectionError, AuthenticationError, KeyNotFoundError,
            OperationError, TimeoutError, BatchOperationError,
        ]:
            assert issubclass(exc_class, RustyClusterError)

    def test_base_stores_cause(self):
        cause = ValueError("original")
        exc = OperationError("wrapped", cause=cause)
        assert exc.cause is cause

    def test_str_includes_cause(self):
        cause = ValueError("root cause")
        exc = OperationError("outer", cause=cause)
        assert "root cause" in str(exc)

    def test_key_not_found_stores_key(self):
        exc = KeyNotFoundError("my:key")
        assert exc.key == "my:key"
        assert "my:key" in str(exc)

    def test_batch_operation_error_stores_results(self):
        exc = BatchOperationError("batch failed", results=[True, False, True])
        assert exc.results == [True, False, True]


class TestFromGrpcError:
    def test_unauthenticated_maps_to_auth_error(self):
        err = _make_rpc_error(grpc.StatusCode.UNAUTHENTICATED)
        exc = from_grpc_error(err)
        assert isinstance(exc, AuthenticationError)

    def test_unavailable_maps_to_connection_error(self):
        err = _make_rpc_error(grpc.StatusCode.UNAVAILABLE)
        exc = from_grpc_error(err)
        assert isinstance(exc, ConnectionError)

    def test_deadline_exceeded_maps_to_timeout(self):
        err = _make_rpc_error(grpc.StatusCode.DEADLINE_EXCEEDED)
        exc = from_grpc_error(err)
        assert isinstance(exc, TimeoutError)

    def test_internal_maps_to_operation_error(self):
        err = _make_rpc_error(grpc.StatusCode.INTERNAL)
        exc = from_grpc_error(err)
        assert isinstance(exc, OperationError)

    def test_cause_is_preserved(self):
        err = _make_rpc_error(grpc.StatusCode.INTERNAL, "server exploded")
        exc = from_grpc_error(err)
        assert exc.cause is err

    def test_details_in_message(self):
        err = _make_rpc_error(grpc.StatusCode.INTERNAL, "disk full")
        exc = from_grpc_error(err)
        assert "disk full" in str(exc)


class TestIsRetryable:
    @pytest.mark.parametrize("code", [
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
    ])
    def test_retryable_codes(self, code):
        err = _make_rpc_error(code)
        assert is_retryable(err) is True

    @pytest.mark.parametrize("code", [
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.UNIMPLEMENTED,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.ALREADY_EXISTS,
    ])
    def test_non_retryable_codes(self, code):
        err = _make_rpc_error(code)
        assert is_retryable(err) is False

    def test_unauthenticated_is_handled_by_reauth_not_retry(self):
        """UNAUTHENTICATED triggers re-auth flow, not a standard retry."""
        err = _make_rpc_error(grpc.StatusCode.UNAUTHENTICATED)
        assert is_reauth_required(err) is True
        # It IS technically retryable (after reauth), just handled differently
        assert is_retryable(err) is True


class TestIsReauthRequired:
    def test_unauthenticated_requires_reauth(self):
        err = _make_rpc_error(grpc.StatusCode.UNAUTHENTICATED)
        assert is_reauth_required(err) is True

    def test_other_codes_do_not_require_reauth(self):
        err = _make_rpc_error(grpc.StatusCode.INTERNAL)
        assert is_reauth_required(err) is False
