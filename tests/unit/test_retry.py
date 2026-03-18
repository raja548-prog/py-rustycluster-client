"""Unit tests for RetryPolicy and with_retry decorator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import grpc
import pytest

from rustycluster.exceptions import ConnectionError, OperationError, TimeoutError
from rustycluster.retry import RetryPolicy, with_retry


class FakeRpcError(grpc.RpcError):
    """Minimal grpc.RpcError subclass suitable for raising in tests."""
    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code
    def code(self) -> grpc.StatusCode:
        return self._code
    def details(self) -> str:
        return f"simulated {self._code.name}"


def _rpc_error(code: grpc.StatusCode) -> grpc.RpcError:
    return FakeRpcError(code)


class TestWithRetryDecorator:
    def test_succeeds_on_first_attempt(self):
        fn = MagicMock(return_value="ok")
        decorated = with_retry(max_retries=3, backoff_base=0.01, backoff_max=1.0)(fn)
        result = decorated()
        assert result == "ok"
        fn.assert_called_once()

    @patch("time.sleep")
    def test_retries_on_unavailable(self, mock_sleep):
        fn = MagicMock(side_effect=[
            _rpc_error(grpc.StatusCode.UNAVAILABLE),
            _rpc_error(grpc.StatusCode.UNAVAILABLE),
            "success",
        ])
        decorated = with_retry(max_retries=3, backoff_base=0.01, backoff_max=1.0)(fn)
        result = decorated()
        assert result == "success"
        assert fn.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("time.sleep")
    def test_raises_after_max_retries_exhausted(self, mock_sleep):
        err = _rpc_error(grpc.StatusCode.UNAVAILABLE)
        fn = MagicMock(side_effect=err)
        decorated = with_retry(max_retries=2, backoff_base=0.01, backoff_max=1.0)(fn)
        with pytest.raises(ConnectionError):
            decorated()
        assert fn.call_count == 3  # 1 initial + 2 retries

    def test_does_not_retry_invalid_argument(self):
        fn = MagicMock(side_effect=_rpc_error(grpc.StatusCode.INVALID_ARGUMENT))
        decorated = with_retry(max_retries=3, backoff_base=0.01, backoff_max=1.0)(fn)
        with pytest.raises(OperationError):
            decorated()
        fn.assert_called_once()  # No retries

    def test_does_not_retry_not_found(self):
        fn = MagicMock(side_effect=_rpc_error(grpc.StatusCode.NOT_FOUND))
        decorated = with_retry(max_retries=3, backoff_base=0.01, backoff_max=1.0)(fn)
        with pytest.raises(OperationError):
            decorated()
        fn.assert_called_once()

    @patch("time.sleep")
    def test_calls_on_reauth_for_unauthenticated(self, mock_sleep):
        on_reauth = MagicMock()
        fn = MagicMock(side_effect=[
            _rpc_error(grpc.StatusCode.UNAUTHENTICATED),
            "success_after_reauth",
        ])
        decorated = with_retry(
            max_retries=3, backoff_base=0.01, backoff_max=1.0, on_reauth=on_reauth
        )(fn)
        result = decorated()
        assert result == "success_after_reauth"
        on_reauth.assert_called_once()

    @patch("time.sleep")
    def test_exponential_backoff_delays(self, mock_sleep):
        err = _rpc_error(grpc.StatusCode.UNAVAILABLE)
        fn = MagicMock(side_effect=[err, err, err, err])
        decorated = with_retry(max_retries=3, backoff_base=1.0, backoff_max=100.0)(fn)
        with pytest.raises(ConnectionError):
            decorated()
        # Delays should be 1.0, 2.0, 4.0
        delays = [c.args[0] for c in mock_sleep.call_args_list]
        assert delays == [1.0, 2.0, 4.0]

    @patch("time.sleep")
    def test_backoff_capped_at_max(self, mock_sleep):
        err = _rpc_error(grpc.StatusCode.UNAVAILABLE)
        fn = MagicMock(side_effect=[err, err, err, err])
        decorated = with_retry(max_retries=3, backoff_base=1.0, backoff_max=3.0)(fn)
        with pytest.raises(ConnectionError):
            decorated()
        delays = [c.args[0] for c in mock_sleep.call_args_list]
        assert all(d <= 3.0 for d in delays)


class TestRetryPolicy:
    @patch("time.sleep")
    def test_call_succeeds(self, mock_sleep):
        policy = RetryPolicy(max_retries=2, backoff_base=0.01, backoff_max=1.0)
        fn = MagicMock(return_value=42)
        result = policy.call(fn, "arg1", key="val")
        assert result == 42
        fn.assert_called_once_with("arg1", key="val")

    @patch("time.sleep")
    def test_call_retries_and_succeeds(self, mock_sleep):
        policy = RetryPolicy(max_retries=3, backoff_base=0.01, backoff_max=1.0)
        fn = MagicMock(side_effect=[
            _rpc_error(grpc.StatusCode.UNAVAILABLE),
            "ok",
        ])
        result = policy.call(fn)
        assert result == "ok"
