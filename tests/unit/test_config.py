"""Unit tests for RustyClusterConfig."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from rustycluster.config import RustyClusterConfig


class TestRustyClusterConfig:
    def test_defaults(self):
        config = RustyClusterConfig()
        assert config.nodes == "localhost:50051"
        assert config.node_targets == [("localhost", 50051)]
        assert config.target == "localhost:50051"
        assert config.use_tls is False
        assert config.timeout_seconds == 10.0
        assert config.max_retries == 3
        assert config.log_level == "WARNING"

    def test_target(self):
        config = RustyClusterConfig(nodes="myserver:9090")
        assert config.target == "myserver:9090"

    def test_multiple_nodes_parsed_in_order(self):
        config = RustyClusterConfig(
            nodes="primary:50051,secondary:50052,tertiary:50053"
        )
        assert config.node_targets == [
            ("primary", 50051),
            ("secondary", 50052),
            ("tertiary", 50053),
        ]
        assert config.targets == ["primary:50051", "secondary:50052", "tertiary:50053"]
        assert config.target == "primary:50051"

    def test_nodes_with_whitespace_is_trimmed(self):
        config = RustyClusterConfig(nodes=" host1:1 , host2:2 ")
        assert config.targets == ["host1:1", "host2:2"]

    def test_password_is_secret(self):
        config = RustyClusterConfig(password="supersecret")
        # Should not expose raw password in repr
        assert "supersecret" not in repr(config)
        # But get_secret_value() should work
        assert config.password.get_secret_value() == "supersecret"

    def test_invalid_port_too_high(self):
        with pytest.raises(ValidationError):
            RustyClusterConfig(nodes="localhost:99999")

    def test_invalid_port_zero(self):
        with pytest.raises(ValidationError):
            RustyClusterConfig(nodes="localhost:0")

    def test_invalid_node_missing_port(self):
        with pytest.raises(ValidationError):
            RustyClusterConfig(nodes="localhost")

    def test_invalid_node_empty(self):
        with pytest.raises(ValidationError):
            RustyClusterConfig(nodes="")

    def test_invalid_node_non_integer_port(self):
        with pytest.raises(ValidationError):
            RustyClusterConfig(nodes="localhost:abc")

    def test_invalid_log_level(self):
        with pytest.raises(ValidationError):
            RustyClusterConfig(log_level="VERBOSE")

    def test_log_level_case_insensitive(self):
        config = RustyClusterConfig(log_level="debug")
        assert config.log_level == "DEBUG"

    def test_from_env(self):
        env = {
            "RUSTYCLUSTER_NODES": "prod-a:50052,prod-b:50053",
            "RUSTYCLUSTER_USERNAME": "user1",
            "RUSTYCLUSTER_PASSWORD": "pass1",
            "RUSTYCLUSTER_USE_TLS": "true",
            "RUSTYCLUSTER_TIMEOUT_SECONDS": "20.0",
            "RUSTYCLUSTER_MAX_RETRIES": "5",
            "RUSTYCLUSTER_LOG_LEVEL": "INFO",
        }
        with patch.dict(os.environ, env, clear=False):
            config = RustyClusterConfig.from_env()

        assert config.nodes == "prod-a:50052,prod-b:50053"
        assert config.target == "prod-a:50052"
        assert config.username == "user1"
        assert config.password.get_secret_value() == "pass1"
        assert config.use_tls is True
        assert config.timeout_seconds == 20.0
        assert config.max_retries == 5
        assert config.log_level == "INFO"

    def test_from_env_defaults_when_not_set(self):
        # Ensure no RUSTYCLUSTER_ vars are set
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("RUSTYCLUSTER_")}
        with patch.dict(os.environ, clean_env, clear=True):
            config = RustyClusterConfig.from_env()
        assert config.nodes == "localhost:50051"
        assert config.target == "localhost:50051"

    def test_tls_path_validation_nonexistent(self, tmp_path):
        with pytest.raises(ValidationError, match="tls_ca_cert_path does not exist"):
            RustyClusterConfig(
                use_tls=True,
                tls_ca_cert_path=Path("/nonexistent/ca.crt"),
            )

    def test_tls_path_validation_valid(self, tmp_path):
        ca_cert = tmp_path / "ca.crt"
        ca_cert.write_text("fake cert")
        # Should not raise
        config = RustyClusterConfig(use_tls=True, tls_ca_cert_path=ca_cert)
        assert config.tls_ca_cert_path == ca_cert

    def test_repr_does_not_leak_password(self):
        config = RustyClusterConfig(username="admin", password="topsecret")
        r = repr(config)
        assert "topsecret" not in r
        assert "admin" in r
