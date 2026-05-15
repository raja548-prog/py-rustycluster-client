"""Unit tests for RustyClusterConfig and RustyClusterSettings."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from rustycluster.config import RustyClusterConfig, RustyClusterSettings
from rustycluster.exceptions import ConfigurationError


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


def _write_yaml(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "rustycluster.yaml"
    p.write_text(textwrap.dedent(body))
    return p


class TestRustyClusterSettings:
    def test_defaults_merge_into_each_cluster(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rustycluster:
              defaults:
                username: shared_user
                password: shared_pass
                timeout_seconds: 7.5
              clusters:
                DB0:
                  nodes: "host-a:50051,host-b:50052"
                DB1:
                  nodes: "host-c:50053"
        """)
        settings = RustyClusterSettings.from_yaml(path)

        assert sorted(settings.clusters) == ["DB0", "DB1"]
        db0 = settings.get("DB0")
        db1 = settings.get("DB1")

        assert db0.username == "shared_user"
        assert db0.password.get_secret_value() == "shared_pass"
        assert db0.timeout_seconds == 7.5
        assert db0.targets == ["host-a:50051", "host-b:50052"]

        assert db1.username == "shared_user"
        assert db1.timeout_seconds == 7.5
        assert db1.targets == ["host-c:50053"]

    def test_cluster_overrides_win(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rustycluster:
              defaults:
                username: shared_user
                timeout_seconds: 10.0
                max_retries: 3
              clusters:
                DB0:
                  nodes: "host-a:50051"
                DB1:
                  nodes: "host-b:50051"
                  timeout_seconds: 2.0
                  username: db1_user
        """)
        settings = RustyClusterSettings.from_yaml(path)

        db0 = settings.get("DB0")
        db1 = settings.get("DB1")

        assert db0.timeout_seconds == 10.0
        assert db0.username == "shared_user"

        assert db1.timeout_seconds == 2.0
        assert db1.username == "db1_user"
        assert db1.max_retries == 3  # inherited

    def test_defaults_optional(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rustycluster:
              clusters:
                only:
                  nodes: "host:50051"
        """)
        settings = RustyClusterSettings.from_yaml(path)
        assert settings.get("only").targets == ["host:50051"]

    def test_missing_clusters_section_raises(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rustycluster:
              defaults:
                username: u
        """)
        with pytest.raises(ConfigurationError, match="clusters"):
            RustyClusterSettings.from_yaml(path)

    def test_empty_clusters_section_raises(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rustycluster:
              clusters: {}
        """)
        with pytest.raises(ConfigurationError, match="at least one cluster"):
            RustyClusterSettings.from_yaml(path)

    def test_cluster_missing_nodes_raises_with_name(self, tmp_path):
        # No defaults.nodes either, so the cluster has none -> validation fails
        # because RustyClusterConfig's default is "localhost:50051"; cover the
        # explicit-empty case instead.
        path = _write_yaml(tmp_path, """
            rustycluster:
              clusters:
                broken:
                  nodes: ""
        """)
        with pytest.raises(ConfigurationError, match="broken"):
            RustyClusterSettings.from_yaml(path)

    def test_unknown_cluster_lookup_raises(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rustycluster:
              clusters:
                DB0:
                  nodes: "host:50051"
        """)
        settings = RustyClusterSettings.from_yaml(path)
        with pytest.raises(ConfigurationError, match="Unknown cluster"):
            settings.get("DB99")

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(ConfigurationError, match="not found"):
            RustyClusterSettings.from_yaml(tmp_path / "does-not-exist.yaml")

    def test_per_cluster_password_is_secret(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rustycluster:
              defaults:
                password: shared_secret
              clusters:
                DB0:
                  nodes: "host:50051"
        """)
        settings = RustyClusterSettings.from_yaml(path)
        assert "shared_secret" not in repr(settings.get("DB0"))

    def test_names_preserve_declaration_order(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rustycluster:
              clusters:
                zeta:
                  nodes: "host:50051"
                alpha:
                  nodes: "host:50052"
                mu:
                  nodes: "host:50053"
        """)
        settings = RustyClusterSettings.from_yaml(path)
        assert settings.names == ["zeta", "alpha", "mu"]

    def test_direct_construction_for_tests(self):
        # Bypassing YAML — used by tests and library embedding.
        settings = RustyClusterSettings(
            clusters={
                "t": RustyClusterConfig(nodes="localhost:50051"),
            },
        )
        assert settings.get("t").target == "localhost:50051"

    def test_direct_construction_rejects_empty_clusters(self):
        with pytest.raises(ValidationError):
            RustyClusterSettings(clusters={})


class TestGetClientResolution:
    """Cover the name-resolution / error paths of `get_client` that run
    *before* any network connection is attempted, so they're safe to test
    without a live server."""

    def test_unknown_name_raises(self):
        from rustycluster import get_client

        settings = RustyClusterSettings(
            clusters={"DB0": RustyClusterConfig(nodes="localhost:50051")},
        )
        with pytest.raises(ConfigurationError, match="Unknown cluster"):
            get_client("nope", settings=settings)

    def test_omitted_name_with_multiple_clusters_raises(self):
        from rustycluster import get_client

        settings = RustyClusterSettings(
            clusters={
                "a": RustyClusterConfig(nodes="localhost:50051"),
                "b": RustyClusterConfig(nodes="localhost:50052"),
            },
        )
        with pytest.raises(ConfigurationError, match="Multiple clusters"):
            get_client(settings=settings)
