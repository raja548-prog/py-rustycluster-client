"""
Configuration models for the RustyCluster client.

Two models live here:

* ``RustyClusterConfig``  — per-cluster configuration (nodes, auth, TLS,
  timeouts, retries, etc.). The same shape the client has always had,
  but now scoped to a single cluster.
* ``RustyClusterSettings`` — top-level container that loads a YAML file
  describing one or more named clusters (DB0, DB1, …) plus a shared
  ``defaults`` block. Cluster-level fields override the defaults.

Typical use is via ``rustycluster.get_client(name)``, which loads
``RustyClusterSettings`` from ``rustycluster.yaml`` automatically.

YAML layout::

    rustycluster:
      defaults:
        username: admin
        password: secret
        timeout_seconds: 10.0
      clusters:
        DB0:
          nodes: "localhost:50051,localhost:50052"
        DB1:
          nodes: "localhost:50054,localhost:50055"
          timeout_seconds: 5.0   # per-cluster override
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic import SecretStr


class RustyClusterConfig(BaseModel):
    """
    Per-cluster configuration for the RustyCluster gRPC client.

    Attributes:
        nodes: Comma-separated `host:port` list. The first entry is the
            primary; subsequent entries are tried in order on failover.
        username: Authentication username.
        password: Authentication password (stored as SecretStr).
        use_tls: Whether to use TLS for the gRPC channel.
        tls_ca_cert_path: Path to the CA certificate file (for TLS).
        tls_client_cert_path: Path to client certificate (for mTLS).
        tls_client_key_path: Path to client private key (for mTLS).
        timeout_seconds: Per-RPC timeout in seconds.
        max_retries: Maximum number of retry attempts for transient failures.
        retry_backoff_base: Base delay in seconds for exponential backoff.
        retry_backoff_max: Maximum delay in seconds between retries.
        connection_pool_size: Number of gRPC sub-channels to maintain.
        enable_compression: Whether to enable gRPC compression.
        log_level: Logging level for the rustycluster logger.
    """

    nodes: str = Field(
        default="localhost:50051",
        description="Comma-separated host:port list. First entry is the primary.",
    )
    username: str = Field(default="", description="Authentication username")
    password: SecretStr = Field(default=SecretStr(""), description="Authentication password")
    use_tls: bool = Field(default=False, description="Enable TLS for the channel")
    tls_ca_cert_path: Optional[Path] = Field(default=None, description="Path to CA cert (TLS)")
    tls_client_cert_path: Optional[Path] = Field(default=None, description="Path to client cert (mTLS)")
    tls_client_key_path: Optional[Path] = Field(default=None, description="Path to client key (mTLS)")
    timeout_seconds: float = Field(default=10.0, gt=0, description="Per-RPC timeout in seconds")
    max_retries: int = Field(default=3, ge=0, description="Max retry attempts")
    retry_backoff_base: float = Field(default=0.5, gt=0, description="Base backoff delay (seconds)")
    retry_backoff_max: float = Field(default=30.0, gt=0, description="Max backoff delay (seconds)")
    connection_pool_size: int = Field(default=5, ge=1, description="gRPC channel pool size")
    enable_compression: bool = Field(default=False, description="Enable gRPC compression")
    log_level: str = Field(default="WARNING", description="Logging level (DEBUG, INFO, WARNING, ERROR)")

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in valid:
            raise ValueError(f"log_level must be one of {valid}, got '{v}'")
        return upper

    @field_validator("nodes")
    @classmethod
    def validate_nodes(cls, v: str) -> str:
        entries = [e.strip() for e in v.split(",") if e.strip()]
        if not entries:
            raise ValueError("nodes must contain at least one 'host:port' entry")
        for entry in entries:
            if entry.count(":") != 1:
                raise ValueError(f"node entry '{entry}' must be of the form 'host:port'")
            host, port_str = entry.split(":", 1)
            if not host:
                raise ValueError(f"node entry '{entry}' has an empty host")
            try:
                port = int(port_str)
            except ValueError:
                raise ValueError(f"node entry '{entry}' has a non-integer port") from None
            if not (1 <= port <= 65535):
                raise ValueError(f"node entry '{entry}' has a port out of range 1..65535")
        return ",".join(entries)

    @model_validator(mode="after")
    def validate_tls_paths(self) -> "RustyClusterConfig":
        if self.use_tls:
            if self.tls_ca_cert_path and not self.tls_ca_cert_path.exists():
                raise ValueError(f"tls_ca_cert_path does not exist: {self.tls_ca_cert_path}")
            if self.tls_client_cert_path and not self.tls_client_cert_path.exists():
                raise ValueError(f"tls_client_cert_path does not exist: {self.tls_client_cert_path}")
            if self.tls_client_key_path and not self.tls_client_key_path.exists():
                raise ValueError(f"tls_client_key_path does not exist: {self.tls_client_key_path}")
        return self

    @property
    def node_targets(self) -> list[tuple[str, int]]:
        """Return parsed (host, port) tuples in priority order."""
        result: list[tuple[str, int]] = []
        for entry in self.nodes.split(","):
            entry = entry.strip()
            if not entry:
                continue
            host, port_str = entry.split(":", 1)
            result.append((host, int(port_str)))
        return result

    @property
    def targets(self) -> list[str]:
        """Return 'host:port' strings in priority order."""
        return [f"{h}:{p}" for h, p in self.node_targets]

    @property
    def target(self) -> str:
        """Return the primary gRPC target string (first node)."""
        return self.targets[0]

    @classmethod
    def from_env(cls) -> "RustyClusterConfig":
        """
        Build a single-cluster config from environment variables.

        Multi-cluster setups must use the YAML loader via
        :class:`RustyClusterSettings` — env vars only describe one cluster.

        Environment variables (all prefixed with RUSTYCLUSTER_):
            RUSTYCLUSTER_NODES         (e.g. "localhost:50051,localhost:50052")
            RUSTYCLUSTER_USERNAME
            RUSTYCLUSTER_PASSWORD
            RUSTYCLUSTER_USE_TLS
            RUSTYCLUSTER_TLS_CA_CERT_PATH
            RUSTYCLUSTER_TLS_CLIENT_CERT_PATH
            RUSTYCLUSTER_TLS_CLIENT_KEY_PATH
            RUSTYCLUSTER_TIMEOUT_SECONDS
            RUSTYCLUSTER_MAX_RETRIES
            RUSTYCLUSTER_RETRY_BACKOFF_BASE
            RUSTYCLUSTER_RETRY_BACKOFF_MAX
            RUSTYCLUSTER_CONNECTION_POOL_SIZE
            RUSTYCLUSTER_ENABLE_COMPRESSION
            RUSTYCLUSTER_LOG_LEVEL
        """
        def _get(key: str, default: str = "") -> str:
            return os.environ.get(f"RUSTYCLUSTER_{key}", default)

        kwargs: dict = {
            "nodes": _get("NODES", "localhost:50051"),
            "username": _get("USERNAME", ""),
            "password": _get("PASSWORD", ""),
            "use_tls": _get("USE_TLS", "false").lower() in ("1", "true", "yes"),
            "timeout_seconds": float(_get("TIMEOUT_SECONDS", "10.0")),
            "max_retries": int(_get("MAX_RETRIES", "3")),
            "retry_backoff_base": float(_get("RETRY_BACKOFF_BASE", "0.5")),
            "retry_backoff_max": float(_get("RETRY_BACKOFF_MAX", "30.0")),
            "connection_pool_size": int(_get("CONNECTION_POOL_SIZE", "5")),
            "enable_compression": _get("ENABLE_COMPRESSION", "false").lower() in ("1", "true", "yes"),
            "log_level": _get("LOG_LEVEL", "WARNING"),
        }

        if ca := _get("TLS_CA_CERT_PATH"):
            kwargs["tls_ca_cert_path"] = Path(ca)
        if cert := _get("TLS_CLIENT_CERT_PATH"):
            kwargs["tls_client_cert_path"] = Path(cert)
        if key := _get("TLS_CLIENT_KEY_PATH"):
            kwargs["tls_client_key_path"] = Path(key)

        return cls(**kwargs)

    @classmethod
    def from_dotenv(cls, env_file: str | Path = ".env") -> "RustyClusterConfig":
        """
        Build a single-cluster config from a .env file, then fall back to
        environment variables.
        """
        try:
            from dotenv import load_dotenv
            load_dotenv(dotenv_path=str(env_file), override=False)
        except ImportError as e:
            raise ImportError(
                "python-dotenv is required for from_dotenv(). "
                "Install it with: pip install python-dotenv"
            ) from e
        return cls.from_env()

    def __repr__(self) -> str:
        return (
            f"RustyClusterConfig(nodes={self.nodes!r}, "
            f"username={self.username!r}, use_tls={self.use_tls}, "
            f"timeout_seconds={self.timeout_seconds})"
        )


# Fields on RustyClusterConfig that take a filesystem Path. YAML carries
# them as strings, so we coerce before constructing the model.
_PATH_FIELDS = ("tls_ca_cert_path", "tls_client_cert_path", "tls_client_key_path")


class RustyClusterSettings(BaseModel):
    """
    Top-level multi-cluster settings.

    Loaded from a YAML file with a ``defaults`` block and a ``clusters``
    map. Each cluster inherits ``defaults`` and may override any field.

    Attributes:
        defaults: The raw defaults mapping as it appeared in YAML (kept
            for introspection and re-export; merging has already happened
            into each cluster's ``RustyClusterConfig``).
        clusters: Resolved per-cluster configs, keyed by cluster name.
    """

    defaults: dict[str, Any] = Field(default_factory=dict)
    clusters: dict[str, RustyClusterConfig]

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("clusters")
    @classmethod
    def validate_clusters_non_empty(cls, v: dict[str, RustyClusterConfig]) -> dict[str, RustyClusterConfig]:
        if not v:
            raise ValueError("at least one cluster must be configured")
        for name in v:
            if not name or not name.strip():
                raise ValueError("cluster names must be non-empty strings")
        return v

    def get(self, name: str) -> RustyClusterConfig:
        """Return the config for ``name`` or raise ``ConfigurationError``."""
        from rustycluster.exceptions import ConfigurationError

        if name not in self.clusters:
            raise ConfigurationError(
                f"Unknown cluster '{name}'. Configured: {sorted(self.clusters)}"
            )
        return self.clusters[name]

    @property
    def names(self) -> list[str]:
        """Return the configured cluster names in declaration order."""
        return list(self.clusters)

    @classmethod
    def from_yaml(cls, yaml_file: str | Path = "rustycluster.yaml") -> "RustyClusterSettings":
        """
        Load multi-cluster settings from a YAML file.

        The file must contain a top-level ``rustycluster:`` block with a
        required ``clusters:`` map and an optional ``defaults:`` block.
        For each cluster, fields under ``defaults`` are merged in first
        and then overridden by anything in the cluster's own block.

        Raises:
            ConfigurationError: If the file is missing, malformed, or any
                cluster fails per-field validation.
        """
        from rustycluster.exceptions import ConfigurationError

        try:
            import yaml
        except ImportError as exc:
            raise ImportError(
                "PyYAML is required for from_yaml(). "
                "Install it with: pip install pyyaml"
            ) from exc

        path = Path(yaml_file)
        if not path.exists():
            raise ConfigurationError(f"YAML config file not found: {path.resolve()}")

        try:
            with open(path) as f:
                raw = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            raise ConfigurationError(f"Failed to parse YAML config '{path}': {exc}") from exc

        if not isinstance(raw, dict):
            raise ConfigurationError(
                f"YAML config file '{path}' must contain a mapping at the top level."
            )

        block = raw.get("rustycluster", raw)
        if not isinstance(block, dict):
            raise ConfigurationError(
                f"'rustycluster' in '{path}' must be a mapping, got {type(block).__name__}."
            )

        defaults_raw = block.get("defaults") or {}
        if not isinstance(defaults_raw, dict):
            raise ConfigurationError(
                f"'rustycluster.defaults' in '{path}' must be a mapping, "
                f"got {type(defaults_raw).__name__}."
            )

        clusters_raw = block.get("clusters")
        if clusters_raw is None:
            raise ConfigurationError(
                f"'rustycluster.clusters' is required in '{path}'."
            )
        if not isinstance(clusters_raw, dict):
            raise ConfigurationError(
                f"'rustycluster.clusters' in '{path}' must be a mapping of "
                f"name -> cluster-config, got {type(clusters_raw).__name__}."
            )
        if not clusters_raw:
            raise ConfigurationError(
                f"'rustycluster.clusters' in '{path}' must contain at least one cluster."
            )

        resolved: dict[str, RustyClusterConfig] = {}
        for name, cluster_block in clusters_raw.items():
            if cluster_block is None:
                cluster_block = {}
            if not isinstance(cluster_block, dict):
                raise ConfigurationError(
                    f"Cluster '{name}' in '{path}' must be a mapping, "
                    f"got {type(cluster_block).__name__}."
                )

            merged: dict[str, Any] = {**defaults_raw, **cluster_block}

            for key in _PATH_FIELDS:
                if merged.get(key):
                    merged[key] = Path(merged[key])

            try:
                resolved[str(name)] = RustyClusterConfig(**merged)
            except Exception as exc:
                raise ConfigurationError(
                    f"Invalid configuration for cluster '{name}' in '{path}': {exc}"
                ) from exc

        return cls(defaults=dict(defaults_raw), clusters=resolved)

    def __repr__(self) -> str:
        return f"RustyClusterSettings(clusters={list(self.clusters)!r})"
