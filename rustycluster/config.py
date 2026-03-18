"""
Configuration model for RustyCluster client.

Supports direct instantiation, environment variables, and .env files.

Example:
    # Direct
    config = RustyClusterConfig(host="localhost", port=50051, username="admin", password="secret")

    # From environment variables
    config = RustyClusterConfig.from_env()

    # From .env file
    config = RustyClusterConfig.from_dotenv("/path/to/.env")
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic import SecretStr


class RustyClusterConfig(BaseModel):
    """
    Configuration for the RustyCluster gRPC client.

    Attributes:
        host: Server hostname or IP address.
        port: Server gRPC port.
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

    host: str = Field(default="localhost", description="RustyCluster server host")
    port: int = Field(default=50051, ge=1, le=65535, description="gRPC port")
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
    def target(self) -> str:
        """Return the gRPC target string (host:port)."""
        return f"{self.host}:{self.port}"

    @classmethod
    def from_env(cls) -> "RustyClusterConfig":
        """
        Build config from environment variables.

        Environment variables (all prefixed with RUSTYCLUSTER_):
            RUSTYCLUSTER_HOST
            RUSTYCLUSTER_PORT
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
            "host": _get("HOST", "localhost"),
            "port": int(_get("PORT", "50051")),
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
    def from_yaml(cls, yaml_file: str | Path = "rustycluster.yaml") -> "RustyClusterConfig":
        """
        Build config from a YAML file.

        Supports nested layout (recommended)::

            rustycluster:
              host: localhost
              port: 50051
              username: admin
              password: secret

        Or flat layout::

            host: localhost
            port: 50051

        Args:
            yaml_file: Path to the YAML config file. Defaults to 'rustycluster.yaml'.

        Raises:
            ConfigurationError: If the file is missing or malformed.
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

        # Support both nested (rustycluster: ...) and flat layout
        data = dict(raw.get("rustycluster", raw))

        # Convert path strings to Path objects
        for key in ("tls_ca_cert_path", "tls_client_cert_path", "tls_client_key_path"):
            if data.get(key):
                data[key] = Path(data[key])

        try:
            return cls(**data)
        except Exception as exc:
            raise ConfigurationError(f"Invalid configuration in '{path}': {exc}") from exc

    @classmethod
    def from_dotenv(cls, env_file: str | Path = ".env") -> "RustyClusterConfig":
        """
        Build config from a .env file, then fall back to environment variables.

        Args:
            env_file: Path to the .env file. Defaults to '.env' in the current directory.
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
            f"RustyClusterConfig(host={self.host!r}, port={self.port}, "
            f"username={self.username!r}, use_tls={self.use_tls}, "
            f"timeout_seconds={self.timeout_seconds})"
        )
