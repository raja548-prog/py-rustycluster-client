"""
RustyCluster Python Client
==========================

Enterprise-grade Python gRPC client for the RustyCluster distributed key-value store.

Multi-cluster setup is described in a YAML file (``rustycluster.yaml`` by
default). Each cluster is named and may override any field from a shared
``defaults`` block::

    rustycluster:
      defaults:
        username: admin
        password: secret
      clusters:
        DB0:
          nodes: "localhost:50051,localhost:50052"
        DB1:
          nodes: "localhost:50054,localhost:50055"
          timeout_seconds: 5.0     # per-cluster override

Quickstart::

    from rustycluster import get_client, close_all

    db0 = get_client("DB0")           # builds (or returns cached) DB0 client
    db0.set("hello", "world")
    print(db0.get("hello"))           # "world"

    db1 = get_client("DB1")
    db1.hset("user:1", "name", "Alice")

    close_all()                       # tear down every cached client

For tests or library embedding, pass an explicit ``RustyClusterSettings``
to bypass YAML auto-discovery and the global cache::

    from rustycluster import (
        get_client, RustyClusterConfig, RustyClusterSettings,
    )

    settings = RustyClusterSettings(
        clusters={"test": RustyClusterConfig(nodes="localhost:50051")},
    )
    client = get_client("test", settings=settings)

Async clients have the same shape via ``async_get_client(name)`` and
``async_close_all()``.
"""

from __future__ import annotations

import logging
from typing import Optional

import grpc

from rustycluster.async_client import AsyncRustyClusterClient
from rustycluster.auth import AuthManager
from rustycluster.batch import BatchOperationBuilder
from rustycluster.client import RustyClusterClient
from rustycluster.config import RustyClusterConfig, RustyClusterSettings
from rustycluster.exceptions import (
    AuthenticationError,
    BatchOperationError,
    ConfigurationError,
    ConnectionError,
    KeyNotFoundError,
    OperationError,
    RustyClusterError,
    ScriptError,
    TimeoutError,
)
from rustycluster.failover import NodeManager
from rustycluster.interceptors import build_interceptors
from rustycluster.proto import rustycluster_pb2, rustycluster_pb2_grpc

__version__ = "1.1.0"
__author__ = "RustyCluster"
__all__ = [
    # Factory functions
    "get_client",
    "async_get_client",
    "close_all",
    "async_close_all",
    # Client classes
    "RustyClusterClient",
    "AsyncRustyClusterClient",
    # Config
    "RustyClusterConfig",
    "RustyClusterSettings",
    # Batch builder
    "BatchOperationBuilder",
    # Exceptions
    "RustyClusterError",
    "ConnectionError",
    "AuthenticationError",
    "KeyNotFoundError",
    "OperationError",
    "TimeoutError",
    "BatchOperationError",
    "ConfigurationError",
    "ScriptError",
]


def _build_channel(target: str, config: RustyClusterConfig) -> grpc.Channel:
    """Build a gRPC channel (plain or TLS) for the given target."""
    options = [
        ("grpc.max_send_message_length", 100 * 1024 * 1024),
        ("grpc.max_receive_message_length", 100 * 1024 * 1024),
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
    ]

    if config.use_tls:
        root_certs: Optional[bytes] = None
        client_cert: Optional[bytes] = None
        client_key: Optional[bytes] = None

        if config.tls_ca_cert_path:
            root_certs = config.tls_ca_cert_path.read_bytes()
        if config.tls_client_cert_path:
            client_cert = config.tls_client_cert_path.read_bytes()
        if config.tls_client_key_path:
            client_key = config.tls_client_key_path.read_bytes()

        credentials = grpc.ssl_channel_credentials(
            root_certificates=root_certs,
            private_key=client_key,
            certificate_chain=client_cert,
        )
        return grpc.secure_channel(target, credentials, options=options)

    return grpc.insecure_channel(target, options=options)


def _build_async_channel(target: str, config: RustyClusterConfig) -> "grpc.aio.Channel":
    """Build an async gRPC channel (plain or TLS) for the given target."""
    import grpc.aio

    options = [
        ("grpc.max_send_message_length", 100 * 1024 * 1024),
        ("grpc.max_receive_message_length", 100 * 1024 * 1024),
    ]

    if config.use_tls:
        root_certs = config.tls_ca_cert_path.read_bytes() if config.tls_ca_cert_path else None
        client_cert = config.tls_client_cert_path.read_bytes() if config.tls_client_cert_path else None
        client_key = config.tls_client_key_path.read_bytes() if config.tls_client_key_path else None
        credentials = grpc.ssl_channel_credentials(root_certs, client_key, client_cert)
        return grpc.aio.secure_channel(target, credentials, options=options)

    return grpc.aio.insecure_channel(target, options=options)


# YAML config file search order
_YAML_SEARCH_PATHS = [
    "rustycluster.yaml",
    "config/rustycluster.yaml",
    "~/.rustycluster.yaml",
]


# ---------------------------------------------------------------------------
# Settings discovery + client cache
# ---------------------------------------------------------------------------
#
# Caching only applies to the auto-discovered (ambient) settings path. When
# the caller passes ``settings=`` explicitly we build a fresh client each
# time and leave lifecycle ownership to the caller — this keeps tests and
# embedded use-cases isolated from one another.

_settings_cache: Optional[RustyClusterSettings] = None
_client_cache: dict[str, RustyClusterClient] = {}
_async_client_cache: dict[str, AsyncRustyClusterClient] = {}


def _auto_discover_settings() -> RustyClusterSettings:
    """Load ``RustyClusterSettings`` from the first YAML found in the search path."""
    from pathlib import Path

    for candidate in _YAML_SEARCH_PATHS:
        path = Path(candidate).expanduser()
        if path.exists():
            logging.getLogger("rustycluster").debug(
                "Auto-discovered config: %s", path.resolve()
            )
            return RustyClusterSettings.from_yaml(path)

    raise ConfigurationError(
        "No rustycluster.yaml found. Searched: "
        + ", ".join(_YAML_SEARCH_PATHS)
        + ". Pass an explicit `settings=RustyClusterSettings(...)` or create a YAML file."
    )


def _resolve_name(name: Optional[str], settings: RustyClusterSettings) -> str:
    """Pick the cluster name. If unset, the settings must hold exactly one cluster."""
    if name is not None:
        return name
    names = settings.names
    if len(names) == 1:
        return names[0]
    raise ConfigurationError(
        f"Multiple clusters configured ({names}); pass a name to get_client(...)."
    )


def _apply_log_level(config: RustyClusterConfig) -> None:
    logging.getLogger("rustycluster").setLevel(
        getattr(logging, config.log_level, logging.WARNING)
    )


def get_client(
    name: Optional[str] = None,
    *,
    settings: Optional[RustyClusterSettings] = None,
) -> RustyClusterClient:
    """
    Return a connected, authenticated client for the named cluster.

    When ``settings`` is omitted the YAML is auto-discovered and cached,
    and the resulting client is cached by name — subsequent calls with the
    same name return the same instance. When ``settings`` is passed, every
    call builds a fresh client and the caller owns the lifecycle.

    Args:
        name: The cluster name as it appears under ``clusters:`` in YAML.
            May be omitted only when exactly one cluster is configured.
        settings: Explicit settings instead of YAML auto-discovery.

    Raises:
        ConfigurationError: If ``name`` is unknown, no cluster is
            configured, or no YAML is found and ``settings`` was not
            provided.
        AuthenticationError: If credentials are rejected by the server.
        ConnectionError: If the cluster is unreachable.
    """
    global _settings_cache

    if settings is None:
        if _settings_cache is None:
            _settings_cache = _auto_discover_settings()
        active = _settings_cache
        use_cache = True
    else:
        active = settings
        use_cache = False

    resolved_name = _resolve_name(name, active)

    if use_cache and resolved_name in _client_cache:
        return _client_cache[resolved_name]

    config = active.get(resolved_name)
    _apply_log_level(config)
    logger = logging.getLogger("rustycluster")

    manager = NodeManager(config=config, build_channel=_build_channel)

    logger.info(
        "RustyCluster client '%s' connected to %s (TLS=%s, failover_nodes=%d)",
        resolved_name,
        manager.current_target,
        config.use_tls,
        len(config.targets),
    )

    client = RustyClusterClient(manager=manager, config=config)
    if use_cache:
        _client_cache[resolved_name] = client
    return client


def close_all() -> None:
    """
    Close every cached sync client. Safe to call multiple times.

    Only affects clients created via the auto-discovered settings path;
    clients built with an explicit ``settings=`` argument are not tracked
    here and must be closed by the caller.
    """
    global _settings_cache

    for cluster_name, client in list(_client_cache.items()):
        try:
            client.close()
        except Exception as exc:
            logging.getLogger("rustycluster").warning(
                "Error closing client for cluster '%s': %s", cluster_name, exc
            )
    _client_cache.clear()
    _settings_cache = None


async def async_get_client(
    name: Optional[str] = None,
    *,
    settings: Optional[RustyClusterSettings] = None,
) -> AsyncRustyClusterClient:
    """
    Async counterpart of :func:`get_client`.

    Same caching rules: auto-discovered settings produce cached clients
    keyed by name; explicit ``settings=`` always builds a fresh client.
    """
    global _settings_cache

    import grpc.aio
    from rustycluster.failover import AsyncNodeManager

    if settings is None:
        if _settings_cache is None:
            _settings_cache = _auto_discover_settings()
        active = _settings_cache
        use_cache = True
    else:
        active = settings
        use_cache = False

    resolved_name = _resolve_name(name, active)

    if use_cache and resolved_name in _async_client_cache:
        return _async_client_cache[resolved_name]

    config = active.get(resolved_name)
    _apply_log_level(config)

    manager = AsyncNodeManager(config=config, build_channel=_build_async_channel)
    await manager.connect()

    logging.getLogger("rustycluster").info(
        "Async RustyCluster client '%s' connected to %s (TLS=%s, failover_nodes=%d)",
        resolved_name,
        manager.current_target,
        config.use_tls,
        len(config.targets),
    )

    client = AsyncRustyClusterClient(manager=manager, config=config)
    if use_cache:
        _async_client_cache[resolved_name] = client
    return client


async def async_close_all() -> None:
    """Close every cached async client. Safe to call multiple times."""
    global _settings_cache

    for cluster_name, client in list(_async_client_cache.items()):
        try:
            await client.close()
        except Exception as exc:
            logging.getLogger("rustycluster").warning(
                "Error closing async client for cluster '%s': %s", cluster_name, exc
            )
    _async_client_cache.clear()
    if not _client_cache:
        _settings_cache = None
