"""
RustyCluster Python Client
==========================

Enterprise-grade Python gRPC client for the RustyCluster distributed key-value store.

Quickstart:
    from rustycluster import get_client, RustyClusterConfig

    config = RustyClusterConfig(
        nodes="localhost:50051,localhost:50052",
        username="admin",
        password="secret",
    )

    # Direct usage
    client = get_client(config)
    client.set("hello", "world")
    print(client.get("hello"))  # "world"
    client.close()

    # Context manager (recommended)
    with get_client(config) as client:
        client.hset("user:1", "name", "Alice")

    # From environment variables
    client = get_client()  # reads RUSTYCLUSTER_* env vars

    # Async client
    import asyncio
    from rustycluster import async_get_client

    async def main():
        async with async_get_client(config) as client:
            await client.set("hello", "world")

    asyncio.run(main())
"""

from __future__ import annotations

import logging
from typing import Optional

import grpc

from rustycluster.async_client import AsyncRustyClusterClient
from rustycluster.auth import AuthManager
from rustycluster.batch import BatchOperationBuilder
from rustycluster.client import RustyClusterClient
from rustycluster.config import RustyClusterConfig
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

__version__ = "1.0.0"
__author__ = "RustyCluster"
__all__ = [
    # Factory functions
    "get_client",
    "async_get_client",
    # Client classes
    "RustyClusterClient",
    "AsyncRustyClusterClient",
    # Config
    "RustyClusterConfig",
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



# YAML config file search order
_YAML_SEARCH_PATHS = [
    "rustycluster.yaml",
    "config/rustycluster.yaml",
    "~/.rustycluster.yaml",
]


def _auto_discover_config() -> RustyClusterConfig:
    """
    Auto-discover configuration in this order:
      1. rustycluster.yaml         (current working directory)
      2. config/rustycluster.yaml  (config subfolder)
      3. ~/.rustycluster.yaml      (home directory)
      4. Environment variables     (RUSTYCLUSTER_* prefix)
    """
    import os
    from pathlib import Path

    for candidate in _YAML_SEARCH_PATHS:
        path = Path(candidate).expanduser()
        if path.exists():
            import logging
            logging.getLogger("rustycluster").debug(
                "Auto-discovered config: %s", path.resolve()
            )
            return RustyClusterConfig.from_yaml(path)

    # Fall back to environment variables
    return RustyClusterConfig.from_env()


def get_client(config: Optional[RustyClusterConfig] = None) -> RustyClusterClient:
    """
    Create and return a fully authenticated, ready-to-use RustyClusterClient.

    This is the primary entry point for using the client library.

    1. Builds the gRPC channel (plain or TLS based on config).
    2. Authenticates with username/password.
    3. Wires up a gRPC interceptor that auto-injects the Bearer token.
    4. Returns the initialized RustyClusterClient.

    Args:
        config: RustyClusterConfig instance. If None, loads from environment
                variables (RUSTYCLUSTER_* prefix).

    Returns:
        Initialized RustyClusterClient.

    Raises:
        AuthenticationError: If credentials are rejected by the server.
        ConnectionError: If the server is unreachable.
        ConfigurationError: If config is invalid.

    Example:
        # Direct config
        client = get_client(RustyClusterConfig(
            nodes="localhost:50051,localhost:50052",
            username="admin", password="secret",
        ))

        # From env vars
        client = get_client()

        # From .env file
        config = RustyClusterConfig.from_dotenv(".env")
        client = get_client(config)
    """
    if config is None:
        config = _auto_discover_config()

    # Configure library logging level
    logging.getLogger("rustycluster").setLevel(
        getattr(logging, config.log_level, logging.WARNING)
    )

    logger = logging.getLogger("rustycluster")

    # NodeManager binds to the primary and rotates through the configured
    # nodes on failover. It also performs the initial authentication.
    manager = NodeManager(config=config, build_channel=_build_channel)

    logger.info(
        "RustyCluster client connected to %s (TLS=%s, failover_nodes=%d)",
        manager.current_target,
        config.use_tls,
        len(config.targets),
    )

    return RustyClusterClient(manager=manager, config=config)


async def async_get_client(
    config: Optional[RustyClusterConfig] = None,
) -> AsyncRustyClusterClient:
    """
    Create and return a fully authenticated async RustyClusterClient.

    Requires grpc.aio to be initialized (call grpc.aio.init_grpc_aio() if needed).

    Args:
        config: RustyClusterConfig instance. If None, loads from environment variables.

    Returns:
        Initialized AsyncRustyClusterClient.

    Example:
        import asyncio
        from rustycluster import async_get_client, RustyClusterConfig

        async def main():
            config = RustyClusterConfig(
                nodes="localhost:50051,localhost:50052",
                username="admin", password="secret",
            )
            async with await async_get_client(config) as client:
                await client.set("hello", "world")

        asyncio.run(main())
    """
    import grpc.aio
    from rustycluster.failover import AsyncNodeManager

    if config is None:
        config = _auto_discover_config()

    logging.getLogger("rustycluster").setLevel(
        getattr(logging, config.log_level, logging.WARNING)
    )

    manager = AsyncNodeManager(config=config, build_channel=_build_async_channel)
    await manager.connect()

    logging.getLogger("rustycluster").info(
        "Async RustyCluster client connected to %s (TLS=%s, failover_nodes=%d)",
        manager.current_target,
        config.use_tls,
        len(config.targets),
    )

    return AsyncRustyClusterClient(manager=manager, config=config)


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
