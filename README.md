# RustyCluster Python Client

Enterprise-grade Python gRPC client for [RustyCluster](https://github.com/rustycluster) — a distributed, Redis-compatible key-value store.

---

## Features

- **Zero-config startup** — drop a `rustycluster.yaml` in your project root and call `get_client()`
- **One-line setup** — `get_client(config)` handles channel creation, auth, and interceptor wiring
- **Full API coverage** — All 50+ RPC operations across strings, hashes, sets, numerics, scripts, and batch
- **Automatic token injection** — gRPC interceptor transparently adds Bearer tokens to every call
- **Smart retry** — Exponential backoff on transient errors; no retry on non-recoverable codes
- **Auto re-authentication** — Detects expired tokens and re-auths transparently
- **Async support** — `AsyncRustyClusterClient` via `grpc.aio` for async/await workflows
- **TLS / mTLS** — Full certificate configuration support
- **Type-safe** — Full type annotations, `py.typed` marker, mypy-compatible
- **Flexible config** — YAML file, `.env` file, environment variables, or direct instantiation
- **BatchOperationBuilder** — Fluent builder for batch write operations

---

## Installation

```bash
pip install rustycluster-client pyyaml
```

### From source

```bash
git clone https://github.com/rustycluster/rustycluster-python-client
cd rustycluster-python-client
pip install -e ".[dev]"
pip install pyyaml
```

---

## Quickstart

### Option 1 — YAML config file (recommended)

Create `rustycluster.yaml` in your project root. Declare one or more
clusters under `clusters:`; shared settings go in `defaults:`.

```yaml
rustycluster:
  defaults:
    username: admin
    password: secret
    timeout_seconds: 10.0
  clusters:
    DB0:
      nodes: "localhost:50051,localhost:50052,localhost:50053"
    DB1:
      nodes: "localhost:50054,localhost:50055,localhost:50056"
      timeout_seconds: 5.0       # per-cluster override
```

Then fetch a client by cluster name:

```python
from rustycluster import get_client, close_all

db0 = get_client("DB0")          # builds + caches DB0 client
db0.set("hello", "world")
print(db0.get("hello"))          # "world"

db1 = get_client("DB1")          # different cluster, different client
db1.hset("user:1", "name", "Alice")

close_all()                      # tear down every cached client
```

`get_client("DB0")` is memoised — repeated calls return the same instance.

### Option 2 — Explicit settings (tests / embedding)

Bypass YAML discovery by constructing `RustyClusterSettings` in code.
Clients built this way are *not* cached; the caller owns the lifecycle.

```python
from rustycluster import get_client, RustyClusterConfig, RustyClusterSettings

settings = RustyClusterSettings(
    clusters={
        "DB0": RustyClusterConfig(
            nodes="localhost:50051,localhost:50052",
            username="admin", password="secret",
        ),
    },
)

with get_client("DB0", settings=settings) as client:
    client.set("hello", "world")
```

### Option 3 — Environment variables (single cluster only)

Env vars describe one cluster; multi-cluster setups must use YAML.

```bash
export RUSTYCLUSTER_NODES=myserver:50051,myserver-2:50051
export RUSTYCLUSTER_USERNAME=admin
export RUSTYCLUSTER_PASSWORD=secret
```

```python
from rustycluster import get_client, RustyClusterConfig, RustyClusterSettings

config = RustyClusterConfig.from_env()
settings = RustyClusterSettings(clusters={"default": config})
client = get_client("default", settings=settings)
```

### Option 4 — .env file

```python
from rustycluster import get_client, RustyClusterConfig, RustyClusterSettings

config = RustyClusterConfig.from_dotenv(".env")
settings = RustyClusterSettings(clusters={"default": config})
client = get_client("default", settings=settings)
```

---

## YAML Configuration

### File discovery order

When you call `get_client(name)` without an explicit `settings=` argument,
the client searches for a config file in this order:

| Priority | Location |
|---|---|
| 1 | `./rustycluster.yaml` |
| 2 | `./config/rustycluster.yaml` |
| 3 | `~/.rustycluster.yaml` |

The first match wins. If none are found, `ConfigurationError` is raised.

### Full YAML reference

```yaml
rustycluster:

  # Shared settings — every cluster inherits these.
  defaults:
    username: admin
    password: secret

    # TLS (optional)
    use_tls: false
    # tls_ca_cert_path: /path/to/ca.crt
    # tls_client_cert_path: /path/to/client.crt   # mTLS
    # tls_client_key_path: /path/to/client.key    # mTLS

    # Timeouts & retries
    timeout_seconds: 10.0
    max_retries: 3
    retry_backoff_base: 0.5
    retry_backoff_max: 30.0

    # Pool & misc
    connection_pool_size: 5
    enable_compression: false
    log_level: WARNING        # DEBUG, INFO, WARNING, ERROR

  # Named clusters. `nodes` is required per cluster (first entry is the
  # primary; later entries are tried in order on failover). Any other
  # field overrides `defaults` for this cluster.
  clusters:
    DB0:
      nodes: "localhost:50051,localhost:50052,localhost:50053"
    DB1:
      nodes: "localhost:50054,localhost:50055,localhost:50056"
      timeout_seconds: 5.0
```

### Load YAML explicitly

```python
from rustycluster import get_client, RustyClusterSettings

settings = RustyClusterSettings.from_yaml("path/to/rustycluster.yaml")
client = get_client("DB0", settings=settings)
```

---

## Configuration Reference

All configuration options, their YAML keys, environment variable equivalents, and defaults:

All keys below are valid both at the cluster level and inside `defaults:`
(per-cluster always wins). `nodes` only makes sense per-cluster.

| YAML Key | Env Var | Default | Description |
|---|---|---|---|
| `nodes` | `RUSTYCLUSTER_NODES` | `localhost:50051` | Comma-separated `host:port` list. First entry is primary; rest are failover order. |
| `username` | `RUSTYCLUSTER_USERNAME` | `""` | Auth username |
| `password` | `RUSTYCLUSTER_PASSWORD` | `""` | Auth password |
| `use_tls` | `RUSTYCLUSTER_USE_TLS` | `false` | Enable TLS |
| `tls_ca_cert_path` | `RUSTYCLUSTER_TLS_CA_CERT_PATH` | `None` | CA certificate path |
| `tls_client_cert_path` | `RUSTYCLUSTER_TLS_CLIENT_CERT_PATH` | `None` | Client cert (mTLS) |
| `tls_client_key_path` | `RUSTYCLUSTER_TLS_CLIENT_KEY_PATH` | `None` | Client key (mTLS) |
| `timeout_seconds` | `RUSTYCLUSTER_TIMEOUT_SECONDS` | `10.0` | Per-RPC timeout |
| `max_retries` | `RUSTYCLUSTER_MAX_RETRIES` | `3` | Max retry attempts |
| `retry_backoff_base` | `RUSTYCLUSTER_RETRY_BACKOFF_BASE` | `0.5` | Base backoff (seconds) |
| `retry_backoff_max` | `RUSTYCLUSTER_RETRY_BACKOFF_MAX` | `30.0` | Max backoff (seconds) |
| `connection_pool_size` | `RUSTYCLUSTER_CONNECTION_POOL_SIZE` | `5` | Channel pool size |
| `enable_compression` | `RUSTYCLUSTER_ENABLE_COMPRESSION` | `false` | gRPC compression |
| `log_level` | `RUSTYCLUSTER_LOG_LEVEL` | `WARNING` | Log level |

---

## API Reference

### String Operations

```python
client.set(key, value, skip_replication=False) -> bool
client.get(key) -> str | None
client.delete(key) -> bool
client.set_ex(key, value, ttl) -> bool          # Set with expiry (seconds)
client.set_expiry(key, ttl) -> bool              # Update TTL on existing key
client.set_nx(key, value) -> bool                # Set only if key does not exist
client.exists(key) -> bool
client.mget(*keys) -> list[str | None]           # Multi-get
client.del_multiple(*keys) -> int                # Multi-delete, returns count
client.keys(pattern) -> list[str]                # Pattern match (*, ?, [])
```

### Numeric Operations

```python
client.incr_by(key, value: int) -> int
client.decr_by(key, value: int) -> int
client.incr_by_float(key, value: float) -> float
```

### Hash Operations

```python
client.hset(key, field, value) -> bool
client.hget(key, field) -> str | None
client.hget_all(key) -> dict[str, str]
client.hmset(key, fields: dict[str, str]) -> bool
client.hexists(key, field) -> bool
client.hdel(key, *fields) -> int                 # Returns deleted count
client.hlen(key) -> int
client.hsetnx(key, field, value) -> bool         # Set field if not exists
client.hexpire(key, seconds, *fields) -> int     # TTL on hash fields
client.hincr_by(key, field, value: int) -> int
client.hdecr_by(key, field, value: int) -> int
client.hincr_by_float(key, field, value: float) -> float
client.hscan(key, cursor=0, pattern=None, count=None) -> tuple[int, dict[str, str]]

# Binary-safe variants
client.hset_bytes(key: bytes, field: bytes, value: bytes) -> bool
client.hget_bytes(key: bytes, field: bytes) -> bytes | None
client.hget_all_bytes(key: bytes) -> list[tuple[bytes, bytes]]
client.hscan_bytes(key: bytes, cursor=0, ...) -> tuple[int, list[tuple[bytes, bytes]]]
```

### Set Operations

```python
client.sadd(key, *members) -> int               # Returns count of newly added
client.smembers(key) -> list[str]
client.srem(key, *members) -> int               # Returns removed count
client.scard(key) -> int                        # Cardinality
client.sscan(key, cursor=0, pattern=None, count=None) -> tuple[int, list[str]]
```

### Script Operations

```python
sha = client.load_script(lua_script: str) -> str    # Returns SHA1
result = client.eval_sha(sha, keys, args) -> str
```

### Batch Operations

```python
from rustycluster import BatchOperationBuilder as BOB

ops = [
    BOB.set("k1", "v1"),
    BOB.set_ex("k2", "v2", ttl=3600),
    BOB.delete("k3"),
    BOB.hset("user:1", "name", "Alice"),
    BOB.hmset("user:2", {"name": "Bob", "age": "30"}),
    BOB.sadd("tags", ["python", "grpc"]),
    BOB.incr_by("counter", 5),
    BOB.hdel("user:3", ["field1", "field2"]),
    BOB.del_multiple(["old:key1", "old:key2"]),
]

results = client.batch_write(ops)  # list[bool]
```

### System Operations

```python
client.ping() -> bool
client.health_check() -> bool
client.reauthenticate() -> None    # Force token refresh
client.close() -> None
```

---

## Error Handling

```python
from rustycluster.exceptions import (
    RustyClusterError,      # Base — catch all
    ConnectionError,         # Server unreachable
    AuthenticationError,     # Bad credentials / expired token
    KeyNotFoundError,        # Key does not exist
    OperationError,          # General operation failure
    TimeoutError,            # RPC timed out
    BatchOperationError,     # Batch write failure
    ScriptError,             # Lua script error
    ConfigurationError,      # Bad config / missing YAML file
)

try:
    client.set("key", "value")
except AuthenticationError:
    client.reauthenticate()
except ConnectionError as e:
    print(f"Cannot reach server: {e}")
except TimeoutError:
    print("Request timed out")
except RustyClusterError as e:
    print(f"Unexpected error: {e}")
```

### Retry Behaviour

| gRPC Status | Behaviour | Notes |
|---|---|---|
| `UNAVAILABLE` | ✅ Retry | Server temporarily down |
| `DEADLINE_EXCEEDED` | ✅ Retry | Timeout, retry with backoff |
| `INTERNAL` | ✅ Retry | Server-side error |
| `UNAUTHENTICATED` | 🔄 Re-auth | Re-authenticates, then retries once |
| `INVALID_ARGUMENT` | ❌ No retry | Bad request, won't fix itself |
| `NOT_FOUND` | ❌ No retry | Key missing |
| `UNIMPLEMENTED` | ❌ No retry | Server doesn't support this RPC |

---

## TLS / mTLS

```yaml
rustycluster:
  host: secure-server
  port: 50051
  username: admin
  password: secret
  use_tls: true
  tls_ca_cert_path: /certs/ca.crt
  tls_client_cert_path: /certs/client.crt   # mTLS only
  tls_client_key_path: /certs/client.key    # mTLS only
```

Or via code:

```python
config = RustyClusterConfig(
    host="secure-server",
    port=50051,
    username="admin",
    password="secret",
    use_tls=True,
    tls_ca_cert_path="/certs/ca.crt",
    tls_client_cert_path="/certs/client.crt",
    tls_client_key_path="/certs/client.key",
)
```

---

## Async Usage

```python
import asyncio
from rustycluster import async_get_client, RustyClusterConfig

async def main():
    # Uses rustycluster.yaml if present, otherwise env vars
    async with await async_get_client() as client:
        await client.set("hello", "world")
        value = await client.get("hello")
        print(value)  # "world"

        # Async hash operations
        await client.hmset("user:1", {"name": "Alice", "role": "admin"})
        data = await client.hget_all("user:1")

        # Async batch
        from rustycluster.batch import BatchOperationBuilder as BOB
        results = await client.batch_write([
            BOB.set("k1", "v1"),
            BOB.hset("h", "f", "v"),
        ])

asyncio.run(main())
```

---

## Scan Iteration Pattern

For large datasets, use scan methods in a loop:

```python
# Iterate all fields in a large hash
cursor = 0
all_fields = {}

while True:
    cursor, batch = client.hscan("big:hash", cursor=cursor, count=100)
    all_fields.update(batch)
    if cursor == 0:
        break

# Iterate all members in a large set
cursor = 0
all_members = []

while True:
    cursor, batch = client.sscan("big:set", cursor=cursor, count=100)
    all_members.extend(batch)
    if cursor == 0:
        break
```

---

## Project Structure

```
rustycluster-client/
├── rustycluster.yaml             # ← Drop your config here
├── rustycluster/
│   ├── __init__.py               # get_client(), async_get_client(), public API
│   ├── client.py                 # RustyClusterClient (sync)
│   ├── async_client.py           # AsyncRustyClusterClient
│   ├── config.py                 # RustyClusterConfig (Pydantic)
│   ├── auth.py                   # AuthManager
│   ├── batch.py                  # BatchOperationBuilder
│   ├── exceptions.py             # Exception hierarchy
│   ├── interceptors.py           # gRPC interceptors (auth + logging)
│   ├── retry.py                  # RetryPolicy + with_retry decorator
│   ├── py.typed                  # PEP 561 marker
│   └── proto/
│       ├── rustycluster_pb2.py
│       └── rustycluster_pb2_grpc.py
├── tests/
│   ├── unit/
│   │   ├── test_config.py
│   │   ├── test_exceptions.py
│   │   ├── test_retry.py
│   │   ├── test_client.py
│   │   └── test_batch.py
│   └── integration/
│       └── test_integration.py
├── scripts/
│   └── generate_proto.sh
├── proto/
│   └── rustycluster.proto
├── pyproject.toml
├── .env.example
└── README.md
```

---

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"
pip install pyyaml

# Regenerate proto stubs (macOS + Linux compatible)
./scripts/generate_proto.sh

# Run all tests
pytest

# Run unit tests only
pytest tests/unit/

# Run integration tests (spins up an in-process mock server)
pytest tests/integration/

# Type checking
mypy rustycluster/

# Linting
ruff check rustycluster/ tests/
```

---

## License

MIT
