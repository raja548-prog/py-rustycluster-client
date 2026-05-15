# RustyCluster Python Client — Integration Specification

Version: 1.1.1 | License: MIT | Python: ≥ 3.10

---

## Table of Contents

1. [Overview](#1-overview)
2. [Installation](#2-installation)
3. [Quick Start](#3-quick-start)
4. [Configuration](#4-configuration)
5. [Client Factory Functions](#5-client-factory-functions)
6. [Data Types and Operations](#6-data-types-and-operations)
   - [Strings](#61-strings)
   - [Hashes](#62-hashes)
   - [Sets](#63-sets)
   - [Sorted Sets](#64-sorted-sets)
   - [Lists and Queues](#65-lists-and-queues)
   - [Streams](#66-streams)
   - [Scripts](#67-scripts)
7. [Batch Operations](#7-batch-operations)
8. [System Operations](#8-system-operations)
9. [Async Client](#9-async-client)
10. [Error Handling](#10-error-handling)
11. [Retry and Failover](#11-retry-and-failover)
12. [RQ Compatibility Layer](#12-rq-compatibility-layer)
13. [TLS / mTLS Setup](#13-tls--mtls-setup)
14. [Complete API Reference](#14-complete-api-reference)

---

## 1. Overview

**RustyCluster** is a distributed, gRPC-based key-value store with a Redis-compatible API. It supports clustering, replication, and site-level replication, accessed exclusively over gRPC (not the Redis wire protocol).

**`py-rustycluster-client`** is the official Python client for RustyCluster. It provides:

- A **synchronous** and **async** client with identical method surfaces
- **Automatic retry** with exponential backoff on transient failures
- **Multi-node failover** — the client moves to the next node when the primary is unreachable
- **Session-based authentication** with automatic re-authentication on token expiry
- **Batch writes** — execute many operations in a single atomic RPC call
- **RQ-compatible job queue** — drop-in replacement for Python RQ, backed by RustyCluster lists

### Data Structures Supported

| Type | Operations |
|------|-----------|
| String | get, set, delete, TTL, atomic increments, bulk ops |
| Hash | field-level get/set/delete, scan, binary-safe variants |
| Set | add, remove, members, cardinality, scan |
| Sorted Set | add with score, range queries, score lookup |
| List | head/tail push & pop, blocking pop, slice, search |
| Stream | append entries, read by ID range |
| Script | load Lua script, execute by SHA |

---

## 2. Installation

```bash
pip install py-rustycluster-client
```

**Dependencies** (installed automatically):

| Package | Minimum Version | Purpose |
|---------|----------------|---------|
| `grpcio` | 1.60.0 | gRPC transport |
| `protobuf` | 4.25.0 | Message serialization |
| `pydantic` | 2.0.0 | Config validation |
| `python-dotenv` | 1.0.0 | `.env` file support |

---

## 3. Quick Start

### Synchronous

```python
from rustycluster import get_client, close_all

# Client is cached — subsequent calls with "DB0" return the same instance
db0 = get_client("DB0")

db0.set("hello", "world")
print(db0.get("hello"))           # "world"

db0.hset("user:1", "name", "Alice")
print(db0.hget_all("user:1"))     # {"name": "Alice"}

close_all()  # close all cached connections on shutdown
```

### Asynchronous

```python
import asyncio
from rustycluster import async_get_client, async_close_all

async def main():
    client = await async_get_client("DB0")
    await client.set("hello", "world")
    value = await client.get("hello")
    print(value)  # "world"
    await async_close_all()

asyncio.run(main())
```

### Context Manager

```python
from rustycluster import get_client

with get_client("DB0") as client:
    client.set("key", "value")
# connection closed automatically
```

---

## 4. Configuration

### 4.1 YAML File (recommended for multi-cluster)

Create `rustycluster.yaml` in your project root. The client auto-discovers it when you call `get_client(...)`.

**Discovery order:**
1. `./rustycluster.yaml`
2. `./config/rustycluster.yaml`
3. `~/.rustycluster.yaml`

```yaml
rustycluster:

  defaults:                        # inherited by every cluster
    username: admin
    password: secret
    use_tls: false
    timeout_seconds: 10.0
    max_retries: 3
    retry_backoff_base: 0.5
    retry_backoff_max: 30.0
    connection_pool_size: 5
    enable_compression: false
    log_level: WARNING             # DEBUG | INFO | WARNING | ERROR | CRITICAL

  clusters:
    DB0:
      nodes: "host1:50051,host2:50051"   # primary,fallback
    DB1:
      nodes: "host3:50051"
      timeout_seconds: 5.0               # per-cluster override
```

The `nodes` field is a comma-separated list of `host:port` pairs. The **first entry is the primary**; subsequent entries are failover targets tried in order.

### 4.2 Environment Variables

For single-cluster deployments or CI/CD environments. All variables use the `RUSTYCLUSTER_` prefix.

| Variable | Default | Description |
|----------|---------|-------------|
| `RUSTYCLUSTER_NODES` | `localhost:50051` | Comma-separated `host:port` list |
| `RUSTYCLUSTER_USERNAME` | `""` | Authentication username |
| `RUSTYCLUSTER_PASSWORD` | `""` | Authentication password |
| `RUSTYCLUSTER_USE_TLS` | `false` | Enable TLS (`true`/`1`/`yes`) |
| `RUSTYCLUSTER_TLS_CA_CERT_PATH` | _(none)_ | Path to CA certificate |
| `RUSTYCLUSTER_TLS_CLIENT_CERT_PATH` | _(none)_ | Path to client cert (mTLS) |
| `RUSTYCLUSTER_TLS_CLIENT_KEY_PATH` | _(none)_ | Path to client key (mTLS) |
| `RUSTYCLUSTER_TIMEOUT_SECONDS` | `10.0` | Per-RPC deadline in seconds |
| `RUSTYCLUSTER_MAX_RETRIES` | `3` | Max retry attempts |
| `RUSTYCLUSTER_RETRY_BACKOFF_BASE` | `0.5` | Initial backoff delay (seconds) |
| `RUSTYCLUSTER_RETRY_BACKOFF_MAX` | `30.0` | Max backoff delay (seconds) |
| `RUSTYCLUSTER_CONNECTION_POOL_SIZE` | `5` | gRPC sub-channel count |
| `RUSTYCLUSTER_ENABLE_COMPRESSION` | `false` | gRPC message compression |
| `RUSTYCLUSTER_LOG_LEVEL` | `WARNING` | `rustycluster` logger level |

```python
from rustycluster import get_client, RustyClusterConfig, RustyClusterSettings

config = RustyClusterConfig.from_env()
settings = RustyClusterSettings(clusters={"main": config})
client = get_client("main", settings=settings)
```

### 4.3 .env File

```python
config = RustyClusterConfig.from_dotenv(".env")
```

The `.env` file must contain `RUSTYCLUSTER_*` variables. Requires `python-dotenv` (included in the package dependencies).

### 4.4 Programmatic Configuration

Use this for tests, embedded libraries, or when you need multiple isolated clients.

```python
from rustycluster import get_client, RustyClusterConfig, RustyClusterSettings

config = RustyClusterConfig(
    nodes="server1:50051,server2:50051",
    username="admin",
    password="secret",
    timeout_seconds=5.0,
    max_retries=2,
    retry_backoff_base=0.5,
    retry_backoff_max=10.0,
)

settings = RustyClusterSettings(clusters={"main": config})

# settings= bypasses the global cache; caller owns the lifecycle
client = get_client("main", settings=settings)
client.set("k", "v")
client.close()
```

### 4.5 Config Field Reference

| Field | Type | Default | Constraints |
|-------|------|---------|-------------|
| `nodes` | `str` | `"localhost:50051"` | comma-separated `host:port`; at least one entry |
| `username` | `str` | `""` | — |
| `password` | `SecretStr` | `""` | — |
| `use_tls` | `bool` | `False` | — |
| `tls_ca_cert_path` | `Path \| None` | `None` | file must exist if set |
| `tls_client_cert_path` | `Path \| None` | `None` | file must exist if set |
| `tls_client_key_path` | `Path \| None` | `None` | file must exist if set |
| `timeout_seconds` | `float` | `10.0` | > 0 |
| `max_retries` | `int` | `3` | ≥ 0 |
| `retry_backoff_base` | `float` | `0.5` | > 0 |
| `retry_backoff_max` | `float` | `30.0` | > 0 |
| `connection_pool_size` | `int` | `5` | ≥ 1 |
| `enable_compression` | `bool` | `False` | — |
| `log_level` | `str` | `"WARNING"` | DEBUG / INFO / WARNING / ERROR / CRITICAL |

---

## 5. Client Factory Functions

All four functions are importable from the top-level `rustycluster` package.

### `get_client(name=None, *, settings=None) -> RustyClusterClient`

Returns a connected, authenticated synchronous client.

- When `settings` is omitted, the YAML is auto-discovered and the client is **cached by name** — multiple calls with the same name return the same instance.
- When `settings` is provided, a **fresh client** is created each time and the caller owns its lifecycle.
- If only one cluster is configured, `name` may be omitted.

```python
from rustycluster import get_client

client = get_client("DB0")
```

### `async_get_client(name=None, *, settings=None) -> AsyncRustyClusterClient`

Async counterpart. Must be awaited. Same caching rules as `get_client`.

```python
client = await async_get_client("DB0")
```

### `close_all()`

Closes all cached synchronous clients and clears the settings cache. Safe to call multiple times. Does not affect clients built with an explicit `settings=` argument.

### `async_close_all()`

Async counterpart of `close_all`.

---

## 6. Data Types and Operations

All examples below use a synchronous client. For the async client, add `await` before each call.

### 6.1 Strings

Basic key-value storage with optional TTL, atomic counters, and bulk operations.

#### `set(key, value, *, skip_replication=False, skip_site_replication=False) -> bool`

```python
client.set("session:abc", "user_data")
```

#### `get(key) -> str | None`

Returns the value, or `None` if the key does not exist.

```python
value = client.get("session:abc")   # "user_data"
missing = client.get("no:such:key") # None
```

#### `delete(key, *, skip_replication=False, skip_site_replication=False) -> bool`

```python
client.delete("session:abc")
```

#### `set_ex(key, value, ttl, *, skip_replication=False, skip_site_replication=False) -> bool`

Set a key with a time-to-live in seconds.

```python
client.set_ex("otp:1234", "token_value", 300)  # expires in 5 minutes
```

#### `set_expiry(key, ttl, *, skip_replication=False, skip_site_replication=False) -> bool`

Apply a TTL to an already-existing key.

```python
client.set_expiry("user:1", 7200)
```

#### `set_nx(key, value, *, skip_replication=False, skip_site_replication=False) -> bool`

Set only if the key does not exist. Returns `True` if the key was created.

```python
acquired = client.set_nx("lock:resource", "holder_id")
```

#### `exists(key) -> bool`

```python
if client.exists("user:1"):
    ...
```

#### `mget(*keys) -> list[str | None]`

Returns values in the same order as the keys. `None` for missing keys.

```python
vals = client.mget("k1", "k2", "k3")  # [v1, None, v3]
```

#### `del_multiple(*keys, *, skip_replication=False, skip_site_replication=False) -> int`

Delete multiple keys atomically. Returns the number of keys actually deleted.

```python
count = client.del_multiple("k1", "k2", "k3")
```

#### `keys(pattern) -> list[str]`

Glob-style pattern matching (`*`, `?`, `[abc]`).

```python
user_keys = client.keys("user:*")
```

#### `incr_by(key, value, *, skip_replication=False, skip_site_replication=False) -> int`
#### `decr_by(key, value, *, skip_replication=False, skip_site_replication=False) -> int`
#### `incr_by_float(key, value, *, skip_replication=False, skip_site_replication=False) -> float`

Atomic counter operations. The key is created at 0 if it does not exist.

```python
new_count = client.incr_by("page:views", 1)
new_balance = client.incr_by_float("account:balance", 9.99)
```

---

### 6.2 Hashes

A hash is a map of string fields to string values stored under a single key.

#### `hset(key, field, value, ...) -> bool`
#### `hget(key, field) -> str | None`
#### `hget_all(key) -> dict[str, str]`
#### `hmset(key, fields: dict[str, str], ...) -> bool`

```python
client.hset("user:1", "name", "Alice")
client.hmset("user:1", {"email": "alice@example.com", "role": "admin"})

name = client.hget("user:1", "name")       # "Alice"
data = client.hget_all("user:1")           # {"name": "Alice", "email": ..., "role": ...}
```

#### `hexists(key, field) -> bool`
#### `hdel(key, *fields, ...) -> int`

```python
client.hexists("user:1", "email")   # True
client.hdel("user:1", "temp1", "temp2")
```

#### `hlen(key) -> int`

Number of fields in the hash.

#### `hsetnx(key, field, value, ...) -> bool`

Set field only if it does not exist. Returns `True` if set.

#### `hexpire(key, seconds, *fields, ...) -> int`

Set a TTL on specific fields. Returns the number of fields that had the expiry applied.

```python
client.hexpire("session:1", 3600, "token", "refresh_token")
```

#### `hincr_by(key, field, value, ...) -> int`
#### `hdecr_by(key, field, value, ...) -> int`
#### `hincr_by_float(key, field, value, ...) -> float`

```python
client.hincr_by("user:stats", "logins", 1)
```

#### `hscan(key, cursor=0, pattern=None, count=None) -> tuple[int, dict[str, str]]`

Cursor-based iteration for large hashes. Returns `(next_cursor, {field: value, ...})`. When `next_cursor == 0`, the full hash has been scanned.

```python
cursor = 0
while True:
    cursor, fields = client.hscan("big:hash", cursor=cursor, count=100)
    process(fields)
    if cursor == 0:
        break
```

#### Binary-safe hash variants

Use these when keys, fields, or values contain arbitrary bytes (e.g., serialized objects, MessagePack data).

| Method | Signature |
|--------|-----------|
| `hset_bytes` | `(key: bytes, field: bytes, value: bytes, ...) -> bool` |
| `hget_bytes` | `(key: bytes, field: bytes) -> bytes \| None` |
| `hget_all_bytes` | `(key: bytes) -> list[tuple[bytes, bytes]]` |
| `hscan_bytes` | `(key: bytes, cursor=0, pattern=None, count=None) -> tuple[int, list[tuple[bytes, bytes]]]` |

```python
client.hset_bytes(b"bin:key", b"field", b"\x00\x01\x02\xff")
raw = client.hget_bytes(b"bin:key", b"field")
```

---

### 6.3 Sets

Unordered collections of unique string members.

#### `sadd(key, *members, ...) -> int`

Returns the number of members actually added (ignores duplicates).

```python
client.sadd("tags:post:1", "python", "grpc", "redis")
```

#### `smembers(key) -> list[str]`

```python
members = client.smembers("tags:post:1")
```

#### `srem(key, *members, ...) -> int`
#### `scard(key) -> int`

```python
client.srem("tags:post:1", "redis")
size = client.scard("tags:post:1")
```

#### `sscan(key, cursor=0, pattern=None, count=None) -> tuple[int, list[str]]`

Same cursor-based iteration semantics as `hscan`.

---

### 6.4 Sorted Sets

Members are stored with a floating-point score and iterated in score order.

#### `zadd(key, score, member, ...) -> int`

Returns 1 if a new member was added, 0 if an existing member's score was updated.

```python
client.zadd("leaderboard", 1500.0, "player_alice")
client.zadd("leaderboard", 1200.0, "player_bob")
```

#### `zrem(key, *members, ...) -> int`
#### `zcard(key) -> int`

#### `zscore(key, member) -> float | None`

```python
score = client.zscore("leaderboard", "player_alice")  # 1500.0
```

#### `zrange(key, start, stop, with_scores=False) -> list`

Retrieve members by rank (0-based index, -1 = last). Returns `list[str]` or `list[tuple[str, float]]` when `with_scores=True`.

```python
top3 = client.zrange("leaderboard", 0, 2)
top3_with_scores = client.zrange("leaderboard", 0, 2, with_scores=True)
# [("player_alice", 1500.0), ("player_bob", 1200.0), ...]
```

#### `zrange_by_score(key, min, max, with_scores=False, offset=0, count=0) -> list`

Retrieve members by score range. Use `offset` and `count` for pagination.

```python
# All players with score 1000–2000
players = client.zrange_by_score("leaderboard", 1000.0, 2000.0)

# Paginate: skip first 10, return next 5
page = client.zrange_by_score("leaderboard", 0, 9999, offset=10, count=5)
```

---

### 6.5 Lists and Queues

Ordered lists that support head/tail insertion and removal, making them suitable as FIFO queues or stacks.

#### Push operations

```python
# Prepend (left push) — list becomes ["c", "b", "a"]
client.lpush("queue", "a", "b", "c")

# Append (right push) — list becomes ["x", "y", "z"]
client.rpush("queue", "x", "y", "z")

# Push only if list exists (returns 0 if key missing)
client.lpushx("queue", "head")
client.rpushx("queue", "tail")
```

#### Pop operations

`lpop` and `rpop` return a `list[str]`. The list is empty when the key does not exist.

```python
first = client.lpop("queue")           # ["x"]
batch = client.lpop("queue", count=3)  # up to 3 values from the head
last  = client.rpop("queue")           # ["z"]
```

#### Blocking pop — `blpop` / `brpop`

Blocks until an element is available or `timeout` seconds elapse. Accepts multiple keys; the first non-empty list wins. Returns `(key, value)` tuple or `None` on timeout.

```python
result = client.blpop("queue:high", "queue:low", timeout=5.0)
if result:
    key, value = result
    process(key, value)
```

Use `timeout=0.0` to block indefinitely.

#### Read operations

```python
all_items   = client.lrange("queue", 0, -1)     # -1 = end (inclusive)
first_three = client.lrange("queue", 0, 2)
length      = client.llen("queue")
item        = client.lindex("queue", 0)          # None if out of range
```

#### Mutation operations

```python
# Overwrite element at index
client.lset("queue", 1, "new_value")

# Remove occurrences: count > 0 from head, count < 0 from tail, 0 = all
removed = client.lrem("queue", 2, "target_value")

# Keep only indices 0..99
client.ltrim("queue", 0, 99)

# Insert before or after a pivot
client.linsert("queue", "pivot", "new_value", before=True)

# Find positions of an element
positions = client.lpos("queue", "target", count=0)  # all matches
```

---

### 6.6 Streams

Append-only log of entries, each with an auto-generated or custom ID.

#### `xadd(key, fields, id="*", ...) -> str`

Appends an entry. Pass `id="*"` (default) to let the server generate a monotonic ID. Returns the resolved ID.

```python
entry_id = client.xadd(
    "events:orders",
    {"order_id": "ORD-001", "status": "placed", "total": "99.99"},
)
# "1699999999999-0"
```

#### `xread(streams, count=None) -> list[tuple[str, str, dict[str, str]]]`

Read entries from one or more streams. Each element in `streams` is `(key, last_seen_id)`.

Use `"0"` to read from the beginning, or the last seen ID to read only new entries.

```python
# Read all entries from the beginning
entries = client.xread([("events:orders", "0")])
for key, entry_id, fields in entries:
    print(key, entry_id, fields)

# Read only new entries since a known ID
new_entries = client.xread(
    [("events:orders", "1699999999999-0")],
    count=100,
)
```

---

### 6.7 Scripts

Load a Lua script once and execute it repeatedly by SHA hash, reducing bandwidth.

#### `load_script(script) -> str`

Uploads a Lua script and returns its SHA1 hash.

```python
sha = client.load_script("""
    local val = redis.call('GET', KEYS[1])
    if val then
        redis.call('SET', KEYS[2], val)
        return 1
    end
    return 0
""")
```

#### `eval_sha(sha, keys, args, ...) -> str`

Execute the previously loaded script.

```python
result = client.eval_sha(sha, keys=["src:key"], args=["dst:key"])
```

---

## 7. Batch Operations

Execute multiple write operations in a single atomic RPC call. All operations in the batch succeed or the call raises `BatchOperationError`.

### Building Operations

Import the builder:

```python
from rustycluster import BatchOperationBuilder as BOB
# or
from rustycluster.batch import BatchOperationBuilder as BOB
```

### Executing a Batch

```python
ops = [
    BOB.set("k1", "v1"),
    BOB.set_ex("session:1", "data", 3600),
    BOB.delete("old:key"),
    BOB.hset("user:1", "name", "Alice"),
    BOB.hmset("user:1", {"email": "a@example.com", "role": "admin"}),
    BOB.hdel("user:1", ["temp_field"]),
    BOB.sadd("tags", ["python", "grpc"]),
    BOB.lpush("queue", ["job1", "job2"]),
    BOB.incr_by("counter", 5),
    BOB.del_multiple(["stale1", "stale2"]),
]

results = client.batch_write(ops)
# [True, True, True, True, True, True, True, True, True, True]
```

The `skip_replication` and `skip_site_replication` flags can be passed to `batch_write` and apply to all operations.

### Available Batch Operations

| Builder Method | Parameters |
|---------------|-----------|
| `BOB.set(key, value)` | — |
| `BOB.set_ex(key, value, ttl)` | ttl in seconds |
| `BOB.set_nx(key, value)` | — |
| `BOB.set_expiry(key, ttl)` | — |
| `BOB.delete(key)` | — |
| `BOB.del_multiple(keys)` | `keys: list[str]` |
| `BOB.incr_by(key, value)` | `value: int` |
| `BOB.decr_by(key, value)` | `value: int` |
| `BOB.incr_by_float(key, value)` | `value: float` |
| `BOB.hset(key, field, value)` | — |
| `BOB.hmset(key, fields)` | `fields: dict[str, str]` |
| `BOB.hdel(key, fields)` | `fields: list[str]` |
| `BOB.hsetnx(key, field, value)` | — |
| `BOB.hexpire(key, seconds, fields)` | `fields: list[str]` |
| `BOB.hincrby(key, field, value)` | `value: int` |
| `BOB.hdecrby(key, field, value)` | `value: int` |
| `BOB.hincr_by_float(key, field, value)` | `value: float` |
| `BOB.hset_bytes(key, field, value)` | all `bytes` |
| `BOB.sadd(key, members)` | `members: list[str]` |
| `BOB.srem(key, members)` | `members: list[str]` |
| `BOB.lpush(key, values)` | `values: list[str]` |
| `BOB.rpush(key, values)` | `values: list[str]` |
| `BOB.lpushx(key, values)` | `values: list[str]` |
| `BOB.rpushx(key, values)` | `values: list[str]` |
| `BOB.lpop(key, count=None)` | — |
| `BOB.rpop(key, count=None)` | — |
| `BOB.lset(key, index, value)` | `index: int` |
| `BOB.lrem(key, count, element)` | — |
| `BOB.xadd(key, fields, id="*")` | `fields: dict[str, str]` |
| `BOB.load_script(script)` | — |
| `BOB.eval_sha(sha, keys, args)` | — |

---

## 8. System Operations

```python
alive   = client.ping()          # True / False
healthy = client.health_check()  # True / False

# Force re-authentication (e.g. after a credentials rotation)
client.reauthenticate()
```

---

## 9. Async Client

`AsyncRustyClusterClient` mirrors every method on the synchronous client. All methods are coroutines — prefix each call with `await`.

```python
import asyncio
from rustycluster import async_get_client, async_close_all

async def main():
    # Context manager
    async with await async_get_client("DB0") as client:
        await client.set("hello", "world")
        value = await client.get("hello")
        print(value)

        members = await client.smembers("tags")
        await client.zadd("leaderboard", 1000.0, "alice")

        result = await client.blpop("queue", timeout=2.0)

asyncio.run(main())
```

Use `await async_close_all()` in place of `close_all()` for cleanup.

---

## 10. Error Handling

### Exception Hierarchy

```
RustyClusterError                    # base — catch all
├── ConnectionError                  # server unreachable / all nodes exhausted
├── AuthenticationError              # bad credentials or expired token
├── KeyNotFoundError                 # key does not exist (has .key attribute)
├── OperationError                   # operation rejected by the server
├── TimeoutError                     # request exceeded timeout_seconds
├── BatchOperationError              # one or more batch ops failed (.results: list[bool])
├── ConfigurationError               # bad config / missing YAML
└── ScriptError                      # Lua script load or execution failed
```

All exceptions include a `.cause` attribute with the original gRPC `RpcError` when applicable.

### Catching Errors

```python
from rustycluster import (
    get_client,
    RustyClusterError,
    ConnectionError,
    AuthenticationError,
    KeyNotFoundError,
    BatchOperationError,
    TimeoutError,
)

client = get_client("DB0")

try:
    value = client.get("my:key")
except KeyNotFoundError as e:
    print(f"Missing key: {e.key}")
except TimeoutError:
    print("Request timed out")
except ConnectionError:
    print("All nodes unreachable")
except AuthenticationError:
    print("Authentication failed")
except RustyClusterError as e:
    print(f"Unexpected error: {e}")
```

### Batch Error Handling

```python
from rustycluster import BatchOperationError

try:
    results = client.batch_write(ops)
except BatchOperationError as e:
    print(f"Batch failed: {e}")
    print(f"Per-operation results: {e.results}")
    failed = [i for i, ok in enumerate(e.results) if not ok]
    print(f"Failed operation indices: {failed}")
```

---

## 11. Retry and Failover

### Automatic Retry

The client automatically retries transient failures with exponential backoff. Configure via `RustyClusterConfig`:

```yaml
max_retries: 3           # number of retry attempts (0 = no retries)
retry_backoff_base: 0.5  # initial delay in seconds
retry_backoff_max: 30.0  # cap on backoff delay
```

**Backoff formula:**

```
delay = min(backoff_base * 2^attempt, backoff_max)

# With base=0.5, max=30.0:
# Attempt 1: 0.5s
# Attempt 2: 1.0s
# Attempt 3: 2.0s
# Attempt 4+: capped at 30.0s
```

### Retry Decision by Error Type

| gRPC Status Code | Action |
|-----------------|--------|
| `UNAVAILABLE` | Retry (server temporarily down) |
| `DEADLINE_EXCEEDED` | Retry (timeout) |
| `INTERNAL` | Retry (server-side error) |
| `UNKNOWN` | Retry |
| `UNAUTHENTICATED` | Re-authenticate once, then retry |
| `INVALID_ARGUMENT` | **No retry** — bad request |
| `NOT_FOUND` | **No retry** — key missing |
| `PERMISSION_DENIED` | **No retry** — authorization failure |
| `UNIMPLEMENTED` | **No retry** — unsupported RPC |
| `ALREADY_EXISTS` | **No retry** |

### Multi-node Failover

When the primary node exhausts all retry attempts, the client automatically moves to the next node in the `nodes` list, re-authenticates, and continues. This is transparent to the caller.

```yaml
clusters:
  DB0:
    nodes: "primary:50051,secondary1:50051,secondary2:50051"
```

**Important:** Failover is one-directional — once the client moves to a secondary, it stays there until the connection is closed. To reconnect to the primary, call `close_all()` and create a new client.

If all nodes are exhausted, `ConnectionError` is raised.

---

## 12. RQ Compatibility Layer

`rustycluster.rq` is a drop-in replacement for Python [RQ](https://python-rq.org/), backed by RustyCluster lists and sorted sets instead of Redis.

### Migration from RQ

The only change is the import path and the connection object:

```python
# Before (RQ + Redis)
from rq import Queue, Worker, Retry
from rq.job import Job
from redis import Redis
conn = Redis(host="localhost", port=6379)

# After (rustycluster.rq + RustyCluster)
from rustycluster.rq import Queue, Worker, Retry, Job
from rustycluster import get_client
conn = get_client("DB0")   # <- only this line changes
```

Everything else — `Queue(name, connection=conn)`, `q.enqueue(fn, arg)`, `Job.fetch(id, connection=conn)`, `worker.work(burst=True)` — is identical.

### Enqueuing Jobs

```python
from rustycluster import get_client
from rustycluster.rq import Queue, Retry

def send_email(to, subject, body):
    ...  # actual implementation

conn = get_client("DB0")
q = Queue("emails", connection=conn)

# Simple enqueue
job = q.enqueue(send_email, "alice@example.com", "Hello", "World")

# With options
job = q.enqueue(
    send_email,
    "bob@example.com", "Hello", "World",
    job_timeout=60,        # seconds before job is killed
    result_ttl=3600,       # keep result for 1 hour
    retry=Retry(max=3, interval=[0, 5, 30]),  # retry delays in seconds
    at_front=True,         # prepend instead of append
    job_id="custom-id-123",
)
print(job.id)
```

### Processing Jobs

```python
from rustycluster.rq import Queue, Worker

conn = get_client("DB0")
q = Queue("emails", connection=conn)
w = Worker([q], connection=conn)

# Run until the queue is empty
w.work(burst=True)

# Run continuously (blocks)
w.work()
```

### Checking Job Status

```python
from rustycluster.rq import Job, JobStatus

job = Job.fetch("custom-id-123", connection=conn)
job.refresh()   # pull latest state from store

print(job.get_status())    # JobStatus.FINISHED | QUEUED | STARTED | FAILED
print(job.result)          # deserialized return value (when FINISHED)
print(job.exc_info)        # exception traceback string (when FAILED)
print(job.enqueued_at)     # datetime
print(job.started_at)      # datetime
print(job.ended_at)        # datetime

job.cancel()               # mark as CANCELED
```

### Job Registries

```python
from rustycluster.rq import StartedJobRegistry, FinishedJobRegistry, FailedJobRegistry

started  = StartedJobRegistry(queue=q)
finished = FinishedJobRegistry(queue=q)
failed   = FailedJobRegistry(queue=q)

print(len(finished))
ids = finished.get_job_ids()
```

### Retry Policy

```python
from rustycluster.rq import Retry

# Retry up to 3 times with delays: immediate, 5s, 30s
retry = Retry(max=3, interval=[0, 5, 30])

# Fixed delay between all retries
retry = Retry(max=5, interval=10)

# No delay between retries
retry = Retry(max=3)
```

### Getting the Current Job (inside a worker)

```python
from rustycluster.rq import get_current_job

def my_task():
    job = get_current_job()
    print(f"Running as job {job.id}")
```

### Internal Key Schema

| Key | Type | Description |
|-----|------|-------------|
| `rc:queue:{name}` | List | Job ID queue |
| `rc:job:{job_id}` | Hash | Job metadata (fn, args, status, result, …) |
| `rc:started:{name}` | Sorted Set | In-progress jobs (scored by start time) |
| `rc:finished:{name}` | Sorted Set | Completed jobs |
| `rc:failed:{name}` | Sorted Set | Failed jobs |

---

## 13. TLS / mTLS Setup

### TLS (server-side only)

```yaml
rustycluster:
  defaults:
    use_tls: true
    tls_ca_cert_path: /etc/ssl/certs/rustycluster-ca.crt
  clusters:
    DB0:
      nodes: "secure-host:50051"
```

### mTLS (mutual TLS)

Provide all three cert paths:

```yaml
rustycluster:
  defaults:
    use_tls: true
    tls_ca_cert_path:         /etc/ssl/certs/ca.crt
    tls_client_cert_path:     /etc/ssl/client/client.crt
    tls_client_key_path:      /etc/ssl/client/client.key
  clusters:
    DB0:
      nodes: "secure-host:50051"
```

Or programmatically:

```python
from pathlib import Path
from rustycluster import RustyClusterConfig, RustyClusterSettings, get_client

config = RustyClusterConfig(
    nodes="secure-host:50051",
    use_tls=True,
    tls_ca_cert_path=Path("/etc/ssl/certs/ca.crt"),
    tls_client_cert_path=Path("/etc/ssl/client/client.crt"),
    tls_client_key_path=Path("/etc/ssl/client/client.key"),
)
settings = RustyClusterSettings(clusters={"prod": config})
client = get_client("prod", settings=settings)
```

---

## 14. Complete API Reference

### `RustyClusterClient` / `AsyncRustyClusterClient`

All methods below exist on both clients. On `AsyncRustyClusterClient` every method is a coroutine (use `await`).

#### Lifecycle

| Method | Returns | Notes |
|--------|---------|-------|
| `close()` | `None` | Close the gRPC channel |
| `reauthenticate()` | `None` | Force token refresh |
| `__enter__` / `__exit__` | — | Context manager |
| `__aenter__` / `__aexit__` | — | Async context manager |

#### System

| Method | Returns |
|--------|---------|
| `ping()` | `bool` |
| `health_check()` | `bool` |

#### Strings

| Method | Returns |
|--------|---------|
| `set(key, value, *, skip_replication=False, skip_site_replication=False)` | `bool` |
| `get(key)` | `str \| None` |
| `delete(key, ...)` | `bool` |
| `set_ex(key, value, ttl, ...)` | `bool` |
| `set_expiry(key, ttl, ...)` | `bool` |
| `set_nx(key, value, ...)` | `bool` |
| `exists(key)` | `bool` |
| `mget(*keys)` | `list[str \| None]` |
| `del_multiple(*keys, ...)` | `int` |
| `keys(pattern)` | `list[str]` |
| `incr_by(key, value, ...)` | `int` |
| `decr_by(key, value, ...)` | `int` |
| `incr_by_float(key, value, ...)` | `float` |

#### Hashes

| Method | Returns |
|--------|---------|
| `hset(key, field, value, ...)` | `bool` |
| `hget(key, field)` | `str \| None` |
| `hget_all(key)` | `dict[str, str]` |
| `hmset(key, fields, ...)` | `bool` |
| `hexists(key, field)` | `bool` |
| `hdel(key, *fields, ...)` | `int` |
| `hlen(key)` | `int` |
| `hsetnx(key, field, value, ...)` | `bool` |
| `hexpire(key, seconds, *fields, ...)` | `int` |
| `hincr_by(key, field, value, ...)` | `int` |
| `hdecr_by(key, field, value, ...)` | `int` |
| `hincr_by_float(key, field, value, ...)` | `float` |
| `hscan(key, cursor=0, pattern=None, count=None)` | `tuple[int, dict[str, str]]` |
| `hset_bytes(key, field, value, ...)` | `bool` |
| `hget_bytes(key, field)` | `bytes \| None` |
| `hget_all_bytes(key)` | `list[tuple[bytes, bytes]]` |
| `hscan_bytes(key, cursor=0, pattern=None, count=None)` | `tuple[int, list[tuple[bytes, bytes]]]` |

#### Sets

| Method | Returns |
|--------|---------|
| `sadd(key, *members, ...)` | `int` |
| `smembers(key)` | `list[str]` |
| `srem(key, *members, ...)` | `int` |
| `scard(key)` | `int` |
| `sscan(key, cursor=0, pattern=None, count=None)` | `tuple[int, list[str]]` |

#### Sorted Sets

| Method | Returns |
|--------|---------|
| `zadd(key, score, member, ...)` | `int` |
| `zrem(key, *members, ...)` | `int` |
| `zcard(key)` | `int` |
| `zscore(key, member)` | `float \| None` |
| `zrange(key, start, stop, with_scores=False)` | `list[str]` or `list[tuple[str, float]]` |
| `zrange_by_score(key, min, max, with_scores=False, offset=0, count=0)` | `list[str]` or `list[tuple[str, float]]` |

#### Lists

| Method | Returns |
|--------|---------|
| `lpush(key, *values, ...)` | `int` |
| `rpush(key, *values, ...)` | `int` |
| `lpushx(key, *values, ...)` | `int` |
| `rpushx(key, *values, ...)` | `int` |
| `lpop(key, count=None, ...)` | `list[str]` |
| `rpop(key, count=None, ...)` | `list[str]` |
| `blpop(*keys, timeout=0.0, ...)` | `tuple[str, str] \| None` |
| `brpop(*keys, timeout=0.0, ...)` | `tuple[str, str] \| None` |
| `lrange(key, start, stop)` | `list[str]` |
| `llen(key)` | `int` |
| `lindex(key, index)` | `str \| None` |
| `lset(key, index, value, ...)` | `bool` |
| `lrem(key, count, element, ...)` | `int` |
| `ltrim(key, start, stop, ...)` | `bool` |
| `linsert(key, pivot, element, before=True, ...)` | `int` |
| `lpos(key, element, rank=None, count=None)` | `list[int]` |

#### Streams

| Method | Returns |
|--------|---------|
| `xadd(key, fields, id="*", ...)` | `str` (resolved entry ID) |
| `xread(streams, count=None)` | `list[tuple[str, str, dict[str, str]]]` |

#### Scripts

| Method | Returns |
|--------|---------|
| `load_script(script)` | `str` (SHA1 hash) |
| `eval_sha(sha, keys, args, ...)` | `str` |

#### Batch

| Method | Returns |
|--------|---------|
| `batch_write(operations, skip_replication=False, skip_site_replication=False)` | `list[bool]` |

---

*Generated from `py-rustycluster-client` source. For bugs or questions, refer to the project repository.*
