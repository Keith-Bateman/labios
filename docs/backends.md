# LABIOS Backend Guide

Backends are thin last-mile adapters for external user storage. Label
normalization, shuffling, scheduling, pipelines, and coordination happen before
the backend receives the full `LabelData`.

Internal DragonflyDB and NATS are runtime plumbing. In particular, `kv://`
connects to a user's configured Redis-compatible service, never LABIOS's
warehouse.

## BackendStore contract

The callable contract in `<labios/backend/backend.h>` is:

```text
template<typename B>
concept BackendStore = requires(B backend, const labios::LabelData& label,
                                std::span<const std::byte> data) {
    { backend.put(label, data) } -> std::same_as<labios::BackendResult>;
    { backend.get(label) } -> std::same_as<labios::BackendDataResult>;
    { backend.del(label) } -> std::same_as<labios::BackendResult>;
    { backend.query(label) } -> std::same_as<labios::BackendQueryResult>;
    { backend.scheme() } -> std::same_as<std::string_view>;
};
```

Return members are `success`, `error`, and, where applicable, `data` or
`json_data`:

```text
labios::BackendResult status{true, {}};
labios::BackendDataResult value{true, {}, std::vector<std::byte>{}};
labios::BackendQueryResult query{true, {}, "{}"};
```

## Implemented schemes

| Scheme | Adapter | External target |
|---|---|---|
| `file://` | `PosixBackend` | Worker-visible filesystem attachment |
| `sqlite://` | `SQLiteBackend` | Worker-visible SQLite database |
| `kv://` | `KVBackend` | Optional user Redis-compatible service |
| `clio://` | `ClioCoreBackend` | Optional [clio-core](https://github.com/iowarp/clio-core) Context Transfer Engine (CTE) runtime |
| `observe://` | Dispatcher-local handler | Registered runtime observations |

S3, vector, graph, and parallel-filesystem adapters are planned and must not be
presented as callable backends.

### `clio://` (ClioCoreBackend)

`ClioCoreBackend` wraps clio-core's CTE Tag+Blob client
(`clio::cte::core::CLIO_CTE_CLIENT`). The URI's authority becomes a CTE Tag
(prefixed by `backends.clio_tag_prefix`, default `labios:`), and the URI path
becomes the Blob name within that tag — the same authority/path split
`KVBackend` uses for its key derivation.

It requires:

- Building labios with `-DLABIOS_ENABLE_CLIO_BACKEND=ON` and a discoverable
  clio-core install (`find_package(clio-core CONFIG REQUIRED)`, so pass
  `-DCMAKE_PREFIX_PATH=<clio-core install prefix>` if it isn't on the default
  search path).
- A running `clio_run` runtime (`clio_run start`), separate from the labios
  processes — the client auto-connects to it on first use, the same way
  `kv://` depends on a separately-running Redis-compatible service.

## Backend factory

`labios-worker` no longer hardcodes which backends to construct. At startup
it calls `labios::build_backend_registry(cfg, storage_root, sqlite_path,
kv_redis)` (`include/labios/backend/factory.h`), which registers each backend
according to the `[backends]` table in `labios.toml`:

```toml
[backends]
file_enabled = true
sqlite_enabled = true
kv_enabled = false
clio_enabled = false
clio_tag_prefix = "labios:"
```

`kv_enabled` also requires `LABIOS_KV_HOST`/`LABIOS_KV_PORT` to be set (the
Redis connection itself is still constructed by `main()`, since it must
outlive the registry). `clio_enabled` is a no-op unless the binary was built
with `LABIOS_ENABLE_CLIO_BACKEND`. Every field has a `LABIOS_BACKEND_*_ENABLED`
environment override following the existing env-overrides-file convention.

## Registering an adapter

`BackendRegistry::register_backend` accepts the backend value and derives its
scheme; it does not accept a separate name or `unique_ptr`:

```text
labios::BackendRegistry registry;
registry.register_backend(labios::PosixBackend("/srv/user-data"));
auto* backend = registry.resolve("file");
```

A new adapter follows the same shape:

```text
class MyBackend {
public:
    labios::BackendResult put(const labios::LabelData& label,
                              std::span<const std::byte> data);
    labios::BackendDataResult get(const labios::LabelData& label);
    labios::BackendResult del(const labios::LabelData& label);
    labios::BackendQueryResult query(const labios::LabelData& label);
    std::string_view scheme() { return "myscheme"; }
};

static_assert(labios::BackendStore<MyBackend>);
```

The adapter must preserve the label's operation, resource scope, version,
isolation, durability, and result semantics. It must return categorized detail
to the worker and must not perform scheduler optimization. Tests that emulate a
user service may add a Compose fixture, but that fixture remains external
backend infrastructure rather than runtime plumbing.
