#pragma once
#include <labios/backend/registry.h>
#include <labios/config.h>
#include <labios/transport/redis.h>

#include <filesystem>

namespace labios {

/// Builds a BackendRegistry from Config::backends, registering PosixBackend,
/// SQLiteBackend, KVBackend, and (when built with LABIOS_HAVE_CLIO_BACKEND)
/// ClioCoreBackend according to which schemes are enabled.
///
/// `kv_redis` must outlive the returned registry when kv_enabled; pass
/// nullptr to skip kv:// regardless of config (e.g. no KV connection info).
/// `sqlite_path` names the SQLite database file used for sqlite://.
BackendRegistry build_backend_registry(
    const Config& cfg,
    const std::filesystem::path& storage_root,
    const std::filesystem::path& sqlite_path,
    transport::RedisConnection* kv_redis);

} // namespace labios
