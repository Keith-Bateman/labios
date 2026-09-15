#include <labios/backend/factory.h>

#include <labios/backend/kv_backend.h>
#include <labios/backend/posix_backend.h>
#include <labios/backend/sqlite_backend.h>

#ifdef LABIOS_HAVE_CLIO_BACKEND
#include <labios/backend/clio_backend.h>
#endif

namespace labios {

BackendRegistry build_backend_registry(
    const Config& cfg,
    const std::filesystem::path& storage_root,
    const std::filesystem::path& sqlite_path,
    transport::RedisConnection* kv_redis) {
    BackendRegistry backends;

    if (cfg.backends.file_enabled) {
        backends.register_backend(PosixBackend(storage_root));
    }

    if (cfg.backends.sqlite_enabled) {
        backends.register_backend(SQLiteBackend(sqlite_path.string()));
    }

    if (cfg.backends.kv_enabled && kv_redis != nullptr) {
        backends.register_backend(KVBackend(*kv_redis));
    }

#ifdef LABIOS_HAVE_CLIO_BACKEND
    if (cfg.backends.clio_enabled) {
        backends.register_backend(ClioCoreBackend(cfg.backends.clio_tag_prefix));
    }
#endif

    return backends;
}

} // namespace labios
