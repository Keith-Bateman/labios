#pragma once
#include <labios/backend/backend.h>
#include <string>
#include <string_view>
#include <utility>

namespace labios {

/// Backend adapter over clio-core's Context Transfer Engine (clio:// scheme).
/// The dest/source URI authority becomes a CTE Tag (prefixed by
/// `tag_prefix`), and the URI path becomes the Blob name within that tag.
/// Requires a running `clio_run` runtime; the client auto-connects to it on
/// construction (see CLIO_CTE_CLIENT_INIT in clio_cte/core/core_client.h).
class ClioCoreBackend {
public:
    explicit ClioCoreBackend(std::string tag_prefix = "labios:");

    BackendResult put(const LabelData& label, std::span<const std::byte> data);
    BackendDataResult get(const LabelData& label);
    BackendResult del(const LabelData& label);
    BackendQueryResult query(const LabelData& label);
    std::string_view scheme() const { return "clio"; }

private:
    std::string tag_prefix_;
    std::pair<std::string, std::string> tag_and_blob(const LabelData& label) const;
};

static_assert(BackendStore<ClioCoreBackend>);

} // namespace labios
