#include <labios/backend/clio_backend.h>
#include <labios/uri.h>

#include <clio_cte/core/core_client.h>
#include <clio_runtime/ipc_manager.h>

#include <cstring>
#include <stdexcept>

namespace labios {

namespace cte = clio::cte::core;

ClioCoreBackend::ClioCoreBackend(std::string tag_prefix)
    : tag_prefix_(std::move(tag_prefix)) {
    if (!cte::CLIO_CTE_CLIENT_INIT()) {
        throw std::runtime_error(
            "ClioCoreBackend: failed to connect to clio_run runtime "
            "(is `clio_run start` running?)");
    }
}

std::pair<std::string, std::string> ClioCoreBackend::tag_and_blob(
    const LabelData& label) const {
    std::string uri = !label.dest_uri.empty() ? label.dest_uri : label.source_uri;
    if (uri.empty()) {
        return {tag_prefix_ + "default", std::to_string(label.id)};
    }
    auto parsed = parse_uri(uri);
    std::string tag = tag_prefix_ + (!parsed.authority.empty() ? parsed.authority : "default");
    std::string blob = parsed.path;
    if (!blob.empty() && blob.front() == '/') blob.erase(0, 1);
    if (blob.empty()) blob = std::to_string(label.id);
    return {tag, blob};
}

BackendResult ClioCoreBackend::put(const LabelData& label,
                                    std::span<const std::byte> data) {
    auto [tag_name, blob_name] = tag_and_blob(label);
    auto* client = cte::CLIO_CTE_CLIENT;

    auto tag_future = client->AsyncGetOrCreateTag(tag_name);
    tag_future.Wait();
    if (tag_future->GetReturnCode() != 0) {
        return {false, "clio: failed to get-or-create tag " + tag_name};
    }
    auto tag_id = tag_future->tag_id_;

    auto buf = CLIO_IPC->AllocateBuffer(data.size());
    if (!data.empty()) {
        std::memcpy(buf.ptr_, data.data(), data.size());
    }
    ctp::ipc::ShmPtr<> shm_data(buf.shm_);

    auto put_future = client->AsyncPutBlob(tag_id, blob_name, /*offset=*/0,
                                            data.size(), shm_data);
    put_future.Wait();
    CLIO_IPC->FreeBuffer(buf);

    if (put_future->GetReturnCode() != 0) {
        return {false, "clio: put failed for " + tag_name + "/" + blob_name};
    }
    return {};
}

BackendDataResult ClioCoreBackend::get(const LabelData& label) {
    auto [tag_name, blob_name] = tag_and_blob(label);
    auto* client = cte::CLIO_CTE_CLIENT;

    auto tag_future = client->AsyncGetOrCreateTag(tag_name);
    tag_future.Wait();
    if (tag_future->GetReturnCode() != 0) {
        return {false, "clio: tag not found: " + tag_name, {}};
    }
    auto tag_id = tag_future->tag_id_;

    auto size_future = client->AsyncGetBlobSize(tag_id, blob_name);
    size_future.Wait();
    if (size_future->GetReturnCode() != 0) {
        return {false, "clio: blob not found: " + tag_name + "/" + blob_name, {}};
    }
    auto size = size_future->size_;

    std::vector<std::byte> out(size);
    if (size > 0) {
        auto buf = CLIO_IPC->AllocateBuffer(size);
        ctp::ipc::ShmPtr<> shm_data(buf.shm_);

        auto get_future = client->AsyncGetBlob(tag_id, blob_name, /*offset=*/0,
                                                size, /*flags=*/0, shm_data);
        get_future.Wait();
        bool ok = get_future->GetReturnCode() == 0;
        if (ok) {
            std::memcpy(out.data(), buf.ptr_, size);
        }
        CLIO_IPC->FreeBuffer(buf);
        if (!ok) {
            return {false, "clio: get failed for " + tag_name + "/" + blob_name, {}};
        }
    }
    return {true, {}, std::move(out)};
}

BackendResult ClioCoreBackend::del(const LabelData& label) {
    auto [tag_name, blob_name] = tag_and_blob(label);
    auto* client = cte::CLIO_CTE_CLIENT;

    auto tag_future = client->AsyncGetOrCreateTag(tag_name);
    tag_future.Wait();
    if (tag_future->GetReturnCode() != 0) {
        return {false, "clio: tag not found: " + tag_name};
    }
    auto tag_id = tag_future->tag_id_;

    auto del_future = client->AsyncDelBlob(tag_id, blob_name);
    del_future.Wait();
    if (del_future->GetReturnCode() != 0) {
        return {false, "clio: del failed for " + tag_name + "/" + blob_name};
    }
    return {};
}

BackendQueryResult ClioCoreBackend::query(const LabelData& label) {
    auto [tag_name, blob_name] = tag_and_blob(label);
    (void)blob_name;
    auto* client = cte::CLIO_CTE_CLIENT;

    auto tag_future = client->AsyncGetOrCreateTag(tag_name);
    tag_future.Wait();
    if (tag_future->GetReturnCode() != 0) {
        return {false, "clio: tag not found: " + tag_name, "{}"};
    }
    auto tag_id = tag_future->tag_id_;

    auto list_future = client->AsyncGetContainedBlobs(tag_id);
    list_future.Wait();
    if (list_future->GetReturnCode() != 0) {
        return {false, "clio: failed to list blobs for " + tag_name, "{}"};
    }

    std::string json = "{\"keys\":[";
    const auto& names = list_future->blob_names_;
    for (size_t i = 0; i < names.size(); ++i) {
        if (i > 0) json += ",";
        json += "\"" + names[i] + "\"";
    }
    json += "]}";
    return {true, {}, json};
}

} // namespace labios
