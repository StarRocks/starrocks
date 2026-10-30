// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "http/action/lake/dump_tablet_metadata_action.h"

#include <fmt/format.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "base/status.h"
#include "base/string/string_parser.hpp"
#include "common/config_lake_fwd.h"
#include "common/logging.h"
#include "gen_cpp/lake_types.pb.h"
#include "http/action/lake/dump_tablet_metadata_serializer.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/storage_env.h"

namespace starrocks::lake {
namespace {

std::string serialize_status(const Status& status) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    const std::string code = status.code_as_string();
    const std::string_view message = status.message();
    writer.StartObject();
    writer.Key("status");
    writer.String(code.data(), static_cast<rapidjson::SizeType>(code.size()));
    writer.Key("message");
    writer.String(message.empty() ? "" : message.data(), static_cast<rapidjson::SizeType>(message.size()));
    writer.EndObject();
    return {buffer.GetString(), buffer.GetSize()};
}

void send_status_response(HttpRequest* req, const Status& status) {
    HttpChannel::send_reply_json(req, HttpStatus::OK, serialize_status(status));
}

struct DumpTabletMetadataRequestContext {
    int64_t tablet_id = 0;
    int64_t version = 0;
    int64_t memory_limit_bytes = 0;
    int64_t json_size_limit_bytes = 0;
    std::atomic<int32_t>* active_requests = nullptr;

    DumpTabletMetadataRequestContext() = default;
    ~DumpTabletMetadataRequestContext() {
        if (active_requests != nullptr) {
            active_requests->fetch_sub(1, std::memory_order_acq_rel);
        }
    }
    DumpTabletMetadataRequestContext(const DumpTabletMetadataRequestContext&) = delete;
    DumpTabletMetadataRequestContext& operator=(const DumpTabletMetadataRequestContext&) = delete;
};

bool parse_positive_int64(std::string_view value, int64_t* result) {
    StringParser::ParseResult parse_result;
    const int64_t parsed =
            StringParser::string_to_int<int64_t>(value.data(), static_cast<int>(value.size()), &parse_result);
    if (parse_result != StringParser::PARSE_SUCCESS || parsed <= 0) {
        return false;
    }
    *result = parsed;
    return true;
}

bool try_acquire_admission(std::atomic<int32_t>* active_requests, int32_t max_concurrency,
                           int32_t* active_at_rejection) {
    int32_t active = active_requests->load(std::memory_order_relaxed);
    while (active < max_concurrency) {
        if (active_requests->compare_exchange_weak(active, active + 1, std::memory_order_acq_rel,
                                                   std::memory_order_relaxed)) {
            return true;
        }
    }
    *active_at_rejection = active;
    return false;
}

StatusOr<std::string> prepare_cached_metadata(TabletManager* tablet_manager,
                                              const DumpTabletMetadataRequestContext& request_context) {
    if (tablet_manager == nullptr) {
        return Status::ServiceUnavailable("lake tablet manager is unavailable on this compute node");
    }

    MemTracker request_tracker(
            request_context.memory_limit_bytes,
            fmt::format("dump_tablet_metadata-{}-{}", request_context.tablet_id, request_context.version),
            RuntimeEnv::GetInstance()->process_mem_tracker());
    SCOPED_THREAD_LOCAL_MEM_SETTER(&request_tracker, true);

    TRY_CATCH_ALLOC_SCOPE_START()
    const std::string key =
            tablet_manager->tablet_metadata_location(request_context.tablet_id, request_context.version);
    auto metadata = tablet_manager->metacache()->lookup_tablet_metadata(key);
    if (metadata == nullptr) {
        return Status::NotFound(
                "tablet metadata is not cached on this compute node; this API only inspects the current compute "
                "node's in-memory metadata cache. To inspect metadata in object storage, download the file with "
                "the AWS CLI and parse it with meta_tool");
    }
    if (metadata->id() != request_context.tablet_id || metadata->version() != request_context.version) {
        return Status::Corruption(fmt::format(
                "cached tablet metadata identity does not match the request: requested tablet_id={} version={}, "
                "cached tablet_id={} version={}",
                request_context.tablet_id, request_context.version, metadata->id(), metadata->version()));
    }

    auto json_or = serialize_dump_tablet_metadata(*metadata);
    if (!json_or.ok()) {
        LOG(WARNING) << "failed to serialize cached tablet metadata tablet_id=" << request_context.tablet_id
                     << " version=" << request_context.version << " status=" << json_or.status();
        return json_or.status();
    }
    auto response = std::move(json_or).value();
    if (response.size() > static_cast<size_t>(request_context.json_size_limit_bytes)) {
        return Status::CapacityLimitExceed(
                fmt::format("JSON size limit is {} bytes", request_context.json_size_limit_bytes));
    }
    return response;
    TRY_CATCH_ALLOC_SCOPE_END()
}

Status send_cached_metadata(HttpRequest* req, TabletManager* tablet_manager,
                            const DumpTabletMetadataRequestContext& request_context) {
    auto response_or = prepare_cached_metadata(tablet_manager, request_context);
    if (!response_or.ok()) {
        if (response_or.status().is_mem_limit_exceeded()) {
            LOG(WARNING) << "memory limit exceeded while preparing tablet metadata diagnostic tablet_id="
                         << request_context.tablet_id << " version=" << request_context.version
                         << " status=" << response_or.status();
            return Status::MemoryLimitExceeded(
                    fmt::format("per-request memory limit is {} bytes", request_context.memory_limit_bytes));
        }
        if (response_or.status().code() == TStatusCode::RUNTIME_ERROR) {
            LOG(WARNING) << "runtime error while preparing tablet metadata diagnostic tablet_id="
                         << request_context.tablet_id << " version=" << request_context.version
                         << " status=" << response_or.status();
            return Status::RuntimeError("tablet metadata diagnostic failed unexpectedly");
        }
        return response_or.status();
    }

    HttpChannel::send_reply_json(req, HttpStatus::OK, response_or.value());
    return Status::OK();
}

} // namespace

int DumpTabletMetadataAction::on_header(HttpRequest* req) {
    const auto start = std::chrono::steady_clock::now();
    const auto& query = req->query_params();
    int64_t tablet_id = 0;
    int64_t version = 0;
    if (!parse_positive_int64(req->param("TabletId"), &tablet_id)) {
        send_status_response(req, Status::InvalidArgument("TabletId must be a positive 64-bit integer"));
        return -1;
    }

    const auto version_it = query.find("version");
    if (version_it == query.end()) {
        send_status_response(req,
                             Status::InvalidArgument(query.empty() ? "version query parameter is required"
                                                                   : "only the version query parameter is supported"));
        return -1;
    }
    if (query.size() != 1) {
        send_status_response(req, Status::InvalidArgument("only the version query parameter is supported"));
        return -1;
    }
    if (!parse_positive_int64(version_it->second, &version)) {
        send_status_response(req, Status::InvalidArgument("version must be a positive 64-bit integer"));
        return -1;
    }

    auto context = std::make_unique<DumpTabletMetadataRequestContext>();
    context->tablet_id = tablet_id;
    context->version = version;
    context->memory_limit_bytes = config::lake_dump_tablet_metadata_per_request_memory_limit_bytes;
    context->json_size_limit_bytes = config::lake_dump_tablet_metadata_per_request_json_size_limit_bytes;
    const int32_t max_concurrency = config::lake_dump_tablet_metadata_max_concurrency;
    auto reject_invalid_config = [req](std::string_view config_name, int64_t config_value) {
        send_status_response(req, Status::ServiceUnavailable(fmt::format(
                                          "configuration {} has value {}, but the minimum allowed value is 1",
                                          config_name, config_value)));
    };
    if (context->memory_limit_bytes <= 0) {
        reject_invalid_config("lake_dump_tablet_metadata_per_request_memory_limit_bytes", context->memory_limit_bytes);
        return -1;
    }
    if (context->json_size_limit_bytes <= 0) {
        reject_invalid_config("lake_dump_tablet_metadata_per_request_json_size_limit_bytes",
                              context->json_size_limit_bytes);
        return -1;
    }
    if (max_concurrency <= 0) {
        reject_invalid_config("lake_dump_tablet_metadata_max_concurrency", max_concurrency);
        return -1;
    }
    int32_t active_at_rejection = 0;
    if (!try_acquire_admission(&_active_requests, max_concurrency, &active_at_rejection)) {
        const Status status = Status::ResourceBusy(
                fmt::format("tablet metadata diagnostic has {} active requests, reaching the configured maximum of {}",
                            active_at_rejection, max_concurrency));
        send_status_response(req, status);
        const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
        LOG(INFO) << "dump_tablet_metadata tablet_id=" << tablet_id << " version=" << version
                  << " result=" << status.code_as_string() << " elapsed_ms=" << elapsed_ms << " busy=true";
        return -1;
    }
    context->active_requests = &_active_requests;

    req->set_handler_ctx(context.release());
    return 0;
}

void DumpTabletMetadataAction::handle(HttpRequest* req) {
    const auto start = std::chrono::steady_clock::now();
    auto* context = static_cast<DumpTabletMetadataRequestContext*>(req->handler_ctx());
    if (context == nullptr) {
        send_status_response(req, Status::InternalError("tablet metadata diagnostic request context is missing"));
        return;
    }
    const int64_t tablet_id = context->tablet_id;
    const int64_t version = context->version;

    TabletManager* tablet_manager = _tablet_manager;
    if (tablet_manager == nullptr) {
        tablet_manager = StorageEnv::GetInstance()->lake_tablet_manager();
    }
    Status result = send_cached_metadata(req, tablet_manager, *context);
    if (!result.ok()) {
        send_status_response(req, result);
    }
    const auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    LOG(INFO) << "dump_tablet_metadata tablet_id=" << tablet_id << " version=" << version << " result=" << result
              << " elapsed_ms=" << elapsed_ms << " busy=false";
}

void DumpTabletMetadataAction::free_handler_ctx(void* handler_ctx) {
    delete static_cast<DumpTabletMetadataRequestContext*>(handler_ctx);
}

} // namespace starrocks::lake
