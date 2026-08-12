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

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>

#include "common/logging.h"
#include "http/action/lake/dump_tablet_metadata_serializer.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"
#include "storage/lake/exact_tablet_metadata_reader.h"
#include "storage/lake/tablet_manager.h"
#include "storage/storage_env.h"

namespace starrocks::lake {
namespace {

constexpr uint64_t kMaxMetadataBytes = 16ULL << 20;
constexpr uint64_t kMaxBundleFooterBytes = 16ULL << 20;
constexpr size_t kMaxResponseBytes = 64ULL << 20;
constexpr int64_t kMaxTrackedMemoryBytes = 256LL << 20;

constexpr std::string_view kInvalidArgumentBody =
        R"({"code":"INVALID_ARGUMENT","message":"invalid diagnostic request"})";
constexpr std::string_view kMetadataNotFoundBody =
        R"({"code":"METADATA_NOT_FOUND","message":"tablet metadata is unavailable"})";
constexpr std::string_view kMetadataTooLargeBody =
        R"({"code":"METADATA_TOO_LARGE","message":"tablet metadata exceeds a diagnostic limit"})";
constexpr std::string_view kStorageReadFailedBody =
        R"({"code":"STORAGE_READ_FAILED","message":"tablet metadata storage read failed"})";
constexpr std::string_view kBusyBody =
        R"({"code":"DIAGNOSTIC_BUSY","message":"another tablet metadata diagnostic is active"})";
constexpr std::string_view kCorruptMetadataBody =
        R"({"code":"CORRUPT_METADATA","message":"tablet metadata is corrupt"})";
constexpr std::string_view kSerializationFailedBody =
        R"({"code":"SERIALIZATION_FAILED","message":"tablet metadata serialization failed"})";

struct DumpTabletMetadataRequestContext {
    int64_t tablet_id = 0;
    int64_t version = 0;
    TabletMetadataStorageFormat format = TabletMetadataStorageFormat::kStandalone;
    ConcurrentLimiterGuard admission;

    DumpTabletMetadataRequestContext() = default;
    DumpTabletMetadataRequestContext(const DumpTabletMetadataRequestContext&) = delete;
    DumpTabletMetadataRequestContext& operator=(const DumpTabletMetadataRequestContext&) = delete;
};

enum class PipelineStage : uint8_t { kRead, kSerialize };

void add_diagnostic_headers(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CACHE_CONTROL, "no-store");
    req->add_output_header("X-Content-Type-Options", "nosniff");
}

void send_json(HttpRequest* req, HttpStatus status, std::string_view body) {
    add_diagnostic_headers(req);
    HttpChannel::send_reply_json(req, status, body);
}

bool parse_positive_decimal(std::string_view value, int64_t* result) {
    if (value.empty()) {
        return false;
    }
    uint64_t parsed = 0;
    constexpr uint64_t kMax = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    for (const unsigned char ch : value) {
        if (ch < '0' || ch > '9') {
            return false;
        }
        const uint64_t digit = ch - '0';
        if (parsed > (kMax - digit) / 10) {
            return false;
        }
        parsed = parsed * 10 + digit;
    }
    if (parsed == 0) {
        return false;
    }
    *result = static_cast<int64_t>(parsed);
    return true;
}

Status run_pipeline(HttpRequest* req, TabletManager* tablet_manager,
                    const DumpTabletMetadataRequestContext& request_context, PipelineStage* stage) {
    MemTracker* prior_tracker = CurrentThread::mem_tracker();
    MemTracker request_tracker(kMaxTrackedMemoryBytes, "dump_tablet_metadata",
                               RuntimeEnv::GetInstance()->process_mem_tracker());
    SCOPED_THREAD_LOCAL_MEM_SETTER(&request_tracker, true);

    TRY_CATCH_ALLOC_SCOPE_START()
    if (tablet_manager == nullptr) {
        return Status::ServiceUnavailable("lake tablet manager is unavailable");
    }

    ExactTabletMetadataReader reader(tablet_manager->location_provider(), {kMaxMetadataBytes, kMaxBundleFooterBytes});
    auto metadata_or = reader.read(request_context.tablet_id, request_context.version, request_context.format);
    if (!metadata_or.ok()) {
        return metadata_or.status();
    }
    auto metadata = std::move(metadata_or).value();
    if (metadata->SpaceUsedLong() > kMaxTrackedMemoryBytes) {
        return Status::CapacityLimitExceed("tablet metadata protobuf exceeds the diagnostic memory limit");
    }

    *stage = PipelineStage::kSerialize;
    auto json_or = serialize_dump_tablet_metadata(*metadata, kMaxResponseBytes);
    if (!json_or.ok()) {
        return json_or.status();
    }
    auto response = std::move(json_or).value();
    add_diagnostic_headers(req);
    {
        CurrentThreadMemTrackerSetter restore_caller_tracker(prior_tracker);
        HttpChannel::send_reply_json(req, HttpStatus::OK, response.body);
    }
    return Status::OK();
    TRY_CATCH_ALLOC_SCOPE_END()
}

void send_pipeline_error(HttpRequest* req, const Status& status, PipelineStage stage, std::string_view* result_code) {
    if (status.is_capacity_limit_exceeded() || status.is_mem_limit_exceeded()) {
        *result_code = "METADATA_TOO_LARGE";
        send_json(req, HttpStatus::REQUEST_ENTITY_TOO_LARGE, kMetadataTooLargeBody);
    } else if (stage == PipelineStage::kSerialize) {
        *result_code = "SERIALIZATION_FAILED";
        send_json(req, HttpStatus::INTERNAL_SERVER_ERROR, kSerializationFailedBody);
    } else if (status.is_not_found()) {
        *result_code = "METADATA_NOT_FOUND";
        send_json(req, HttpStatus::NOT_FOUND, kMetadataNotFoundBody);
    } else if (status.is_corruption() || status.is_invalid_argument()) {
        *result_code = "CORRUPT_METADATA";
        send_json(req, HttpStatus::INTERNAL_SERVER_ERROR, kCorruptMetadataBody);
    } else {
        *result_code = "STORAGE_READ_FAILED";
        send_json(req, HttpStatus::BAD_GATEWAY, kStorageReadFailedBody);
    }
}

} // namespace

int DumpTabletMetadataAction::on_header(HttpRequest* req) {
    const auto& query = req->query_params();
    int64_t tablet_id = 0;
    int64_t version = 0;
    const auto version_it = query.find("version");
    const auto bundle_it = query.find("is_bundle");
    if (query.size() != 2 || version_it == query.end() || bundle_it == query.end() ||
        req->query_param_count("version") != 1 || req->query_param_count("is_bundle") != 1 ||
        !parse_positive_decimal(req->route_param("TabletId"), &tablet_id) ||
        !parse_positive_decimal(version_it->second, &version) ||
        (bundle_it->second != "true" && bundle_it->second != "false") ||
        (version == 1 && bundle_it->second == "true")) {
        send_json(req, HttpStatus::BAD_REQUEST, kInvalidArgumentBody);
        return -1;
    }

    auto context = std::make_unique<DumpTabletMetadataRequestContext>();
    context->tablet_id = tablet_id;
    context->version = version;
    context->format = bundle_it->second == "true" ? TabletMetadataStorageFormat::kBundle
                                                  : TabletMetadataStorageFormat::kStandalone;
    if (!context->admission.set_limiter(&_limiter)) {
        send_json(req, HttpStatus::SERVICE_UNAVAILABLE, kBusyBody);
        LOG(INFO) << "dump_tablet_metadata tablet_id=" << tablet_id << " version=" << version
                  << " result=DIAGNOSTIC_BUSY busy=true";
        return -1;
    }

    req->set_handler_ctx(context.release());
    return 0;
}

void DumpTabletMetadataAction::handle(HttpRequest* req) {
    const auto start = std::chrono::steady_clock::now();
    auto* context = static_cast<DumpTabletMetadataRequestContext*>(req->handler_ctx());
    if (context == nullptr) {
        send_json(req, HttpStatus::BAD_REQUEST, kInvalidArgumentBody);
        return;
    }

    TabletManager* tablet_manager = _tablet_manager;
    if (tablet_manager == nullptr) {
        tablet_manager = StorageEnv::GetInstance()->lake_tablet_manager();
    }
    PipelineStage stage = PipelineStage::kRead;
    std::string_view result_code = "OK";
    const Status status = run_pipeline(req, tablet_manager, *context, &stage);
    if (!status.ok()) {
        send_pipeline_error(req, status, stage, &result_code);
    }
    const auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    LOG(INFO) << "dump_tablet_metadata tablet_id=" << context->tablet_id << " version=" << context->version
              << " result=" << result_code << " elapsed_ms=" << elapsed_ms << " busy=false";
}

void DumpTabletMetadataAction::free_handler_ctx(void* handler_ctx) {
    delete static_cast<DumpTabletMetadataRequestContext*>(handler_ctx);
}

} // namespace starrocks::lake
