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
#include <string>
#include <string_view>

#include "base/coding.h"
#include "common/config_lake_fwd.h"
#include "common/logging.h"
#include "common/storage_define.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "gen_cpp/lake_types.pb.h"
#include "http/action/lake/dump_tablet_metadata_serializer.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"
#include "storage/lake/lake_proto_normalizer.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/protobuf_file.h"
#include "storage/storage_env.h"
#include "storage/utils.h"

namespace starrocks::lake {
namespace {

constexpr uint64_t kMaxLogicalMetadataBytes = 16ULL << 20;
constexpr size_t kMaxResponseBytes = 32ULL << 20;
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

enum class MetadataLayout : uint8_t { kNonBundled, kBundled };

struct DumpTabletMetadataRequestContext {
    int64_t tablet_id = 0;
    int64_t version = 0;
    MetadataLayout layout = MetadataLayout::kNonBundled;
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

Status validate_metadata_identity(const TabletMetadataPB& metadata, int64_t tablet_id, int64_t version) {
    if (metadata.id() != tablet_id || metadata.version() != version) {
        return Status::Corruption("tablet metadata identity does not match the diagnostic request");
    }
    return Status::OK();
}

struct BoundedMetadataInput {
    std::unique_ptr<RandomAccessFile> file;
    int64_t size = 0;
};

StatusOr<BoundedMetadataInput> open_bounded_metadata_object(const std::shared_ptr<FileSystem>& fs,
                                                            const std::string& path, int64_t max_object_size) {
    if (max_object_size <= 0) {
        return Status::CapacityLimitExceed("dump_tablet_metadata physical object limit is not positive");
    }
    RandomAccessFileOptions options{.skip_fill_local_cache = true, .skip_disk_cache = true};
    ASSIGN_OR_RETURN(auto file, fs->new_random_access_file(options, path));
    ASSIGN_OR_RETURN(const int64_t size, file->get_size());
    if (size <= 0) {
        return Status::Corruption("tablet metadata object is empty");
    }
    if (size > max_object_size) {
        return Status::CapacityLimitExceed("tablet metadata object exceeds the diagnostic size limit");
    }
    return BoundedMetadataInput{std::move(file), size};
}

StatusOr<TabletMetadataPtr> read_standalone_metadata(const std::shared_ptr<FileSystem>& fs, const std::string& path,
                                                     int64_t tablet_id, int64_t version, bool remap_tablet_id,
                                                     int64_t max_object_size) {
    ASSIGN_OR_RETURN(auto input, open_bounded_metadata_object(fs, path, max_object_size));
    std::string content(static_cast<size_t>(input.size), '\0');
    RETURN_IF_ERROR(input.file->read_at_fully(0, content.data(), input.size));

    auto metadata = std::make_shared<TabletMetadataPB>();
    RETURN_IF_ERROR(ProtobufFileWithHeader::load_from_buffer(metadata.get(), content, LAKE_META_HEADER_MAGIC_NUMBER,
                                                             /*allow_plain_protobuf_fallback=*/true));
    if (metadata->version() != version || (!remap_tablet_id && metadata->id() != tablet_id)) {
        return Status::Corruption("tablet metadata identity does not match the diagnostic request");
    }
    normalize_tablet_metadata_after_load(metadata.get());
    if (remap_tablet_id) {
        metadata->set_id(tablet_id);
    }
    return metadata;
}

StatusOr<TabletMetadataPtr> read_bundle_metadata(BoundedMetadataInput input, const std::string& path, int64_t tablet_id,
                                                 int64_t version) {
    std::string content(static_cast<size_t>(input.size), '\0');
    RETURN_IF_ERROR(input.file->read_at_fully(0, content.data(), input.size));
    ASSIGN_OR_RETURN(auto bundle, TabletManager::parse_bundle_tablet_metadata(path, content));

    const uint64_t raw_footer_size =
            decode_fixed64_le(reinterpret_cast<const uint8_t*>(content.data() + content.size() - sizeof(uint64_t)));
    const bool checksummed = (raw_footer_size & LAKE_BUNDLE_META_CHECKSUM_FLAG) != 0;
    const uint64_t footer_size = raw_footer_size & ~LAKE_BUNDLE_META_CHECKSUM_FLAG;
    const uint64_t footer_suffix_size = sizeof(uint64_t) + (checksummed ? sizeof(uint32_t) : 0);
    if (content.size() < footer_suffix_size || footer_size > content.size() - footer_suffix_size) {
        return Status::Corruption("invalid tablet metadata bundle footer");
    }
    const uint64_t footer_offset = content.size() - footer_suffix_size - footer_size;

    const auto page_it = bundle->tablet_meta_pages().find(tablet_id);
    if (page_it == bundle->tablet_meta_pages().end()) {
        return Status::NotFound("tablet metadata is absent from the bundle");
    }
    const uint64_t offset = page_it->second.offset();
    const uint64_t size = page_it->second.size();
    if (size == 0 || offset > footer_offset || size > footer_offset - offset) {
        return Status::Corruption("invalid tablet metadata page in bundle");
    }
    const std::string_view page(content.data() + offset, size);
    const auto checksum_it = bundle->tablet_meta_page_checksum().find(tablet_id);
    if (checksum_it != bundle->tablet_meta_page_checksum().end() &&
        olap_adler32(ADLER32_INIT, page.data(), page.size()) != checksum_it->second) {
        return Status::Corruption("tablet metadata page checksum mismatch");
    }

    auto metadata = std::make_shared<TabletMetadataPB>();
    if (!metadata->ParseFromArray(page.data(), static_cast<int>(page.size()))) {
        return Status::Corruption("failed to parse tablet metadata page");
    }
    RETURN_IF_ERROR(validate_metadata_identity(*metadata, tablet_id, version));
    normalize_tablet_metadata_after_load(metadata.get());

    const auto schema_id_it = bundle->tablet_to_schema().find(tablet_id);
    if (schema_id_it == bundle->tablet_to_schema().end()) {
        return Status::Corruption("tablet schema mapping is absent from bundle metadata");
    }
    const auto schema_it = bundle->schemas().find(schema_id_it->second);
    if (schema_it == bundle->schemas().end()) {
        return Status::Corruption("tablet schema is absent from bundle metadata");
    }
    metadata->mutable_schema()->CopyFrom(schema_it->second);
    (*metadata->mutable_historical_schemas())[schema_id_it->second].CopyFrom(schema_it->second);
    force_cloud_native_pk_persistent_index(metadata.get());

    for (const auto& [_, historical_schema_id] : metadata->rowset_to_schema()) {
        const auto historical_schema_it = bundle->schemas().find(historical_schema_id);
        if (historical_schema_it == bundle->schemas().end()) {
            return Status::Corruption("historical tablet schema is absent from bundle metadata");
        }
        (*metadata->mutable_historical_schemas())[historical_schema_id].CopyFrom(historical_schema_it->second);
    }
    return metadata;
}

StatusOr<TabletMetadataPtr> read_exact_metadata(TabletManager* tablet_manager, int64_t tablet_id, int64_t version,
                                                MetadataLayout layout) {
    const std::string logical_path = tablet_manager->tablet_metadata_location(tablet_id, version);
    if (auto cached = tablet_manager->metacache()->lookup_tablet_metadata(logical_path); cached != nullptr) {
        RETURN_IF_ERROR(validate_metadata_identity(*cached, tablet_id, version));
        return cached;
    }

    const bool shared_initial = layout == MetadataLayout::kBundled && version == 1;
    const std::string physical_path =
            layout == MetadataLayout::kNonBundled
                    ? logical_path
                    : (shared_initial ? tablet_manager->tablet_initial_metadata_location(tablet_id)
                                      : tablet_manager->bundle_tablet_metadata_location(tablet_id, version));
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(physical_path));
    const int64_t max_object_size = config::lake_dump_tablet_metadata_max_object_size_bytes;

    if (layout == MetadataLayout::kNonBundled || shared_initial) {
        return read_standalone_metadata(fs, physical_path, tablet_id, version, shared_initial, max_object_size);
    }

    ASSIGN_OR_RETURN(auto input, open_bounded_metadata_object(fs, physical_path, max_object_size));
    return read_bundle_metadata(std::move(input), physical_path, tablet_id, version);
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

    auto metadata_or = read_exact_metadata(tablet_manager, request_context.tablet_id, request_context.version,
                                           request_context.layout);
    if (!metadata_or.ok()) {
        return metadata_or.status();
    }
    auto metadata = std::move(metadata_or).value();
    if (metadata->ByteSizeLong() > kMaxLogicalMetadataBytes) {
        return Status::CapacityLimitExceed("tablet metadata protobuf exceeds the diagnostic size limit");
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
        HttpChannel::send_reply_json(req, HttpStatus::OK, response);
    }
    return Status::OK();
    TRY_CATCH_ALLOC_SCOPE_END()
}

void send_pipeline_error(HttpRequest* req, const Status& status, PipelineStage stage, std::string_view* result_code) {
    if (status.is_capacity_limit_exceeded() || status.is_mem_limit_exceeded()) {
        *result_code = "METADATA_TOO_LARGE";
        send_json(req, HttpStatus::BAD_REQUEST, kMetadataTooLargeBody);
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
    const auto start = std::chrono::steady_clock::now();
    const auto& query = req->query_params();
    int64_t tablet_id = 0;
    int64_t version = 0;
    const auto version_it = query.find("version");
    const auto bundle_it = query.find("is_bundle");
    if (query.size() != 2 || version_it == query.end() || bundle_it == query.end() ||
        !parse_positive_decimal(req->param("TabletId"), &tablet_id) ||
        !parse_positive_decimal(version_it->second, &version) ||
        (bundle_it->second != "true" && bundle_it->second != "false")) {
        send_json(req, HttpStatus::BAD_REQUEST, kInvalidArgumentBody);
        return -1;
    }

    auto context = std::make_unique<DumpTabletMetadataRequestContext>();
    context->tablet_id = tablet_id;
    context->version = version;
    context->layout = bundle_it->second == "true" ? MetadataLayout::kBundled : MetadataLayout::kNonBundled;
    if (!context->admission.set_limiter(&_limiter)) {
        send_json(req, HttpStatus::SERVICE_UNAVAILABLE, kBusyBody);
        const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
        LOG(INFO) << "dump_tablet_metadata tablet_id=" << tablet_id << " version=" << version
                  << " result=DIAGNOSTIC_BUSY elapsed_ms=" << elapsed_ms << " busy=true";
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
    const int64_t tablet_id = context->tablet_id;
    const int64_t version = context->version;

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
    LOG(INFO) << "dump_tablet_metadata tablet_id=" << tablet_id << " version=" << version << " result=" << result_code
              << " elapsed_ms=" << elapsed_ms << " busy=false";
}

void DumpTabletMetadataAction::free_handler_ctx(void* handler_ctx) {
    delete static_cast<DumpTabletMetadataRequestContext*>(handler_ctx);
}

} // namespace starrocks::lake
