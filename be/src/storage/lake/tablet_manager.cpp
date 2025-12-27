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

#include "storage/lake/tablet_manager.h"

#include <butil/time.h>
#include <bvar/bvar.h>

#include <atomic>
#include <utility>

#include "agent/master_info.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "exec/schema_scanner/schema_be_tablets_scanner.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/strings/util.h"
#include "storage/lake/cloud_native_index_compaction_task.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/filenames.h"
#include "storage/lake/horizontal_compaction_task.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/options.h"
#include "storage/lake/table_schema_service.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/vacuum.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/lake/vertical_compaction_task.h"
#include "storage/metadata_util.h"
#include "storage/protobuf_file.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema_map.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"
#include "util/failpoint/fail_point.h"
#include "util/raw_container.h"
#include "util/trace.h"

// TODO: Eliminate the explicit dependency on staros worker
#ifdef USE_STAROS
#include "service/staros_worker.h"
#endif

namespace starrocks::lake {
static bvar::LatencyRecorder g_get_tablet_metadata_latency("lake", "get_tablet_metadata");
static bvar::LatencyRecorder g_put_tablet_metadata_latency("lake", "put_tablet_metadata");
static bvar::LatencyRecorder g_get_txn_log_latency("lake", "get_txn_log");
static bvar::LatencyRecorder g_put_txn_log_latency("lake", "put_txn_log");
static bvar::LatencyRecorder g_del_txn_log_latency("lake", "del_txn_log");
static bvar::Adder<int64_t> g_read_bundle_tablet_meta_cnt("lake", "lake_read_bundle_tablet_meta_cnt");
static bvar::Adder<int64_t> g_read_bundle_tablet_meta_real_access_cnt("lake",
                                                                      "lake_read_bundle_tablet_meta_real_access_cnt");
static bvar::LatencyRecorder g_read_bundle_tablet_meta_latency("lake", "lake_read_bundle_tablet_meta_latency");

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
static std::pair<int64_t, int64_t> get_table_partition_id(const staros::starlet::ShardInfo& shard_info) {
    const auto& properties = shard_info.properties;

    int64_t table_id = -1;
    auto table_id_iter = properties.find("tableId");
    if (table_id_iter != properties.end()) {
        table_id = std::atol(table_id_iter->second.data());
    }

    int64_t partition_id = -1;
    auto partition_id_iter = properties.find("partitionId");
    if (partition_id_iter != properties.end()) {
        partition_id = std::atol(partition_id_iter->second.data());
    }

    return std::make_pair(table_id, partition_id);
}
#endif

TabletManager::TabletManager(std::shared_ptr<LocationProvider> location_provider, UpdateManager* update_mgr,
                             int64_t cache_capacity)
        : _location_provider(std::move(location_provider)),
          _metacache(std::make_unique<Metacache>(cache_capacity)),
          _compaction_scheduler(std::make_unique<CompactionScheduler>(this)),
          _update_mgr(update_mgr),
          _table_schema_service(std::make_unique<TableSchemaService>(this)) {
    _update_mgr->set_tablet_mgr(this);
}

TabletManager::TabletManager(std::shared_ptr<LocationProvider> location_provider, int64_t cache_capacity)
        : _location_provider(std::move(location_provider)),
          _metacache(std::make_unique<Metacache>(cache_capacity)),
          _table_schema_service(std::make_unique<TableSchemaService>(this)) {}

TabletManager::~TabletManager() = default;

std::string TabletManager::tablet_root_location(int64_t tablet_id) const {
    return _location_provider->root_location(tablet_id);
}

std::string TabletManager::real_tablet_root_location(int64_t tablet_id) const {
    auto location_or = _location_provider->real_location(tablet_root_location(tablet_id));
    return location_or.ok() ? location_or.value() : "";
}

std::string TabletManager::tablet_metadata_root_location(int64_t tablet_id) const {
    return _location_provider->metadata_root_location(tablet_id);
}

std::string TabletManager::tablet_metadata_location(int64_t tablet_id, int64_t version) const {
    return _location_provider->tablet_metadata_location(tablet_id, version);
}

std::string TabletManager::tablet_initial_metadata_location(int64_t tablet_id) const {
    return _location_provider->tablet_initial_metadata_location(tablet_id);
}

std::string TabletManager::bundle_tablet_metadata_location(int64_t tablet_id, int64_t version) const {
    return _location_provider->bundle_tablet_metadata_location(tablet_id, version);
}

std::string TabletManager::txn_log_location(int64_t tablet_id, int64_t txn_id) const {
    return _location_provider->txn_log_location(tablet_id, txn_id);
}

std::string TabletManager::txn_log_location(int64_t tablet_id, int64_t txn_id, const PUniqueId& load_id) const {
    return _location_provider->txn_log_location(tablet_id, txn_id, load_id);
}

std::string TabletManager::combined_txn_log_location(int64_t tablet_id, int64_t txn_id) const {
    return _location_provider->combined_txn_log_location(tablet_id, txn_id);
}

std::string TabletManager::txn_slog_location(int64_t tablet_id, int64_t txn_id) const {
    return _location_provider->txn_slog_location(tablet_id, txn_id);
}

std::string TabletManager::txn_vlog_location(int64_t tablet_id, int64_t version) const {
    return _location_provider->txn_vlog_location(tablet_id, version);
}

std::string TabletManager::segment_location(int64_t tablet_id, std::string_view segment_name) const {
    return _location_provider->segment_location(tablet_id, segment_name);
}

std::string TabletManager::del_location(int64_t tablet_id, std::string_view del_name) const {
    return _location_provider->del_location(tablet_id, del_name);
}

std::string TabletManager::delvec_location(int64_t tablet_id, std::string_view delvec_name) const {
    return _location_provider->delvec_location(tablet_id, delvec_name);
}

std::string TabletManager::sst_location(int64_t tablet_id, std::string_view sst_name) const {
    return _location_provider->sst_location(tablet_id, sst_name);
}

std::string TabletManager::global_schema_cache_key(int64_t schema_id) {
    return fmt::format("GS{}", schema_id);
}

std::string TabletManager::tablet_schema_cache_key(int64_t tablet_id) {
    return fmt::format("TS{}", tablet_id);
}

std::string TabletManager::tablet_latest_metadata_cache_key(int64_t tablet_id) {
    return fmt::format("TL{}", tablet_id);
}

Status TabletManager::drop_local_cache(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return fs->drop_local_cache(path);
}

// current lru cache does not support updating value size, so use refill to update.
void TabletManager::update_segment_cache_size(std::string_view key, size_t mem_cost, intptr_t segment_addr_hint) {
    TEST_SYNC_POINT_CALLBACK("lake::TabletManager::update_segment_cache_size", nullptr);
    _metacache->cache_segment_if_present(key, mem_cost, segment_addr_hint);
}

void TabletManager::prune_metacache() {
    _metacache->prune();
}

UpdateManager* TabletManager::update_mgr() {
    return _update_mgr;
}

Status TabletManager::create_tablet(const TCreateTabletReq& req) {
    // generate tablet metadata pb
    auto tablet_metadata_pb = std::make_shared<TabletMetadataPB>();
    tablet_metadata_pb->set_id(req.tablet_id);
    tablet_metadata_pb->set_version(kInitialVersion);
    tablet_metadata_pb->set_next_rowset_id(1);
    tablet_metadata_pb->set_cumulative_point(0);
    tablet_metadata_pb->set_gtid(req.gtid);

    if (req.__isset.enable_persistent_index) {
        tablet_metadata_pb->set_enable_persistent_index(req.enable_persistent_index);
        if (req.__isset.persistent_index_type) {
            switch (req.persistent_index_type) {
            case TPersistentIndexType::LOCAL:
                tablet_metadata_pb->set_persistent_index_type(PersistentIndexTypePB::LOCAL);
                break;
            case TPersistentIndexType::CLOUD_NATIVE:
                tablet_metadata_pb->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
                break;
            default:
                return Status::InternalError(
                        strings::Substitute("Unknown persistent index type, tabletId:$0", req.tablet_id));
            }
        }
    }

    if (req.__isset.flat_json_config) {
        FlatJsonConfig flat_json_config;
        flat_json_config.update(req.flat_json_config);
        flat_json_config.to_pb(tablet_metadata_pb->mutable_flat_json_config());
    }

    if (req.__isset.compaction_strategy) {
        switch (req.compaction_strategy) {
        case TCompactionStrategy::DEFAULT:
            tablet_metadata_pb->set_compaction_strategy(CompactionStrategyPB::DEFAULT);
            break;
        case TCompactionStrategy::REAL_TIME:
            tablet_metadata_pb->set_compaction_strategy(CompactionStrategyPB::REAL_TIME);
            break;
        default:
            return Status::InternalError(
                    strings::Substitute("Unknown compaction strategy, tabletId:$0", req.tablet_id));
        }
    } else {
        tablet_metadata_pb->set_compaction_strategy(CompactionStrategyPB::DEFAULT);
    }

    auto compress_type = req.__isset.compression_type ? req.compression_type : TCompressionType::LZ4_FRAME;
    RETURN_IF_ERROR(
            convert_t_schema_to_pb_schema(req.tablet_schema, compress_type, tablet_metadata_pb->mutable_schema()));
    auto compession_level = req.__isset.compression_level ? req.compression_level : -1;
    tablet_metadata_pb->mutable_schema()->set_compression_level(compession_level);
    if (req.create_schema_file) {
        RETURN_IF_ERROR(create_schema_file(req.tablet_id, tablet_metadata_pb->schema()));
    }

    if (req.enable_tablet_creation_optimization) {
        return put_tablet_metadata(std::move(tablet_metadata_pb), tablet_initial_metadata_location(req.tablet_id));
    }

    return put_tablet_metadata(std::move(tablet_metadata_pb));
}

StatusOr<Tablet> TabletManager::get_tablet(int64_t tablet_id) {
    Tablet tablet(this, tablet_id);
    if (auto metadata = get_latest_cached_tablet_metadata(tablet_id); metadata != nullptr) {
        tablet.set_version_hint(metadata->version());
    }
    return tablet;
}

int64_t TabletManager::get_average_row_size_from_latest_metadata(int64_t tablet_id) {
    auto metadata = get_latest_cached_tablet_metadata(tablet_id);
    if (metadata == nullptr) {
        return 0;
    }
    // calc avg row size by rowsets
    int64_t total_size = 0;
    int64_t total_rows = 0;
    // Pick 10 rowsets is enough
    int rowset_count = 0;
    for (const auto& rowset : metadata->rowsets()) {
        if (rowset_count++ >= 10) {
            break;
        }
        // `data_size()` is compressed, so multiply by 3 to get uncompressed size
        total_size += rowset.data_size() * 3;
        total_rows += rowset.num_rows();
    }
    TEST_SYNC_POINT_CALLBACK("TabletManager::get_average_row_size_from_latest_metadata", &total_size);
    return total_rows == 0 ? 0 : total_size / total_rows;
}

Status TabletManager::put_tablet_metadata(const TabletMetadataPtr& metadata, const std::string& metadata_location) {
    TEST_ERROR_POINT("TabletManager::put_tablet_metadata");
    // write metadata file
    auto t0 = butil::gettimeofday_us();

    ProtobufFile file(metadata_location);
    RETURN_IF_ERROR(file.save(*metadata));

    _metacache->cache_tablet_metadata(metadata_location, metadata);
    bool skip_cache_latest_metadata = false;
    TEST_SYNC_POINT_CALLBACK("TabletManager::skip_cache_latest_metadata", &skip_cache_latest_metadata);
    if (skip_cache_latest_metadata) {
        return Status::OK();
    }
    _metacache->cache_tablet_metadata(tablet_latest_metadata_cache_key(metadata->id()), metadata);

    auto t1 = butil::gettimeofday_us();
    g_put_tablet_metadata_latency << (t1 - t0);
    TRACE("end write tablet metadata");
    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const TabletMetadataPtr& metadata) {
    return put_tablet_metadata(metadata, tablet_metadata_location(metadata->id(), metadata->version()));
}

Status TabletManager::cache_tablet_metadata(const TabletMetadataPtr& metadata) {
    auto metadata_location = tablet_metadata_location(metadata->id(), metadata->version());
    if (auto ptr = _metacache->lookup_tablet_metadata(metadata_location); ptr != nullptr) {
        return Status::OK();
    }
    _metacache->cache_tablet_metadata(metadata_location, metadata);
    _metacache->cache_tablet_metadata(tablet_latest_metadata_cache_key(metadata->id()), metadata);
    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const TabletMetadata& metadata) {
    auto metadata_ptr = std::make_shared<TabletMetadata>(metadata);
    return put_tablet_metadata(std::move(metadata_ptr));
}

DEFINE_FAIL_POINT(get_real_location_failed);
DEFINE_FAIL_POINT(tablet_meta_not_found);
// NOTE: tablet_metas is non-const and we will clear schemas for optimization.
// Callers should ensure thread safety.
Status TabletManager::put_bundle_tablet_metadata(std::map<int64_t, TabletMetadataPB>& tablet_metas) {
    TEST_ERROR_POINT("TabletManager::put_bundle_tablet_metadata");
    if (tablet_metas.empty()) {
        return Status::InternalError("tablet_metas cannot be empty");
    }

    BundleTabletMetadataPB bundle_meta;
    ASSIGN_OR_RETURN(auto partition_location,
                     _location_provider->real_location(tablet_metadata_root_location(tablet_metas.begin()->first)));
    std::unordered_map<int64_t, TabletSchemaPB> unique_schemas;
    for (auto& [tablet_id, meta] : tablet_metas) {
        (*bundle_meta.mutable_tablet_to_schema())[tablet_id] = meta.schema().id();
        unique_schemas.emplace(meta.schema().id(), meta.schema());
        for (const auto& [schema_id, schema] : meta.historical_schemas()) {
            unique_schemas.emplace(schema_id, schema);
        }
    }

    for (auto& [schema_id, schema] : unique_schemas) {
        (*bundle_meta.mutable_schemas())[schema_id] = std::move(schema);
    }

    auto make_page_pointer = [](int64_t offset, int64_t size) {
        PagePointerPB pointer;
        pointer.set_offset(offset);
        pointer.set_size(size);
        return pointer;
    };

    const std::string meta_location =
            bundle_tablet_metadata_location(tablet_metas.begin()->first, tablet_metas.begin()->second.version());

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(meta_location));
    WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto meta_file, fs->new_writable_file(opts, meta_location));
    std::string serialized_buf;
    int64_t current_offset = 0;
    for (auto& [tablet_id, meta] : tablet_metas) {
        meta.clear_schema();
        meta.mutable_historical_schemas()->clear();
        serialized_buf.clear();
        if (!meta.SerializeToString(&serialized_buf)) {
            return Status::InternalError("Failed to serialize tablet metadata");
        }

        (*bundle_meta.mutable_tablet_meta_pages())[tablet_id] =
                make_page_pointer(current_offset, serialized_buf.size());
        RETURN_IF_ERROR(meta_file->append(Slice(serialized_buf)));
        current_offset += serialized_buf.size();
    }

    serialized_buf.clear();
    if (!bundle_meta.SerializeToString(&serialized_buf)) {
        return Status::IOError("Failed to write shared metadata header");
    }
    RETURN_IF_ERROR(meta_file->append(Slice(serialized_buf)));
    std::string fixed_buf;
    put_fixed64_le(&fixed_buf, serialized_buf.size());
    RETURN_IF_ERROR(meta_file->append(Slice(fixed_buf)));
    RETURN_IF_ERROR(meta_file->close());
    _metacache->cache_aggregation_partition(partition_location, true);
    return Status::OK();
}

Status TabletManager::corrupted_tablet_meta_handler(const Status& s, const std::string& metadata_location) {
    if (s.is_corruption() && config::lake_clear_corrupted_cache_meta) {
        auto drop_status = drop_local_cache(metadata_location);
        TEST_SYNC_POINT_CALLBACK("TabletManager::corrupted_tablet_meta_handler", &drop_status);
        if (!drop_status.ok()) {
            LOG(WARNING) << "clear corrupted cache for " << metadata_location << " failed, "
                         << "error: " << drop_status;
            return s; // return error so load tablet meta can be retried
        }
        LOG(INFO) << "clear corrupted cache for " << metadata_location;
        return Status::OK();
    } else {
        return s;
    }
}

StatusOr<TabletMetadataPtr> TabletManager::load_tablet_metadata(const string& metadata_location, bool fill_data_cache,
                                                                int64_t expected_gtid,
                                                                const std::shared_ptr<FileSystem>& fs) {
    TEST_ERROR_POINT("TabletManager::load_tablet_metadata");
    auto t0 = butil::gettimeofday_us();
    auto metadata = std::make_shared<TabletMetadataPB>();
    ProtobufFile file(metadata_location, fs);
    auto s = file.load(metadata.get(), fill_data_cache);
    if (!s.ok()) {
        RETURN_IF_ERROR(corrupted_tablet_meta_handler(s, metadata_location));
        // reset metadata
        metadata = std::make_shared<TabletMetadataPB>();
        // read again
        RETURN_IF_ERROR(file.load(metadata.get(), fill_data_cache));
    }

    if (expected_gtid > 0 && metadata->gtid() > 0 && expected_gtid != metadata->gtid()) {
        auto drop_status = drop_local_cache(metadata_location);
        if (!drop_status.ok()) {
            LOG(WARNING) << "clear dirty cache for " << metadata_location << " failed, "
                         << "error: " << drop_status;
            return drop_status;
        }
        LOG(INFO) << "clear dirty cache for " << metadata_location;
        return Status::NotFound("Not found expected tablet metadata");
    }

    g_get_tablet_metadata_latency << (butil::gettimeofday_us() - t0);
    return metadata;
}

TabletMetadataPtr TabletManager::get_latest_cached_tablet_metadata(int64_t tablet_id) {
    return _metacache->lookup_tablet_metadata(tablet_latest_metadata_cache_key(tablet_id));
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(int64_t tablet_id, int64_t version, bool fill_cache,
                                                               int64_t expected_gtid,
                                                               const std::shared_ptr<FileSystem>& fs) {
    CacheOptions cache_opts{.fill_meta_cache = fill_cache, .fill_data_cache = fill_cache};
    return get_tablet_metadata(tablet_id, version, cache_opts, expected_gtid, fs);
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(int64_t tablet_id, int64_t version, bool fill_meta_cache,
                                                               bool fill_data_cache, int64_t expected_gtid,
                                                               const std::shared_ptr<FileSystem>& fs) {
    CacheOptions cache_opts{.fill_meta_cache = fill_meta_cache, .fill_data_cache = fill_data_cache};
    return get_tablet_metadata(tablet_id, version, cache_opts, expected_gtid, fs);
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(int64_t tablet_id, int64_t version,
                                                               const CacheOptions& cache_opts, int64_t expected_gtid,
                                                               const std::shared_ptr<FileSystem>& fs) {
    TEST_ERROR_POINT("TabletManager::get_tablet_metadata");
    StatusOr<TabletMetadataPtr> tablet_metadata_or;
    auto cache_key = _location_provider->real_location(tablet_metadata_root_location(tablet_id));
    if (cache_key.ok() && _metacache->lookup_aggregation_partition(*cache_key)) {
        tablet_metadata_or = get_single_tablet_metadata(tablet_id, version, cache_opts, expected_gtid, fs);
        if (tablet_metadata_or.status().is_not_found()) {
            tablet_metadata_or =
                    get_tablet_metadata(tablet_metadata_location(tablet_id, version), cache_opts, expected_gtid, fs);
        }
    } else {
        tablet_metadata_or =
                get_tablet_metadata(tablet_metadata_location(tablet_id, version), cache_opts, expected_gtid, fs);
    }

    if (!tablet_metadata_or.ok()) {
        return tablet_metadata_or.status();
    }

    auto tablet_metadata = std::make_shared<TabletMetadata>(*tablet_metadata_or.value());
    tablet_metadata->set_id(tablet_id);
    return tablet_metadata;
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const string& path, bool fill_cache,
                                                               int64_t expected_gtid,
                                                               const std::shared_ptr<FileSystem>& fs) {
    CacheOptions cache_opts{.fill_meta_cache = fill_cache, .fill_data_cache = fill_cache};
    return get_tablet_metadata(path, cache_opts, expected_gtid, fs);
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const string& path, const CacheOptions& cache_opts,
                                                               int64_t expected_gtid,
                                                               const std::shared_ptr<FileSystem>& fs) {
    if (auto ptr = _metacache->lookup_tablet_metadata(path); ptr != nullptr) {
        TRACE("got cached tablet metadata");
        return ptr;
    }
    StatusOr<TabletMetadataPtr> metadata_or;
    auto [tablet_id, version] = parse_tablet_metadata_filename(basename(path));
    auto cache_key = _location_provider->real_location(tablet_metadata_root_location(tablet_id));
    if (cache_key.ok() && _metacache->lookup_aggregation_partition(*cache_key)) {
        metadata_or = get_single_tablet_metadata(tablet_id, version, cache_opts, expected_gtid, fs);
        if (metadata_or.status().is_not_found()) {
            metadata_or = load_tablet_metadata(path, cache_opts.fill_data_cache, expected_gtid, fs);
        }
    } else {
        metadata_or = load_tablet_metadata(path, cache_opts.fill_data_cache, expected_gtid, fs);
        if (metadata_or.status().is_not_found()) {
            metadata_or = get_single_tablet_metadata(tablet_id, version, cache_opts, expected_gtid, fs);
            if (metadata_or.ok() && cache_key.ok()) {
                _metacache->cache_aggregation_partition(*cache_key, true);
            }
        }
    }

    if (metadata_or.status().is_not_found() && tablet_id != 0 && version == kInitialVersion) {
        // If the metadata is not found, we will try to read the initial metadata at least
        std::string new_path = join_path(prefix_name(path), tablet_initial_metadata_filename());
        metadata_or = load_tablet_metadata(new_path, cache_opts.fill_data_cache, expected_gtid, fs);
        // set tablet id for initial metadata
        if (metadata_or.ok()) {
            auto metadata = const_cast<starrocks::TabletMetadataPB*>(metadata_or.value().get());
            metadata->set_id(tablet_id);
        }
    }

    if (!metadata_or.ok()) {
        return metadata_or.status();
    }

    if (cache_opts.fill_meta_cache) {
        _metacache->cache_tablet_metadata(path, metadata_or.value());
    }
    TRACE("end read tablet metadata");
    return metadata_or.value();
}

StatusOr<BundleTabletMetadataPtr> TabletManager::parse_bundle_tablet_metadata(const std::string& path,
                                                                              const std::string& serialized_string) {
    auto file_size = serialized_string.size();
    auto footer_size = sizeof(uint64_t);
    auto bundle_metadata_size = decode_fixed64_le((uint8_t*)(serialized_string.data() + file_size - footer_size));
    RETURN_IF(file_size < footer_size + bundle_metadata_size || bundle_metadata_size == 0,
              Status::Corruption(strings::Substitute(
                      "deserialized shared metadata($0) failed, file_size($1), bundle_metadata_size($2)", path,
                      file_size, bundle_metadata_size)));

    auto bundle_metadata = std::make_shared<BundleTabletMetadataPB>();
    std::string_view bundle_metadata_str =
            std::string_view(serialized_string.data() + file_size - footer_size - bundle_metadata_size);
    RETURN_IF(!bundle_metadata->ParseFromArray(bundle_metadata_str.data(), bundle_metadata_size),
              Status::Corruption(strings::Substitute("deserialized shared metadata failed")));
#ifdef BE_TEST
    bool inject_error = false;
    TEST_SYNC_POINT_CALLBACK("TabletManager::parse_bundle_tablet_metadata::corruption", &inject_error);
    if (inject_error) {
        return Status::Corruption("injected error");
    }
#endif
    return bundle_metadata;
}

StatusOr<TabletMetadataPtrs> TabletManager::get_metas_from_bundle_tablet_metadata(const std::string& location,
                                                                                  FileSystem* input_fs) {
    std::unique_ptr<RandomAccessFile> input_file;
    RandomAccessFileOptions opts{.skip_fill_local_cache = true};
    if (input_fs == nullptr) {
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(location));
        ASSIGN_OR_RETURN(input_file, fs->new_random_access_file(opts, location));
    } else {
        ASSIGN_OR_RETURN(input_file, input_fs->new_random_access_file(opts, location));
    }
    ASSIGN_OR_RETURN(auto serialized_string, input_file->read_all());

    auto file_size = serialized_string.size();
    ASSIGN_OR_RETURN(auto bundle_metadata, TabletManager::parse_bundle_tablet_metadata(location, serialized_string));
    TabletMetadataPtrs metadatas;
    metadatas.reserve(bundle_metadata->tablet_meta_pages().size());
    for (const auto& tablet_page : bundle_metadata->tablet_meta_pages()) {
        const PagePointerPB& page_pointer = tablet_page.second;
        auto offset = page_pointer.offset();
        auto size = page_pointer.size();
        RETURN_IF(offset + size > file_size,
                  Status::InternalError(
                          fmt::format("Invalid page pointer for tablet {}, offset: {}, size: {}, file size: {}",
                                      tablet_page.first, offset, size, file_size)));

        auto metadata = std::make_shared<starrocks::TabletMetadataPB>();
        std::string_view metadata_str = std::string_view(serialized_string.data() + offset);
        RETURN_IF(
                !metadata->ParseFromArray(metadata_str.data(), size),
                Status::InternalError(fmt::format("Failed to parse tablet metadata for tablet {}, offset: {}, size: {}",
                                                  tablet_page.first, offset, size)));
        RETURN_IF(metadata->id() != tablet_page.first,
                  Status::InternalError(fmt::format("Tablet ID mismatch in bundle metadata, expected: {}, found: {}",
                                                    tablet_page.first, metadata->id())));
        metadatas.push_back(std::move(metadata));
    }
    return metadatas;
}

StatusOr<TabletMetadataPtr> TabletManager::get_single_tablet_metadata(int64_t tablet_id, int64_t version,
                                                                      bool fill_cache, int64_t expected_gtid,
                                                                      const std::shared_ptr<FileSystem>& fs) {
    CacheOptions cache_opts{.fill_meta_cache = fill_cache, .fill_data_cache = fill_cache};
    return get_single_tablet_metadata(tablet_id, version, cache_opts, expected_gtid, fs);
}

DEFINE_FAIL_POINT(tablet_schema_not_found_in_bundle_metadata);
StatusOr<TabletMetadataPtr> TabletManager::get_single_tablet_metadata(int64_t tablet_id, int64_t version,
                                                                      const CacheOptions& cache_opts,
                                                                      int64_t expected_gtid,
                                                                      const std::shared_ptr<FileSystem>& fs) {
    auto tablet_path = tablet_metadata_location(tablet_id, version);
    if (auto ptr = _metacache->lookup_tablet_metadata(tablet_path); ptr != nullptr) {
        return ptr;
    }
    if (version == kInitialVersion) {
        return Status::NotFound("Not found expected tablet metadata");
    }
    auto path = bundle_tablet_metadata_location(tablet_id, version);
    ASSIGN_OR_RETURN(auto real_path, _location_provider->real_location(path));
    std::shared_ptr<FileSystem> file_system;
    if (!fs) {
        ASSIGN_OR_RETURN(file_system, FileSystem::CreateSharedFromString(path));
    } else {
        file_system = fs;
    }
    RandomAccessFileOptions opts{.skip_fill_local_cache = !cache_opts.fill_data_cache};
    // TODO(zhangqiang)
    // `read_all` only need to one api call and not increase the IOPS
    // but it will incur additional IO bandwidth overhead
    // Perhaps we need to consider the additional costs of IO bandwidth and IOPS later.
    g_read_bundle_tablet_meta_cnt << 1;
    auto t0 = butil::gettimeofday_us();
    // use real path as key, so that every tablet can share a same path of bundle tablet meta.
    ASSIGN_OR_RETURN(auto serialized_string,
                     _bundle_tablet_metadata_group.Do(real_path, [&]() -> StatusOr<std::string> {
                         g_read_bundle_tablet_meta_real_access_cnt << 1;
                         ASSIGN_OR_RETURN(auto input_file, file_system->new_random_access_file(opts, path));
                         return input_file->read_all();
                     }));
    g_read_bundle_tablet_meta_latency << (butil::gettimeofday_us() - t0);

    auto file_size = serialized_string.size();
    BundleTabletMetadataPtr bundle_metadata;
    auto bundle_metadata_status = parse_bundle_tablet_metadata(path, serialized_string);
    if (!bundle_metadata_status.ok()) {
        RETURN_IF_ERROR(corrupted_tablet_meta_handler(bundle_metadata_status.status(), path));
        // read bundle metadata again
        ASSIGN_OR_RETURN(auto input_file, file_system->new_random_access_file(opts, path));
        ASSIGN_OR_RETURN(serialized_string, input_file->read_all());
        file_size = serialized_string.size();
        ASSIGN_OR_RETURN(bundle_metadata, parse_bundle_tablet_metadata(path, serialized_string));
    } else {
        bundle_metadata = bundle_metadata_status.value();
    }

    auto meta_it = bundle_metadata->tablet_meta_pages().find(tablet_id);
    size_t offset = 0;
    size_t size = 0;
    if (meta_it == bundle_metadata->tablet_meta_pages().end()) {
        return Status::Corruption(strings::Substitute("can not find tablet $0 from shared tablet metadata", tablet_id));
    } else {
        const PagePointerPB& page_pointer = meta_it->second;
        offset = page_pointer.offset();
        size = page_pointer.size();
    }

    if (file_size < offset + size) {
        return Status::Corruption(
                strings::Substitute("deserialized shared metadata($0) failed, file_size($1) too small($2/$3)", path,
                                    file_size, offset, size));
    }

    auto metadata = std::make_shared<TabletMetadataPB>();
    std::string_view metadata_str = std::string_view(serialized_string.data() + offset);
    if (!metadata->ParseFromArray(metadata_str.data(), size)) {
        auto corrupted_status =
                Status::Corruption(strings::Substitute("deserialized tablet $0 metadata failed", tablet_id));
        (void)corrupted_tablet_meta_handler(corrupted_status, path);
        return corrupted_status;
    }

    FAIL_POINT_TRIGGER_EXECUTE(tablet_schema_not_found_in_bundle_metadata, { tablet_id = 10003; });
    auto schema_id = bundle_metadata->tablet_to_schema().find(tablet_id);
    if (schema_id == bundle_metadata->tablet_to_schema().end()) {
        return Status::Corruption(
                strings::Substitute("tablet $0 metadata can not find schema_id in shared metadata", tablet_id));
    }
    auto schema_it = bundle_metadata->schemas().find(schema_id->second);
    if (schema_it == bundle_metadata->schemas().end()) {
        return Status::Corruption(strings::Substitute("tablet $0 metadata can not find schema($1) in shared metadata",
                                                      tablet_id, schema_id->second));
    } else {
        metadata->mutable_schema()->CopyFrom(schema_it->second);
        auto& item = (*metadata->mutable_historical_schemas())[schema_id->second];
        item.CopyFrom(schema_it->second);
    }

    for (auto& [_, schema_id] : metadata->rowset_to_schema()) {
        schema_it = bundle_metadata->schemas().find(schema_id);
        if (schema_it == bundle_metadata->schemas().end()) {
            return Status::Corruption(strings::Substitute(
                    "tablet $0 metadata can not find schema($1) in shared metadata", tablet_id, schema_id));
        } else {
            auto& item = (*metadata->mutable_historical_schemas())[schema_id];
            item.CopyFrom(schema_it->second);
        }
    }

    if (expected_gtid > 0 && metadata->gtid() > 0 && expected_gtid != metadata->gtid()) {
        auto drop_status = drop_local_cache(tablet_path);
        if (!drop_status.ok()) {
            LOG(WARNING) << "clear dirty cache for " << tablet_path << " failed, "
                         << "error: " << drop_status;
            return drop_status;
        }
        LOG(INFO) << "clear dirty cache for " << tablet_path;
        return Status::NotFound("Not found expected tablet metadata");
    }

    if (cache_opts.fill_meta_cache) {
        _metacache->cache_tablet_metadata(tablet_path, metadata);
    }

    return metadata;
}

Status TabletManager::delete_tablet_metadata(int64_t tablet_id, int64_t version) {
    auto location = tablet_metadata_location(tablet_id, version);
    _metacache->erase(location);
    if (version <= kInitialVersion) {
        return ignore_not_found(fs::delete_file(location));
    }
    return fs::delete_file(location);
}

StatusOr<TabletMetadataIter> TabletManager::list_tablet_metadata(int64_t tablet_id) {
    std::set<std::string> objects;
    // TODO: construct prefix in LocationProvider
    std::string prefix = fmt::format("{:016X}_", tablet_id);

    auto root = _location_provider->metadata_root_location(tablet_id);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root));
    auto scan_cb = [&](std::string_view name) {
        if (HasPrefixString(name, prefix)) {
            objects.insert(join_path(root, name));
        }
        return true;
    };

    RETURN_IF_ERROR(fs->iterate_dir(root, scan_cb));

    if (objects.empty()) {
        // Put tablet initial metadata
        objects.insert(join_path(root, tablet_initial_metadata_filename()));
    }

    return TabletMetadataIter{this, tablet_id, std::move(objects)};
}

StatusOr<TxnLogPtr> TabletManager::load_txn_log(const std::string& txn_log_path, bool fill_cache) {
    TEST_ERROR_POINT("TabletManager::load_txn_log");
    auto t0 = butil::gettimeofday_us();
    auto meta = std::make_shared<TxnLog>();
    ProtobufFile file(txn_log_path);
    RETURN_IF_ERROR(file.load(meta.get(), fill_cache));
    auto t1 = butil::gettimeofday_us();
    g_get_txn_log_latency << (t1 - t0);
    return std::move(meta);
}

StatusOr<TxnLogPtr> TabletManager::get_txn_vlog(const std::string& path, bool fill_cache) {
    TEST_ERROR_POINT("TabletManager::get_txn_vlog");
    return get_txn_log(path, fill_cache);
}

StatusOr<TxnLogPtr> TabletManager::get_txn_slog(const std::string& path, bool fill_cache) {
    TEST_ERROR_POINT("TabletManager::get_txn_slog");
    return get_txn_log(path, fill_cache);
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(const std::string& path, bool fill_cache) {
    if (auto ptr = _metacache->lookup_txn_log(path); ptr != nullptr) {
        TRACE("got cached txn log");
        return ptr;
    }
    ASSIGN_OR_RETURN(auto ptr, load_txn_log(path, fill_cache));
    if (fill_cache) {
        _metacache->cache_txn_log(path, ptr);
    }
    TRACE("end load txn log");
    return ptr;
}

StatusOr<CombinedTxnLogPtr> TabletManager::load_combined_txn_log(const std::string& path, bool fill_cache) {
    TEST_ERROR_POINT("TabletManager::get_combined_txn_log");
    auto log = std::make_shared<CombinedTxnLogPB>();
    ProtobufFile file(path);
    RETURN_IF_ERROR(file.load(log.get(), fill_cache));
    if (fill_cache) {
        _metacache->cache_combined_txn_log(path, log);
    }
    return log;
}

StatusOr<CombinedTxnLogPtr> TabletManager::get_combined_txn_log(const std::string& path, bool fill_cache) {
    ASSIGN_OR_RETURN(auto cache_key, _location_provider->real_location(path));
    if (auto ptr = _metacache->lookup_combined_txn_log(cache_key); ptr != nullptr) {
        TRACE("got cached combined txn log");
        return ptr;
    }
    return _combined_txn_log_group.Do(cache_key, [&]() { return load_combined_txn_log(path, fill_cache); });
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(int64_t tablet_id, int64_t txn_id) {
    return get_txn_log(txn_log_location(tablet_id, txn_id));
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(int64_t tablet_id, int64_t txn_id, const PUniqueId& load_id) {
    return get_txn_log(txn_log_location(tablet_id, txn_id, load_id));
}

StatusOr<TxnLogPtr> TabletManager::get_txn_slog(int64_t tablet_id, int64_t txn_id) {
    return get_txn_log(txn_slog_location(tablet_id, txn_id));
}

StatusOr<TxnLogPtr> TabletManager::get_txn_vlog(int64_t tablet_id, int64_t version) {
    return get_txn_log(txn_vlog_location(tablet_id, version), false);
}

Status TabletManager::put_txn_log(const TxnLogPtr& log, const std::string& path) {
    if (UNLIKELY(!log->has_tablet_id())) {
        return Status::InvalidArgument("txn log does not have tablet id");
    }
    if (UNLIKELY(!log->has_txn_id())) {
        return Status::InvalidArgument("txn log does not have txn id");
    }
    auto t0 = butil::gettimeofday_us();

    ProtobufFile file(path);
    RETURN_IF_ERROR(file.save(*log));

    _metacache->cache_txn_log(path, log);

    VLOG(2) << "put log path " << path << " log " << log->DebugString();

    auto t1 = butil::gettimeofday_us();
    g_put_txn_log_latency << (t1 - t0);
    return Status::OK();
}

Status TabletManager::put_txn_log(const TxnLogPtr& log) {
    if (log->has_load_id()) {
        return put_txn_log(log, txn_log_location(log->tablet_id(), log->txn_id(), log->load_id()));
    } else {
        return put_txn_log(log, txn_log_location(log->tablet_id(), log->txn_id()));
    }
}

Status TabletManager::put_txn_log(const TxnLog& log) {
    return put_txn_log(std::make_shared<TxnLog>(log));
}

Status TabletManager::put_txn_slog(const TxnLogPtr& log) {
    return put_txn_log(log, txn_slog_location(log->tablet_id(), log->txn_id()));
}

Status TabletManager::put_txn_slog(const TxnLogPtr& log, const std::string& path) {
    return put_txn_log(log, path);
}

Status TabletManager::put_txn_vlog(const TxnLogPtr& log, int64_t version) {
    return put_txn_log(log, txn_vlog_location(log->tablet_id(), version));
}

DEFINE_FAIL_POINT(put_combined_txn_log_success);
DEFINE_FAIL_POINT(put_combined_txn_log_fail);
Status TabletManager::put_combined_txn_log(const starrocks::CombinedTxnLogPB& logs) {
    FAIL_POINT_TRIGGER_RETURN(put_combined_txn_log_success, Status::OK());
    FAIL_POINT_TRIGGER_RETURN(put_combined_txn_log_fail, Status::InternalError("write combined_txn_log_fail"));
    if (UNLIKELY(logs.txn_logs_size() == 0)) {
        return Status::InvalidArgument("empty CombinedTxnLogPB");
    }
    auto tablet_id = logs.txn_logs(0).tablet_id();
    auto txn_id = logs.txn_logs(0).txn_id();
#ifndef NDEBUG
    // Ensure that all tablets belongs to the same partition.
    auto partition_id = logs.txn_logs(0).partition_id();
    for (const auto& log : logs.txn_logs()) {
        DCHECK(log.has_tablet_id());
        DCHECK(log.has_partition_id());
        DCHECK(log.has_txn_id());
        DCHECK_EQ(partition_id, log.partition_id());
        DCHECK_EQ(txn_id, log.txn_id());
    }
#endif
    auto path = _location_provider->combined_txn_log_location(tablet_id, txn_id);
    ProtobufFile file(path);
    return file.save(logs);
}

StatusOr<int64_t> TabletManager::get_tablet_data_size(int64_t tablet_id, int64_t* version_hint) {
    int64_t size = 0;
    TabletMetadataPtr metadata;
    if (version_hint != nullptr && *version_hint > 0) {
        ASSIGN_OR_RETURN(metadata, get_tablet_metadata(tablet_id, *version_hint));
        for (const auto& rowset : metadata->rowsets()) {
            size += rowset.data_size();
        }
        VLOG(2) << "get tablet " << tablet_id << " data size from version hint: " << *version_hint
                << ", size: " << size;
    } else {
        ASSIGN_OR_RETURN(TabletMetadataIter metadata_iter, list_tablet_metadata(tablet_id));
        if (!metadata_iter.has_next()) {
            return Status::NotFound(fmt::format("tablet {} metadata not found", tablet_id));
        }
        ASSIGN_OR_RETURN(metadata, metadata_iter.next());
        if (version_hint != nullptr) {
            *version_hint = metadata->version();
        }
        for (const auto& rowset : metadata->rowsets()) {
            size += rowset.data_size();
        }
        VLOG(2) << "get tablet " << tablet_id << " data size from version : " << metadata->version()
                << ", size: " << size;
    }

    return size;
}

StatusOr<int64_t> TabletManager::get_tablet_num_rows(int64_t tablet_id, int64_t version) {
    DCHECK(version != 0);
    int64_t num_rows = 0;
    TabletMetadataPtr metadata;
    ASSIGN_OR_RETURN(metadata, get_tablet_metadata(tablet_id, version));

    for (const auto& rowset : metadata->rowsets()) {
        num_rows += rowset.num_rows();
    }
    VLOG(2) << "get tablet " << tablet_id << " num_rows from version hint: " << version << ", num_rows: " << num_rows;

    return num_rows;
}

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
bool TabletManager::is_tablet_in_worker(int64_t tablet_id) {
    bool in_worker = true;
    if (g_worker != nullptr) {
        auto shard_info_or = g_worker->get_shard_info(tablet_id);
        if (absl::IsNotFound(shard_info_or.status())) {
            in_worker = false;
        }
    }
    TEST_SYNC_POINT_CALLBACK("is_tablet_in_worker:1", &in_worker);
    // think the tablet is assigned to this worker by default,
    // for we may take action if tablet is not in the worker
    return in_worker;
}
#endif // USE_STAROS

StatusOr<TabletSchemaPtr> TabletManager::get_tablet_schema(int64_t tablet_id, int64_t* version_hint) {
    // 1. direct lookup in cache, if there is schema info for the tablet
    auto cache_key = tablet_schema_cache_key(tablet_id);
    auto ptr = _metacache->lookup_tablet_schema(cache_key);
    RETURN_IF(ptr != nullptr, ptr);

    // Cache miss, load tablet metadata from remote storage use the hint version
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    // TODO: Eliminate the explicit dependency on staros worker
    // 2. leverage `indexId` to lookup the global_schema from cache and if missing from file.
    if (g_worker != nullptr) {
        auto shard_info_or = g_worker->retrieve_shard_info(tablet_id);
        if (shard_info_or.ok()) {
            const auto& shard_info = shard_info_or.value();
            const auto& properties = shard_info.properties;
            auto index_id_iter = properties.find("indexId");
            if (index_id_iter != properties.end()) {
                auto index_id = std::atol(index_id_iter->second.data());
                auto res = get_tablet_schema_by_id(tablet_id, index_id);
                if (res.ok()) {
                    return res;
                } else if (res.status().is_not_found()) {
                    // version 3.0 does not have schema file, ignore this error.
                } else {
                    return res.status();
                }
            } else {
                // no "indexId" property, will extract the tablet schema from the tablet metadata.
            }
        }
    }
#endif // USE_STAROS

    // 3. use version_hint to look from cache, and if miss, load from file
    TabletMetadataPtr metadata;
    if (version_hint != nullptr && *version_hint > 0) {
        if (auto res = get_tablet_metadata(tablet_id, *version_hint); res.ok()) {
            metadata = std::move(res).value();
        }
    }

    // 4. version hint not works, get tablet metadata by list directory. The most expensive way!
    if (metadata == nullptr) {
        // TODO: limit the list size
        ASSIGN_OR_RETURN(TabletMetadataIter metadata_iter, list_tablet_metadata(tablet_id));
        if (!metadata_iter.has_next()) {
            return Status::NotFound(fmt::format("tablet {} metadata not found", tablet_id));
        }
        ASSIGN_OR_RETURN(metadata, metadata_iter.next());
        if (version_hint != nullptr) {
            *version_hint = metadata->version();
        }
    }

    auto [schema, inserted] = GlobalTabletSchemaMap::Instance()->emplace(metadata->schema());
    if (UNLIKELY(schema == nullptr)) {
        return Status::InternalError(fmt::format("tablet schema {} failed to emplace in TabletSchemaMap", tablet_id));
    }

    auto cache_size = inserted ? schema->mem_usage() : 0;
    _metacache->cache_tablet_schema(cache_key, schema, cache_size);

    return schema;
}

StatusOr<TabletSchemaPtr> TabletManager::get_tablet_schema_by_id(int64_t tablet_id, int64_t schema_id) {
    auto global_cache_key = global_schema_cache_key(schema_id);
    auto schema = _metacache->lookup_tablet_schema(global_cache_key);
    TEST_SYNC_POINT_CALLBACK("get_tablet_schema_by_id.1", &schema);
    if (schema != nullptr) {
        return schema;
    }
    // else: Cache miss, read the schema file
    auto schema_file_path = join_path(tablet_root_location(tablet_id), schema_filename(schema_id));
    auto schema_or = _schema_group.Do(global_cache_key, [&]() { return load_and_parse_schema_file(schema_file_path); });
    //                                ^^^^^^^^^^^^^^^^ Do not use "schema_file_path" as the key for singleflight, as
    // our path is a virtual path rather than a real path (when the same file is accessed by different tablets, the
    // "schema_file_path" here is different), so using "schema_file_path" cannot achieve optimal effect.
    TEST_SYNC_POINT_CALLBACK("get_tablet_schema_by_id.2", &schema_or);
    if (schema_or.ok()) {
        schema = std::move(schema_or).value();
        // Save the schema into the in-memory cache, use the schema id as the cache key
        _metacache->cache_tablet_schema(global_cache_key, schema, 0 /*TODO*/);
        return std::move(schema);
    } else {
        return schema_or.status();
    }
}

// We will select the max version tablet schema as the output rowset schema.
// If table is non-primary key table, the last input rowset should be the latest rowset.
// However, if table is primary key table, the last input rowset maybe generated by compaction and is not
// generated by the latest tablet schema.
// So we will check the schema version of all input rowsets and select the latest tablet schema.
StatusOr<TabletSchemaPtr> TabletManager::get_output_rowset_schema(std::vector<uint32_t>& input_rowset,
                                                                  const TabletMetadata* metadata) {
    if (metadata->rowset_to_schema().empty() || metadata->schema().keys_type() == PRIMARY_KEYS ||
        input_rowset.size() <= 0) {
        // We can't pick schema from input rowset because when do column mode partial update, it will lost
        // latest add column data. And alsp primary key table doesn't support partial segment compaction now,
        // so we can use latest schema as compaction schema safely.
        return GlobalTabletSchemaMap::Instance()->emplace(metadata->schema()).first;
    }
    TabletSchemaPtr tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(metadata->schema()).first;
    struct Finder {
        uint32_t id;
        bool operator()(const RowsetMetadata& r) const { return r.id() == id; }
    };

    auto input_rowsets_num = input_rowset.size();
    int64_t max_schema_version = -1;
    int64_t select_schema_id = tablet_schema->id();
    for (int i = 0; i < input_rowsets_num; i++) {
        auto iter = std::find_if(metadata->rowsets().begin(), metadata->rowsets().end(), Finder{input_rowset[i]});
        if (UNLIKELY(iter == metadata->rowsets().end())) {
            return Status::InternalError(fmt::format("input rowset {} not found", input_rowset[i]));
        }
        auto rowset_it = metadata->rowset_to_schema().find(input_rowset[i]);
        if (rowset_it != metadata->rowset_to_schema().end()) {
            auto schema_it = metadata->historical_schemas().find(rowset_it->second);
            if (schema_it != metadata->historical_schemas().end()) {
                if (schema_it->second.schema_version() >= max_schema_version) {
                    max_schema_version = schema_it->second.schema_version();
                    select_schema_id = schema_it->first;
                }
            } else {
                return Status::InternalError(fmt::format("can not find input rowset schema, id {}", rowset_it->second));
            }
        } else {
            // if downgrade to old version and ingeste some rowset and upgrade again, the rowset maybe not found in
            // `rowset_to_schema`. And we will consider it as the latest tablet schema.
            max_schema_version = tablet_schema->schema_version();
            select_schema_id = tablet_schema->id();
        }
    }

    if (select_schema_id != tablet_schema->id()) {
        auto schema_it = metadata->historical_schemas().find(select_schema_id);
        tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(schema_it->second).first;
    }

    return tablet_schema;
}

StatusOr<CompactionTaskPtr> TabletManager::compact(CompactionTaskContext* context) {
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    if (g_worker != nullptr && (context->table_id == 0 || context->partition_id == 0)) {
        auto shard_info_or = g_worker->retrieve_shard_info(context->tablet_id);
        if (shard_info_or.ok()) {
            auto id_pair = get_table_partition_id(shard_info_or.value());
            if (context->table_id == 0) {
                context->table_id = id_pair.first;
            }
            if (context->partition_id == 0) {
                context->partition_id = id_pair.second;
            }
        }
    }
#endif

    ASSIGN_OR_RETURN(auto tablet, get_tablet(context->tablet_id, context->version));
    const auto& tablet_metadata = tablet.metadata();
    ASSIGN_OR_RETURN(auto compaction_policy,
                     CompactionPolicy::create(this, tablet_metadata, context->force_base_compaction));
    ASSIGN_OR_RETURN(auto input_rowsets, compaction_policy->pick_rowsets());
    return compact(context, std::move(input_rowsets));
}

StatusOr<CompactionTaskPtr> TabletManager::compact(CompactionTaskContext* context,
                                                   std::vector<RowsetPtr> input_rowsets) {
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    // Retrieve table_id and partition_id from shard info if not already set.
    // This is needed for parallel compaction which may call this overload directly.
    if (g_worker != nullptr && (context->table_id == 0 || context->partition_id == 0)) {
        auto shard_info_or = g_worker->retrieve_shard_info(context->tablet_id);
        if (shard_info_or.ok()) {
            auto id_pair = get_table_partition_id(shard_info_or.value());
            if (context->table_id == 0) {
                context->table_id = id_pair.first;
            }
            if (context->partition_id == 0) {
                context->partition_id = id_pair.second;
            }
        }
    }
#endif

    ASSIGN_OR_RETURN(auto tablet, get_tablet(context->tablet_id, context->version));
    auto tablet_metadata = tablet.metadata();
    ASSIGN_OR_RETURN(auto compaction_policy,
                     CompactionPolicy::create(this, tablet_metadata, context->force_base_compaction));
    ASSIGN_OR_RETURN(auto algorithm, compaction_policy->choose_compaction_algorithm(input_rowsets));
    std::vector<uint32_t> input_rowsets_id;
    size_t total_input_rowsets_file_size = 0;
    for (auto& rowset : input_rowsets) {
        input_rowsets_id.emplace_back(rowset->id());
        total_input_rowsets_file_size += rowset->data_size();
    }
    context->stats->input_file_size += total_input_rowsets_file_size;
    ASSIGN_OR_RETURN(auto tablet_schema, get_output_rowset_schema(input_rowsets_id, tablet_metadata.get()));
    if (algorithm == VERTICAL_COMPACTION) {
        return std::make_shared<VerticalCompactionTask>(std::move(tablet), std::move(input_rowsets), context,
                                                        std::move(tablet_schema));
    } else if (algorithm == HORIZONTAL_COMPACTION) {
        return std::make_shared<HorizontalCompactionTask>(std::move(tablet), std::move(input_rowsets), context,
                                                          std::move(tablet_schema));
    } else {
        DCHECK(algorithm == CLOUD_NATIVE_INDEX_COMPACTION);
        return std::make_shared<CloudNativeIndexCompactionTask>(std::move(tablet), std::move(input_rowsets), context,
                                                                std::move(tablet_schema));
    }
}

// Store a copy of the tablet schema in a separate schema file named SCHEMA_{indexId}.
Status TabletManager::create_schema_file(int64_t tablet_id, const TabletSchemaPB& schema_pb) {
    auto schema_file_path = _location_provider->schema_file_location(tablet_id, schema_pb.id());
    VLOG(3) << "Creating schema file of id " << schema_pb.id() << " for tablet " << tablet_id;
    ProtobufFile file(schema_file_path);
    RETURN_IF_ERROR(file.save(schema_pb));

    // Save the schema into the in-memory cache
    auto [schema, inserted] = GlobalTabletSchemaMap::Instance()->emplace(schema_pb);
    if (UNLIKELY(schema == nullptr)) {
        return Status::InternalError("failed to emplace the schema hash map");
    }
    auto cache_key = global_schema_cache_key(schema_pb.id());
    auto cache_size = inserted ? schema->mem_usage() : 0;
    _metacache->cache_tablet_schema(cache_key, schema, cache_size /*TODO*/);
    return Status::OK();
}

StatusOr<TabletSchemaPtr> TabletManager::load_and_parse_schema_file(const std::string& path) {
    TabletSchemaPB schema_pb;
    ProtobufFile file(path);
    RETURN_IF_ERROR(file.load(&schema_pb));
    auto [schema, inserted] = GlobalTabletSchemaMap::Instance()->emplace(schema_pb);
    if (UNLIKELY(schema == nullptr)) {
        return Status::InternalError("failed to emplace the schema hash map");
    }
    return std::move(schema);
}

void TabletManager::update_metacache_limit(size_t new_capacity) {
    _metacache->update_capacity(new_capacity);
}

int64_t TabletManager::in_writing_data_size(int64_t tablet_id) {
    {
        std::shared_lock rdlock(_meta_lock);
        const auto& it = _tablet_in_writing_size.find(tablet_id);
        if (it != _tablet_in_writing_size.end()) {
            VLOG(2) << "tablet " << tablet_id << " in writing data size: " << it->second;
            return it->second;
        }
    }
    return add_in_writing_data_size(tablet_id, 0);
}

int64_t TabletManager::add_in_writing_data_size(int64_t tablet_id, int64_t size) {
    {
        std::unique_lock wrlock(_meta_lock);
        const auto& it = _tablet_in_writing_size.find(tablet_id);
        if (it != _tablet_in_writing_size.end()) {
            it->second += size;
            return it->second;
        }
    }

    int64_t base_size = get_tablet_data_size(tablet_id, nullptr).value_or(0);

    std::unique_lock wrlock(_meta_lock);
    const auto& it = _tablet_in_writing_size.find(tablet_id);
    if (it != _tablet_in_writing_size.end()) {
        it->second += size;
    } else {
        _tablet_in_writing_size[tablet_id] = base_size + size;
    }
    return _tablet_in_writing_size[tablet_id];
}

void TabletManager::clean_in_writing_data_size() {
#ifdef USE_STAROS
    std::unique_lock wrlock(_meta_lock);
    for (auto it = _tablet_in_writing_size.begin(); it != _tablet_in_writing_size.end();) {
        VLOG(2) << "clean in writing data size of tablet " << it->first << " size: " << it->second;
        if (!is_tablet_in_worker(it->first)) {
            it = _tablet_in_writing_size.erase(it);
        } else {
            ++it;
        }
    }
#endif
}

void TabletManager::TEST_set_global_schema_cache(int64_t schema_id, TabletSchemaPtr schema) {
    auto cache_key = global_schema_cache_key(schema_id);
    _metacache->cache_tablet_schema(cache_key, std::move(schema), 0);
}

StatusOr<VersionedTablet> TabletManager::get_tablet(int64_t tablet_id, int64_t version, bool fill_meta_cache,
                                                    bool fill_data_cache) {
    CacheOptions cache_opts{.fill_meta_cache = fill_meta_cache, .fill_data_cache = fill_data_cache};
    ASSIGN_OR_RETURN(auto metadata, get_tablet_metadata(tablet_id, version, cache_opts));
    return VersionedTablet(this, std::move(metadata));
}

StatusOr<SegmentPtr> TabletManager::load_segment(const FileInfo& segment_info, int segment_id, size_t* footer_size_hint,
                                                 const LakeIOOptions& lake_io_opts, bool fill_meta_cache,
                                                 TabletSchemaPtr tablet_schema) {
    // NOTE: if partial compaction is turned on, `segment_id` might not be the same as cached segment id
    //       for example, in tablet X, segment `a` has segment id 10, if partial compaction happens,
    //                    in tablet X+1, segment `a` might still exists, but its actual id will not be 10.
    //       but in meta cache, segment `a` still has segment id 10, it is not changed.
    auto segment = metacache()->lookup_segment(segment_info.path);
    if (segment == nullptr) {
        std::shared_ptr<FileSystem> fs;
        if (segment_info.fs) {
            fs = segment_info.fs;
        } else {
            ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(segment_info.path));
        }
        segment = std::make_shared<Segment>(std::move(fs), segment_info, segment_id, std::move(tablet_schema), this);
        if (fill_meta_cache) {
            // NOTE: the returned segment may be not the same as the parameter passed in
            // Use the one in cache if the same key already exists
            if (auto cached_segment = _metacache->cache_segment_if_absent(segment_info.path, segment);
                cached_segment != nullptr) {
                segment = cached_segment;
            }
        }
    }
    // segment->open will read the footer, and it is time-consuming.
    // separate it from static Segment::open is to prevent a large number of cache misses,
    // and many temporary segment objects generation when loading the same segment concurrently.
    RETURN_IF_ERROR(segment->open(footer_size_hint, nullptr, lake_io_opts));
    return segment;
}

StatusOr<SegmentPtr> TabletManager::load_segment(const FileInfo& segment_info, int segment_id,
                                                 const LakeIOOptions& lake_io_opts, bool fill_meta_cache,
                                                 TabletSchemaPtr tablet_schema) {
    size_t footer_size_hint = 16 * 1024;
    return load_segment(segment_info, segment_id, &footer_size_hint, lake_io_opts, fill_meta_cache,
                        std::move(tablet_schema));
}

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
StatusOr<TabletBasicInfo> TabletManager::get_tablet_basic_info(
        int64_t tablet_id, int64_t table_id, int64_t partition_id, const std::set<int64_t>& authorized_table_ids,
        const std::unordered_map<int64_t, int64_t>& partition_versions) {
    auto shard_info_or = g_worker->retrieve_shard_info(tablet_id);
    if (!shard_info_or.ok()) {
        return Status::InternalError(fmt::format("fail to get shard info of tablet: {}, err: {}", tablet_id,
                                                 shard_info_or.status().message()));
    }

    auto shard_info = shard_info_or.value();
    auto id_pair = get_table_partition_id(shard_info);
    auto shard_table_id = id_pair.first;
    auto shard_partition_id = id_pair.second;

    if ((partition_id != -1 && partition_id != shard_partition_id) || (table_id != -1 && table_id != shard_table_id) ||
        authorized_table_ids.find(shard_table_id) == authorized_table_ids.end()) {
        return Status::NotAuthorized(fmt::format("tablet: {}, table_id: {}, partition_id: {} not authorized", tablet_id,
                                                 table_id, partition_id));
    }

    auto search = partition_versions.find(shard_partition_id);
    if (search == partition_versions.end()) {
        return Status::NotFound(fmt::format("partition: {} not found, tablet: {}", shard_partition_id, tablet_id));
    }

    // Don't fill meta cache to avoid polluting the cache
    int64_t version = search->second;
    auto tablet_or = get_tablet(tablet_id, version, /*fill_meta_cache=*/false, /*fill_data_cache=*/true);
    if (!tablet_or.ok()) {
        return Status::InternalError(fmt::format("fail to get tablet: {}, version: {}, err: {}", tablet_id, version,
                                                 tablet_or.status().to_string()));
    }

    auto info = tablet_or.value().get_basic_info();
    info.table_id = shard_table_id;
    info.partition_id = shard_partition_id;

    return info;
}
#endif // USE_STAROS

void TabletManager::get_tablets_basic_info(int64_t table_id, int64_t partition_id, int64_t tablet_id,
                                           const std::set<int64_t>& authorized_table_ids,
                                           const std::unordered_map<int64_t, int64_t>& partition_versions,
                                           std::vector<TabletBasicInfo>& tablet_infos) {
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    if (g_worker == nullptr) {
        return;
    }

    if (tablet_id != -1) {
        // process the tablet with the given tablet_id
        auto tablet_info_or =
                get_tablet_basic_info(tablet_id, table_id, partition_id, authorized_table_ids, partition_versions);
        auto st = tablet_info_or.status();
        if (st.ok()) {
            tablet_infos.emplace_back(std::move(tablet_info_or.value()));
        } else if (!st.is_not_authorized() && !st.is_not_found()) {
            LOG(WARNING) << "fail to get tablet basic info, err: " << st;
        }
    } else {
        // iterate all shards and get the tablets belong to the given table_id and partition_id
        auto shard_ids = g_worker->shard_ids();
        for (const auto& shard_id : shard_ids) {
            auto tablet_info_or =
                    get_tablet_basic_info(shard_id, table_id, partition_id, authorized_table_ids, partition_versions);
            auto st = tablet_info_or.status();
            if (st.ok()) {
                tablet_infos.emplace_back(std::move(tablet_info_or.value()));
            } else if (!st.is_not_authorized() && !st.is_not_found()) {
                LOG(WARNING) << "fail to get tablet basic info, err: " << st;
            }
        }

        // order by table_id, partition_id, tablet_id by default
        std::sort(tablet_infos.begin(), tablet_infos.end(), [](const TabletBasicInfo& a, const TabletBasicInfo& b) {
            if (a.partition_id == b.partition_id) {
                return a.tablet_id < b.tablet_id;
            }
            if (a.table_id == b.table_id) {
                return a.partition_id < b.partition_id;
            }
            return a.table_id < b.table_id;
        });
    }
#endif // USE_STAROS
}

void TabletManager::stop() {
    _compaction_scheduler->stop();
}

StatusOr<TabletAndRowsets> TabletManager::capture_tablet_and_rowsets(int64_t tablet_id, int64_t from_version,
                                                                     int64_t to_version) {
    DCHECK(from_version <= to_version);
    auto tablet_ptr = std::make_shared<Tablet>(this, tablet_id);
    std::vector<std::shared_ptr<BaseRowset>> rowsets;

    if (from_version == 0 || from_version == kInitialVersion) {
        ASSIGN_OR_RETURN(auto current_version_tablet_meta, get_tablet_metadata(tablet_id, to_version));
        for (int i = 0, size = current_version_tablet_meta->rowsets_size(); i < size; i++) {
            // inc rowset
            auto rowset = std::make_shared<Rowset>(this, current_version_tablet_meta, i, 0);
            rowsets.emplace_back(std::static_pointer_cast<BaseRowset>(rowset));
        }
        return std::make_tuple(std::move(tablet_ptr), std::move(rowsets));
    }

    TabletMetadataPtr pre_version_tablet_meta = nullptr;
    for (int version = from_version; version <= to_version; version++) {
        ASSIGN_OR_RETURN(auto current_version_tablet_meta, get_tablet_metadata(tablet_id, version));
        // indicates the tablet metadata is generated by load
        if (current_version_tablet_meta->compaction_inputs_size() == 0) {
            std::unordered_set<uint32_t> rowset_ids;
            if (pre_version_tablet_meta == nullptr) {
                ASSIGN_OR_RETURN(auto tmp_tablet_meta, get_tablet_metadata(tablet_id, version - 1));
                pre_version_tablet_meta = std::move(tmp_tablet_meta);
            }

            for (int i = 0, size = pre_version_tablet_meta->rowsets_size(); i < size; ++i) {
                rowset_ids.emplace(pre_version_tablet_meta->rowsets(i).id());
            }

            for (int i = 0, size = current_version_tablet_meta->rowsets_size(); i < size; i++) {
                if (rowset_ids.count(current_version_tablet_meta->rowsets(i).id()) == 0) {
                    // inc rowset
                    auto rowset = std::make_shared<Rowset>(this, current_version_tablet_meta, i, 0);
                    rowsets.emplace_back(std::static_pointer_cast<BaseRowset>(rowset));
                }
            }
            pre_version_tablet_meta = std::move(current_version_tablet_meta);
        } else {
            pre_version_tablet_meta = nullptr;
        }
    }

    return std::make_tuple(std::move(tablet_ptr), std::move(rowsets));
}

void TabletManager::cache_schema(const TabletSchemaPtr& schema) {
    // GlobalTabletSchemaMap and metadata cache overlap in functionality, but because many places
    // previously relied on GlobalTabletSchemaMap, caching is still performed in GlobalTabletSchemaMap
    // here. In the future, it may be possible to refactor and remove GlobalTabletSchemaMap.
    auto [cached_schema, inserted] = GlobalTabletSchemaMap::Instance()->emplace(schema);
    auto cache_key = global_schema_cache_key(cached_schema->id());
    auto cache_size = inserted ? cached_schema->mem_usage() : 0;
    _metacache->cache_tablet_schema(cache_key, cached_schema, cache_size);
}

TabletSchemaPtr TabletManager::get_cached_schema(int64_t schema_id) {
    auto cache_key = global_schema_cache_key(schema_id);
    return _metacache->lookup_tablet_schema(cache_key);
}

} // namespace starrocks::lake