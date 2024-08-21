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
#include "fmt/format.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/strings/util.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/horizontal_compaction_task.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
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

TabletManager::TabletManager(LocationProvider* location_provider, UpdateManager* update_mgr, int64_t cache_capacity)
        : _location_provider(location_provider),
          _metacache(std::make_unique<Metacache>(cache_capacity)),
          _compaction_scheduler(std::make_unique<CompactionScheduler>(this)),
          _update_mgr(update_mgr) {
    _update_mgr->set_tablet_mgr(this);
}

TabletManager::~TabletManager() = default;

std::string TabletManager::tablet_root_location(int64_t tablet_id) const {
    return _location_provider->root_location(tablet_id);
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

std::string TabletManager::txn_log_location(int64_t tablet_id, int64_t txn_id) const {
    return _location_provider->txn_log_location(tablet_id, txn_id);
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

// current lru cache does not support updating value size, so use refill to update.
void TabletManager::update_segment_cache_size(std::string_view key, intptr_t segment_addr_hint) {
    // use write lock to protect parallel segment size update
    std::unique_lock wrlock(_meta_lock);
    auto segment = _metacache->lookup_segment(key);
    if (segment == nullptr) {
        return;
    }
    if (segment_addr_hint != 0 && segment_addr_hint != reinterpret_cast<intptr_t>(segment.get())) {
        // the segment in cache is not the one as expected, skip the cache update
        return;
    }
    _metacache->cache_segment(key, std::move(segment));
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

    auto compress_type = req.__isset.compression_type ? req.compression_type : TCompressionType::LZ4_FRAME;
    RETURN_IF_ERROR(
            convert_t_schema_to_pb_schema(req.tablet_schema, compress_type, tablet_metadata_pb->mutable_schema()));
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

Status TabletManager::put_tablet_metadata(const TabletMetadata& metadata) {
    auto metadata_ptr = std::make_shared<TabletMetadata>(metadata);
    return put_tablet_metadata(std::move(metadata_ptr));
}

StatusOr<TabletMetadataPtr> TabletManager::load_tablet_metadata(const string& metadata_location, bool fill_cache) {
    TEST_ERROR_POINT("TabletManager::load_tablet_metadata");
    auto t0 = butil::gettimeofday_us();
    auto metadata = std::make_shared<TabletMetadataPB>();
    ProtobufFile file(metadata_location);
    RETURN_IF_ERROR(file.load(metadata.get(), fill_cache));
    g_get_tablet_metadata_latency << (butil::gettimeofday_us() - t0);
    return std::move(metadata);
}

TabletMetadataPtr TabletManager::get_latest_cached_tablet_metadata(int64_t tablet_id) {
    return _metacache->lookup_tablet_metadata(tablet_latest_metadata_cache_key(tablet_id));
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(int64_t tablet_id, int64_t version, bool fill_cache) {
    if (version <= kInitialVersion) {
        // Handle tablet initial metadata
        auto initial_metadata = get_tablet_metadata(tablet_initial_metadata_location(tablet_id), fill_cache);
        if (initial_metadata.ok()) {
            auto tablet_metadata = std::make_shared<TabletMetadata>(*initial_metadata.value());
            tablet_metadata->set_id(tablet_id);
            return tablet_metadata;
        }
    }
    return get_tablet_metadata(tablet_metadata_location(tablet_id, version), fill_cache);
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const string& path, bool fill_cache) {
    if (auto ptr = _metacache->lookup_tablet_metadata(path); ptr != nullptr) {
        TRACE("got cached tablet metadata");
        return ptr;
    }
    ASSIGN_OR_RETURN(auto ptr, load_tablet_metadata(path, fill_cache));
    if (fill_cache) {
        _metacache->cache_tablet_metadata(path, ptr);
    }
    TRACE("end read tablet metadata");
    return ptr;
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
    std::vector<std::string> objects{};
    // TODO: construct prefix in LocationProvider
    std::string prefix = fmt::format("{:016X}_", tablet_id);

    auto root = _location_provider->metadata_root_location(tablet_id);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root));
    auto scan_cb = [&](std::string_view name) {
        if (HasPrefixString(name, prefix)) {
            objects.emplace_back(join_path(root, name));
        }
        return true;
    };

    RETURN_IF_ERROR(fs->iterate_dir(root, scan_cb));

    if (objects.empty()) {
        // Put tablet initial metadata
        objects.emplace_back(join_path(root, tablet_initial_metadata_filename()));
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

StatusOr<TxnLogPtr> TabletManager::get_txn_log(int64_t tablet_id, int64_t txn_id) {
    return get_txn_log(txn_log_location(tablet_id, txn_id));
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

    auto t1 = butil::gettimeofday_us();
    g_put_txn_log_latency << (t1 - t0);
    return Status::OK();
}

Status TabletManager::put_txn_log(const TxnLogPtr& log) {
    return put_txn_log(log, txn_log_location(log->tablet_id(), log->txn_id()));
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

#ifdef USE_STAROS
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
#ifdef USE_STAROS
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
    auto schema_or = _schema_group.Do(schema_file_path, [&]() { return load_and_parse_schema_file(schema_file_path); });
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
    if (metadata->rowset_to_schema().empty() || input_rowset.size() <= 0) {
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
    ASSIGN_OR_RETURN(auto tablet, get_tablet(context->tablet_id, context->version));
    auto tablet_metadata = tablet.metadata();
    ASSIGN_OR_RETURN(auto compaction_policy, CompactionPolicy::create(this, tablet_metadata));
    ASSIGN_OR_RETURN(auto input_rowsets, compaction_policy->pick_rowsets());
    ASSIGN_OR_RETURN(auto algorithm, compaction_policy->choose_compaction_algorithm(input_rowsets));
    std::vector<uint32_t> input_rowsets_id;
    for (auto& rowset : input_rowsets) {
        input_rowsets_id.emplace_back(rowset->id());
    }
    ASSIGN_OR_RETURN(auto tablet_schema, get_output_rowset_schema(input_rowsets_id, tablet_metadata.get()));
    if (algorithm == VERTICAL_COMPACTION) {
        return std::make_shared<VerticalCompactionTask>(std::move(tablet), std::move(input_rowsets), context,
                                                        std::move(tablet_schema));
    } else {
        DCHECK(algorithm == HORIZONTAL_COMPACTION);
        return std::make_shared<HorizontalCompactionTask>(std::move(tablet), std::move(input_rowsets), context,
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
            VLOG(1) << "tablet " << tablet_id << " in writing data size: " << it->second;
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
        VLOG(1) << "clean in writing data size of tablet " << it->first << " size: " << it->second;
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

StatusOr<VersionedTablet> TabletManager::get_tablet(int64_t tablet_id, int64_t version) {
    ASSIGN_OR_RETURN(auto metadata, get_tablet_metadata(tablet_id, version));
    return VersionedTablet(this, std::move(metadata));
}

StatusOr<SegmentPtr> TabletManager::load_segment(const FileInfo& segment_info, int segment_id, size_t* footer_size_hint,
                                                 const LakeIOOptions& lake_io_opts, bool fill_metadata_cache,
                                                 TabletSchemaPtr tablet_schema) {
    // NOTE: if partial compaction is turned on, `segment_id` might not be the same as cached segment id
    //       for example, in tablet X, segment `a` has segment id 10, if partial compaction happens,
    //                    in tablet X+1, segment `a` might still exists, but its actual id will not be 10.
    //       but in meta cache, segment `a` still has segment id 10, it is not changed.
    auto segment = metacache()->lookup_segment(segment_info.path);
    if (segment == nullptr) {
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(segment_info.path));
        segment = std::make_shared<Segment>(std::move(fs), segment_info, segment_id, std::move(tablet_schema), this);
        if (fill_metadata_cache) {
            // NOTE: the returned segment may be not the same as the parameter passed in
            // Use the one in cache if the same key already exists
            if (auto cached_segment = metacache()->cache_segment_if_absent(segment_info.path, segment);
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
                                                 const LakeIOOptions& lake_io_opts, bool fill_metadata_cache,
                                                 TabletSchemaPtr tablet_schema) {
    size_t footer_size_hint = 16 * 1024;
    return load_segment(segment_info, segment_id, &footer_size_hint, lake_io_opts, fill_metadata_cache,
                        std::move(tablet_schema));
}

} // namespace starrocks::lake
