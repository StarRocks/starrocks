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
#include <variant>

#include "agent/master_info.h"
#include "common/compiler_util.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/strings/util.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/horizontal_compaction_task.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/txn_log_applier.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/vacuum.h"
#include "storage/lake/vertical_compaction_task.h"
#include "storage/metadata_util.h"
#include "storage/protobuf_file.h"
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

std::string TabletManager::tablet_metadata_lock_location(int64_t tablet_id, int64_t version,
                                                         int64_t expire_time) const {
    return _location_provider->tablet_metadata_lock_location(tablet_id, version, expire_time);
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
    tablet_metadata_pb->set_version(1);
    tablet_metadata_pb->set_next_rowset_id(1);
    tablet_metadata_pb->set_cumulative_point(0);

    if (req.__isset.enable_persistent_index) {
        tablet_metadata_pb->set_enable_persistent_index(req.enable_persistent_index);
        if (req.__isset.persistent_index_type) {
            switch (req.persistent_index_type) {
            case TPersistentIndexType::LOCAL:
                tablet_metadata_pb->set_persistent_index_type(PersistentIndexTypePB::LOCAL);
                break;
            default:
                return Status::InternalError(
                        strings::Substitute("Unknown persistent index type, tabletId:$0", req.tablet_id));
            }
        }
    }

    // Note: ignore the parameter "base_tablet_id" of `TCreateTabletReq`, because we don't support linked schema
    // change, there is no need to keep the column unique id consistent between the new tablet and base tablet.
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
    uint32_t next_unique_id = req.tablet_schema.columns.size();
    for (uint32_t col_idx = 0; col_idx < next_unique_id; ++col_idx) {
        col_idx_to_unique_id[col_idx] = col_idx;
    }
    RETURN_IF_ERROR(starrocks::convert_t_schema_to_pb_schema(
            req.tablet_schema, next_unique_id, col_idx_to_unique_id, tablet_metadata_pb->mutable_schema(),
            req.__isset.compression_type ? req.compression_type : TCompressionType::LZ4_FRAME));
    if (req.create_schema_file) {
        RETURN_IF_ERROR(create_schema_file(req.tablet_id, tablet_metadata_pb->schema()));
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

Status TabletManager::delete_tablet(int64_t tablet_id) {
    std::vector<std::string> objects;
    // TODO: construct prefix in LocationProvider or a common place
    const auto tablet_prefix = fmt::format("{:016X}_", tablet_id);
    auto root_path = _location_provider->metadata_root_location(tablet_id);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_path));
    auto scan_cb = [&](std::string_view name) {
        if (HasPrefixString(name, tablet_prefix)) {
            objects.emplace_back(join_path(root_path, name));
        }
        return true;
    };
    auto st = fs->iterate_dir(root_path, scan_cb);
    if (!st.ok() && !st.is_not_found()) {
        return st;
    }

    root_path = _location_provider->txn_log_root_location(tablet_id);
    // It's ok to ignore the error here.
    (void)fs->iterate_dir(root_path, scan_cb);

    for (const auto& obj : objects) {
        _metacache->erase(obj);
        (void)fs->delete_file(obj);
    }
    //drop tablet schema from metacache;
    _metacache->erase(tablet_schema_cache_key(tablet_id));
    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const TabletMetadataPtr& metadata) {
    TEST_ERROR_POINT("TabletManager::put_tablet_metadata");
    // write metadata file
    auto t0 = butil::gettimeofday_us();
    auto filepath = tablet_metadata_location(metadata->id(), metadata->version());

    ProtobufFile file(filepath);
    RETURN_IF_ERROR(file.save(*metadata));

    _metacache->cache_tablet_metadata(filepath, metadata);
    _metacache->cache_tablet_metadata(tablet_latest_metadata_cache_key(metadata->id()), metadata);

    auto t1 = butil::gettimeofday_us();
    g_put_tablet_metadata_latency << (t1 - t0);
    TRACE("end write tablet metadata");
    return Status::OK();
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

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(int64_t tablet_id, int64_t version) {
    return get_tablet_metadata(tablet_metadata_location(tablet_id, version));
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
    return fs::delete_file(location);
}

StatusOr<TabletMetadataIter> TabletManager::list_tablet_metadata(int64_t tablet_id, bool filter_tablet) {
    std::vector<std::string> objects{};
    // TODO: construct prefix in LocationProvider
    std::string prefix;
    if (filter_tablet) {
        prefix = fmt::format("{:016X}_", tablet_id);
    }

    auto root = _location_provider->metadata_root_location(tablet_id);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root));
    auto scan_cb = [&](std::string_view name) {
        if (HasPrefixString(name, prefix)) {
            objects.emplace_back(join_path(root, name));
        }
        return true;
    };

    RETURN_IF_ERROR(fs->iterate_dir(root, scan_cb));
    return TabletMetadataIter{this, std::move(objects)};
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

Status TabletManager::delete_txn_log(int64_t tablet_id, int64_t txn_id) {
    auto t0 = butil::gettimeofday_us();
    auto location = txn_log_location(tablet_id, txn_id);
    _metacache->erase(location);
    auto st = fs::delete_file(location);
    auto t1 = butil::gettimeofday_us();
    g_del_txn_log_latency << (t1 - t0);
    TRACE("end delete txn log");
    return st.is_not_found() ? Status::OK() : st;
}

Status TabletManager::delete_txn_vlog(int64_t tablet_id, int64_t version) {
    auto t0 = butil::gettimeofday_us();
    auto location = txn_vlog_location(tablet_id, version);
    _metacache->erase(location);
    auto st = fs::delete_file(location);
    auto t1 = butil::gettimeofday_us();
    g_del_txn_log_latency << (t1 - t0);
    TRACE("end delete txn vlog");
    return st.is_not_found() ? Status::OK() : st;
}

StatusOr<TxnLogIter> TabletManager::list_txn_log(int64_t tablet_id, bool filter_tablet) {
    std::vector<std::string> objects{};
    // TODO: construct prefix in LocationProvider
    std::string prefix;
    if (filter_tablet) {
        prefix = fmt::format("{:016X}_", tablet_id);
    }

    auto root = _location_provider->txn_log_root_location(tablet_id);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root));
    auto scan_cb = [&](std::string_view name) {
        if (HasPrefixString(name, prefix)) {
            objects.emplace_back(join_path(root, name));
        }
        return true;
    };

    RETURN_IF_ERROR(fs->iterate_dir(root, scan_cb));
    return TxnLogIter{this, std::move(objects)};
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

Status TabletManager::put_txn_slog(const TxnLogPtr& log) {
    return put_txn_log(log, txn_slog_location(log->tablet_id(), log->txn_id()));
}

Status TabletManager::put_txn_slog(const TxnLogPtr& log, const std::string& path) {
    return put_txn_log(log, path);
}

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
                auto schema_id = std::atol(index_id_iter->second.data());
                auto global_cache_key = global_schema_cache_key(schema_id);
                auto schema = _metacache->lookup_tablet_schema(global_cache_key);
                if (schema != nullptr) {
                    return schema;
                }
                // else: Cache miss, read the schema file
                auto schema_file_path = join_path(tablet_root_location(tablet_id), schema_filename(schema_id));
                auto schema_or = load_and_parse_schema_file(schema_file_path);
                if (schema_or.ok()) {
                    VLOG(3) << "Got tablet schema of id " << schema_id << " for tablet " << tablet_id;
                    schema = std::move(schema_or).value();
                    // Save the schema into the in-memory cache, use the schema id as the cache key
                    _metacache->cache_tablet_schema(global_cache_key, schema, 0 /*TODO*/);
                    return std::move(schema);
                } else if (schema_or.status().is_not_found()) {
                    // version 3.0 will not generate the tablet schema file, ignore the not found error and
                    // try to extract the tablet schema from the tablet metadata.
                } else {
                    return schema_or.status();
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
        ASSIGN_OR_RETURN(TabletMetadataIter metadata_iter, list_tablet_metadata(tablet_id, true));
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

StatusOr<CompactionTaskPtr> TabletManager::compact(int64_t tablet_id, int64_t version, int64_t txn_id) {
    ASSIGN_OR_RETURN(auto tablet, get_tablet(tablet_id));
    auto tablet_ptr = std::make_shared<Tablet>(tablet);
    tablet_ptr->set_version_hint(version);
    ASSIGN_OR_RETURN(auto compaction_policy, CompactionPolicy::create_compaction_policy(tablet_ptr));
    ASSIGN_OR_RETURN(auto input_rowsets, compaction_policy->pick_rowsets(version));
    ASSIGN_OR_RETURN(auto algorithm, compaction_policy->choose_compaction_algorithm(input_rowsets));
    if (algorithm == VERTICAL_COMPACTION) {
        return std::make_shared<VerticalCompactionTask>(txn_id, version, std::move(tablet_ptr),
                                                        std::move(input_rowsets));
    } else {
        DCHECK(algorithm == HORIZONTAL_COMPACTION);
        return std::make_shared<HorizontalCompactionTask>(txn_id, version, std::move(tablet_ptr),
                                                          std::move(input_rowsets));
    }
}

Status TabletManager::put_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time) {
    auto path = tablet_metadata_lock_location(tablet_id, version, expire_time);
    TabletMetadataLockPB lock;
    ProtobufFile file(path);
    return file.save(lock);
}

Status TabletManager::delete_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time) {
    auto location = tablet_metadata_lock_location(tablet_id, version, expire_time);
    auto st = fs::delete_file(location);
    return st.is_not_found() ? Status::OK() : st;
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

void TabletManager::TEST_set_global_schema_cache(int64_t schema_id, TabletSchemaPtr schema) {
    auto cache_key = global_schema_cache_key(schema_id);
    _metacache->cache_tablet_schema(cache_key, std::move(schema), 0);
}

} // namespace starrocks::lake
