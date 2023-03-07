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

#include <bthread/bthread.h>

#include <atomic>
#include <chrono>
#include <variant>

#include "agent/agent_server.h"
#include "agent/master_info.h"
#include "common/compiler_util.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/strings/util.h"
#include "runtime/exec_env.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/gc.h"
#include "storage/lake/horizontal_compaction_task.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/txn_log_applier.h"
#include "storage/lake/update_manager.h"
#include "storage/metadata_util.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema_map.h"
#include "util/lru_cache.h"
#include "util/raw_container.h"

namespace starrocks::lake {

static void* gc_checker(void* arg);
static StatusOr<double> publish(Tablet* tablet, int64_t base_version, int64_t new_version, const int64_t* txns,
                                int txns_size);

TabletManager::TabletManager(LocationProvider* location_provider, UpdateManager* update_mgr, int64_t cache_capacity)
        : _location_provider(location_provider),
          _metacache(new_lru_cache(cache_capacity)),
          _update_mgr(update_mgr),
          _gc_checker_tid(INVALID_BTHREAD) {
    _update_mgr->set_tablet_mgr(this);
}

TabletManager::~TabletManager() {
    if (_gc_checker_tid != INVALID_BTHREAD) {
        [[maybe_unused]] void* ret = nullptr;
        // We don't care about the return value of bthread_stop or bthread_join.
        (void)bthread_stop(_gc_checker_tid);
        (void)bthread_join(_gc_checker_tid, &ret);
    }
}

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

std::string TabletManager::txn_vlog_location(int64_t tablet_id, int64_t version) const {
    return _location_provider->txn_vlog_location(tablet_id, version);
}

std::string TabletManager::segment_location(int64_t tablet_id, std::string_view segment_name) const {
    return _location_provider->segment_location(tablet_id, segment_name);
}

std::string TabletManager::del_location(int64_t tablet_id, std::string_view del_name) const {
    return _location_provider->del_location(tablet_id, del_name);
}

std::string TabletManager::delvec_location(int64_t tablet_id, int64_t version) const {
    return _location_provider->tablet_delvec_location(tablet_id, version);
}

std::string TabletManager::tablet_metadata_lock_location(int64_t tablet_id, int64_t version,
                                                         int64_t expire_time) const {
    return _location_provider->tablet_metadata_lock_location(tablet_id, version, expire_time);
}

std::string TabletManager::tablet_schema_cache_key(int64_t tablet_id) {
    return fmt::format("schema_{}", tablet_id);
}

bool TabletManager::fill_metacache(std::string_view key, CacheValue* ptr, int size) {
    Cache::Handle* handle = _metacache->insert(CacheKey(key), ptr, size, cache_value_deleter);
    if (handle == nullptr) {
        delete ptr;
        return false;
    } else {
        _metacache->release(handle);
        return true;
    }
}

TabletMetadataPtr TabletManager::lookup_tablet_metadata(std::string_view key) {
    auto handle = _metacache->lookup(CacheKey(key));
    if (handle == nullptr) {
        return nullptr;
    }
    auto value = static_cast<CacheValue*>(_metacache->value(handle));
    auto metadata = std::get<TabletMetadataPtr>(*value);
    _metacache->release(handle);
    return metadata;
}

TabletSchemaPtr TabletManager::lookup_tablet_schema(std::string_view key) {
    auto handle = _metacache->lookup(CacheKey(key));
    if (handle == nullptr) {
        return nullptr;
    }
    auto value = static_cast<CacheValue*>(_metacache->value(handle));
    auto schema = std::get<TabletSchemaPtr>(*value);
    _metacache->release(handle);
    return schema;
}

TxnLogPtr TabletManager::lookup_txn_log(std::string_view key) {
    auto handle = _metacache->lookup(CacheKey(key));
    if (handle == nullptr) {
        return nullptr;
    }
    auto value = static_cast<CacheValue*>(_metacache->value(handle));
    auto log = std::get<TxnLogPtr>(*value);
    _metacache->release(handle);
    return log;
}

SegmentPtr TabletManager::lookup_segment(std::string_view key) {
    auto handle = _metacache->lookup(CacheKey(key));
    if (handle == nullptr) {
        return nullptr;
    }
    auto value = static_cast<CacheValue*>(_metacache->value(handle));
    auto segment = std::get<SegmentPtr>(*value);
    _metacache->release(handle);
    return segment;
}

void TabletManager::cache_segment(std::string_view key, SegmentPtr segment) {
    auto mem_cost = segment->mem_usage();
    auto value = std::make_unique<CacheValue>(std::move(segment));
    (void)fill_metacache(key, value.release(), (int)mem_cost);
}

DelVectorPtr TabletManager::lookup_delvec(std::string_view key) {
    auto handle = _metacache->lookup(CacheKey(key));
    if (handle == nullptr) {
        return nullptr;
    }
    auto value = static_cast<CacheValue*>(_metacache->value(handle));
    auto delvec = std::get<DelVectorPtr>(*value);
    _metacache->release(handle);
    return delvec;
}

void TabletManager::cache_delvec(std::string_view key, DelVectorPtr delvec) {
    auto mem_cost = delvec->memory_usage();
    auto value = std::make_unique<CacheValue>(std::move(delvec));
    (void)fill_metacache(key, value.release(), (int)mem_cost);
}

void TabletManager::erase_metacache(std::string_view key) {
    _metacache->erase(CacheKey(key));
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
    LOG(INFO) << "lake create tablet " << fmt::format("tid:{}", req.tablet_id);

    if (req.__isset.base_tablet_id && req.base_tablet_id > 0) {
        struct Finder {
            std::string_view name;
            bool operator()(const TabletColumn& c) const { return c.name() == name; }
        };
        ASSIGN_OR_RETURN(auto base_tablet, get_tablet(req.base_tablet_id));
        ASSIGN_OR_RETURN(auto base_schema, base_tablet.get_schema());
        std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
        TTabletSchema mutable_new_schema = req.tablet_schema;
        uint32_t next_unique_id = base_schema->next_column_unique_id();
        const auto& old_columns = base_schema->columns();
        auto& new_columns = mutable_new_schema.columns;
        for (uint32_t i = 0, sz = new_columns.size(); i < sz; ++i) {
            auto it = std::find_if(old_columns.begin(), old_columns.end(), Finder{new_columns[i].column_name});
            if (it != old_columns.end() && it->has_default_value()) {
                new_columns[i].__set_default_value(it->default_value());
                col_idx_to_unique_id[i] = it->unique_id();
            } else if (it != old_columns.end()) {
                col_idx_to_unique_id[i] = it->unique_id();
            } else {
                col_idx_to_unique_id[i] = next_unique_id++;
            }
        }
        RETURN_IF_ERROR(starrocks::convert_t_schema_to_pb_schema(
                mutable_new_schema, next_unique_id, col_idx_to_unique_id, tablet_metadata_pb->mutable_schema(),
                req.__isset.compression_type ? req.compression_type : TCompressionType::LZ4_FRAME));
    } else {
        std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
        uint32_t next_unique_id = req.tablet_schema.columns.size();
        for (uint32_t col_idx = 0; col_idx < next_unique_id; ++col_idx) {
            col_idx_to_unique_id[col_idx] = col_idx;
        }
        RETURN_IF_ERROR(starrocks::convert_t_schema_to_pb_schema(
                req.tablet_schema, next_unique_id, col_idx_to_unique_id, tablet_metadata_pb->mutable_schema(),
                req.__isset.compression_type ? req.compression_type : TCompressionType::LZ4_FRAME));
    }
    return put_tablet_metadata(std::move(tablet_metadata_pb));
}

StatusOr<Tablet> TabletManager::get_tablet(int64_t tablet_id) {
    return Tablet(this, tablet_id);
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
        erase_metacache(obj);
        (void)fs->delete_file(obj);
    }
    //drop tablet schema from metacache;
    erase_metacache(tablet_schema_cache_key(tablet_id));
    return Status::OK();
}

Status TabletManager::put_tablet_metadata(TabletMetadataPtr metadata) {
    // write metadata file
    auto filepath = _location_provider->tablet_metadata_location(metadata->id(), metadata->version());
    auto options = WritableFileOptions{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto writer_file = fs::new_writable_file(options, filepath);
    if (!writer_file.ok()) return writer_file.status();
    RETURN_IF_ERROR((*writer_file)->append(metadata->SerializeAsString()));
    RETURN_IF_ERROR((*writer_file)->close());

    // put into metacache
    auto metadata_location = tablet_metadata_location(metadata->id(), metadata->version());
    auto value_ptr = std::make_unique<CacheValue>(metadata);
    bool inserted = fill_metacache(metadata_location, value_ptr.release(), static_cast<int>(metadata->SpaceUsedLong()));
    LOG_IF(WARNING, !inserted) << "Failed to put into meta cache " << metadata_location;
    return Status::OK();
}

Status TabletManager::put_tablet_metadata(const TabletMetadata& metadata) {
    auto metadata_ptr = std::make_shared<TabletMetadata>(metadata);
    return put_tablet_metadata(std::move(metadata_ptr));
}

StatusOr<TabletMetadataPtr> TabletManager::load_tablet_metadata(const string& metadata_location, bool fill_cache) {
    MetaFileReader reader(metadata_location, fill_cache);
    RETURN_IF_ERROR(reader.load());
    return reader.get_meta();
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(int64_t tablet_id, int64_t version) {
    return get_tablet_metadata(tablet_metadata_location(tablet_id, version));
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const string& path, bool fill_cache) {
    if (auto ptr = lookup_tablet_metadata(path); ptr != nullptr) {
        return ptr;
    }
    ASSIGN_OR_RETURN(auto ptr, load_tablet_metadata(path, fill_cache));
    if (fill_cache) {
        auto value_ptr = std::make_unique<CacheValue>(ptr);
        bool inserted = fill_metacache(path, value_ptr.release(), static_cast<int>(ptr->SpaceUsedLong()));
        LOG_IF(WARNING, !inserted) << "Failed to put tablet metadata into cache " << path;
    }
    return ptr;
}

Status TabletManager::delete_tablet_metadata(int64_t tablet_id, int64_t version) {
    auto location = tablet_metadata_location(tablet_id, version);
    erase_metacache(location);
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
    std::string read_buf;
    RandomAccessFileOptions opts{.skip_fill_local_cache = !fill_cache};
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(opts, txn_log_path));
    ASSIGN_OR_RETURN(auto size, rf->get_size());
    if (UNLIKELY(size > std::numeric_limits<int>::max())) {
        return Status::Corruption("file size exceeded the int range");
    }
    raw::stl_string_resize_uninitialized(&read_buf, size);
    RETURN_IF_ERROR(rf->read_at_fully(0, read_buf.data(), size));

    std::shared_ptr<TxnLog> meta = std::make_shared<TxnLog>();
    bool parsed = meta->ParseFromArray(read_buf.data(), static_cast<int>(size));
    if (!parsed) {
        return Status::Corruption(fmt::format("failed to parse txn log {}", txn_log_path));
    }
    return std::move(meta);
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(const std::string& path, bool fill_cache) {
    if (auto ptr = lookup_txn_log(path); ptr != nullptr) {
        return ptr;
    }
    ASSIGN_OR_RETURN(auto ptr, load_txn_log(path, fill_cache));
    if (fill_cache) {
        auto value_ptr = std::make_unique<CacheValue>(ptr);
        bool inserted = fill_metacache(path, value_ptr.release(), static_cast<int>(ptr->SpaceUsedLong()));
        LOG_IF(WARNING, !inserted) << "Failed to cache " << path;
    }
    return ptr;
}

StatusOr<TxnLogPtr> TabletManager::get_txn_log(int64_t tablet_id, int64_t txn_id) {
    return get_txn_log(txn_log_location(tablet_id, txn_id));
}

StatusOr<TxnLogPtr> TabletManager::get_txn_vlog(int64_t tablet_id, int64_t version) {
    return get_txn_log(txn_vlog_location(tablet_id, version), false);
}

Status TabletManager::put_txn_log(TxnLogPtr log) {
    if (UNLIKELY(!log->has_tablet_id())) {
        return Status::InvalidArgument("txn log does not have tablet id");
    }
    if (UNLIKELY(!log->has_txn_id())) {
        return Status::InvalidArgument("txn log does not have txn id");
    }
    auto options = WritableFileOptions{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto txn_log_path = txn_log_location(log->tablet_id(), log->txn_id());
    VLOG(5) << "Writing " << txn_log_path;
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(options, txn_log_path));
    RETURN_IF_ERROR(wf->append(log->SerializeAsString()));
    RETURN_IF_ERROR(wf->close());

    // put txnlog into cache
    auto value_ptr = std::make_unique<CacheValue>(log);
    bool inserted = fill_metacache(txn_log_path, value_ptr.release(), static_cast<int>(log->SpaceUsedLong()));
    LOG_IF(WARNING, !inserted) << "Failed to put txnlog into cache " << txn_log_path;
    return Status::OK();
}

Status TabletManager::put_txn_log(const TxnLog& log) {
    return put_txn_log(std::make_shared<TxnLog>(log));
}

Status TabletManager::delete_txn_log(int64_t tablet_id, int64_t txn_id) {
    auto location = txn_log_location(tablet_id, txn_id);
    erase_metacache(location);
    auto st = fs::delete_file(location);
    return st.is_not_found() ? Status::OK() : st;
}

Status TabletManager::delete_txn_vlog(int64_t tablet_id, int64_t version) {
    auto location = txn_vlog_location(tablet_id, version);
    erase_metacache(location);
    auto st = fs::delete_file(location);
    return st.is_not_found() ? Status::OK() : st;
}

Status TabletManager::delete_segment(int64_t tablet_id, std::string_view segment_name) {
    erase_metacache(segment_name);
    auto st = fs::delete_file(segment_location(tablet_id, segment_name));
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

StatusOr<TabletSchemaPtr> TabletManager::get_tablet_schema(int64_t tablet_id) {
    auto cache_key = tablet_schema_cache_key(tablet_id);
    auto ptr = lookup_tablet_schema(cache_key);
    RETURN_IF(ptr != nullptr, ptr);
    // TODO: limit the list size
    ASSIGN_OR_RETURN(TabletMetadataIter metadata_iter, list_tablet_metadata(tablet_id, true));
    if (!metadata_iter.has_next()) {
        return Status::NotFound(fmt::format("tablet {} metadata not found", tablet_id));
    }
    ASSIGN_OR_RETURN(auto metadata, metadata_iter.next());
    auto [schema, inserted] = GlobalTabletSchemaMap::Instance()->emplace(metadata->schema());
    if (UNLIKELY(schema == nullptr)) {
        return Status::InternalError(fmt::format("tablet schema {} failed to emplace in TabletSchemaMap", tablet_id));
    }
    auto cache_value = std::make_unique<CacheValue>(schema);
    auto cache_size = inserted ? (int)schema->mem_usage() : 0;
    (void)fill_metacache(cache_key, cache_value.release(), cache_size);
    return schema;
}

StatusOr<double> TabletManager::publish_version(int64_t tablet_id, int64_t base_version, int64_t new_version,
                                                const int64_t* txns, int txns_size) {
    ASSIGN_OR_RETURN(auto tablet, get_tablet(tablet_id));
    return publish(&tablet, base_version, new_version, txns, txns_size);
}

StatusOr<double> publish(Tablet* tablet, int64_t base_version, int64_t new_version, const int64_t* txns,
                         int txns_size) {
    // Read base version metadata
    auto res = tablet->get_metadata(base_version);
    if (res.status().is_not_found()) {
        auto target_metadata_or = tablet->get_metadata(new_version);
        if (target_metadata_or.ok()) {
            // base version metadata does not exist but the new version metadata has been generated, maybe
            // this is a duplicated publish version request.
            return compaction_score(**target_metadata_or);
        }
    }

    if (!res.ok()) {
        LOG(WARNING) << "Fail to get " << tablet->metadata_location(base_version) << ": " << res.status();
        return res.status();
    }

    auto base_metadata = std::move(res).value();
    auto new_metadata = std::make_shared<TabletMetadataPB>(*base_metadata);
    auto log_applier = new_txn_log_applier(*tablet, new_metadata, new_version);

    auto init_st = log_applier->init();
    if (!init_st.ok()) {
        if (init_st.is_already_exist()) {
            auto target_metadata_or = tablet->get_metadata(new_version);
            if (target_metadata_or.ok()) {
                // try to publish already finished txn
                return compaction_score(**target_metadata_or);
            } else {
                return target_metadata_or.status();
            }
        } else {
            return init_st;
        }
    }

    // Apply txn logs
    int64_t alter_version = -1;
    for (int i = 0; i < txns_size; i++) {
        auto txn_id = txns[i];
        auto txn_log_st = tablet->get_txn_log(txn_id);

        if (txn_log_st.status().is_not_found()) {
            auto target_metadata_or = tablet->get_metadata(new_version);
            if (target_metadata_or.ok()) {
                // txn log does not exist but the new version metadata has been generated, maybe
                // this is a duplicated publish version request.
                return compaction_score(**target_metadata_or);
            }
        }

        if (!txn_log_st.ok()) {
            LOG(WARNING) << "Fail to get " << tablet->txn_log_location(txn_id) << ": " << txn_log_st.status();
            return txn_log_st.status();
        }

        auto& txn_log = txn_log_st.value();
        if (txn_log->has_op_schema_change()) {
            alter_version = txn_log->op_schema_change().alter_version();
        }

        auto st = log_applier->apply(*txn_log);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to apply " << tablet->txn_log_location(txn_id) << ": " << st;
            return st;
        }
    }

    // Apply vtxn logs for schema change
    // Should firstly apply schema change txn log, then apply txn version logs,
    // because the rowsets in txn log are older.
    if (alter_version != -1 && alter_version + 1 < new_version) {
        DCHECK(base_version == 1 && txns_size == 1);
        for (int64_t v = alter_version + 1; v < new_version; ++v) {
            auto txn_vlog = tablet->get_txn_vlog(v);
            if (txn_vlog.status().is_not_found()) {
                auto target_metadata_or = tablet->get_metadata(new_version);
                if (target_metadata_or.ok()) {
                    // txn version log does not exist but the new version metadata has been generated, maybe
                    // this is a duplicated publish version request.
                    return compaction_score(**target_metadata_or);
                }
            }

            if (!txn_vlog.ok()) {
                LOG(WARNING) << "Fail to get " << tablet->txn_vlog_location(v) << ": " << txn_vlog.status();
                return txn_vlog.status();
            }

            auto st = log_applier->apply(**txn_vlog);
            if (!st.ok()) {
                LOG(WARNING) << "Fail to apply " << tablet->txn_vlog_location(v) << ": " << st;
                return st;
            }
        }
    }

    // Save new metadata
    RETURN_IF_ERROR(log_applier->finish());

    // Delete txn logs
    for (int i = 0; i < txns_size; i++) {
        auto txn_id = txns[i];
        auto st = tablet->delete_txn_log(txn_id);
        LOG_IF(WARNING, !st.ok()) << "Fail to delete " << tablet->txn_log_location(txn_id) << ": " << st;
    }
    // Delete vtxn logs
    if (alter_version != -1 && alter_version + 1 < new_version) {
        for (int64_t v = alter_version + 1; v < new_version; ++v) {
            auto st = tablet->delete_txn_vlog(v);
            LOG_IF(WARNING, !st.ok()) << "Fail to delete " << tablet->txn_vlog_location(v) << ": " << st;
        }
    }
    return compaction_score(*new_metadata);
}

StatusOr<CompactionTaskPtr> TabletManager::compact(int64_t tablet_id, int64_t version, int64_t txn_id) {
    ASSIGN_OR_RETURN(auto tablet, get_tablet(tablet_id));
    auto tablet_ptr = std::make_shared<Tablet>(tablet);
    ASSIGN_OR_RETURN(auto compaction_policy, CompactionPolicy::create_compaction_policy(tablet_ptr));
    ASSIGN_OR_RETURN(auto input_rowsets, compaction_policy->pick_rowsets(version));
    return std::make_shared<HorizontalCompactionTask>(txn_id, version, std::move(tablet_ptr), std::move(input_rowsets));
}

void TabletManager::abort_txn(int64_t tablet_id, const int64_t* txns, int txns_size) {
    // TODO: batch deletion
    for (int i = 0; i < txns_size; i++) {
        auto txn_id = txns[i];
        auto txn_log_or = get_txn_log(tablet_id, txn_id);
        if (!txn_log_or.ok()) {
            LOG_IF(WARNING, !txn_log_or.status().is_not_found())
                    << "Fail to get txn log " << txn_log_location(tablet_id, txn_id) << ": " << txn_log_or.status();
            continue;
        }

        TxnLogPtr txn_log = std::move(txn_log_or).value();
        if (txn_log->has_op_write()) {
            for (const auto& segment : txn_log->op_write().rowset().segments()) {
                auto st = delete_segment(tablet_id, segment);
                LOG_IF(WARNING, !st.ok() && !st.is_not_found()) << "Fail to delete " << segment << ": " << st;
            }
        }
        if (txn_log->has_op_compaction()) {
            for (const auto& segment : txn_log->op_compaction().output_rowset().segments()) {
                auto st = delete_segment(tablet_id, segment);
                LOG_IF(WARNING, !st.ok() && !st.is_not_found()) << "Fail to delete " << segment << ": " << st;
            }
        }
        if (txn_log->has_op_schema_change() && !txn_log->op_schema_change().linked_segment()) {
            for (const auto& rowset : txn_log->op_schema_change().rowsets()) {
                for (const auto& segment : rowset.segments()) {
                    auto st = delete_segment(tablet_id, segment);
                    LOG_IF(WARNING, !st.ok() && !st.is_not_found()) << "Fail to delete " << segment << ": " << st;
                }
            }
        }
        auto st = delete_txn_log(tablet_id, txn_id);
        LOG_IF(WARNING, !st.ok() && !st.is_not_found())
                << "Fail to delete " << txn_log_location(tablet_id, txn_id) << ": " << st;
    }
}

Status TabletManager::publish_log_version(int64_t tablet_id, int64_t txn_id, int64 log_version) {
    auto txn_log_path = txn_log_location(tablet_id, txn_id);
    auto txn_vlog_path = txn_vlog_location(tablet_id, log_version);
    // TODO: use rename() API if supported by the underlying filesystem.
    auto st = fs::copy_file(txn_log_path, txn_vlog_path);
    if (st.is_not_found()) {
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(txn_vlog_path));
        auto check_st = fs->path_exists(txn_vlog_path);
        if (check_st.ok()) {
            return Status::OK();
        } else {
            LOG_IF(WARNING, !check_st.is_not_found())
                    << "Fail to check the existance of " << txn_vlog_path << ": " << check_st;
            return st;
        }
    } else if (!st.ok()) {
        return st;
    } else {
        (void)fs::delete_file(txn_log_path);
        return Status::OK();
    }
}

Status TabletManager::put_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time) {
    auto options = WritableFileOptions{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto tablet_metadata_lock_path = tablet_metadata_lock_location(tablet_id, version, expire_time);
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(options, tablet_metadata_lock_path));
    auto tablet_metadata_lock = std::make_unique<TabletMetadataLockPB>();
    RETURN_IF_ERROR(wf->append(tablet_metadata_lock->SerializeAsString()));
    RETURN_IF_ERROR(wf->close());

    return Status::OK();
}

Status TabletManager::delete_tablet_metadata_lock(int64_t tablet_id, int64_t version, int64_t expire_time) {
    auto location = tablet_metadata_lock_location(tablet_id, version, expire_time);
    auto st = fs::delete_file(location);
    return st.is_not_found() ? Status::OK() : st;
}

std::set<int64_t> TabletManager::owned_tablets() {
    return _location_provider->owned_tablets();
}

void TabletManager::start_gc() {
    int r = bthread_start_background(&_gc_checker_tid, nullptr, gc_checker, this);
    PLOG_IF(FATAL, r != 0) << "Fail to call bthread_start_background";
}

static void metadata_gc(TabletManager* tablet_mgr, const std::set<std::string>& roots, int64_t min_active_txn_id) {
    auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::CLONE);
    auto num_running = std::atomic<int>(roots.size());
    for (const auto& root : roots) {
        auto st = thread_pool->submit_func([&, root]() {
            auto t1 = std::chrono::steady_clock::now();
            auto r = metadata_gc(root, tablet_mgr, min_active_txn_id);
            auto t2 = std::chrono::steady_clock::now();
            auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
            if (r.ok()) {
                LOG(INFO) << "Finished garbage collection of metadata for directory " << root << ". cost:" << cost
                          << "ms";
            } else {
                LOG(WARNING) << "Fail to do garbage collection of metadata for directory " << root << ". cost:" << cost
                             << "ms error:" << r;
            }
            num_running.fetch_sub(1);
        });
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit task to threadpool: " << st;
            num_running.fetch_sub(1);
        }
    }
    while (num_running.load() > 0) {
        LOG_EVERY_N(INFO, 10) << "Waiting for GC tasks to finish...";
        bthread_usleep(/*1s=*/1000 * 1000);
    }
}

static void data_gc(TabletManager* tablet_mgr, const std::set<std::string>& roots) {
    auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::CLONE);
    auto num_running = std::atomic<int>(roots.size());
    for (const auto& root : roots) {
        auto st = thread_pool->submit_func([&, root]() {
            auto t1 = std::chrono::steady_clock::now();
            auto r = datafile_gc(root, tablet_mgr);
            auto t2 = std::chrono::steady_clock::now();
            auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
            if (r.ok()) {
                LOG(INFO) << "Finished garbage collection of data for directory " << root << ". cost:" << cost << "ms";
            } else {
                LOG(WARNING) << "Fail to do garbage collection of data for directory " << root << ". cost:" << cost
                             << "ms error:" << r;
            }
            num_running.fetch_sub(1);
        });
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit task to threadpool: " << st;
            num_running.fetch_sub(1);
        }
    }
    while (num_running.load() > 0) {
        LOG_EVERY_N(INFO, 10) << "Waiting for GC tasks to finish...";
        bthread_usleep(/*1s=*/1000 * 1000);
    }
}

void* gc_checker(void* arg) {
    auto tablet_mgr = static_cast<TabletManager*>(arg);
    auto lp = tablet_mgr->location_provider();
    // NOTE: Share the same thread pool with local tablet's clone task.
    int64_t curr = butil::gettimeofday_s();
    int64_t gc_time[2] = {curr + config::lake_gc_metadata_check_interval,
                          curr + config::lake_gc_segment_check_interval};
    while (!bthread_stopped(bthread_self())) {
        int64_t now = butil::gettimeofday_s();
        int64_t min_gc_time = std::min(gc_time[0], gc_time[1]);
        if (min_gc_time > now) {
            // NOTE: When the work load of bthread workers is high, the real sleep interval may be much longer than the
            // configured value, which is ok now.
            (void)bthread_usleep((min_gc_time - now) * 1000ULL * 1000ULL);
        }

        std::set<std::string> roots;
        (void)lp->list_root_locations(&roots);

        if (min_gc_time == gc_time[0]) {
            auto master_info = get_master_info();
            metadata_gc(tablet_mgr, roots, master_info.min_active_txn_id);
            gc_time[0] = butil::gettimeofday_s() + config::lake_gc_metadata_check_interval;
        } else {
            data_gc(tablet_mgr, roots);
            gc_time[1] = butil::gettimeofday_s() + config::lake_gc_segment_check_interval;
        }
    }
    return nullptr;
}

} // namespace starrocks::lake
