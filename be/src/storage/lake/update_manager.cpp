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

#include "storage/lake/update_manager.h"

#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/lake_local_persistent_index.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/lake_primary_key_compaction_conflict_resolver.h"
#include "storage/lake/local_pk_index_manager.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_compaction_state.h"
#include "storage/rows_mapper.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/tablet_manager.h"
#include "testutil/sync_point.h"
#include "util/failpoint/fail_point.h"
#include "util/pretty_printer.h"
#include "util/trace.h"

namespace starrocks::lake {

UpdateManager::UpdateManager(std::shared_ptr<LocationProvider> location_provider, MemTracker* mem_tracker)
        : _index_cache(std::numeric_limits<size_t>::max()),
          _update_state_cache(std::numeric_limits<size_t>::max()),
          _compaction_cache(std::numeric_limits<size_t>::max()),
          _location_provider(std::move(location_provider)),
          _pk_index_shards(config::pk_index_map_shard_size) {
    _update_mem_tracker = mem_tracker;
    const int64_t update_mem_limit = _update_mem_tracker->limit();
    const int64_t preload_mem_limit =
            update_mem_limit * std::max(std::min(100, config::lake_pk_preload_memory_limit_percent), 0) / 100;
    _update_state_mem_tracker =
            std::make_unique<MemTracker>(preload_mem_limit, "lake_rowset_update_state", mem_tracker);
    _index_cache_mem_tracker = std::make_unique<MemTracker>(-1, "lake_index_cache", mem_tracker);
    _compaction_state_mem_tracker =
            std::make_unique<MemTracker>(preload_mem_limit, "lake_compaction_state", mem_tracker);
    _index_cache.set_mem_tracker(_index_cache_mem_tracker.get());
    _update_state_cache.set_mem_tracker(_update_state_mem_tracker.get());

    _index_cache.set_capacity(update_mem_limit);

    const int64_t block_cache_mem_limit =
            update_mem_limit * std::max(std::min(100, config::lake_pk_index_block_cache_limit_percent), 0) / 100;
    _block_cache = std::make_unique<PersistentIndexBlockCache>(mem_tracker, block_cache_mem_limit);
}

UpdateManager::~UpdateManager() {
    _index_cache.clear();
    _update_state_cache.clear();
    _compaction_cache.clear();
}

inline std::string cache_key(uint32_t tablet_id, int64_t txn_id) {
    return strings::Substitute("$0_$1", tablet_id, txn_id);
}

PersistentIndexBlockCache::PersistentIndexBlockCache(MemTracker* mem_tracker, int64_t cache_limit)
        : _cache(new_lru_cache(cache_limit)) {
    _mem_tracker = std::make_unique<MemTracker>(cache_limit, "lake_persistent_index_block_cache", mem_tracker);
}

void PersistentIndexBlockCache::update_memory_usage() {
    std::lock_guard<std::mutex> lg(_mutex);
    size_t current_mem_usage = _cache->get_memory_usage();
    if (_memory_usage > current_mem_usage) {
        _mem_tracker->release(_memory_usage - current_mem_usage);
    } else {
        _mem_tracker->consume(current_mem_usage - _memory_usage);
    }
    _memory_usage = current_mem_usage;
}

void RssidFileInfoContainer::add_rssid_to_file(const TabletMetadata& metadata) {
    for (auto& rs : metadata.rowsets()) {
        bool has_segment_size = (rs.segments_size() == rs.segment_size_size());
        bool has_encryption_meta = (rs.segments_size() == rs.segment_encryption_metas_size());
        for (int i = 0; i < rs.segments_size(); i++) {
            FileInfo segment_info{.path = rs.segments(i)};
            if (LIKELY(has_segment_size)) {
                segment_info.size = rs.segment_size(i);
            }
            if (LIKELY(has_encryption_meta)) {
                segment_info.encryption_meta = rs.segment_encryption_metas(i);
            }
            _rssid_to_file_info[rs.id() + i] = segment_info;
            _rssid_to_rowid[rs.id() + i] = rs.id();
        }
    }
}

void RssidFileInfoContainer::add_rssid_to_file(const RowsetMetadataPB& meta, uint32_t rowset_id, uint32_t segment_id,
                                               const std::map<int, FileInfo>& replace_segments) {
    DCHECK(segment_id < meta.segments_size());
    if (replace_segments.count(segment_id) > 0) {
        // partial update
        _rssid_to_file_info[rowset_id + segment_id] = replace_segments.at(segment_id);
        _rssid_to_rowid[rowset_id + segment_id] = rowset_id;
    } else {
        bool has_segment_size = (meta.segments_size() == meta.segment_size_size());
        bool has_encryption_meta = (meta.segments_size() == meta.segment_encryption_metas_size());
        FileInfo segment_info{.path = meta.segments(segment_id)};
        if (LIKELY(has_segment_size)) {
            segment_info.size = meta.segment_size(segment_id);
        }
        if (LIKELY(has_encryption_meta)) {
            segment_info.encryption_meta = meta.segment_encryption_metas(segment_id);
        }
        _rssid_to_file_info[rowset_id + segment_id] = segment_info;
        _rssid_to_rowid[rowset_id + segment_id] = rowset_id;
    }
}

StatusOr<IndexEntry*> UpdateManager::prepare_primary_index(
        const TabletMetadataPtr& metadata, MetaFileBuilder* builder, int64_t base_version, int64_t new_version,
        std::unique_ptr<std::lock_guard<std::shared_timed_mutex>>& guard) {
    auto index_entry = _index_cache.get_or_create(metadata->id());
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& index = index_entry->value();
    // Fetch lock guard before `lake_load`
    guard = index.fetch_guard();
    Status st = index.lake_load(_tablet_mgr, metadata, base_version, builder);
    _index_cache.update_object_size(index_entry, index.memory_usage());
    if (!st.ok()) {
        if (st.is_already_exist()) {
            StarRocksMetrics::instance()->primary_key_table_error_state_total.increment(1);
            builder->set_recover_flag(RecoverFlag::RECOVER_WITH_PUBLISH);
        }
        // If load failed, release lock guard and remove index entry
        // MUST release lock guard before remove index entry
        guard.reset(nullptr);
        _index_cache.remove(index_entry);
        std::string msg = strings::Substitute("prepare_primary_index: load primary index failed: $0", st.to_string());
        LOG(ERROR) << msg;
        return Status::InternalError(msg);
    }
    _block_cache->update_memory_usage();
    st = index.prepare(EditVersion(new_version, 0), 0);
    if (!st.ok()) {
        // If prepare failed, release lock guard and remove index entry
        guard.reset(nullptr);
        _index_cache.remove(index_entry);
        std::string msg =
                strings::Substitute("prepare_primary_index: prepare primary index failed: $0", st.to_string());
        LOG(ERROR) << msg;
        return Status::InternalError(msg);
    }
    return index_entry;
}

void UpdateManager::release_primary_index_cache(IndexEntry* index_entry) {
    if (index_entry != nullptr) {
        _index_cache.release(index_entry);
    }
}

void UpdateManager::remove_primary_index_cache(IndexEntry* index_entry) {
    if (index_entry != nullptr) {
        _index_cache.remove(index_entry);
    }
}

void UpdateManager::unload_and_remove_primary_index(int64_t tablet_id) {
    auto index_entry = _index_cache.get(tablet_id);
    if (index_entry != nullptr) {
        auto& index = index_entry->value();
        auto guard = index.fetch_guard();
        index.unload();
        guard.reset(nullptr);
        _index_cache.remove(index_entry);
    }
}

StatusOr<IndexEntry*> UpdateManager::rebuild_primary_index(
        const TabletMetadataPtr& metadata, MetaFileBuilder* builder, int64_t base_version, int64_t new_version,
        std::unique_ptr<std::lock_guard<std::shared_timed_mutex>>& guard) {
    LOG(INFO) << "rebuild tablet: " << metadata->id() << " primary index, version: " << base_version;
    unload_and_remove_primary_index(metadata->id());
    return prepare_primary_index(metadata, builder, base_version, new_version, guard);
}

DEFINE_FAIL_POINT(hook_publish_primary_key_tablet);
// |metadata| contain last tablet meta info with new version
Status UpdateManager::publish_primary_key_tablet(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                                                 const TabletMetadataPtr& metadata, Tablet* tablet,
                                                 IndexEntry* index_entry, MetaFileBuilder* builder,
                                                 int64_t base_version) {
    FAIL_POINT_TRIGGER_EXECUTE(hook_publish_primary_key_tablet, {
        builder->apply_opwrite(op_write, {}, {});
        return Status::OK();
    });
    auto& index = index_entry->value();
    // 1. load rowset update data to cache, get upsert and delete list
    const uint32_t rowset_id = metadata->next_rowset_id();
    auto tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
    auto state_entry = _update_state_cache.get_or_create(cache_key(tablet->id(), txn_id));
    state_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    // only use state entry once, remove it when publish finish or fail
    DeferOp remove_state_entry([&] { _update_state_cache.remove(state_entry); });
    auto& state = state_entry->value();

    std::vector<std::string> orphan_files;
    std::map<int, FileInfo> replace_segments;
    RssidFileInfoContainer rssid_fileinfo_container;
    rssid_fileinfo_container.add_rssid_to_file(*metadata);
    // Init update state.
    RowsetUpdateStateParams params{
            .op_write = op_write,
            .tablet_schema = tablet_schema,
            .metadata = metadata,
            .tablet = tablet,
            .container = rssid_fileinfo_container,
    };
    state.init(params);
    // Init delvec state.
    PrimaryIndex::DeletesMap new_deletes;
    for (uint32_t segment_id = 0; segment_id < op_write.rowset().segments_size(); segment_id++) {
        new_deletes[rowset_id + segment_id] = {};
    }
    // Rssid of delete files is equal to `rowset_id + op_offset`, and delete is always after upsert now,
    // so we use max segment id as `op_offset`.
    // TODO : support real order of mix upsert and delete in one transaction.
    const uint32_t del_rebuild_rssid = rowset_id + std::max(op_write.rowset().segments_size(), 1) - 1;
    // 2. Handle segment one by one to save memory usage.
    for (uint32_t segment_id = 0; segment_id < op_write.rowset().segments_size(); segment_id++) {
        RETURN_IF_ERROR(state.load_segment(segment_id, params, base_version, true /*reslove conflict*/,
                                           false /*no need lock*/));
        _update_state_cache.update_object_size(state_entry, state.memory_usage());
        // 2.1 rewrite segment file if it is partial update
        RETURN_IF_ERROR(state.rewrite_segment(segment_id, params, &replace_segments, &orphan_files));
        rssid_fileinfo_container.add_rssid_to_file(op_write.rowset(), metadata->next_rowset_id(), segment_id,
                                                   replace_segments);
        // handle merge condition, skip update row which's merge condition column value is smaller than current row
        int32_t condition_column = _get_condition_column(op_write, *tablet_schema);
        // 2.2 update primary index, and generate delete info.
        TRACE_COUNTER_SCOPE_LATENCY_US("update_index_latency_us");
        DCHECK(state.upserts(segment_id) != nullptr);
        if (condition_column < 0) {
            RETURN_IF_ERROR(_do_update(rowset_id, segment_id, state.upserts(segment_id), index, &new_deletes));
        } else {
            RETURN_IF_ERROR(_do_update_with_condition(params, rowset_id, segment_id, condition_column,
                                                      state.upserts(segment_id), index, &new_deletes));
        }
        // 2.3 handle auto increment deletes
        if (state.auto_increment_deletes(segment_id) != nullptr) {
            RETURN_IF_ERROR(
                    index.erase(metadata, *state.auto_increment_deletes(segment_id), &new_deletes, del_rebuild_rssid));
        }
        _index_cache.update_object_size(index_entry, index.memory_usage());
        state.release_segment(segment_id);
        _update_state_cache.update_object_size(state_entry, state.memory_usage());
    }

    // 3. Handle del files one by one.
    for (uint32_t del_id = 0; del_id < op_write.dels_size(); del_id++) {
        RETURN_IF_ERROR(state.load_delete(del_id, params));
        DCHECK(state.deletes(del_id) != nullptr);
        RETURN_IF_ERROR(index.erase(metadata, *state.deletes(del_id), &new_deletes, del_rebuild_rssid));
        _index_cache.update_object_size(index_entry, index.memory_usage());
        state.release_delete(del_id);
    }

    _block_cache->update_memory_usage();
    // 4. generate delvec
    size_t ndelvec = new_deletes.size();
    vector<std::pair<uint32_t, DelVectorPtr>> new_del_vecs(ndelvec);
    size_t idx = 0;
    size_t new_del = 0;
    size_t total_del = 0;
    std::map<uint32_t, size_t> segment_id_to_add_dels;
    for (auto& new_delete : new_deletes) {
        uint32_t rssid = new_delete.first;
        if (rssid >= rowset_id && rssid < rowset_id + op_write.rowset().segments_size()) {
            // it's newly added rowset's segment, do not have latest delvec yet
            new_del_vecs[idx].first = rssid;
            new_del_vecs[idx].second = std::make_shared<DelVector>();
            auto& del_ids = new_delete.second;
            new_del_vecs[idx].second->init(metadata->version(), del_ids.data(), del_ids.size());
            new_del += del_ids.size();
            total_del += del_ids.size();
            segment_id_to_add_dels[rssid] += del_ids.size();
        } else {
            TabletSegmentId tsid;
            tsid.tablet_id = tablet->id();
            tsid.segment_id = rssid;
            DelVectorPtr old_del_vec;
            RETURN_IF_ERROR(get_del_vec(tsid, base_version, builder, false /* file cache */, &old_del_vec));
            new_del_vecs[idx].first = rssid;
            old_del_vec->add_dels_as_new_version(new_delete.second, metadata->version(), &(new_del_vecs[idx].second));
            size_t cur_old = old_del_vec->cardinality();
            size_t cur_add = new_delete.second.size();
            size_t cur_new = new_del_vecs[idx].second->cardinality();
            // For test purpose, we can set recover flag to test recover mode.
            Status test_status = Status::OK();
            TEST_SYNC_POINT_CALLBACK("delvec_inconsistent", &test_status);
            if (!test_status.ok()) {
                builder->set_recover_flag(RecoverFlag::RECOVER_WITH_PUBLISH);
                return test_status;
            }
            if (cur_old + cur_add != cur_new) {
                // should not happen, data inconsistent
                std::string error_msg = strings::Substitute(
                        "delvec inconsistent tablet:$0 rssid:$1 #old:$2 #add:$3 #new:$4 old_v:$5 "
                        "v:$6",
                        tablet->id(), rssid, cur_old, cur_add, cur_new, old_del_vec->version(), metadata->version());
                LOG(ERROR) << error_msg;
                StarRocksMetrics::instance()->primary_key_table_error_state_total.increment(1);
                if (!config::experimental_lake_ignore_pk_consistency_check) {
                    builder->set_recover_flag(RecoverFlag::RECOVER_WITH_PUBLISH);
                    return Status::InternalError(error_msg);
                }
            }
            new_del += cur_add;
            total_del += cur_new;
            segment_id_to_add_dels[rssid] += cur_add;
        }

        idx++;
    }
    new_deletes.clear();

    // 5. update TabletMeta and write to meta file
    for (auto&& each : new_del_vecs) {
        builder->append_delvec(each.second, each.first);
    }
    builder->apply_opwrite(op_write, replace_segments, orphan_files);
    RETURN_IF_ERROR(builder->update_num_del_stat(segment_id_to_add_dels));

    TRACE_COUNTER_INCREMENT("rowsetid", rowset_id);
    TRACE_COUNTER_INCREMENT("upserts", op_write.rowset().segments_size());
    TRACE_COUNTER_INCREMENT("deletes", op_write.dels_size());
    TRACE_COUNTER_INCREMENT("new_del", new_del);
    TRACE_COUNTER_INCREMENT("total_del", total_del);
    TRACE_COUNTER_INCREMENT("upsert_rows", op_write.rowset().num_rows());
    TRACE_COUNTER_INCREMENT("base_version", base_version);
    _print_memory_stats();
    return Status::OK();
}

Status UpdateManager::publish_column_mode_partial_update(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                                                         const TabletMetadataPtr& metadata, Tablet* tablet,
                                                         MetaFileBuilder* builder, int64_t base_version) {
    auto tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
    RssidFileInfoContainer rssid_fileinfo_container;
    rssid_fileinfo_container.add_rssid_to_file(*metadata);

    RowsetUpdateStateParams params{
            .op_write = op_write,
            .tablet_schema = tablet_schema,
            .metadata = metadata,
            .tablet = tablet,
            .container = rssid_fileinfo_container,
    };

    ColumnModePartialUpdateHandler handler(base_version, txn_id, _update_mem_tracker);
    RETURN_IF_ERROR(handler.execute(params, builder));
    return Status::OK();
}

Status UpdateManager::_do_update(uint32_t rowset_id, int32_t upsert_idx, const ColumnUniquePtr& upsert,
                                 PrimaryIndex& index, DeletesMap* new_deletes) {
    TRACE_COUNTER_SCOPE_LATENCY_US("do_update_latency_us");
    return index.upsert(rowset_id + upsert_idx, 0, *upsert, new_deletes);
}

Status UpdateManager::_do_update_with_condition(const RowsetUpdateStateParams& params, uint32_t rowset_id,
                                                int32_t upsert_idx, int32_t condition_column,
                                                const ColumnUniquePtr& upsert, PrimaryIndex& index,
                                                DeletesMap* new_deletes) {
    RETURN_ERROR_IF_FALSE(condition_column >= 0);
    TRACE_COUNTER_SCOPE_LATENCY_US("do_update_latency_us");
    const auto& tablet_column = params.tablet_schema->column(condition_column);
    std::vector<uint32_t> read_column_ids;
    read_column_ids.push_back(condition_column);

    std::vector<uint64_t> old_rowids(upsert->size());
    RETURN_IF_ERROR(index.get(*upsert, &old_rowids));
    bool non_old_value = std::all_of(old_rowids.begin(), old_rowids.end(), [](int id) { return -1 == id; });
    if (!non_old_value) {
        std::map<uint32_t, std::vector<uint32_t>> old_rowids_by_rssid;
        size_t num_default = 0;
        vector<uint32_t> idxes;
        RowsetUpdateState::plan_read_by_rssid(old_rowids, &num_default, &old_rowids_by_rssid, &idxes);
        std::vector<std::unique_ptr<Column>> old_columns(1);
        auto old_unordered_column =
                ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        old_columns[0] = old_unordered_column->clone_empty();
        RETURN_IF_ERROR(get_column_values(params, read_column_ids, num_default > 0, old_rowids_by_rssid, &old_columns));
        auto old_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        old_column->append_selective(*old_columns[0], idxes.data(), 0, idxes.size());

        std::map<uint32_t, std::vector<uint32_t>> new_rowids_by_rssid;
        std::vector<uint32_t> rowids;
        for (int j = 0; j < upsert->size(); ++j) {
            rowids.push_back(j);
        }
        new_rowids_by_rssid[rowset_id + upsert_idx] = rowids;
        // only support condition update on single column
        std::vector<std::unique_ptr<Column>> new_columns(1);
        auto new_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        new_columns[0] = new_column->clone_empty();
        RETURN_IF_ERROR(get_column_values(params, read_column_ids, false, new_rowids_by_rssid, &new_columns));

        int idx_begin = 0;
        int upsert_idx_step = 0;
        for (int j = 0; j < old_column->size(); ++j) {
            if (num_default > 0 && idxes[j] == 0) {
                // plan_read_by_rssid will return idx with 0 if we have default value
                upsert_idx_step++;
            } else {
                int r = old_column->compare_at(j, j, *new_columns[0].get(), -1);
                if (r > 0) {
                    RETURN_IF_ERROR(index.upsert(rowset_id + upsert_idx, 0, *upsert, idx_begin,
                                                 idx_begin + upsert_idx_step, new_deletes));

                    idx_begin = j + 1;
                    upsert_idx_step = 0;

                    // Update delete vector of current segment which is being applied
                    (*new_deletes)[rowset_id + upsert_idx].push_back(j);
                } else {
                    upsert_idx_step++;
                }
            }
        }

        if (idx_begin < old_column->size()) {
            RETURN_IF_ERROR(index.upsert(rowset_id + upsert_idx, 0, *upsert, idx_begin, idx_begin + upsert_idx_step,
                                         new_deletes));
        }
    } else {
        RETURN_IF_ERROR(index.upsert(rowset_id + upsert_idx, 0, *upsert, new_deletes));
    }

    return Status::OK();
}

Status UpdateManager::_handle_index_op(int64_t tablet_id, int64_t base_version, bool need_lock,
                                       const std::function<void(LakePrimaryIndex&)>& op) {
    TRACE_COUNTER_SCOPE_LATENCY_US("handle_index_op_latency_us");
    auto index_entry = _index_cache.get(tablet_id);
    if (index_entry == nullptr) {
        return Status::Uninitialized(fmt::format("Primary index not load yet, tablet_id: {}", tablet_id));
    }
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    // release index entry but keep it in cache
    DeferOp release_index_entry([&] { _index_cache.release(index_entry); });
    auto& index = index_entry->value();
    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> guard = nullptr;
    // Fetch lock guard before check `is_load()`
    if (need_lock) {
        guard = index.try_fetch_guard();
        if (guard == nullptr) {
            return Status::Cancelled(fmt::format("Fail to fetch primary index guard, tablet_id: {}", tablet_id));
        }
    }
    if (!index.is_load(base_version)) {
        return Status::Uninitialized(fmt::format("Primary index not load yet, tablet_id: {}", tablet_id));
    }
    op(index);
    _block_cache->update_memory_usage();

    return Status::OK();
}

Status UpdateManager::get_rowids_from_pkindex(int64_t tablet_id, int64_t base_version,
                                              const std::vector<ColumnUniquePtr>& upserts,
                                              std::vector<std::vector<uint64_t>*>* rss_rowids, bool need_lock) {
    Status st;
    st.update(_handle_index_op(tablet_id, base_version, need_lock, [&](LakePrimaryIndex& index) {
        // get rss_rowids for each segment of rowset
        uint32_t num_segments = upserts.size();
        for (size_t i = 0; i < num_segments; i++) {
            auto& pks = *upserts[i];
            st.update(index.get(pks, (*rss_rowids)[i]));
        }
    }));
    return st;
}

Status UpdateManager::get_rowids_from_pkindex(int64_t tablet_id, int64_t base_version, const ColumnUniquePtr& upsert,
                                              std::vector<uint64_t>* rss_rowids, bool need_lock) {
    Status st;
    st.update(_handle_index_op(tablet_id, base_version, need_lock, [&](LakePrimaryIndex& index) {
        // get rss_rowids for segment's pk
        st.update(index.get(*upsert, rss_rowids));
    }));
    return st;
}

Status UpdateManager::get_column_values(const RowsetUpdateStateParams& params, std::vector<uint32_t>& column_ids,
                                        bool with_default, std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                                        vector<std::unique_ptr<Column>>* columns,
                                        const std::map<string, string>* column_to_expr_value,
                                        AutoIncrementPartialUpdateState* auto_increment_state) {
    TRACE_COUNTER_SCOPE_LATENCY_US("get_column_values_latency_us");
    std::stringstream cost_str;
    MonotonicStopWatch watch;
    watch.start();

    if (with_default && auto_increment_state == nullptr) {
        for (auto i = 0; i < column_ids.size(); ++i) {
            const TabletColumn& tablet_column = params.tablet_schema->column(column_ids[i]);
            bool has_default_value = tablet_column.has_default_value();
            std::string default_value = has_default_value ? tablet_column.default_value() : "";
            if (column_to_expr_value != nullptr) {
                auto iter = column_to_expr_value->find(std::string(tablet_column.name()));
                if (iter != column_to_expr_value->end()) {
                    has_default_value = true;
                    default_value = iter->second;
                }
            }
            if (has_default_value) {
                const TypeInfoPtr& type_info = get_type_info(tablet_column);
                std::unique_ptr<DefaultValueColumnIterator> default_value_iter =
                        std::make_unique<DefaultValueColumnIterator>(true, default_value, tablet_column.is_nullable(),
                                                                     type_info, tablet_column.length(), 1);
                ColumnIteratorOptions iter_opts;
                RETURN_IF_ERROR(default_value_iter->init(iter_opts));
                RETURN_IF_ERROR(default_value_iter->fetch_values_by_rowid(nullptr, 1, (*columns)[i].get()));
            } else {
                (*columns)[i]->append_default();
            }
        }
    }
    cost_str << " [with_default] " << watch.elapsed_time();
    watch.reset();

    std::shared_ptr<FileSystem> fs;
    auto fetch_values_from_segment = [&](const FileInfo& segment_info, uint32_t segment_id,
                                         const TabletSchemaCSPtr& tablet_schema, const std::vector<uint32_t>& rowids,
                                         const std::vector<uint32_t>& read_column_ids) -> Status {
        FileInfo file_info{.path = params.tablet->segment_location(segment_info.path),
                           .encryption_meta = segment_info.encryption_meta};
        if (segment_info.size.has_value()) {
            file_info.size = segment_info.size;
        }
        auto segment = Segment::open(fs, file_info, segment_id, tablet_schema);
        if (!segment.ok()) {
            LOG(WARNING) << "Fail to open rssid: " << segment_id << " path: " << file_info.path << " : "
                         << segment.status();
            return segment.status();
        }
        if ((*segment)->num_rows() == 0) {
            return Status::OK();
        }

        RandomAccessFileOptions opts;
        if (!file_info.encryption_meta.empty()) {
            ASSIGN_OR_RETURN(auto info, KeyCache::instance().unwrap_encryption_meta(file_info.encryption_meta));
            opts.encryption_info = std::move(info);
        }
        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(opts, file_info));
        iter_opts.read_file = read_file.get();
        for (auto i = 0; i < read_column_ids.size(); ++i) {
            const TabletColumn& col = tablet_schema->column(read_column_ids[i]);
            ASSIGN_OR_RETURN(auto col_iter, (*segment)->new_column_iterator_or_default(col, nullptr));
            RETURN_IF_ERROR(col_iter->init(iter_opts));
            RETURN_IF_ERROR(col_iter->fetch_values_by_rowid(rowids.data(), rowids.size(), (*columns)[i].get()));
            // padding char columns
            const auto& field = tablet_schema->schema()->field(read_column_ids[i]);
            if (field->type()->type() == TYPE_CHAR) {
                ChunkHelper::padding_char_column(tablet_schema, *field, (*columns)[i].get());
            }
        }
        return Status::OK();
    };

    for (const auto& [rssid, rowids] : rowids_by_rssid) {
        if (fs == nullptr) {
            auto root_path = params.tablet->metadata_root_location();
            ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(root_path));
        }

        if (params.container.rssid_to_file().count(rssid) == 0) {
            // It may happen when preload partial update state by old tablet meta
            return Status::Cancelled(fmt::format("tablet id {} version {} rowset_segment_id {} no exist",
                                                 params.metadata->id(), params.metadata->version(), rssid));
        }
        // use 0 segment_id is safe, because we need not get either delvector or dcg here
        RETURN_IF_ERROR(fetch_values_from_segment(params.container.rssid_to_file().at(rssid), 0, params.tablet_schema,
                                                  rowids, column_ids));
    }
    if (auto_increment_state != nullptr && with_default) {
        if (fs == nullptr) {
            auto root_path = params.tablet->metadata_root_location();
            ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(root_path));
        }
        uint32_t segment_id = auto_increment_state->segment_id;
        const std::vector<uint32_t>& rowids = auto_increment_state->rowids;
        const std::vector<uint32_t> auto_increment_col_partial_id(1, auto_increment_state->id);
        FileInfo info;
        info.path = params.op_write.rowset().segments(segment_id);
        if (segment_id < params.op_write.rowset().segment_encryption_metas_size()) {
            info.encryption_meta = params.op_write.rowset().segment_encryption_metas(segment_id);
        }
        RETURN_IF_ERROR(fetch_values_from_segment(info, segment_id,
                                                  // use partial segment column offset id to get the column
                                                  auto_increment_state->schema, rowids, auto_increment_col_partial_id));
    }
    cost_str << " [fetch vals by rowid] " << watch.elapsed_time();
    VLOG(2) << "UpdateManager get_column_values " << cost_str.str();
    return Status::OK();
}

Status UpdateManager::get_del_vec(const TabletSegmentId& tsid, int64_t version, const MetaFileBuilder* builder,
                                  bool fill_cache, DelVectorPtr* pdelvec) {
    if (builder != nullptr) {
        // 1. find in meta builder first
        auto found = builder->find_delvec(tsid, pdelvec);
        if (!found.ok()) {
            return found.status();
        }
        if (*found) {
            return Status::OK();
        }
    }
    (*pdelvec).reset(new DelVector());
    // 2. find in delvec file
    return get_del_vec_in_meta(tsid, version, fill_cache, pdelvec->get());
}

// get delvec in meta file
Status UpdateManager::get_del_vec_in_meta(const TabletSegmentId& tsid, int64_t meta_ver, bool fill_cache,
                                          DelVector* delvec) {
    std::string filepath = _tablet_mgr->tablet_metadata_location(tsid.tablet_id, meta_ver);
    LakeIOOptions lake_io_opts;
    lake_io_opts.fill_data_cache = fill_cache;
    ASSIGN_OR_RETURN(auto metadata, _tablet_mgr->get_tablet_metadata(filepath, fill_cache));
    RETURN_IF_ERROR(lake::get_del_vec(_tablet_mgr, *metadata, tsid.segment_id, fill_cache, lake_io_opts, delvec));
    return Status::OK();
}

void UpdateManager::expire_cache() {
    if (MonotonicMillis() - _last_clear_expired_cache_millis > _cache_expire_ms) {
        ssize_t update_state_orig_size = _update_state_cache.size();
        ssize_t update_state_orig_obj_size = _update_state_cache.object_size();
        _update_state_cache.clear_expired();
        ssize_t update_state_size = _update_state_cache.size();
        ssize_t update_state_obj_size = _update_state_cache.object_size();

        ssize_t index_orig_size = _index_cache.size();
        ssize_t index_orig_obj_size = _index_cache.object_size();
        _index_cache.clear_expired();
        ssize_t index_size = _index_cache.size();
        ssize_t index_obj_size = _index_cache.object_size();

        ssize_t compaction_cache_orig_size = _compaction_cache.size();
        ssize_t compaction_cache_orig_obj_size = _compaction_cache.object_size();
        _compaction_cache.clear_expired();
        ssize_t compaction_cache_size = _compaction_cache.size();
        ssize_t compaction_cache_obj_size = _compaction_cache.object_size();

        LOG(INFO) << strings::Substitute(
                "update state cache expire: ($0 $1), index cache expire: ($2 $3), compaction cache expire: ($4 $5)",
                update_state_orig_obj_size - update_state_obj_size,
                PrettyPrinter::print_bytes(update_state_orig_size - update_state_size),
                index_orig_obj_size - index_obj_size, PrettyPrinter::print_bytes(index_orig_size - index_size),
                compaction_cache_orig_obj_size - compaction_cache_obj_size,
                PrettyPrinter::print_bytes(compaction_cache_orig_size - compaction_cache_size));

        _last_clear_expired_cache_millis = MonotonicMillis();
    }
}

void UpdateManager::evict_cache(int64_t memory_urgent_level, int64_t memory_high_level) {
    int64_t capacity = _index_cache.capacity();
    int64_t size = _index_cache.size();
    int64_t memory_urgent = capacity * memory_urgent_level / 100;
    int64_t memory_high = capacity * memory_high_level / 100;

    if (size > memory_urgent) {
        _index_cache.try_evict(memory_urgent);
    }

    size = _index_cache.size();
    if (size > memory_high) {
        int64_t target_memory = std::max((size * 9 / 10), memory_high);
        _index_cache.try_evict(target_memory);
    }
    return;
}

size_t UpdateManager::get_rowset_num_deletes(int64_t tablet_id, int64_t version, const RowsetMetadataPB& rowset_meta) {
    size_t num_dels = 0;
    for (int i = 0; i < rowset_meta.segments_size(); i++) {
        DelVectorPtr delvec;
        TabletSegmentId tsid;
        tsid.tablet_id = tablet_id;
        tsid.segment_id = rowset_meta.id() + i;
        auto st = get_del_vec(tsid, version, nullptr, false /* fill cache */, &delvec);
        if (!st.ok()) {
            LOG(WARNING) << "get_rowset_num_deletes: error get del vector " << st;
            continue;
        }
        num_dels += delvec->cardinality();
    }
    return num_dels;
}

bool UpdateManager::_use_light_publish_primary_compaction(int64_t tablet_id, int64_t txn_id) {
    // Is config enable ?
    if (!config::enable_light_pk_compaction_publish) {
        return false;
    }
    // Is rows mapper file exist?
    auto filename_st = lake_rows_mapper_filename(tablet_id, txn_id);
    if (!filename_st.ok()) {
        return false;
    }
    return fs::path_exist(filename_st.value());
}

Status UpdateManager::light_publish_primary_compaction(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                                                       const TabletMetadata& metadata, const Tablet& tablet,
                                                       IndexEntry* index_entry, MetaFileBuilder* builder,
                                                       int64_t base_version) {
    // 1. init some state
    auto& index = index_entry->value();
    std::vector<uint32_t> input_rowsets_id(op_compaction.input_rowsets().begin(), op_compaction.input_rowsets().end());
    ASSIGN_OR_RETURN(auto tablet_schema, ExecEnv::GetInstance()->lake_tablet_manager()->get_output_rowset_schema(
                                                 input_rowsets_id, &metadata));

    Rowset output_rowset(tablet.tablet_mgr(), tablet.id(), &op_compaction.output_rowset(), -1 /*unused*/,
                         tablet_schema);
    vector<std::pair<uint32_t, DelVectorPtr>> delvecs;
    std::map<uint32_t, size_t> segment_id_to_add_dels;
    // get max rowset id in input rowsets
    uint32_t max_rowset_id =
            *std::max_element(op_compaction.input_rowsets().begin(), op_compaction.input_rowsets().end());

    // 2. update primary index, and generate delete info.
    auto resolver = std::make_unique<LakePrimaryKeyCompactionConflictResolver>(&metadata, &output_rowset, _tablet_mgr,
                                                                               builder, &index, txn_id, base_version,
                                                                               &segment_id_to_add_dels, &delvecs);
    RETURN_IF_ERROR(resolver->execute());
    _index_cache.update_object_size(index_entry, index.memory_usage());
    // 3. update TabletMeta and write to meta file
    for (auto&& each : delvecs) {
        builder->append_delvec(each.second, each.first);
    }
    builder->apply_opcompaction(op_compaction, max_rowset_id, tablet_schema->id());
    RETURN_IF_ERROR(builder->update_num_del_stat(segment_id_to_add_dels));
    RETURN_IF_ERROR(index.apply_opcompaction(metadata, op_compaction));

    TRACE_COUNTER_INCREMENT("output_rowsets_size", output_rowset.num_segments());
    TRACE_COUNTER_INCREMENT("max_rowsetid", max_rowset_id);
    TRACE_COUNTER_INCREMENT("input_rowsets_size", op_compaction.input_rowsets_size());

    _print_memory_stats();

    return Status::OK();
}

DEFINE_FAIL_POINT(hook_publish_primary_key_tablet_compaction);
Status UpdateManager::publish_primary_compaction(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                                                 const TabletMetadata& metadata, const Tablet& tablet,
                                                 IndexEntry* index_entry, MetaFileBuilder* builder,
                                                 int64_t base_version) {
    FAIL_POINT_TRIGGER_EXECUTE(hook_publish_primary_key_tablet_compaction, {
        std::vector<uint32_t> input_rowsets_id(op_compaction.input_rowsets().begin(),
                                               op_compaction.input_rowsets().end());
        ASSIGN_OR_RETURN(auto tablet_schema, ExecEnv::GetInstance()->lake_tablet_manager()->get_output_rowset_schema(
                                                     input_rowsets_id, &metadata));
        builder->apply_opcompaction(
                op_compaction,
                *std::max_element(op_compaction.input_rowsets().begin(), op_compaction.input_rowsets().end()),
                tablet_schema->id());
        return Status::OK();
    });
    if (CompactionUpdateConflictChecker::conflict_check(op_compaction, txn_id, metadata, builder)) {
        // conflict happens
        return Status::OK();
    }
    if (_use_light_publish_primary_compaction(tablet.id(), txn_id)) {
        return light_publish_primary_compaction(op_compaction, txn_id, metadata, tablet, index_entry, builder,
                                                base_version);
    }
    auto& index = index_entry->value();
    // 1. iterate output rowset, update primary index and generate delvec
    std::vector<uint32_t> input_rowsets_id(op_compaction.input_rowsets().begin(), op_compaction.input_rowsets().end());
    ASSIGN_OR_RETURN(auto tablet_schema, ExecEnv::GetInstance()->lake_tablet_manager()->get_output_rowset_schema(
                                                 input_rowsets_id, &metadata));
    Rowset output_rowset(tablet.tablet_mgr(), tablet.id(), &op_compaction.output_rowset(), -1 /*unused*/,
                         tablet_schema);
    auto compaction_entry = _compaction_cache.get_or_create(cache_key(tablet.id(), txn_id));
    compaction_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    // only use state entry once, remove it when publish finish or fail
    DeferOp remove_state_entry([&] { _compaction_cache.remove(compaction_entry); });
    auto& compaction_state = compaction_entry->value();
    size_t total_deletes = 0;
    size_t total_rows = 0;
    vector<std::pair<uint32_t, DelVectorPtr>> delvecs;
    vector<uint32_t> tmp_deletes;
    const uint32_t rowset_id = metadata.next_rowset_id();
    // get max rowset id in input rowsets
    uint32_t max_rowset_id =
            *std::max_element(op_compaction.input_rowsets().begin(), op_compaction.input_rowsets().end());
    // then get the max src rssid, to solve conflict between write and compaction
    auto input_rowset = std::find_if(metadata.rowsets().begin(), metadata.rowsets().end(),
                                     [&](const RowsetMetadata& r) { return r.id() == max_rowset_id; });
    uint32_t max_src_rssid = max_rowset_id + input_rowset->segments_size() - 1;
    std::map<uint32_t, size_t> segment_id_to_add_dels;

    // 2. update primary index, and generate delete info.
    TRACE_COUNTER_INCREMENT("output_rowsets_size", output_rowset.num_segments());
    for (size_t i = 0; i < output_rowset.num_segments(); i++) {
        RETURN_IF_ERROR(compaction_state.load_segments(&output_rowset, this, tablet_schema, i));
        TRACE_COUNTER_INCREMENT("state_bytes", compaction_state.memory_usage());
        auto& pk_col = compaction_state.pk_cols[i];
        total_rows += pk_col->size();
        uint32_t rssid = rowset_id + i;
        tmp_deletes.clear();
        // replace will not grow hashtable, so don't need to check memory limit
        {
            TRACE_COUNTER_SCOPE_LATENCY_US("update_index_latency_us");
            RETURN_IF_ERROR(index.try_replace(rssid, 0, *pk_col, max_src_rssid, &tmp_deletes));
            _index_cache.update_object_size(index_entry, index.memory_usage());
        }
        DelVectorPtr dv = std::make_shared<DelVector>();
        if (tmp_deletes.empty()) {
            dv->init(metadata.version(), nullptr, 0);
        } else {
            dv->init(metadata.version(), tmp_deletes.data(), tmp_deletes.size());
            total_deletes += tmp_deletes.size();
        }
        segment_id_to_add_dels[rssid] += tmp_deletes.size();
        delvecs.emplace_back(rssid, dv);
        compaction_state.release_segments(i);
    }
    _block_cache->update_memory_usage();

    // 3. update TabletMeta and write to meta file
    for (auto&& each : delvecs) {
        builder->append_delvec(each.second, each.first);
    }
    builder->apply_opcompaction(op_compaction, max_rowset_id, tablet_schema->id());
    RETURN_IF_ERROR(builder->update_num_del_stat(segment_id_to_add_dels));

    RETURN_IF_ERROR(index.apply_opcompaction(metadata, op_compaction));

    TRACE_COUNTER_INCREMENT("rowsetid", rowset_id);
    TRACE_COUNTER_INCREMENT("max_rowsetid", max_rowset_id);
    TRACE_COUNTER_INCREMENT("output_rows", total_rows);
    TRACE_COUNTER_INCREMENT("input_rowsets_size", op_compaction.input_rowsets_size());
    TRACE_COUNTER_INCREMENT("total_del", total_deletes);

    _print_memory_stats();

    return Status::OK();
}

bool UpdateManager::try_remove_primary_index_cache(uint32_t tablet_id) {
    return _index_cache.try_remove_by_key(tablet_id);
}

void UpdateManager::unload_primary_index(int64_t tablet_id) {
    auto index_entry = _index_cache.get_or_create(tablet_id);
    index_entry->value().unload();
    _index_cache.release(index_entry);
}

Status UpdateManager::check_meta_version(const Tablet& tablet, int64_t base_version) {
    auto index_entry = _index_cache.get(tablet.id());
    if (index_entry == nullptr) {
        // if primary index not in cache, just return true, and continue publish
        return Status::OK();
    } else {
        auto& index = index_entry->value();
        if (index.data_version() > base_version) {
            // return error, and ignore this publish later
            _index_cache.release(index_entry);
            LOG(INFO) << "Primary index version is greater than the base version. tablet_id: " << tablet.id()
                      << " index_version: " << index.data_version() << " base_version: " << base_version;
            return Status::AlreadyExist("lake primary publish txn already finish");
        } else if (index.data_version() < base_version) {
            // clear cache, and continue publish
            LOG(WARNING)
                    << "Primary index version is less than the base version, remove primary index cache, tablet_id: "
                    << tablet.id() << " index_version: " << index.data_version() << " base_version: " << base_version;
            if (!_index_cache.remove(index_entry)) {
                return Status::InternalError("lake primary index cache ref mismatch");
            }
        } else {
            _index_cache.release(index_entry);
        }
        return Status::OK();
    }
}

void UpdateManager::update_primary_index_data_version(const Tablet& tablet, int64_t version) {
    auto index_entry = _index_cache.get(tablet.id());
    if (index_entry != nullptr) {
        auto& index = index_entry->value();
        index.update_data_version(version);
        _index_cache.release(index_entry);
    }
}

int64_t UpdateManager::get_primary_index_data_version(int64_t tablet_id) {
    auto index_entry = _index_cache.get(tablet_id);
    if (index_entry != nullptr) {
        int64_t version = index_entry->value().data_version();
        _index_cache.release(index_entry);
        return version;
    }
    return 0;
}

void UpdateManager::_print_memory_stats() {
    static std::atomic<int64_t> last_print_ts;
    if (time(nullptr) > last_print_ts.load() + kPrintMemoryStatsInterval && _update_mem_tracker != nullptr) {
        LOG(INFO) << strings::Substitute(
                "[lake update manager memory]index:$0 update_state:$1 compact_state:$2 total:$3/$4",
                PrettyPrinter::print_bytes(_index_cache_mem_tracker->consumption()),
                PrettyPrinter::print_bytes(_update_state_mem_tracker->consumption()),
                PrettyPrinter::print_bytes(_compaction_state_mem_tracker->consumption()),
                PrettyPrinter::print_bytes(_update_mem_tracker->consumption()),
                PrettyPrinter::print_bytes(_update_mem_tracker->limit()));
        last_print_ts.store(time(nullptr));
    }
}

int32_t UpdateManager::_get_condition_column(const TxnLogPB_OpWrite& op_write, const TabletSchema& tablet_schema) {
    const auto& txn_meta = op_write.txn_meta();
    if (txn_meta.has_merge_condition()) {
        for (int i = 0; i < tablet_schema.columns().size(); ++i) {
            if (tablet_schema.column(i).name() == txn_meta.merge_condition()) {
                return i;
            }
        }
    }
    return -1;
}

bool UpdateManager::TEST_check_primary_index_cache_ref(uint32_t tablet_id, uint32_t ref_cnt) {
    auto index_entry = _index_cache.get(tablet_id);
    if (index_entry != nullptr) {
        DeferOp release_index_entry([&] { _index_cache.release(index_entry); });
        if (index_entry->get_ref() != ref_cnt + 1) {
            return false;
        }
    }
    return true;
}

bool UpdateManager::TEST_check_update_state_cache_absent(uint32_t tablet_id, int64_t txn_id) {
    auto state_entry = _update_state_cache.get(cache_key(tablet_id, txn_id));
    if (state_entry == nullptr) {
        return true;
    } else {
        _update_state_cache.release(state_entry);
        return false;
    }
}

bool UpdateManager::TEST_check_compaction_cache_absent(uint32_t tablet_id, int64_t txn_id) {
    auto compaction_entry = _compaction_cache.get(cache_key(tablet_id, txn_id));
    if (compaction_entry == nullptr) {
        return true;
    } else {
        _compaction_cache.release(compaction_entry);
        return false;
    }
}

void UpdateManager::TEST_remove_compaction_cache(uint32_t tablet_id, int64_t txn_id) {
    auto compaction_entry = _compaction_cache.get(cache_key(tablet_id, txn_id));
    if (compaction_entry != nullptr) {
        _compaction_cache.remove(compaction_entry);
    }
}

void UpdateManager::try_remove_cache(uint32_t tablet_id, int64_t txn_id) {
    auto key = cache_key(tablet_id, txn_id);
    _update_state_cache.try_remove_by_key(key);
    _compaction_cache.try_remove_by_key(key);
}

void UpdateManager::preload_update_state(const TxnLog& txnlog, Tablet* tablet) {
    // use process mem tracker instread of load mem tracker here.
    SCOPED_THREAD_LOCAL_MEM_SETTER(GlobalEnv::GetInstance()->process_mem_tracker(), true);
    SCOPED_THREAD_LOCAL_SINGLETON_CHECK_MEM_TRACKER_SETTER(config::enable_pk_strict_memcheck ? _update_mem_tracker
                                                                                             : nullptr);
    // use tabletid-txnid as update state cache's key, so it can retry safe.
    auto state_entry = _update_state_cache.get_or_create(cache_key(tablet->id(), txnlog.txn_id()));
    state_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& state = state_entry->value();
    // get latest metadata from cache, it is not matter if it isn't the real latest metadata.
    auto metadata_ptr = _tablet_mgr->get_latest_cached_tablet_metadata(tablet->id());
    const int segments_size = txnlog.op_write().rowset().segments_size();
    // skip preload if memory limit exceed
    if (metadata_ptr != nullptr && segments_size > 0 && !_update_state_mem_tracker->any_limit_exceeded()) {
        auto tablet_schema = std::make_shared<TabletSchema>(metadata_ptr->schema());
        RssidFileInfoContainer rssid_fileinfo_container;
        rssid_fileinfo_container.add_rssid_to_file(*metadata_ptr);
        RowsetUpdateStateParams params{
                .op_write = txnlog.op_write(),
                .tablet_schema = tablet_schema,
                .metadata = metadata_ptr,
                .tablet = tablet,
                .container = rssid_fileinfo_container,
        };
        state.init(params);
        auto st = Status::OK();
        for (uint32_t segment_id = 0; segment_id < segments_size && !_update_state_mem_tracker->any_limit_exceeded();
             segment_id++) {
            st = state.load_segment(segment_id, params, metadata_ptr->version(), false /* resolve conflict*/,
                                    true /* need lock */);
            _update_state_cache.update_object_size(state_entry, state.memory_usage());
            if (!st.ok()) {
                break;
            }
        }
        if (!st.ok()) {
            _update_state_cache.remove(state_entry);
            if (!st.is_uninitialized() && !st.is_cancelled()) {
                LOG(ERROR) << strings::Substitute("lake primary table preload_update_state id:$0 error:$1",
                                                  tablet->id(), st.to_string());
            } else {
                LOG(INFO) << strings::Substitute("lake primary table preload_update_state id:$0 failed:$1",
                                                 tablet->id(), st.to_string());
            }
            // not return error even it fail, because we can load update state in publish again.
        } else {
            // just release it, will use it again in publish
            _update_state_cache.release(state_entry);
        }
    } else {
        _update_state_cache.remove(state_entry);
    }
    TEST_SYNC_POINT("UpdateManager::preload_update_state:return");
}

void UpdateManager::preload_compaction_state(const TxnLog& txnlog, const Tablet& tablet,
                                             const TabletSchemaCSPtr& tablet_schema) {
    // use process mem tracker instread of load mem tracker here.
    SCOPED_THREAD_LOCAL_MEM_SETTER(GlobalEnv::GetInstance()->process_mem_tracker(), true);
    SCOPED_THREAD_LOCAL_SINGLETON_CHECK_MEM_TRACKER_SETTER(config::enable_pk_strict_memcheck ? _update_mem_tracker
                                                                                             : nullptr);
    // no need to preload if using light compaction publish
    if (StorageEngine::instance()->enable_light_pk_compaction_publish()) {
        return;
    }
    // no need to preload if output rowset is empty.
    const int segments_size = txnlog.op_compaction().output_rowset().segments_size();
    if (segments_size <= 0) return;
    Rowset output_rowset(tablet.tablet_mgr(), tablet.id(), &txnlog.op_compaction().output_rowset(), -1 /*unused*/,
                         tablet_schema);
    // use tabletid-txnid as compaction state cache's key, so it can retry safe.
    auto compaction_entry = _compaction_cache.get_or_create(cache_key(tablet.id(), txnlog.txn_id()));
    compaction_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& compaction_state = compaction_entry->value();
    // preload compaction state, only load first output segment, to avoid too much memory cost
    auto st = Status::OK();
    // skip preload if memory limit exceed
    for (int i = 0; i < segments_size && !_compaction_state_mem_tracker->any_limit_exceeded(); i++) {
        st = compaction_state.load_segments(&output_rowset, this, tablet_schema, i);
        if (!st.ok()) {
            break;
        }
    }
    if (!st.ok()) {
        _compaction_cache.remove(compaction_entry);
        LOG(ERROR) << strings::Substitute("lake primary table preload_compaction_state id:$0 error:$1", tablet.id(),
                                          st.to_string());
        // not return error even it fail, because we can load compaction state in publish again.
    } else {
        // just release it, will use it again in publish
        _compaction_cache.release(compaction_entry);
    }
    TEST_SYNC_POINT("UpdateManager::preload_compaction_state:return");
}

void UpdateManager::set_enable_persistent_index(int64_t tablet_id, bool enable_persistent_index) {
    auto index_entry = _index_cache.get(tablet_id);
    if (index_entry != nullptr) {
        auto& index = index_entry->value();
        index.set_enable_persistent_index(enable_persistent_index);
        _index_cache.release(index_entry);
    }
}

Status UpdateManager::execute_index_major_compaction(const TabletMetadata& metadata, TxnLogPB* txn_log) {
    return LakePersistentIndex::major_compact(_tablet_mgr, metadata, txn_log);
}

Status UpdateManager::pk_index_major_compaction(int64_t tablet_id, DataDir* data_dir) {
    auto index_entry = _index_cache.get(tablet_id);
    if (index_entry == nullptr) {
        return Status::OK();
    }
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& index = index_entry->value();

    // release when function end
    DeferOp index_defer([&]() { _index_cache.release(index_entry); });
    _index_cache.update_object_size(index_entry, index.memory_usage());
    RETURN_IF_ERROR(index.major_compaction(data_dir, tablet_id, index.get_index_lock()));
    index.set_local_pk_index_write_amp_score(0.0);
    return Status::OK();
}

} // namespace starrocks::lake
