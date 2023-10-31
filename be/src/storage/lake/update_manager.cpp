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
#include "gen_cpp/lake_types.pb.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_compaction_state.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "util/pretty_printer.h"
#include "util/trace.h"

namespace starrocks {

namespace lake {

UpdateManager::UpdateManager(LocationProvider* location_provider, MemTracker* mem_tracker)
        : _index_cache(std::numeric_limits<size_t>::max()),
          _update_state_cache(std::numeric_limits<size_t>::max()),
          _location_provider(location_provider),
          _pk_index_shards(config::pk_index_map_shard_size) {
    _update_mem_tracker = mem_tracker;
    _update_state_mem_tracker = std::make_unique<MemTracker>(-1, "lake_rowset_update_state", mem_tracker);
    _index_cache_mem_tracker = std::make_unique<MemTracker>(-1, "lake_index_cache", mem_tracker);
    _compaction_state_mem_tracker = std::make_unique<MemTracker>(-1, "compaction_state_cache", mem_tracker);
    _index_cache.set_mem_tracker(_index_cache_mem_tracker.get());
    _update_state_cache.set_mem_tracker(_update_state_mem_tracker.get());

    int64_t byte_limits = ParseUtil::parse_mem_spec(config::mem_limit, MemInfo::physical_mem());
    int32_t update_mem_percent = std::max(std::min(100, config::update_memory_limit_percent), 0);
    _index_cache.set_capacity(byte_limits * update_mem_percent);
}

Status LakeDelvecLoader::load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    return _update_mgr->get_del_vec(tsid, version, _pk_builder, pdelvec);
}

StatusOr<IndexEntry*> UpdateManager::prepare_primary_index(const TabletMetadata& metadata, Tablet* tablet,
                                                           MetaFileBuilder* builder, int64_t base_version,
                                                           int64_t new_version) {
    auto index_entry = _index_cache.get_or_create(tablet->id());
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& index = index_entry->value();
    Status st = index.lake_load(tablet, metadata, base_version, builder);
    _index_cache.update_object_size(index_entry, index.memory_usage());
    if (!st.ok()) {
        _index_cache.remove(index_entry);
        std::string msg = strings::Substitute("prepare_primary_index: load primary index failed: $0", st.to_string());
        LOG(ERROR) << msg;
        return Status::InternalError(msg);
    }
    st = index.prepare(EditVersion(new_version, 0), 0);
    if (!st.ok()) {
        _index_cache.remove(index_entry);
        std::string msg =
                strings::Substitute("prepare_primary_index: prepare primary index failed: $0", st.to_string());
        LOG(ERROR) << msg;
        return Status::InternalError(msg);
    }
    builder->set_has_update_index();
    return index_entry;
}

Status UpdateManager::commit_primary_index(IndexEntry* index_entry, Tablet* tablet) {
    if (index_entry != nullptr) {
        auto& index = index_entry->value();
        if (index.enable_persistent_index()) {
            // only take affect in local persistent index
            PersistentIndexMetaPB index_meta;
            DataDir* data_dir = StorageEngine::instance()->get_persistent_index_store(tablet->id());
            RETURN_IF_ERROR(TabletMetaManager::get_persistent_index_meta(data_dir, tablet->id(), &index_meta));
            RETURN_IF_ERROR(index.commit(&index_meta));
            RETURN_IF_ERROR(TabletMetaManager::write_persistent_index_meta(data_dir, tablet->id(), index_meta));
            // Call `on_commited` here, which will remove old files is safe.
            // Because if publish version fail after `on_commited`, index will be rebuild.
            RETURN_IF_ERROR(index.on_commited());
            TRACE("commit primary index");
        }
    }

    return Status::OK();
}

void UpdateManager::release_primary_index(IndexEntry* index_entry) {
    if (index_entry != nullptr) {
        _index_cache.release(index_entry);
    }
}

// |metadata| contain last tablet meta info with new version
Status UpdateManager::publish_primary_key_tablet(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                                                 const TabletMetadata& metadata, Tablet* tablet,
                                                 IndexEntry* index_entry, MetaFileBuilder* builder,
                                                 int64_t base_version) {
    auto& index = index_entry->value();
    // 1. load rowset update data to cache, get upsert and delete list
    const uint32_t rowset_id = metadata.next_rowset_id();
    std::unique_ptr<TabletSchema> tablet_schema = std::make_unique<TabletSchema>(metadata.schema());
    auto state_entry = _update_state_cache.get_or_create(strings::Substitute("$0_$1", tablet->id(), txn_id));
    state_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    // only use state entry once, remove it when publish finish or fail
    DeferOp remove_state_entry([&] { _update_state_cache.remove(state_entry); });
    auto& state = state_entry->value();
    RETURN_IF_ERROR(state.load(op_write, metadata, base_version, tablet, builder, true));
    _update_state_cache.update_object_size(state_entry, state.memory_usage());
    // 2. rewrite segment file if it is partial update
    std::vector<std::string> orphan_files;
    std::map<int, std::string> replace_segments;
    RETURN_IF_ERROR(state.rewrite_segment(op_write, metadata, tablet, &replace_segments, &orphan_files));
    PrimaryIndex::DeletesMap new_deletes;
    for (uint32_t i = 0; i < op_write.rowset().segments_size(); i++) {
        new_deletes[rowset_id + i] = {};
    }
    auto& upserts = state.upserts();
    // handle merge condition, skip update row which's merge condition column value is smaller than current row
    int32_t condition_column = _get_condition_column(op_write, *tablet_schema);
    // 3. update primary index, and generate delete info.
    for (uint32_t i = 0; i < upserts.size(); i++) {
        if (upserts[i] != nullptr) {
            if (condition_column < 0) {
                RETURN_IF_ERROR(_do_update(rowset_id, i, upserts, index, tablet->id(), &new_deletes));
            } else {
                RETURN_IF_ERROR(_do_update_with_condition(tablet, metadata, op_write, *tablet_schema, rowset_id, i,
                                                          condition_column, upserts, index, tablet->id(),
                                                          &new_deletes));
            }
            _index_cache.update_object_size(index_entry, index.memory_usage());
        }
    }

    for (const auto& one_delete : state.deletes()) {
        RETURN_IF_ERROR(index.erase(*one_delete, &new_deletes));
    }
    for (const auto& one_delete : state.auto_increment_deletes()) {
        RETURN_IF_ERROR(index.erase(*one_delete, &new_deletes));
    }
    // 4. generate delvec
    size_t ndelvec = new_deletes.size();
    vector<std::pair<uint32_t, DelVectorPtr>> new_del_vecs(ndelvec);
    size_t idx = 0;
    size_t new_del = 0;
    size_t total_del = 0;
    for (auto& new_delete : new_deletes) {
        uint32_t rssid = new_delete.first;
        if (rssid >= rowset_id && rssid < rowset_id + op_write.rowset().segments_size()) {
            // it's newly added rowset's segment, do not have latest delvec yet
            new_del_vecs[idx].first = rssid;
            new_del_vecs[idx].second = std::make_shared<DelVector>();
            auto& del_ids = new_delete.second;
            new_del_vecs[idx].second->init(metadata.version(), del_ids.data(), del_ids.size());
            new_del += del_ids.size();
            total_del += del_ids.size();
        } else {
            TabletSegmentId tsid;
            tsid.tablet_id = tablet->id();
            tsid.segment_id = rssid;
            DelVectorPtr old_del_vec;
            RETURN_IF_ERROR(get_del_vec(tsid, base_version, builder, &old_del_vec));
            new_del_vecs[idx].first = rssid;
            old_del_vec->add_dels_as_new_version(new_delete.second, metadata.version(), &(new_del_vecs[idx].second));
            size_t cur_old = old_del_vec->cardinality();
            size_t cur_add = new_delete.second.size();
            size_t cur_new = new_del_vecs[idx].second->cardinality();
            if (cur_old + cur_add != cur_new) {
                // should not happen, data inconsistent
                LOG(FATAL) << strings::Substitute(
                        "delvec inconsistent tablet:$0 rssid:$1 #old:$2 #add:$3 #new:$4 old_v:$5 "
                        "v:$6",
                        tablet->id(), rssid, cur_old, cur_add, cur_new, old_del_vec->version(), metadata.version());
            }
            new_del += cur_add;
            total_del += cur_new;
        }

        idx++;
    }
    new_deletes.clear();

    // 5. update TabletMeta and write to meta file
    for (auto&& each : new_del_vecs) {
        builder->append_delvec(each.second, each.first);
    }
    builder->apply_opwrite(op_write, replace_segments, orphan_files);

    TRACE_COUNTER_INCREMENT("rowsetid", rowset_id);
    TRACE_COUNTER_INCREMENT("#upserts", upserts.size());
    TRACE_COUNTER_INCREMENT("#deletes", state.deletes().size());
    TRACE_COUNTER_INCREMENT("#new_del", new_del);
    TRACE_COUNTER_INCREMENT("#total_del", total_del);
    _print_memory_stats();
    return Status::OK();
}

Status UpdateManager::_do_update(uint32_t rowset_id, int32_t upsert_idx, const std::vector<ColumnUniquePtr>& upserts,
                                 PrimaryIndex& index, int64_t tablet_id, DeletesMap* new_deletes) {
    return index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], new_deletes);
}

Status UpdateManager::_do_update_with_condition(Tablet* tablet, const TabletMetadata& metadata,
                                                const TxnLogPB_OpWrite& op_write, const TabletSchema& tablet_schema,
                                                uint32_t rowset_id, int32_t upsert_idx, int32_t condition_column,
                                                const std::vector<ColumnUniquePtr>& upserts, PrimaryIndex& index,
                                                int64_t tablet_id, DeletesMap* new_deletes) {
    CHECK(condition_column >= 0);
    auto tablet_column = tablet_schema.column(condition_column);
    std::vector<uint32_t> read_column_ids;
    read_column_ids.push_back(condition_column);

    std::vector<uint64_t> old_rowids(upserts[upsert_idx]->size());
    RETURN_IF_ERROR(index.get(*upserts[upsert_idx], &old_rowids));
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
        RETURN_IF_ERROR(get_column_values(tablet, metadata, op_write, tablet_schema, read_column_ids, num_default > 0,
                                          old_rowids_by_rssid, &old_columns));
        auto old_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        old_column->append_selective(*old_columns[0], idxes.data(), 0, idxes.size());

        std::map<uint32_t, std::vector<uint32_t>> new_rowids_by_rssid;
        std::vector<uint32_t> rowids;
        for (int j = 0; j < upserts[upsert_idx]->size(); ++j) {
            rowids.push_back(j);
        }
        new_rowids_by_rssid[rowset_id + upsert_idx] = rowids;
        // only support condition update on single column
        std::vector<std::unique_ptr<Column>> new_columns(1);
        auto new_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        new_columns[0] = new_column->clone_empty();
        RETURN_IF_ERROR(get_column_values(tablet, metadata, op_write, tablet_schema, read_column_ids, false,
                                          new_rowids_by_rssid, &new_columns));

        int idx_begin = 0;
        int upsert_idx_step = 0;
        for (int j = 0; j < old_column->size(); ++j) {
            if (num_default > 0 && idxes[j] == 0) {
                // plan_read_by_rssid will return idx with 0 if we have default value
                upsert_idx_step++;
            } else {
                int r = old_column->compare_at(j, j, *new_columns[0].get(), -1);
                if (r > 0) {
                    index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], idx_begin,
                                 idx_begin + upsert_idx_step, new_deletes);

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
            RETURN_IF_ERROR(index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], idx_begin,
                                         idx_begin + upsert_idx_step, new_deletes));
        }
    } else {
        RETURN_IF_ERROR(index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], new_deletes));
    }

    return Status::OK();
}

Status UpdateManager::_handle_index_op(Tablet* tablet, int64_t base_version,
                                       const std::function<void(LakePrimaryIndex&)>& op) {
    auto index_entry = _index_cache.get(tablet->id());
    if (index_entry == nullptr) {
        return Status::Uninitialized(fmt::format("Primary index not load yet, tablet_id: {}", tablet->id()));
    }
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    // release index entry but keep it in cache
    DeferOp release_index_entry([&] { _index_cache.release(index_entry); });
    auto& index = index_entry->value();
    if (!index.is_load(base_version)) {
        return Status::Uninitialized(fmt::format("Primary index not load yet, tablet_id: {}", tablet->id()));
    }
    op(index);

    return Status::OK();
}

Status UpdateManager::get_rowids_from_pkindex(Tablet* tablet, int64_t base_version,
                                              const std::vector<ColumnUniquePtr>& upserts,
                                              std::vector<std::vector<uint64_t>*>* rss_rowids) {
    Status st;
    st.update(_handle_index_op(tablet, base_version, [&](LakePrimaryIndex& index) {
        // get rss_rowids for each segment of rowset
        uint32_t num_segments = upserts.size();
        for (size_t i = 0; i < num_segments; i++) {
            auto& pks = *upserts[i];
            st.update(index.get(pks, (*rss_rowids)[i]));
        }
    }));
    return st;
}

Status UpdateManager::get_rowids_from_pkindex(Tablet* tablet, int64_t base_version,
                                              const std::vector<ColumnUniquePtr>& upserts,
                                              std::vector<std::vector<uint64_t>>* rss_rowids) {
    Status st;
    st.update(_handle_index_op(tablet, base_version, [&](LakePrimaryIndex& index) {
        // get rss_rowids for each segment of rowset
        uint32_t num_segments = upserts.size();
        for (size_t i = 0; i < num_segments; i++) {
            auto& pks = *upserts[i];
            st.update(index.get(pks, &((*rss_rowids)[i])));
        }
    }));
    return st;
}

Status UpdateManager::get_column_values(Tablet* tablet, const TabletMetadata& metadata,
                                        const TxnLogPB_OpWrite& op_write, const TabletSchema& tablet_schema,
                                        std::vector<uint32_t>& column_ids, bool with_default,
                                        std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                                        vector<std::unique_ptr<Column>>* columns,
                                        AutoIncrementPartialUpdateState* auto_increment_state) {
    std::stringstream cost_str;
    MonotonicStopWatch watch;
    watch.start();

    if (with_default && auto_increment_state == nullptr) {
        for (auto i = 0; i < column_ids.size(); ++i) {
            const TabletColumn& tablet_column = tablet_schema.column(column_ids[i]);
            if (tablet_column.has_default_value()) {
                const TypeInfoPtr& type_info = get_type_info(tablet_column);
                std::unique_ptr<DefaultValueColumnIterator> default_value_iter =
                        std::make_unique<DefaultValueColumnIterator>(
                                tablet_column.has_default_value(), tablet_column.default_value(),
                                tablet_column.is_nullable(), type_info, tablet_column.length(), 1);
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

    std::unordered_map<uint32_t, std::string> rssid_to_path;
    rowset_rssid_to_path(metadata, op_write, rssid_to_path);
    cost_str << " [catch rssid_to_path] " << watch.elapsed_time();
    watch.reset();

    std::shared_ptr<FileSystem> fs;
    auto fetch_values_from_segment = [&](std::string segment_name, uint32_t segment_id,
                                         const TabletSchema* tablet_schema,
                                         const std::vector<uint32_t>& rowids) -> Status {
        std::string path = tablet->segment_location(segment_name);
        auto segment = Segment::open(fs, path, segment_id, tablet_schema);
        if (!segment.ok()) {
            LOG(WARNING) << "Fail to open rssid: " << segment_id << " path: " << path << " : " << segment.status();
            return segment.status();
        }
        if ((*segment)->num_rows() == 0) {
            return Status::OK();
        }

        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(path));
        iter_opts.read_file = read_file.get();
        for (auto i = 0; i < column_ids.size(); ++i) {
            ASSIGN_OR_RETURN(auto col_iter, (*segment)->new_column_iterator(column_ids[i]));
            RETURN_IF_ERROR(col_iter->init(iter_opts));
            RETURN_IF_ERROR(col_iter->fetch_values_by_rowid(rowids.data(), rowids.size(), (*columns)[i].get()));
        }
        return Status::OK();
    };

    for (const auto& [rssid, rowids] : rowids_by_rssid) {
        if (fs == nullptr) {
            auto root_path = tablet->metadata_root_location();
            ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(root_path));
        }

        // use 0 segment_id is safe, because we need not get either delvector or dcg here
        RETURN_IF_ERROR(fetch_values_from_segment(rssid_to_path[rssid], 0, &tablet_schema, rowids));
    }
    if (auto_increment_state != nullptr && with_default) {
        if (fs == nullptr) {
            auto root_path = tablet->metadata_root_location();
            ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(root_path));
        }
        uint32_t segment_id = auto_increment_state->segment_id;
        const std::vector<uint32_t>& rowids = auto_increment_state->rowids;

        RETURN_IF_ERROR(fetch_values_from_segment(op_write.rowset().segments(segment_id), segment_id,
                                                  auto_increment_state->schema.get(), rowids));
    }
    cost_str << " [fetch vals by rowid] " << watch.elapsed_time();
    VLOG(2) << "UpdateManager get_column_values " << cost_str.str();
    return Status::OK();
}

Status UpdateManager::get_del_vec(const TabletSegmentId& tsid, int64_t version, const MetaFileBuilder* builder,
                                  DelVectorPtr* pdelvec) {
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
    return get_del_vec_in_meta(tsid, version, pdelvec->get());
}

// get delvec in meta file
Status UpdateManager::get_del_vec_in_meta(const TabletSegmentId& tsid, int64_t meta_ver, DelVector* delvec) {
    std::string filepath = _tablet_mgr->tablet_metadata_location(tsid.tablet_id, meta_ver);
    ASSIGN_OR_RETURN(auto metadata, _tablet_mgr->get_tablet_metadata(filepath, false));
    RETURN_IF_ERROR(lake::get_del_vec(_tablet_mgr, *metadata, tsid.segment_id, delvec));
    return Status::OK();
}

void UpdateManager::expire_cache() {
    if (MonotonicMillis() - _last_clear_expired_cache_millis > _cache_expire_ms) {
        _update_state_cache.clear_expired();
        _index_cache.clear_expired();
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
        auto st = get_del_vec(tsid, version, nullptr, &delvec);
        if (!st.ok()) {
            LOG(WARNING) << "get_rowset_num_deletes: error get del vector " << st;
            continue;
        }
        num_dels += delvec->cardinality();
    }
    return num_dels;
}

Status UpdateManager::publish_primary_compaction(const TxnLogPB_OpCompaction& op_compaction,
                                                 const TabletMetadata& metadata, Tablet* tablet,
                                                 IndexEntry* index_entry, MetaFileBuilder* builder,
                                                 int64_t base_version) {
    std::stringstream cost_str;
    MonotonicStopWatch watch;
    watch.start();
    auto& index = index_entry->value();
    // 1. iterate output rowset, update primary index and generate delvec
    std::unique_ptr<TabletSchema> tablet_schema = std::make_unique<TabletSchema>(metadata.schema());
    RowsetPtr output_rowset =
            std::make_shared<Rowset>(tablet, std::make_shared<RowsetMetadata>(op_compaction.output_rowset()));
    auto compaction_state = std::make_unique<CompactionState>(output_rowset.get(), this);
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

    // 2. update primary index, and generate delete info.
    for (size_t i = 0; i < compaction_state->pk_cols.size(); i++) {
        RETURN_IF_ERROR(compaction_state->load_segments(output_rowset.get(), *tablet_schema, i));
        auto& pk_col = compaction_state->pk_cols[i];
        total_rows += pk_col->size();
        uint32_t rssid = rowset_id + i;
        tmp_deletes.clear();
        // replace will not grow hashtable, so don't need to check memory limit
        RETURN_IF_ERROR(index.try_replace(rssid, 0, *pk_col, max_src_rssid, &tmp_deletes));
        DelVectorPtr dv = std::make_shared<DelVector>();
        if (tmp_deletes.empty()) {
            dv->init(metadata.version(), nullptr, 0);
        } else {
            dv->init(metadata.version(), tmp_deletes.data(), tmp_deletes.size());
            total_deletes += tmp_deletes.size();
        }
        delvecs.emplace_back(rssid, dv);
        compaction_state->release_segments(i);
    }
    cost_str << " [generate delvecs] " << watch.elapsed_time();
    watch.reset();

    // 3. update TabletMeta and write to meta file
    for (auto&& each : delvecs) {
        builder->append_delvec(each.second, each.first);
    }
    builder->apply_opcompaction(op_compaction);
    cost_str << " [apply meta] " << watch.elapsed_time();

    VLOG(2) << strings::Substitute(
            "lake publish_primary_compaction: tablet_id:$0 input_rowset_size:$1 max_rowset_id:$2"
            " total_deletes:$3 total_rows:$4 base_ver:$5 new_ver:$6 cost:$7",
            tablet->id(), op_compaction.input_rowsets_size(), max_rowset_id, total_deletes, total_rows, base_version,
            metadata.version(), cost_str.str());
    _print_memory_stats();

    return Status::OK();
}

void UpdateManager::remove_primary_index_cache(uint32_t tablet_id) {
    bool succ = false;
    auto index_entry = _index_cache.get(tablet_id);
    if (index_entry != nullptr) {
        _index_cache.remove(index_entry);
        succ = true;
    }
    LOG(WARNING) << "Lake update manager remove primary index cache, tablet_id: " << tablet_id << " , succ: " << succ;
}

bool UpdateManager::try_remove_primary_index_cache(uint32_t tablet_id) {
    return _index_cache.try_remove_by_key(tablet_id);
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

bool UpdateManager::TEST_check_update_state_cache_noexist(uint32_t tablet_id, int64_t txn_id) {
    auto state_entry = _update_state_cache.get(strings::Substitute("$0_$1", tablet_id, txn_id));
    if (state_entry == nullptr) {
        return true;
    } else {
        return false;
    }
}

void UpdateManager::preload_update_state(const TxnLog& txnlog, Tablet* tablet) {
    // use tabletid-txnid as update state cache's key, so it can retry safe.
    auto state_entry = _update_state_cache.get_or_create(strings::Substitute("$0_$1", tablet->id(), txnlog.txn_id()));
    state_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& state = state_entry->value();
    _update_state_cache.update_object_size(state_entry, state.memory_usage());
    // get latest metadata from cache, it is not matter if it isn't the real latest metadata.
    auto metadata_ptr = _tablet_mgr->get_latest_cached_tablet_metadata(tablet->id());
    if (metadata_ptr != nullptr) {
        auto st = state.load(txnlog.op_write(), *metadata_ptr, metadata_ptr->version(), tablet, nullptr, false);
        if (!st.ok()) {
            _update_state_cache.remove(state_entry);
            if (!st.is_uninitialized()) {
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
}

} // namespace lake

} // namespace starrocks
