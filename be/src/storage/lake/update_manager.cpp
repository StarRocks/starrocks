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
#include "storage/del_vector.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_compaction_state.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/tablet_manager.h"
#include "util/pretty_printer.h"

namespace starrocks {

namespace lake {

UpdateManager::UpdateManager(LocationProvider* location_provider, MemTracker* mem_tracker)
        : _index_cache(std::numeric_limits<size_t>::max()),
          _update_state_cache(std::numeric_limits<size_t>::max()),
          _location_provider(location_provider) {
    _update_mem_tracker = mem_tracker;
    _update_state_mem_tracker = std::make_unique<MemTracker>(-1, "lake_rowset_update_state", mem_tracker);
    _index_cache_mem_tracker = std::make_unique<MemTracker>(-1, "lake_index_cache", mem_tracker);

    _index_cache.set_mem_tracker(_index_cache_mem_tracker.get());
    _update_state_cache.set_mem_tracker(_update_state_mem_tracker.get());
}

Status LakeDelvecLoader::load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    return _update_mgr->get_del_vec(tsid, version, _pk_builder, pdelvec);
}

// |metadata| contain last tablet meta info with new version
Status UpdateManager::publish_primary_key_tablet(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata,
                                                 Tablet* tablet, MetaFileBuilder* builder, int64_t base_version) {
    std::stringstream cost_str;
    MonotonicStopWatch watch;
    watch.start();
    // 1. load rowset update data to cache, get upsert and delete list
    const uint32_t rowset_id = metadata.next_rowset_id();
    auto state_entry = _update_state_cache.get_or_create(strings::Substitute("$0_$1", tablet->id(), rowset_id));
    state_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    // only use state entry once, remove it when publish finish or fail
    DeferOp remove_state_entry([&] { _update_state_cache.remove(state_entry); });
    auto& state = state_entry->value();
    RETURN_IF_ERROR(state.load(op_write, metadata, base_version, tablet, builder));
    cost_str << " [UpdateStateCache load] " << watch.elapsed_time();
    watch.reset();
    // 2. rewrite segment file if it is partial update
    RETURN_IF_ERROR(state.rewrite_segment(op_write, metadata, tablet));
    cost_str << " [UpdateStateCache rewrite segment] " << watch.elapsed_time();
    watch.reset();
    // 3. update primary index
    auto index_entry = _index_cache.get_or_create(tablet->id());
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& index = index_entry->value();
    Status st = index.lake_load(tablet, metadata, base_version, builder);
    if (!st.ok()) {
        _index_cache.remove(index_entry);
        std::string msg =
                strings::Substitute("publish_primary_key_tablet: load primary index failed: $0", st.to_string());
        LOG(ERROR) << msg;
        return st;
    }
    // release index entry but keep it in cache
    DeferOp release_index_entry([&] { _index_cache.release(index_entry); });
    PrimaryIndex::DeletesMap new_deletes;
    for (uint32_t i = 0; i < op_write.rowset().segments_size(); i++) {
        new_deletes[rowset_id + i] = {};
    }
    auto& upserts = state.upserts();
    for (uint32_t i = 0; i < upserts.size(); i++) {
        if (upserts[i] != nullptr) {
            RETURN_IF_ERROR(_do_update(rowset_id, i, upserts, index, tablet->id(), &new_deletes));
            _index_cache.update_object_size(index_entry, index.memory_usage());
        }
    }

    for (const auto& one_delete : state.deletes()) {
        index.erase(*one_delete, &new_deletes);
    }
    cost_str << " [update primary index] " << watch.elapsed_time();
    watch.reset();
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
    cost_str << " [generate delvecs] " << watch.elapsed_time();
    watch.reset();

    // 5. update TabletMeta and write to meta file
    for (auto&& each : new_del_vecs) {
        builder->append_delvec(each.second, each.first);
    }
    builder->apply_opwrite(op_write);
    cost_str << " [apply meta] " << watch.elapsed_time();

    LOG(INFO) << strings::Substitute(
            "lake publish_primary_key_tablet tablet:$0 rowsetid:$1 upserts:$2 deletes:$3 new_del:$4 total_del:$5 "
            "base_ver:$6 new_ver:$7 cost:$8",
            tablet->id(), rowset_id, upserts.size(), state.deletes().size(), new_del, total_del, base_version,
            metadata.version(), cost_str.str());
    _print_memory_stats();

    return Status::OK();
}

Status UpdateManager::_do_update(std::uint32_t rowset_id, std::int32_t upsert_idx,
                                 const std::vector<ColumnUniquePtr>& upserts, PrimaryIndex& index,
                                 std::int64_t tablet_id, DeletesMap* new_deletes) {
    index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], new_deletes);

    return Status::OK();
}

Status UpdateManager::get_rowids_from_pkindex(Tablet* tablet, const TabletMetadata& metadata,
                                              const std::vector<ColumnUniquePtr>& upserts, const int64_t base_version,
                                              const MetaFileBuilder* builder,
                                              std::vector<std::vector<uint64_t>*>* rss_rowids) {
    auto index_entry = _index_cache.get_or_create(tablet->id());
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& index = index_entry->value();
    auto st = index.lake_load(tablet, metadata, base_version, builder);
    _index_cache.update_object_size(index_entry, index.memory_usage());
    if (!st.ok()) {
        _index_cache.remove(index_entry);
        std::string msg = strings::Substitute("prepare_partial_update_states error: load primary index failed: $0",
                                              st.to_string());
        LOG(ERROR) << msg;
        return Status::InternalError(msg);
    }
    // release index entry but keep it in cache
    DeferOp release_index_entry([&] { _index_cache.release(index_entry); });

    // get rss_rowids for each segment of rowset
    uint32_t num_segments = upserts.size();
    for (size_t i = 0; i < num_segments; i++) {
        auto& pks = *upserts[i];
        index.get(pks, (*rss_rowids)[i]);
    }

    return Status::OK();
}

Status UpdateManager::get_column_values(Tablet* tablet, const TabletMetadata& metadata,
                                        const TabletSchema& tablet_schema, std::vector<uint32_t>& column_ids,
                                        bool with_default, std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                                        vector<std::unique_ptr<Column>>* columns) {
    std::stringstream cost_str;
    MonotonicStopWatch watch;
    watch.start();

    if (with_default) {
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
                default_value_iter->fetch_values_by_rowid(nullptr, 1, (*columns)[i].get());
            } else {
                (*columns)[i]->append_default();
            }
        }
    }
    cost_str << " [with_default] " << watch.elapsed_time();
    watch.reset();

    std::unordered_map<uint32_t, std::string> rssid_to_path;
    rowset_rssid_to_path(metadata, rssid_to_path);
    cost_str << " [catch rssid_to_path] " << watch.elapsed_time();
    watch.reset();

    std::shared_ptr<FileSystem> fs;
    for (const auto& [rssid, rowids] : rowids_by_rssid) {
        if (fs == nullptr) {
            auto root_path = tablet->metadata_root_location();
            ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(root_path));
        }
        std::string path = tablet->segment_location(rssid_to_path[rssid]);
        auto segment = Segment::open(fs, path, rssid, &tablet_schema);
        if (!segment.ok()) {
            LOG(WARNING) << "Fail to open rssid: " << rssid << " path: " << path << " : " << segment.status();
            return segment.status();
        }
        if ((*segment)->num_rows() == 0) {
            continue;
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
    }
    cost_str << " [fetch vals by rowid] " << watch.elapsed_time();
    LOG(INFO) << "UpdateManager get_column_values " << cost_str.str();
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
    std::string filepath = _location_provider->tablet_metadata_location(tsid.tablet_id, meta_ver);
    MetaFileReader reader(filepath, false);
    RETURN_IF_ERROR(reader.load());
    RETURN_IF_ERROR(reader.get_del_vec(_tablet_mgr, tsid.segment_id, delvec));
    return Status::OK();
}

void UpdateManager::expire_cache() {
    if (MonotonicMillis() - _last_clear_expired_cache_millis > _cache_expire_ms) {
        _update_state_cache.clear_expired();
        _index_cache.clear_expired();
        _last_clear_expired_cache_millis = MonotonicMillis();
    }
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
                                                 MetaFileBuilder* builder, int64_t base_version) {
    std::stringstream cost_str;
    MonotonicStopWatch watch;
    watch.start();
    // 1. load primary index
    auto index_entry = _index_cache.get_or_create(tablet->id());
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& index = index_entry->value();
    Status st = index.lake_load(tablet, metadata, base_version, builder);
    if (!st.ok()) {
        _index_cache.remove(index_entry);
        LOG(ERROR) << strings::Substitute("publish_primary_key_tablet: load primary index failed: $0", st.to_string());
        return st;
    }
    cost_str << " [primary index load] " << watch.elapsed_time();
    watch.reset();
    // release index entry but keep it in cache
    DeferOp release_index_entry([&] { _index_cache.release(index_entry); });
    // 2. iterate output rowset, update primary index and generate delvec
    std::unique_ptr<TabletSchema> tablet_schema = std::make_unique<TabletSchema>(metadata.schema());
    RowsetPtr output_rowset =
            std::make_shared<Rowset>(tablet, std::make_shared<RowsetMetadata>(op_compaction.output_rowset()));
    auto compaction_state = std::make_unique<CompactionState>(output_rowset.get());
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

    for (size_t i = 0; i < compaction_state->pk_cols.size(); i++) {
        RETURN_IF_ERROR(compaction_state->load_segments(output_rowset.get(), *tablet_schema, i));
        auto& pk_col = compaction_state->pk_cols[i];
        total_rows += pk_col->size();
        uint32_t rssid = rowset_id + i;
        tmp_deletes.clear();
        // replace will not grow hashtable, so don't need to check memory limit
        index.try_replace(rssid, 0, *pk_col, max_src_rssid, &tmp_deletes);
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

    LOG(INFO) << strings::Substitute(
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

Status UpdateManager::check_meta_version(const Tablet& tablet, int64_t base_version) {
    auto index_entry = _index_cache.get(tablet.id());
    if (index_entry == nullptr) {
        // if primary index not in cache, just return true, and continue publish
        return Status::OK();
    } else {
        auto& index = index_entry->value();
        if (index.data_version() != base_version) {
            // clear cache, and continue publish
            LOG(WARNING) << "Lake check_meta_version and remove primary index cache, tablet_id: " << tablet.id()
                         << " index_ver: " << index.data_version() << " base_ver: " << base_version;
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
        LOG(INFO) << strings::Substitute("[lake update manager memory]index:$0 update_state:$1 total:$2/$3",
                                         PrettyPrinter::print_bytes(_index_cache_mem_tracker->consumption()),
                                         PrettyPrinter::print_bytes(_update_state_mem_tracker->consumption()),
                                         PrettyPrinter::print_bytes(_update_mem_tracker->consumption()),
                                         PrettyPrinter::print_bytes(_update_mem_tracker->limit()));
        last_print_ts.store(time(nullptr));
    }
}

} // namespace lake

} // namespace starrocks
