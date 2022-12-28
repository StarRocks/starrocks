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

#include "gen_cpp/lake_types.pb.h"
#include "storage/del_vector.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"

namespace starrocks {

namespace lake {

// |metadata| contain last tablet meta info with new version
Status UpdateManager::publish_primary_key_tablet(const TxnLogPB_OpWrite& op_write, TabletMetadata* metadata,
                                                 Tablet* tablet, MetaFileBuilder* builder, int64_t base_version) {
    // 1. load rowset update data to cache, get upsert and delete list
    const uint32_t rowset_id = metadata->next_rowset_id();
    auto state_entry = _update_state_cache.get_or_create(strings::Substitute("$0_$1", tablet->id(), rowset_id));
    state_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    // only use state entry once, remove it when publish finish or fail
    DeferOp remove_state_entry([&] { _update_state_cache.remove(state_entry); });
    auto& state = state_entry->value();
    RETURN_IF_ERROR(state.load(op_write, base_version, metadata, tablet));
    // 2. rewrite segment file if it is partial update
    RETURN_IF_ERROR(state.rewrite_segment(op_write, tablet, metadata));
    // 3. update primary index
    auto index_entry = _index_cache.get_or_create(tablet->id());
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& index = index_entry->value();
    Status st = index.lake_load(tablet, metadata, base_version);
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
    // 4. generate delvec
    size_t ndelvec = new_deletes.size();
    vector<std::pair<uint32_t, DelVectorPtr>> new_del_vecs(ndelvec);
    size_t idx = 0;
    size_t old_total_del = 0;
    size_t new_del = 0;
    size_t total_del = 0;
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
        } else {
            TabletSegmentId tsid;
            tsid.tablet_id = tablet->id();
            tsid.segment_id = rssid;
            DelVectorPtr old_del_vec;
            RETURN_IF_ERROR(get_latest_del_vec(tsid, base_version, builder, &old_del_vec));
            new_del_vecs[idx].first = rssid;
            old_del_vec->add_dels_as_new_version(new_delete.second, metadata->version(), &(new_del_vecs[idx].second));
            size_t cur_old = old_del_vec->cardinality();
            size_t cur_add = new_delete.second.size();
            size_t cur_new = new_del_vecs[idx].second->cardinality();
            if (cur_old + cur_add != cur_new) {
                // should not happen, data inconsistent
                LOG(FATAL) << strings::Substitute(
                        "delvec inconsistent tablet:$0 rssid:$1 #old:$2 #add:$3 #new:$4 old_v:$5 "
                        "v:$6",
                        tablet->id(), rssid, cur_old, cur_add, cur_new, old_del_vec->version(), metadata->version());
            }
            old_total_del += cur_old;
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
    builder->apply_opwrite(op_write);

    LOG(INFO) << strings::Substitute(
            "lake publish_primary_key_tablet tablet:$0 rowsetid:$1 upserts:$2 deletes:$3 new_del:$4 total_del:$5 "
            "base_ver:$6 new_ver:$7",
            tablet->id(), rowset_id, upserts.size(), state.deletes().size(), new_del, total_del, base_version,
            metadata->version());

    return Status::OK();
}

Status UpdateManager::_do_update(std::uint32_t rowset_id, std::int32_t upsert_idx,
                                 const std::vector<ColumnUniquePtr>& upserts, PrimaryIndex& index,
                                 std::int64_t tablet_id, DeletesMap* new_deletes) {
    index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], new_deletes);

    return Status::OK();
}

Status UpdateManager::get_rowids_from_pkindex(Tablet* tablet, TabletMetadata* metadata,
                                              const std::vector<ColumnUniquePtr>& upserts, const int64_t base_version,
                                              std::vector<std::vector<uint64_t>*>* rss_rowids) {
    auto index_entry = _index_cache.get_or_create(tablet->id());
    index_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    auto& index = index_entry->value();
    auto st = index.lake_load(tablet, metadata, base_version);
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

Status UpdateManager::get_column_values(Tablet* tablet, TabletMetadata* metadata, TabletSchema* tablet_schema,
                                        std::vector<uint32_t>& column_ids, bool with_default,
                                        std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                                        vector<std::unique_ptr<Column>>* columns) {
    if (with_default) {
        for (auto i = 0; i < column_ids.size(); ++i) {
            const TabletColumn& tablet_column = tablet_schema->column(column_ids[i]);
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

    // TODO(yixin): cache rssid_to_path
    std::unordered_map<uint32_t, std::string> rssid_to_path;
    for (auto& rs : metadata->rowsets()) {
        for (int i = 0; i < rs.segments_size(); i++) {
            rssid_to_path[rs.id() + i] = tablet->segment_location(rs.segments(i));
        }
    }

    std::shared_ptr<FileSystem> fs;
    for (const auto& [rssid, rowids] : rowids_by_rssid) {
        if (fs == nullptr) {
            auto root_path = tablet->metadata_root_location();
            ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(root_path));
        }
        auto segment = Segment::open(fs, rssid_to_path[rssid], rssid, tablet_schema);
        if (!segment.ok()) {
            LOG(WARNING) << "Fail to open rssid: " << rssid << " path: " << rssid_to_path[rssid] << " : "
                         << segment.status();
            return segment.status();
        }
        if ((*segment)->num_rows() == 0) {
            continue;
        }
        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(rssid_to_path[rssid]));
        iter_opts.read_file = read_file.get();
        for (auto i = 0; i < column_ids.size(); ++i) {
            ASSIGN_OR_RETURN(auto col_iter, (*segment)->new_column_iterator(column_ids[i]));
            RETURN_IF_ERROR(col_iter->init(iter_opts));
            RETURN_IF_ERROR(col_iter->fetch_values_by_rowid(rowids.data(), rowids.size(), (*columns)[i].get()));
        }
    }
    return Status::OK();
}

Status UpdateManager::get_latest_del_vec(const TabletSegmentId& tsid, int64_t base_version, MetaFileBuilder* builder,
                                         DelVectorPtr* pdelvec) {
    // 1. find in meta builder first
    auto found = builder->find_delvec(tsid, pdelvec);
    if (!found.ok()) {
        return found.status();
    }
    if (*found) {
        return Status::OK();
    }
    {
        // 2. find in cache
        std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
        auto itr = _del_vec_cache.find(tsid);
        if (itr != _del_vec_cache.end()) {
            *pdelvec = itr->second;
            return Status::OK();
        }
    }
    // 3. find in file
    (*pdelvec).reset(new DelVector());
    int64_t latest_version = 0;
    RETURN_IF_ERROR(get_del_vec_in_meta(tsid, base_version, pdelvec->get(), &latest_version));
    std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
    _del_vec_cache.emplace(tsid, *pdelvec);
    _del_vec_cache_ver = std::max(_del_vec_cache_ver, latest_version);
    return Status::OK();
}

Status UpdateManager::get_del_vec(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec) {
    {
        std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
        if (version <= _del_vec_cache_ver) { // if delvec_cache is meet the requirements
            auto itr = _del_vec_cache.find(tsid);
            if (itr != _del_vec_cache.end()) {
                if (version >= itr->second->version()) {
                    LOG(INFO) << strings::Substitute(
                            "get_del_vec cached tablet_segment=$0 version=$1 actual_version=$2", tsid.to_string(),
                            version, itr->second->version());
                    // cache valid
                    *pdelvec = itr->second;
                    return Status::OK();
                }
            }
        }
    }
    (*pdelvec).reset(new DelVector());
    int64_t latest_version = 0;
    RETURN_IF_ERROR(get_del_vec_in_meta(tsid, version, pdelvec->get(), &latest_version));
    if ((*pdelvec)->version() == latest_version) {
        std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
        auto itr = _del_vec_cache.find(tsid);
        if (itr == _del_vec_cache.end()) {
            _del_vec_cache.emplace(tsid, *pdelvec);
        } else if (latest_version > itr->second->version()) {
            // update cache to latest version
            itr->second = (*pdelvec);
        }
        _del_vec_cache_ver = std::max(_del_vec_cache_ver, latest_version);
    }
    return Status::OK();
}

// get delvec in meta file
Status UpdateManager::get_del_vec_in_meta(const TabletSegmentId& tsid, int64_t meta_ver, DelVector* delvec,
                                          int64_t* latest_version) {
    std::string filepath = _location_provider->tablet_metadata_location(tsid.tablet_id, meta_ver);
    MetaFileReader reader(filepath, false);
    RETURN_IF_ERROR(reader.load());
    RETURN_IF_ERROR(reader.get_del_vec(_location_provider, tsid.segment_id, delvec, latest_version));
    return Status::OK();
}

Status UpdateManager::set_cached_del_vec(
        const std::vector<std::pair<TabletSegmentId, DelVectorPtr>>& cache_delvec_updates, int64_t version) {
    std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
    for (const auto& each : cache_delvec_updates) {
        auto itr = _del_vec_cache.find(each.first);
        if (itr != _del_vec_cache.end()) {
            if (each.second->version() <= itr->second->version()) {
                string msg = strings::Substitute("UpdateManager::set_cached_del_vec: new version($0) < old version($1)",
                                                 each.second->version(), itr->second->version());
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            } else {
                itr->second = each.second;
            }
        } else {
            _del_vec_cache.emplace(each.first, each.second);
        }
    }
    _del_vec_cache_ver = version;
    return Status::OK();
}

void UpdateManager::expire_cache() {
    if (MonotonicMillis() - _last_clear_expired_cache_millis > _cache_expire_ms) {
        _update_state_cache.clear_expired();
        _index_cache.clear_expired();
        _last_clear_expired_cache_millis = MonotonicMillis();
    }
}

void UpdateManager::clear_cached_del_vec(const std::vector<TabletSegmentId>& tsids) {
    std::lock_guard<std::mutex> lg(_del_vec_cache_lock);
    for (const auto& tsid : tsids) {
        auto itr = _del_vec_cache.find(tsid);
        if (itr != _del_vec_cache.end()) {
            _del_vec_cache.erase(itr);
        }
    }
}

} // namespace lake

} // namespace starrocks