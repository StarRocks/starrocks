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

#include "meta_file.h"

#include <memory>

#include "fs/fs_util.h"
#include "storage/del_vector.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/metacache.h"
#include "storage/lake/update_manager.h"
#include "storage/protobuf_file.h"
#include "util/coding.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "util/trace.h"

namespace starrocks::lake {

static std::string delvec_cache_key(int64_t tablet_id, const DelvecPagePB& page) {
    DelvecCacheKeyPB cache_key_pb;
    cache_key_pb.set_id(tablet_id);
    cache_key_pb.mutable_delvec_page()->CopyFrom(page);
    return cache_key_pb.SerializeAsString();
}

MetaFileBuilder::MetaFileBuilder(const Tablet& tablet, std::shared_ptr<TabletMetadata> metadata)
        : _tablet(tablet), _tablet_meta(std::move(metadata)), _update_mgr(_tablet.update_mgr()) {}

void MetaFileBuilder::append_delvec(const DelVectorPtr& delvec, uint32_t segment_id) {
    if (delvec->cardinality() > 0) {
        const uint64_t offset = _buf.size();
        std::string delvec_str;
        delvec->save_to(&delvec_str);
        _buf.insert(_buf.end(), delvec_str.begin(), delvec_str.end());
        const uint64_t size = _buf.size() - offset;
        _delvecs[segment_id].set_offset(offset);
        _delvecs[segment_id].set_size(size);
        _segmentid_to_delvec[segment_id] = delvec;
    }
}

void MetaFileBuilder::append_dcg(uint32_t rssid,
                                 const std::vector<std::pair<std::string, std::string>>& file_with_encryption_metas,
                                 const std::vector<std::vector<ColumnUID>>& unique_column_id_list) {
    DeltaColumnGroupVerPB& dcg_ver = (*_tablet_meta->mutable_dcg_meta()->mutable_dcgs())[rssid];
    DeltaColumnGroupVerPB new_dcg_ver;
    std::unordered_set<ColumnUID> need_to_remove_cuids_filter;

    // 1. append new dcgs
    DCHECK(file_with_encryption_metas.size() == unique_column_id_list.size());
    for (int i = 0; i < file_with_encryption_metas.size(); i++) {
        new_dcg_ver.add_column_files(file_with_encryption_metas[i].first);
        if (!file_with_encryption_metas[i].second.empty()) {
            new_dcg_ver.add_encryption_metas(file_with_encryption_metas[i].second);
        }
        DeltaColumnGroupColumnIdsPB unique_cids;
        for (const ColumnUID uid : unique_column_id_list[i]) {
            unique_cids.add_column_ids(uid);
            // Build filter so we can remove old columns at second step.
            need_to_remove_cuids_filter.insert(uid);
        }
        new_dcg_ver.add_unique_column_ids()->CopyFrom(unique_cids);
        new_dcg_ver.add_versions(_tablet_meta->version());
    }
    // 2. remove old dcgs
    DCHECK(dcg_ver.unique_column_ids_size() == dcg_ver.column_files_size());
    DCHECK(dcg_ver.unique_column_ids_size() == dcg_ver.versions_size());
    for (int i = 0; i < dcg_ver.unique_column_ids_size(); i++) {
        auto* mcids = dcg_ver.mutable_unique_column_ids(i)->mutable_column_ids();
        mcids->erase(std::remove_if(mcids->begin(), mcids->end(),
                                    [&](uint32 cuid) { return need_to_remove_cuids_filter.count(cuid) > 0; }),
                     mcids->end());
        if (!mcids->empty()) {
            new_dcg_ver.add_unique_column_ids()->CopyFrom(dcg_ver.unique_column_ids(i));
            new_dcg_ver.add_column_files(dcg_ver.column_files(i));
            new_dcg_ver.add_versions(dcg_ver.versions(i));
            if (i < dcg_ver.encryption_metas_size()) {
                new_dcg_ver.add_encryption_metas(dcg_ver.encryption_metas(i));
            }
        } else {
            // Put this `.cols` files into orphan files
            FileMetaPB file_meta;
            file_meta.set_name(dcg_ver.column_files(i));
            _tablet_meta->mutable_orphan_files()->Add(std::move(file_meta));
        }
    }

    (*_tablet_meta->mutable_dcg_meta()->mutable_dcgs())[rssid] = new_dcg_ver;
}

void MetaFileBuilder::apply_opwrite(const TxnLogPB_OpWrite& op_write, const std::map<int, FileInfo>& replace_segments,
                                    const std::vector<std::string>& orphan_files) {
    auto rowset = _tablet_meta->add_rowsets();
    rowset->CopyFrom(op_write.rowset());

    auto segment_size_size = rowset->segment_size_size();
    auto segment_file_size = rowset->segments_size();
    LOG_IF(ERROR, segment_size_size > 0 && segment_size_size != segment_file_size)
            << "segment_size size != segment file size, tablet: " << _tablet.id() << ", rowset: " << rowset->id()
            << ", segment file size: " << segment_file_size << ", segment_size size: " << segment_size_size;

    for (const auto& replace_seg : replace_segments) {
        // when handle partial update, replace old segments with new rewrite segments
        rowset->set_segments(replace_seg.first, replace_seg.second.path);
        if (replace_seg.first < rowset->segment_encryption_metas_size()) {
            rowset->set_segment_encryption_metas(replace_seg.first, replace_seg.second.encryption_meta);
        }
    }
    if (!replace_segments.empty()) {
        // NOT record segment size in rowset generated by partial update
        rowset->clear_segment_size();
    }

    rowset->set_id(_tablet_meta->next_rowset_id());
    rowset->set_version(_tablet_meta->version());
    // collect del files
    for (int i = 0; i < op_write.dels_size(); i++) {
        DelfileWithRowsetId del_file_with_rid;
        del_file_with_rid.set_name(op_write.dels(i));
        del_file_with_rid.set_origin_rowset_id(rowset->id());
        // For now, op_offset is always max segment's id
        del_file_with_rid.set_op_offset(std::max(op_write.rowset().segments_size(), 1) - 1);
        if (op_write.del_encryption_metas_size() > 0) {
            CHECK(op_write.del_encryption_metas_size() == op_write.dels_size())
                    << fmt::format("del_encryption_metas_size:{} != dels_size:{}", op_write.del_encryption_metas_size(),
                                   op_write.dels_size());
            del_file_with_rid.set_encryption_meta(op_write.del_encryption_metas(i));
        }
        rowset->add_del_files()->CopyFrom(del_file_with_rid);
    }
    // if rowset don't contain segment files, still inc next_rowset_id
    _tablet_meta->set_next_rowset_id(_tablet_meta->next_rowset_id() + std::max(1, rowset->segments_size()));
    // collect trash files
    for (const auto& orphan_file : orphan_files) {
        DCHECK(is_segment(orphan_file));
        FileMetaPB file_meta;
        file_meta.set_name(orphan_file);
        _tablet_meta->mutable_orphan_files()->Add(std::move(file_meta));
    }
    if (!_tablet_meta->rowset_to_schema().empty()) {
        auto schema_id = _tablet_meta->schema().id();
        (*_tablet_meta->mutable_rowset_to_schema())[rowset->id()] = schema_id;
        if (_tablet_meta->historical_schemas().count(schema_id) <= 0) {
            auto& item = (*_tablet_meta->mutable_historical_schemas())[schema_id];
            item.CopyFrom(_tablet_meta->schema());
        }
    }
}

void MetaFileBuilder::apply_column_mode_partial_update(const TxnLogPB_OpWrite& op_write) {
    // remove all segments that only contains partial columns.
    for (const auto& segment : op_write.rowset().segments()) {
        FileMetaPB file_meta;
        file_meta.set_name(segment);
        _tablet_meta->mutable_orphan_files()->Add(std::move(file_meta));
    }
}

// delete from protobuf Map and return deleted count
template <typename T>
static int delete_from_protobuf_map(T* protobuf_map, const std::vector<std::pair<uint32_t, uint32_t>>& delete_sid_range,
                                    const std::function<void(const T&)>& gc_func) {
    // collect item that had been deleted.
    T gc_map;
    int erase_cnt = 0;
    auto it = protobuf_map->begin();
    while (it != protobuf_map->end()) {
        bool need_del = false;
        for (const auto& range : delete_sid_range) {
            if (it->first >= range.first && it->first <= range.second) {
                need_del = true;
                break;
            }
        }
        if (need_del) {
            gc_map[it->first] = it->second;
            it = protobuf_map->erase(it);
            erase_cnt++;
        } else {
            it++;
        }
    }
    gc_func(gc_map);
    return erase_cnt;
}

// When using cloud native persistent index, the del files which are above rebuild point,
// need to be transfer to compaction's output rowset.
// Use this function to collect all del files that need to be transfer.
void MetaFileBuilder::_collect_del_files_above_rebuild_point(RowsetMetadataPB* rowset,
                                                             std::vector<DelfileWithRowsetId>* collect_del_files) {
    if (!_tablet_meta->enable_persistent_index() ||
        _tablet_meta->persistent_index_type() != PersistentIndexTypePB::CLOUD_NATIVE) {
        // do nothing, unpersisted del files is collect only for cloud native persistent index.
        return;
    }
    if (rowset->del_files_size() == 0) {
        return;
    }
    const auto& sstables = _tablet_meta->sstable_meta().sstables();
    // Rebuild persistent index from `rebuild_rss_rowid_point`
    const uint64_t rebuild_rss_rowid_point = sstables.empty() ? 0 : sstables.rbegin()->max_rss_rowid();
    const uint32_t rebuild_rss_id = rebuild_rss_rowid_point >> 32;
    if (LakePersistentIndex::needs_rowset_rebuild(*rowset, rebuild_rss_id)) {
        // Above rebuild point
        for (const auto& each : rowset->del_files()) {
            collect_del_files->push_back(each);
        }
        // These del files will be collect and transfer to compaction's output rowset.
        rowset->clear_del_files();
    }
}

// check the last input rowset to determine whether this is partial compaction,
// if is, modify last intput rowset `segments` info, for example, following
// `e` and `f` will be removed from last input rowset.
// before(last rowset input segments): x y a b c d e f
// segments in compaction: a b c d
// output segments:                    x y m n e f
// after (last rowset input segments): a b c d
void trim_partial_compaction_last_input_rowset(const MutableTabletMetadataPtr& metadata,
                                               const TxnLogPB_OpCompaction& op_compaction,
                                               RowsetMetadataPB& last_input_rowset) {
    if (op_compaction.input_rowsets_size() < 1) {
        return;
    }
    if (op_compaction.input_rowsets(op_compaction.input_rowsets_size() - 1) != last_input_rowset.id()) {
        return;
    }
    if (op_compaction.has_output_rowset() && op_compaction.output_rowset().segments_size() > 0 &&
        last_input_rowset.segments_size() > 0) {
        // iterate all segments in last input rowset, find if any of them exists in
        // compaction output rowset, if is, erase them from last input rowset
        size_t before = last_input_rowset.segments_size();
        auto iter = last_input_rowset.mutable_segments()->begin();
        while (iter != last_input_rowset.mutable_segments()->end()) {
            auto it = std::find_if(op_compaction.output_rowset().segments().begin(),
                                   op_compaction.output_rowset().segments().end(),
                                   [iter](const std::string& segment) { return *iter == segment; });
            if (it != op_compaction.output_rowset().segments().end()) {
                iter = last_input_rowset.mutable_segments()->erase(iter);
            } else {
                ++iter;
            }
        }
        size_t after = last_input_rowset.segments_size();
        if (after - before > 0) {
            LOG(INFO) << "find partial compaction, tablet: " << metadata->id() << ", version: " << metadata->version()
                      << ", last input rowset id: " << last_input_rowset.id()
                      << ", uncompacted segment count: " << (before - after);
        }
    }
}

void MetaFileBuilder::apply_opcompaction(const TxnLogPB_OpCompaction& op_compaction,
                                         uint32_t max_compact_input_rowset_id, int64_t output_rowset_schema_id) {
    // delete input rowsets
    std::stringstream del_range_ss;
    std::vector<std::pair<uint32_t, uint32_t>> delete_delvec_sid_range;
    struct Finder {
        uint32_t id;
        bool operator()(const uint32_t rowid) const { return rowid == id; }
    };

    struct RowsetFinder {
        uint32_t id;
        bool operator()(const RowsetMetadata& r) const { return r.id() == id; }
    };

    // Only used for cloud native persistent index.
    std::vector<DelfileWithRowsetId> collect_del_files;
    auto it = _tablet_meta->mutable_rowsets()->begin();
    while (it != _tablet_meta->mutable_rowsets()->end()) {
        auto search_it = std::find_if(op_compaction.input_rowsets().begin(), op_compaction.input_rowsets().end(),
                                      Finder{it->id()});
        if (search_it != op_compaction.input_rowsets().end()) {
            // find it
            delete_delvec_sid_range.emplace_back(it->id(), it->id() + it->segments_size() - 1);
            // Collect del files.
            _collect_del_files_above_rebuild_point(&(*it), &collect_del_files);
            _tablet_meta->mutable_compaction_inputs()->Add(std::move(*it));
            it = _tablet_meta->mutable_rowsets()->erase(it);
            del_range_ss << "[" << delete_delvec_sid_range.back().first << "," << delete_delvec_sid_range.back().second
                         << "] ";
        } else {
            it++;
        }
    }
    // delete delvec by input rowsets
    auto delvecs = _tablet_meta->mutable_delvec_meta()->mutable_delvecs();
    using T_DELVEC = std::decay_t<decltype(*delvecs)>;
    int delvec_erase_cnt =
            delete_from_protobuf_map<T_DELVEC>(delvecs, delete_delvec_sid_range, [](const T_DELVEC& gc_map) {});
    // delete dcg by input rowsets
    auto dcgs = _tablet_meta->mutable_dcg_meta()->mutable_dcgs();
    using T_DCG = std::decay_t<decltype(*dcgs)>;
    int dcg_erase_cnt = delete_from_protobuf_map<T_DCG>(dcgs, delete_delvec_sid_range, [&](const T_DCG& gc_map) {
        for (const auto& each : gc_map) {
            for (const auto& each_file : each.second.column_files()) {
                FileMetaPB file_meta;
                file_meta.set_name(each_file);
                // Put useless `.cols` files into orphan files
                _tablet_meta->mutable_orphan_files()->Add(std::move(file_meta));
            }
        }
    });

    // remove compacted sst
    for (auto& input_sstable : op_compaction.input_sstables()) {
        FileMetaPB file_meta;
        file_meta.set_name(input_sstable.filename());
        file_meta.set_size(input_sstable.filesize());
        _tablet_meta->mutable_orphan_files()->Add(std::move(file_meta));
    }

    // add output rowset
    bool has_output_rowset = false;
    uint32_t output_rowset_id = 0;
    if (op_compaction.has_output_rowset() &&
        (op_compaction.output_rowset().segments_size() > 0 || !collect_del_files.empty())) {
        // NOTICE: we need output rowset in two scenarios:
        // 1. We have output segments after compactions.
        // 2. We need del files to rebuild cloud native PK index.
        auto rowset = _tablet_meta->add_rowsets();
        rowset->CopyFrom(op_compaction.output_rowset());
        rowset->set_id(_tablet_meta->next_rowset_id());
        rowset->set_max_compact_input_rowset_id(max_compact_input_rowset_id);
        rowset->set_version(_tablet_meta->version());
        for (const auto& each : collect_del_files) {
            rowset->add_del_files()->CopyFrom(each);
        }
        _tablet_meta->set_next_rowset_id(_tablet_meta->next_rowset_id() + std::max(1, rowset->segments_size()));
        has_output_rowset = true;
        output_rowset_id = rowset->id();
    }

    // update rowset schema id
    if (!_tablet_meta->rowset_to_schema().empty()) {
        for (int i = 0; i < op_compaction.input_rowsets_size(); i++) {
            _tablet_meta->mutable_rowset_to_schema()->erase(op_compaction.input_rowsets(i));
        }

        if (has_output_rowset) {
            _tablet_meta->mutable_rowset_to_schema()->insert({output_rowset_id, output_rowset_schema_id});
        }

        std::unordered_set<int64_t> schema_id;
        for (auto& pair : _tablet_meta->rowset_to_schema()) {
            schema_id.insert(pair.second);
        }

        for (auto it = _tablet_meta->mutable_historical_schemas()->begin();
             it != _tablet_meta->mutable_historical_schemas()->end();) {
            if (schema_id.find(it->first) == schema_id.end()) {
                it = _tablet_meta->mutable_historical_schemas()->erase(it);
            } else {
                it++;
            }
        }
    }

    VLOG(2) << fmt::format(
            "MetaFileBuilder apply_opcompaction, id:{} input range:{} delvec del cnt:{} dcg del cnt:{} output:{}",
            _tablet_meta->id(), del_range_ss.str(), delvec_erase_cnt, dcg_erase_cnt,
            op_compaction.output_rowset().ShortDebugString());
}

void MetaFileBuilder::apply_opcompaction_with_conflict(const TxnLogPB_OpCompaction& op_compaction) {
    // add output segments to orphan files
    for (int i = 0; i < op_compaction.output_rowset().segments_size(); i++) {
        FileMetaPB file_meta;
        file_meta.set_name(op_compaction.output_rowset().segments(i));
        _tablet_meta->mutable_orphan_files()->Add(std::move(file_meta));
    }
}

Status MetaFileBuilder::update_num_del_stat(const std::map<uint32_t, size_t>& segment_id_to_add_dels) {
    std::map<uint32_t, RowsetMetadataPB*> segment_id_to_rowset;
    for (int i = 0; i < _tablet_meta->rowsets_size(); i++) {
        auto* mutable_rowset = _tablet_meta->mutable_rowsets(i);
        for (int j = 0; j < mutable_rowset->segments_size(); j++) {
            segment_id_to_rowset[mutable_rowset->id() + j] = mutable_rowset;
        }
    }
    for (const auto& each : segment_id_to_add_dels) {
        if (segment_id_to_rowset.count(each.first) == 0) {
            // Maybe happen when primary index is in error state.
            std::string err_msg =
                    fmt::format("unexpected segment id: {} tablet id: {}", each.first, _tablet_meta->id());
            LOG(ERROR) << err_msg;
            if (!config::experimental_lake_ignore_pk_consistency_check) {
                set_recover_flag(RecoverFlag::RECOVER_WITHOUT_PUBLISH);
                return Status::InternalError(err_msg);
            }
        } else {
            const int64_t prev_num_dels = segment_id_to_rowset[each.first]->num_dels();
            if (each.second > std::numeric_limits<int64_t>::max() - prev_num_dels) {
                // Can't be possible
                LOG(ERROR) << "Integer overflow detected";
                return Status::InternalError("Integer overflow detected");
            }
            segment_id_to_rowset[each.first]->set_num_dels(prev_num_dels + each.second);
        }
    }
    return Status::OK();
}

Status MetaFileBuilder::_finalize_delvec(int64_t version, int64_t txn_id) {
    if (!is_primary_key(_tablet_meta.get())) return Status::OK();

    // 1. update delvec page in meta
    for (auto&& each_delvec : *(_tablet_meta->mutable_delvec_meta()->mutable_delvecs())) {
        auto iter = _delvecs.find(each_delvec.first);
        if (iter != _delvecs.end()) {
            each_delvec.second.set_version(version);
            each_delvec.second.set_offset(iter->second.offset());
            each_delvec.second.set_size(iter->second.size());
            // record from cache key to segment id, so we can fill up cache later
            _cache_key_to_segment_id[delvec_cache_key(_tablet_meta->id(), each_delvec.second)] = iter->first;
            _delvecs.erase(iter);
        }
    }

    // 2. insert new delvec to meta
    for (auto&& each_delvec : _delvecs) {
        each_delvec.second.set_version(version);
        (*_tablet_meta->mutable_delvec_meta()->mutable_delvecs())[each_delvec.first] = each_delvec.second;
        // record from cache key to segment id, so we can fill up cache later
        _cache_key_to_segment_id[delvec_cache_key(_tablet_meta->id(), each_delvec.second)] = each_delvec.first;
    }

    // 3. write to delvec file
    if (_buf.size() > 0) {
        auto delvec_file_name = gen_delvec_filename(txn_id);
        auto delvec_file_path = _tablet.delvec_location(delvec_file_name);
        // keep delete vector file name in tablet meta
        auto& item = (*_tablet_meta->mutable_delvec_meta()->mutable_version_to_file())[version];
        item.set_name(delvec_file_name);
        item.set_size(_buf.size());
        auto options = WritableFileOptions{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto writer_file, fs::new_writable_file(options, delvec_file_path));
        RETURN_IF_ERROR(writer_file->append(Slice(_buf.data(), _buf.size())));
        RETURN_IF_ERROR(writer_file->close());
        TRACE("end write delvel");
    }

    // 4. clear delvec file record in version_to_file if it's not refered any more
    // collect all versions still refered
    std::set<int64_t> refered_versions;
    for (const auto& item : _tablet_meta->delvec_meta().delvecs()) {
        refered_versions.insert(item.second.version());
    }

    auto itr = _tablet_meta->mutable_delvec_meta()->mutable_version_to_file()->begin();
    for (; itr != _tablet_meta->mutable_delvec_meta()->mutable_version_to_file()->end();) {
        // this delvec file not be refered any more, clear this record safely
        if (refered_versions.find(itr->first) == refered_versions.end()) {
            VLOG(2) << "Remove delvec file record from delvec meta, version: " << itr->first
                    << ", file: " << itr->second.name();
            _tablet_meta->mutable_orphan_files()->Add(std::move(itr->second));
            itr = _tablet_meta->mutable_delvec_meta()->mutable_version_to_file()->erase(itr);
        } else {
            ++itr;
        }
    }

    return Status::OK();
}

Status MetaFileBuilder::finalize(int64_t txn_id) {
    auto version = _tablet_meta->version();
    // finalize delvec
    RETURN_IF_ERROR(_finalize_delvec(version, txn_id));
    RETURN_IF_ERROR(_tablet.put_metadata(_tablet_meta));
    _update_mgr->update_primary_index_data_version(_tablet, version);
    _fill_delvec_cache();
    return Status::OK();
}

StatusOr<bool> MetaFileBuilder::find_delvec(const TabletSegmentId& tsid, DelVectorPtr* pdelvec) const {
    auto iter = _delvecs.find(tsid.segment_id);
    if (iter != _delvecs.end()) {
        (*pdelvec) = std::make_shared<DelVector>();
        // read delvec from write buf
        RETURN_IF_ERROR((*pdelvec)->load(iter->second.version(),
                                         reinterpret_cast<const char*>(_buf.data()) + iter->second.offset(),
                                         iter->second.size()));
        return true;
    }
    return false;
}

void MetaFileBuilder::_fill_delvec_cache() {
    for (const auto& cache_item : _cache_key_to_segment_id) {
        // find delvec ptr by segment id
        auto delvec_iter = _segmentid_to_delvec.find(cache_item.second);
        if (delvec_iter != _segmentid_to_delvec.end() && delvec_iter->second != nullptr) {
            _tablet.tablet_mgr()->metacache()->cache_delvec(cache_item.first, delvec_iter->second);
        }
    }
}

void MetaFileBuilder::finalize_sstable_meta(const PersistentIndexSstableMetaPB& sstable_meta) {
    _tablet_meta->mutable_sstable_meta()->CopyFrom(sstable_meta);
}

Status get_del_vec(TabletManager* tablet_mgr, const TabletMetadata& metadata, uint32_t segment_id, bool fill_cache,
                   DelVector* delvec) {
    // find delvec by segment id
    auto iter = metadata.delvec_meta().delvecs().find(segment_id);
    if (iter != metadata.delvec_meta().delvecs().end()) {
        VLOG(2) << fmt::format("get_del_vec {} segid {}", metadata.delvec_meta().ShortDebugString(), segment_id);
        std::string buf;
        raw::stl_string_resize_uninitialized(&buf, iter->second.size());
        // find in cache
        std::string cache_key = delvec_cache_key(metadata.id(), iter->second);
        auto cached_delvec = tablet_mgr->metacache()->lookup_delvec(cache_key);
        if (cached_delvec != nullptr) {
            delvec->copy_from(*cached_delvec);
            return Status::OK();
        }

        // lookup delvec file name and then read it
        auto iter2 = metadata.delvec_meta().version_to_file().find(iter->second.version());
        if (iter2 == metadata.delvec_meta().version_to_file().end()) {
            LOG(ERROR) << "Can't find delvec file name for tablet: " << metadata.id()
                       << ", version: " << iter->second.version();
            return Status::InternalError("Can't find delvec file name");
        }
        const auto& delvec_name = iter2->second.name();
        RandomAccessFileOptions opts{.skip_fill_local_cache = !fill_cache};
        ASSIGN_OR_RETURN(auto rf,
                         fs::new_random_access_file(opts, tablet_mgr->delvec_location(metadata.id(), delvec_name)));
        RETURN_IF_ERROR(rf->read_at_fully(iter->second.offset(), buf.data(), iter->second.size()));
        // parse delvec
        RETURN_IF_ERROR(delvec->load(iter->second.version(), buf.data(), iter->second.size()));
        // put in cache
        if (fill_cache) {
            auto delvec_cache_ptr = std::make_shared<DelVector>();
            delvec_cache_ptr->copy_from(*delvec);
            tablet_mgr->metacache()->cache_delvec(cache_key, delvec_cache_ptr);
        }
        TRACE("end load delvec");
        return Status::OK();
    }
    VLOG(2) << fmt::format("get_del_vec not found, segmentid {} tablet_meta {}", segment_id,
                           metadata.delvec_meta().ShortDebugString());
    return Status::OK();
}

bool is_primary_key(TabletMetadata* metadata) {
    return metadata->schema().keys_type() == KeysType::PRIMARY_KEYS;
}

bool is_primary_key(const TabletMetadata& metadata) {
    return metadata.schema().keys_type() == KeysType::PRIMARY_KEYS;
}

} // namespace starrocks::lake
