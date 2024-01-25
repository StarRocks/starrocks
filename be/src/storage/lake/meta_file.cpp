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

MetaFileBuilder::MetaFileBuilder(Tablet tablet, std::shared_ptr<TabletMetadata> metadata)
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

void MetaFileBuilder::apply_opwrite(const TxnLogPB_OpWrite& op_write, const std::map<int, FileInfo>& replace_segments,
                                    const std::vector<std::string>& orphan_files) {
    auto rowset = _tablet_meta->add_rowsets();
    rowset->CopyFrom(op_write.rowset());

    auto segment_size_size = rowset->segment_size_size();
    auto segment_file_size = rowset->segments_size();
    bool upgrage_from_old_version = (segment_size_size != segment_file_size);
    LOG_IF(ERROR, segment_size_size > 0 && segment_size_size != segment_file_size)
            << "segment_size size != segment file size, tablet: " << _tablet.id() << ", rowset: " << rowset->id()
            << ", segment file size: " << segment_file_size << ", segment_size size: " << segment_size_size;

    for (const auto& replace_seg : replace_segments) {
        // when handle partial update, replace old segments with new rewrite segments
        rowset->set_segments(replace_seg.first, replace_seg.second.path);

        // update new rewrite segments size
        if (LIKELY(!upgrage_from_old_version)) {
            rowset->set_segment_size(replace_seg.first, replace_seg.second.size.value());
        }
    }

    rowset->set_id(_tablet_meta->next_rowset_id());
    // if rowset don't contain segment files, still inc next_rowset_id
    _tablet_meta->set_next_rowset_id(_tablet_meta->next_rowset_id() + std::max(1, rowset->segments_size()));
    // collect trash files
    for (const auto& orphan_file : orphan_files) {
        DCHECK(is_segment(orphan_file));
        FileMetaPB file_meta;
        file_meta.set_name(orphan_file);
        _tablet_meta->mutable_orphan_files()->Add(std::move(file_meta));
    }
    for (const auto& del_file : op_write.dels()) {
        FileMetaPB file_meta;
        file_meta.set_name(del_file);
        _tablet_meta->mutable_orphan_files()->Add(std::move(file_meta));
    }
}

void MetaFileBuilder::apply_opcompaction(const TxnLogPB_OpCompaction& op_compaction) {
    // delete input rowsets
    std::stringstream del_range_ss;
    std::vector<std::pair<uint32_t, uint32_t>> delete_delvec_sid_range;
    struct Finder {
        uint32_t id;
        bool operator()(const uint32_t rowid) const { return rowid == id; }
    };
    auto it = _tablet_meta->mutable_rowsets()->begin();
    while (it != _tablet_meta->mutable_rowsets()->end()) {
        auto search_it = std::find_if(op_compaction.input_rowsets().begin(), op_compaction.input_rowsets().end(),
                                      Finder{it->id()});
        if (search_it != op_compaction.input_rowsets().end()) {
            // find it
            delete_delvec_sid_range.emplace_back(it->id(), it->id() + it->segments_size() - 1);
            _tablet_meta->mutable_compaction_inputs()->Add(std::move(*it));
            it = _tablet_meta->mutable_rowsets()->erase(it);
            del_range_ss << "[" << delete_delvec_sid_range.back().first << "," << delete_delvec_sid_range.back().second
                         << "] ";
        } else {
            it++;
        }
    }
    // delete delvec by input rowsets
    int delvec_erase_cnt = 0;
    auto delvec_it = _tablet_meta->mutable_delvec_meta()->mutable_delvecs()->begin();
    while (delvec_it != _tablet_meta->mutable_delvec_meta()->mutable_delvecs()->end()) {
        bool need_del = false;
        for (const auto& range : delete_delvec_sid_range) {
            if (delvec_it->first >= range.first && delvec_it->first <= range.second) {
                need_del = true;
                break;
            }
        }
        if (need_del) {
            delvec_it = _tablet_meta->mutable_delvec_meta()->mutable_delvecs()->erase(delvec_it);
            delvec_erase_cnt++;
        } else {
            delvec_it++;
        }
    }

    // add output rowset
    if (op_compaction.has_output_rowset() && op_compaction.output_rowset().segments_size() > 0) {
        auto rowset = _tablet_meta->add_rowsets();
        rowset->CopyFrom(op_compaction.output_rowset());
        rowset->set_id(_tablet_meta->next_rowset_id());
        _tablet_meta->set_next_rowset_id(_tablet_meta->next_rowset_id() + rowset->segments_size());
    }

    VLOG(2) << fmt::format("MetaFileBuilder apply_opcompaction, id:{} input range:{} delvec del cnt:{} output:{}",
                           _tablet_meta->id(), del_range_ss.str(), delvec_erase_cnt,
                           op_compaction.output_rowset().ShortDebugString());
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
    // Set _has_finalized at last, and if failure happens before this, we need to clear pk index
    // and retry publish later.
    _has_finalized = true;
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

void MetaFileBuilder::handle_failure() {
    if (is_primary_key(_tablet_meta.get()) && !_has_finalized && _has_update_index) {
        // if we meet failures and have not finalized yet, have to clear primary index cache,
        // then we can retry again.
        _update_mgr->remove_primary_index_cache(_tablet_meta->id());
    }
}

Status get_del_vec(TabletManager* tablet_mgr, const TabletMetadata& metadata, uint32_t segment_id, DelVector* delvec) {
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
        RandomAccessFileOptions opts{.skip_fill_local_cache = true};
        ASSIGN_OR_RETURN(auto rf,
                         fs::new_random_access_file(opts, tablet_mgr->delvec_location(metadata.id(), delvec_name)));
        RETURN_IF_ERROR(rf->read_at_fully(iter->second.offset(), buf.data(), iter->second.size()));
        // parse delvec
        RETURN_IF_ERROR(delvec->load(iter->second.version(), buf.data(), iter->second.size()));
        // put in cache
        auto delvec_cache_ptr = std::make_shared<DelVector>();
        delvec_cache_ptr->copy_from(*delvec);
        tablet_mgr->metacache()->cache_delvec(cache_key, delvec_cache_ptr);
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

void rowset_rssid_to_path(const TabletMetadata& metadata, const TxnLogPB_OpWrite& op_write,
                          std::unordered_map<uint32_t, FileInfo>& rssid_to_file_info) {
    auto get_file_info_from_rowset = [&](const RowsetMetadataPB& meta, const uint32_t rowset_id) -> void {
        bool has_segment_size = (meta.segments_size() == meta.segment_size_size());
        for (int i = 0; i < meta.segments_size(); i++) {
            FileInfo segment_info{.path = meta.segments(i)};
            if (LIKELY(has_segment_size)) {
                segment_info.size = meta.segment_size(i);
            }
            rssid_to_file_info[rowset_id + i] = segment_info;
        }
    };

    for (auto& rs : metadata.rowsets()) {
        get_file_info_from_rowset(rs, rs.id());
    }
    const uint32_t rowset_id = metadata.next_rowset_id();
    for (int i = 0; i < op_write.rowset().segments_size(); i++) {
        get_file_info_from_rowset(op_write.rowset(), rowset_id);
    }
}

} // namespace starrocks::lake
