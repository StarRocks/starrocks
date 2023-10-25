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
#include "gutil/strings/escaping.h"
#include "storage/del_vector.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/update_manager.h"
#include "util/coding.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "util/trace.h"

namespace starrocks {
namespace lake {

static std::string delvec_cache_key(int64_t tablet_id, const DelvecPagePB& page) {
    DelvecCacheKeyPB cache_key_pb;
    cache_key_pb.set_id(tablet_id);
    cache_key_pb.mutable_delvec_page()->CopyFrom(page);
    return cache_key_pb.SerializeAsString();
}

MetaFileBuilder::MetaFileBuilder(Tablet tablet, std::shared_ptr<TabletMetadata> metadata)
        : _tablet(tablet), _tablet_meta(std::move(metadata)), _update_mgr(_tablet.update_mgr()) {
    _trash_files = std::make_shared<std::vector<std::string>>();
}

void MetaFileBuilder::append_delvec(DelVectorPtr delvec, uint32_t segment_id) {
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

void MetaFileBuilder::apply_opwrite(const TxnLogPB_OpWrite& op_write,
                                    const std::map<int, std::string>& replace_segments,
                                    const std::vector<std::string>& orphan_files) {
    auto rowset = _tablet_meta->add_rowsets();
    rowset->CopyFrom(op_write.rowset());
    for (const auto& replace_seg : replace_segments) {
        // when handle partial update, replace old segments with new rewrite segments
        rowset->set_segments(replace_seg.first, replace_seg.second);
    }
    rowset->set_id(_tablet_meta->next_rowset_id());
    // if rowset don't contain segment files, still inc next_rowset_id
    _tablet_meta->set_next_rowset_id(_tablet_meta->next_rowset_id() + std::max(1, rowset->segments_size()));
    // collect trash files
    for (const auto& orphan_file : orphan_files) {
        DCHECK(is_segment(orphan_file));
        _trash_files->push_back(_tablet.segment_location(orphan_file));
    }
    for (const auto& del_file : op_write.dels()) {
        _trash_files->push_back(_tablet.del_location(del_file));
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
            delete_delvec_sid_range.push_back(std::make_pair(it->id(), it->id() + it->segments_size() - 1));
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
            _tablet.tablet_mgr()->cache_delvec(cache_item.first, delvec_iter->second);
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

MetaFileReader::MetaFileReader(const std::string& filepath, bool fill_cache) {
    RandomAccessFileOptions opts{.skip_fill_local_cache = !fill_cache};
    auto rf = fs::new_random_access_file(opts, filepath);
    if (rf.ok()) {
        _access_file = std::move(*rf);
    } else {
        _err_status = rf.status();
    }
    _tablet_meta = std::make_unique<TabletMetadata>();
    _load = false;
}

Status MetaFileReader::load() {
    if (_access_file == nullptr) return _err_status;

    ASSIGN_OR_RETURN(auto file_size, _access_file->get_size());
    if (file_size <= 4) {
        return Status::Corruption(
                fmt::format("meta file {} is corrupt, invalid file size {}", _access_file->filename(), file_size));
    }
    std::string metadata_str;
    raw::stl_string_resize_uninitialized(&metadata_str, file_size);
    RETURN_IF_ERROR(_access_file->read_at_fully(0, metadata_str.data(), file_size));
    bool parsed = _tablet_meta->ParseFromArray(metadata_str.data(), static_cast<int>(file_size));
    if (!parsed) {
        return Status::Corruption(fmt::format("failed to parse tablet meta {}", _access_file->filename()));
    }
    _load = true;
    TRACE("end load tablet metadata");
    return Status::OK();
}

Status MetaFileReader::load_by_cache(const std::string& filepath, TabletManager* tablet_mgr) {
    // 1. lookup meta cache first
    if (auto ptr = tablet_mgr->lookup_tablet_metadata(filepath); ptr != nullptr) {
        _tablet_meta = ptr;
        _load = true;
        return Status::OK();
    } else {
        // 2. load directly
        return load();
    }
}

Status MetaFileReader::get_del_vec(TabletManager* tablet_mgr, uint32_t segment_id, DelVector* delvec) {
    if (_access_file == nullptr) return _err_status;
    if (!_load) return Status::InternalError("meta file reader not loaded");
    // find delvec by segment id
    auto iter = _tablet_meta->delvec_meta().delvecs().find(segment_id);
    if (iter != _tablet_meta->delvec_meta().delvecs().end()) {
        VLOG(2) << fmt::format("MetaFileReader get_del_vec {} segid {}", _tablet_meta->delvec_meta().ShortDebugString(),
                               segment_id);
        std::string buf;
        raw::stl_string_resize_uninitialized(&buf, iter->second.size());
        // find in cache
        std::string cache_key = delvec_cache_key(_tablet_meta->id(), iter->second);
        DelVectorPtr delvec_cache_ptr = tablet_mgr->lookup_delvec(cache_key);
        if (delvec_cache_ptr != nullptr) {
            delvec->copy_from(*delvec_cache_ptr);
            return Status::OK();
        }

        // lookup delvec file name and then read it
        auto iter2 = _tablet_meta->delvec_meta().version_to_file().find(iter->second.version());
        if (iter2 == _tablet_meta->delvec_meta().version_to_file().end()) {
            LOG(ERROR) << "Can't find delvec file name for tablet: " << _tablet_meta->id()
                       << ", version: " << iter->second.version();
            return Status::InternalError("Can't find delvec file name");
        }
        const auto& delvec_name = iter2->second.name();
        RandomAccessFileOptions opts{.skip_fill_local_cache = true};
        ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(
                                          opts, tablet_mgr->delvec_location(_tablet_meta->id(), delvec_name)));
        RETURN_IF_ERROR(rf->read_at_fully(iter->second.offset(), buf.data(), iter->second.size()));
        // parse delvec
        RETURN_IF_ERROR(delvec->load(iter->second.version(), buf.data(), iter->second.size()));
        // put in cache
        delvec_cache_ptr = std::make_shared<DelVector>();
        delvec_cache_ptr->copy_from(*delvec);
        tablet_mgr->cache_delvec(cache_key, delvec_cache_ptr);
        TRACE("end load delvec");
        return Status::OK();
    }
    VLOG(2) << fmt::format("MetaFileReader get_del_vec not found, segmentid {} tablet_meta {}", segment_id,
                           _tablet_meta->delvec_meta().ShortDebugString());
    return Status::OK();
}

StatusOr<TabletMetadataPtr> MetaFileReader::get_meta() {
    if (_access_file == nullptr) return _err_status;
    if (!_load) return Status::InternalError("meta file reader not loaded");
    return std::move(_tablet_meta);
}

bool is_primary_key(TabletMetadata* metadata) {
    return metadata->schema().keys_type() == KeysType::PRIMARY_KEYS;
}

bool is_primary_key(const TabletMetadata& metadata) {
    return metadata.schema().keys_type() == KeysType::PRIMARY_KEYS;
}

void rowset_rssid_to_path(const TabletMetadata& metadata, const TxnLogPB_OpWrite& op_write,
                          std::unordered_map<uint32_t, std::string>& rssid_to_path) {
    for (auto& rs : metadata.rowsets()) {
        for (int i = 0; i < rs.segments_size(); i++) {
            rssid_to_path[rs.id() + i] = rs.segments(i);
        }
    }
    const uint32_t rowset_id = metadata.next_rowset_id();
    for (int i = 0; i < op_write.rowset().segments_size(); i++) {
        rssid_to_path[rowset_id + i] = op_write.rowset().segments(i);
    }
}

} // namespace lake
} // namespace starrocks
