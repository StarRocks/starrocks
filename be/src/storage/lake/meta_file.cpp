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

namespace starrocks {
namespace lake {

static std::string delvec_cache_key(int64_t tablet_id, const DelvecPagePB& page) {
    DelvecCacheKeyPB cache_key_pb;
    cache_key_pb.set_id(tablet_id);
    cache_key_pb.mutable_delvec_page()->CopyFrom(page);
    std::string cache_key;
    cache_key_pb.SerializeToString(&cache_key);
    return cache_key;
}

MetaFileBuilder::MetaFileBuilder(Tablet tablet, std::shared_ptr<TabletMetadata> metadata)
        : _tablet(tablet), _tablet_meta(std::move(metadata)), _update_mgr(_tablet.update_mgr()) {}

void MetaFileBuilder::append_delvec(DelVectorPtr delvec, uint32_t segment_id) {
    if (delvec->cardinality() > 0) {
        const uint64_t offset = _buf.size();
        std::string delvec_str;
        delvec->save_to(&delvec_str);
        _buf.insert(_buf.end(), delvec_str.begin(), delvec_str.end());
        const uint64_t size = _buf.size() - offset;
        DCHECK(_delvecs.find(segment_id) == _delvecs.end());
        _delvecs[segment_id].set_offset(offset);
        _delvecs[segment_id].set_size(size);
    }
}

void MetaFileBuilder::apply_opwrite(const TxnLogPB_OpWrite& op_write) {
    auto rowset = _tablet_meta->add_rowsets();
    rowset->CopyFrom(op_write.rowset());
    rowset->set_id(_tablet_meta->next_rowset_id());
    // if rowset don't contain segment files, still inc next_rowset_id
    _tablet_meta->set_next_rowset_id(_tablet_meta->next_rowset_id() + std::max(1, rowset->segments_size()));
}

void MetaFileBuilder::apply_opcompaction(const TxnLogPB_OpCompaction& op_compaction) {
    // delete input rowsets
    std::stringstream del_range_ss;
    std::vector<std::pair<uint32_t, uint32_t> > delete_delvec_sid_range;
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

    LOG(INFO) << fmt::format("MetaFileBuilder apply_opcompaction, id:{} input range:{} delvec del cnt:{} output:{}",
                             _tablet_meta->id(), del_range_ss.str(), delvec_erase_cnt,
                             op_compaction.output_rowset().ShortDebugString());
}

Status MetaFileBuilder::_finalize_delvec(int64_t version) {
    if (!is_primary_key(_tablet_meta.get())) return Status::OK();
    // 1. update delvec page in meta
    for (auto&& each_delvec : *(_tablet_meta->mutable_delvec_meta()->mutable_delvecs())) {
        auto iter = _delvecs.find(each_delvec.first);
        if (iter != _delvecs.end()) {
            each_delvec.second.set_version(version);
            each_delvec.second.set_offset(iter->second.offset());
            each_delvec.second.set_size(iter->second.size());
            _delvecs.erase(iter);
        }
    }

    // 2. insert new delvec to meta
    for (auto&& each_delvec : _delvecs) {
        each_delvec.second.set_version(version);
        (*_tablet_meta->mutable_delvec_meta()->mutable_delvecs())[each_delvec.first] = each_delvec.second;
    }

    // 3. write to delvec file
    if (_buf.size() > 0) {
        MonotonicStopWatch watch;
        watch.start();
        auto filepath = _tablet.delvec_location(version);
        auto options = WritableFileOptions{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto writer_file = fs::new_writable_file(options, filepath);
        if (!writer_file.ok()) {
            return writer_file.status();
        }
        RETURN_IF_ERROR((*writer_file)->append(Slice(_buf.data(), _buf.size())));
        RETURN_IF_ERROR((*writer_file)->close());
        if (watch.elapsed_time() > /*100ms=*/100 * 1000 * 1000) {
            LOG(INFO) << "MetaFileBuilder sync delvec cost(ms): " << watch.elapsed_time() / 1000000;
        }
    }
    return Status::OK();
}

Status MetaFileBuilder::finalize() {
    MonotonicStopWatch watch;
    watch.start();

    auto version = _tablet_meta->version();
    // finalize delvec
    RETURN_IF_ERROR(_finalize_delvec(version));
    RETURN_IF_ERROR(_tablet.put_metadata(_tablet_meta));
    if (watch.elapsed_time() > /*100ms=*/100 * 1000 * 1000) {
        LOG(INFO) << "MetaFileBuilder finalize cost(ms): " << watch.elapsed_time() / 1000000;
    }
    _update_mgr->update_primary_index_data_version(_tablet, version);
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

void MetaFileBuilder::handle_failure() {
    if (is_primary_key(_tablet_meta.get()) && !_has_finalized) {
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

    MonotonicStopWatch watch;
    watch.start();

    auto file_size_st = _access_file->get_size();
    if (!file_size_st.ok()) {
        return Status::IOError(fmt::format("meta file {} get size failed", _access_file->filename()));
    }
    const uint64_t file_size = *file_size_st;
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
    if (watch.elapsed_time() > /*100ms=*/100 * 1000 * 1000) {
        LOG(INFO) << "MetaFileReader load cost(ms): " << watch.elapsed_time() / 1000000;
    }
    return Status::OK();
}

Status MetaFileReader::get_del_vec(TabletManager* tablet_mgr, uint32_t segment_id, DelVector* delvec) {
    if (_access_file == nullptr) return _err_status;
    if (!_load) return Status::InternalError("meta file reader not loaded");
    const LocationProvider* location_provider = tablet_mgr->location_provider();
    // find delvec by segment id
    auto iter = _tablet_meta->delvec_meta().delvecs().find(segment_id);
    if (iter != _tablet_meta->delvec_meta().delvecs().end()) {
        // found it!
        MonotonicStopWatch watch;
        watch.start();
        VLOG(2) << fmt::format("MetaFileReader get_del_vec {} segid {}", _tablet_meta->delvec_meta().ShortDebugString(),
                               segment_id);
        std::string buf;
        raw::stl_string_resize_uninitialized(&buf, iter->second.size());
        // read from delvec file by each_delvec.version()
        const std::string filepath =
                location_provider->tablet_delvec_location(_tablet_meta->id(), iter->second.version());
        // find in cache
        std::string cache_key = delvec_cache_key(_tablet_meta->id(), iter->second);
        DelVectorPtr delvec_cache_ptr = tablet_mgr->lookup_delvec(cache_key);
        if (delvec_cache_ptr != nullptr) {
            delvec->copy_from(*delvec_cache_ptr);
            return Status::OK();
        }
        RandomAccessFileOptions opts{.skip_fill_local_cache = true};
        auto rf = fs::new_random_access_file(opts, filepath);
        if (!rf.ok()) {
            return rf.status();
        }
        RETURN_IF_ERROR((*rf)->read_at_fully(iter->second.offset(), buf.data(), iter->second.size()));
        // parse delvec
        RETURN_IF_ERROR(delvec->load(iter->second.version(), buf.data(), iter->second.size()));
        // put in cache
        delvec_cache_ptr = std::make_shared<DelVector>();
        delvec_cache_ptr->copy_from(*delvec);
        tablet_mgr->cache_delvec(cache_key, delvec_cache_ptr);
        if (watch.elapsed_time() > /*100ms=*/100 * 1000 * 1000) {
            LOG(INFO) << "MetaFileReader read delvec cost(ms): " << watch.elapsed_time() / 1000000;
        }
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
