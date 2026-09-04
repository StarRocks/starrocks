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

#include "storage/lake/lake_primary_index.h"

#include <utility>
#include <vector>

#include "base/debug/trace.h"
#include "base/testutil/sync_point.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"

namespace starrocks::lake {

LakePrimaryIndex::~LakePrimaryIndex() = default;

Status LakePrimaryIndex::lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                                   const MetaFileBuilder* builder) {
    TRACE_COUNTER_SCOPE_LATENCY_US("primary_index_load_latency_us");
    std::lock_guard<std::mutex> lg(_load_lock);
    if (_loaded) {
        return _status;
    }
    // _do_lake_load may need tablet id to fetch tablet schema/encoding type.
    // Set it before loading to avoid using the default value (0).
    _tablet_id = metadata->id();
    _status = _do_lake_load(tablet_mgr, metadata, base_version, builder);
    TEST_SYNC_POINT_CALLBACK("lake_index_load.1", &_status);
    if (_status.ok()) {
        // update data version when memory index or persistent index load finish.
        _data_version = base_version;
    }
    _loaded = true;
    TRACE("end load pk index");
    if (!_status.ok()) {
        LOG(WARNING) << "load LakePrimaryIndex error: " << _status << " tablet:" << _tablet_id;
    }
    return _status;
}

bool LakePrimaryIndex::is_load(int64_t base_version) {
    std::lock_guard<std::mutex> lg(_load_lock);
    return _loaded && _data_version >= base_version;
}

bool LakePrimaryIndex::is_loaded() const {
    std::lock_guard<std::mutex> lg(_load_lock);
    return _loaded;
}

Status LakePrimaryIndex::get_load_status() const {
    std::lock_guard<std::mutex> lg(_load_lock);
    return _status;
}

void LakePrimaryIndex::unload() {
    std::lock_guard<std::mutex> lg(_load_lock);
    _unload_without_lock();
}

void LakePrimaryIndex::_unload_without_lock() {
    if (!_loaded) {
        return;
    }
    LOG(INFO) << "unload lake primary index tablet:" << _tablet_id << " memory: " << memory_usage();
    _index.reset();
    _status = Status::OK();
    _loaded = false;
}

std::size_t LakePrimaryIndex::memory_usage() const {
    return _index != nullptr ? _index->memory_usage() : 0;
}

Status LakePrimaryIndex::_do_lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                       int64_t base_version, const MetaFileBuilder* builder) {
    // A shared-data primary-key tablet has exactly one index implementation. The metadata is
    // normalized to enabled + CLOUD_NATIVE at load time (force_cloud_native_pk_persistent_index),
    // so there is nothing to choose between.
    DCHECK(_index == nullptr);
    _index = std::make_shared<LakePersistentIndex>(tablet_mgr, metadata->id());
    RETURN_IF_ERROR(_index->init(metadata));
    return _index->load_from_lake_tablet(tablet_mgr, metadata, base_version, builder);
}

Status LakePrimaryIndex::apply_opcompaction(const TabletMetadataPtr& metadata,
                                            const TxnLogPB_OpCompaction& op_compaction) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->apply_opcompaction(metadata, op_compaction);
}

Status LakePrimaryIndex::ingest_sst(const FileMetaPB& sst_meta, const PersistentIndexSstableRangePB& sst_range,
                                    uint32_t rssid, int64_t version, const DelvecPagePB& delvec_page,
                                    DelVectorPtr delvec) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->ingest_sst(sst_meta, sst_range, rssid, version, delvec_page, std::move(delvec));
}

Status LakePrimaryIndex::commit(const TabletMetadataPtr& metadata, MetaFileBuilder* builder,
                                int64_t generation_version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("primary_index_commit_latency_us");
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->commit(builder, generation_version);
}

Status LakePrimaryIndex::sync_flush_persistent_index(int64_t wait_timeout_us) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->sync_flush_all_memtables(wait_timeout_us);
}

int32_t LakePrimaryIndex::current_fileset_index() const {
    auto* index = _index.get();
    return index != nullptr ? index->current_fileset_index() : -1;
}

StatusOr<AsyncCompactCBPtr> LakePrimaryIndex::early_sst_compact(
        lake::LakePersistentIndexParallelCompactMgr* compact_mgr, TabletManager* tablet_mgr,
        const TabletMetadataPtr& metadata, int32_t fileset_start_idx) {
    auto* index = _index.get();
    if (index == nullptr) {
        return nullptr;
    }
    return index->early_sst_compact(compact_mgr, tablet_mgr, metadata, fileset_start_idx);
}

Status LakePrimaryIndex::flush_memtable(bool force) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->flush_memtable(force);
}

void LakePrimaryIndex::reset_publish_sst_stats() {
    auto* index = _index.get();
    if (index != nullptr) index->reset_publish_sst_stats();
}

int32_t LakePrimaryIndex::publish_sst_flush_count() const {
    auto* index = _index.get();
    return index != nullptr ? index->publish_sst_flush_count() : 0;
}

int64_t LakePrimaryIndex::publish_sst_flush_bytes() const {
    auto* index = _index.get();
    return index != nullptr ? index->publish_sst_flush_bytes() : 0;
}

// ---- Forwards to the loaded index --------------------------------------------------------------
//
// The bodies live on LakePersistentIndex, next to the batch key/value API they marshal into and the
// encoded key size they need. What is left here is this class's one remaining job: turning "no index
// is loaded" into a status, which the index itself has no way to express.

Status LakePrimaryIndex::get(const Column& pks, std::vector<uint64_t>* rowids) const {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("get on an unloaded lake primary index");
    }
    return index->get(pks, rowids);
}

Status LakePrimaryIndex::upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, uint32_t idx_begin,
                                uint32_t idx_end, DeletesMap* deletes) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("upsert on an unloaded lake primary index");
    }
    return index->upsert(rssid, rowid_start, pks, idx_begin, idx_end, deletes);
}

Status LakePrimaryIndex::upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, ParallelPublishSlot* slot,
                                ParallelUpsertContext* ctx) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("upsert on an unloaded lake primary index");
    }
    return index->upsert(rssid, rowid_start, pks, slot, ctx);
}

Status LakePrimaryIndex::upsert(uint32_t rssid, const std::vector<uint32_t>& rowids, const Column& pks,
                                ParallelPublishSlot* slot, ParallelUpsertContext* ctx) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("upsert on an unloaded lake primary index");
    }
    return index->upsert(rssid, rowids, pks, slot, ctx);
}

Status LakePrimaryIndex::erase(const TabletMetadataPtr& metadata, const Column& pks, DeletesMap* deletes,
                               uint32_t del_rssid) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("erase on an unloaded lake primary index");
    }
    return index->erase(pks, deletes, del_rssid);
}

Status LakePrimaryIndex::bulk_erase(const TabletMetadataPtr& metadata, const Column& pks, DeletesMap* deletes,
                                    uint32_t del_rssid, const FileMetaPB& del_sst_meta,
                                    const PersistentIndexSstableRangePB& del_sst_range, int64_t version) {
    // Unlike erase(), there is no in-memory fallback: a pre-built tombstone sstable can only be
    // ingested by the cloud-native index, so an unloaded index is a broken invariant, not a fallback.
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("bulk_erase requires a cloud-native LakePersistentIndex.");
    }
    return index->bulk_erase(pks, deletes, del_rssid, del_sst_meta, del_sst_range, version);
}

Status LakePrimaryIndex::replace(uint32_t rssid, uint32_t rowid_start, const std::vector<uint32_t>& replace_indexes,
                                 const Column& pks) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("replace on an unloaded lake primary index");
    }
    return index->replace(rssid, rowid_start, replace_indexes, pks);
}

Status LakePrimaryIndex::try_replace(uint32_t rssid, uint32_t rowid_start, const Column& pks, uint32_t max_src_rssid,
                                     std::vector<uint32_t>* failed) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("try_replace on an unloaded lake primary index");
    }
    return index->try_replace(rssid, rowid_start, pks, max_src_rssid, failed);
}

Status LakePrimaryIndex::parallel_get(ThreadPoolToken* token, SegmentPKIterator* segment_pk_iterator,
                                      DeletesMap* new_deletes) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("parallel_get on an unloaded lake primary index");
    }
    return index->parallel_get(token, segment_pk_iterator, new_deletes);
}

Status LakePrimaryIndex::batch_parallel_get_rss_rowids(ThreadPoolToken* token,
                                                       std::vector<std::unique_ptr<SegmentPKIterator>>& pk_iters,
                                                       std::vector<std::vector<uint64_t>>* rss_rowids_per_segment,
                                                       std::vector<Filter>* owned_per_segment) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("batch_parallel_get_rss_rowids on an unloaded lake primary index");
    }
    return index->batch_parallel_get_rss_rowids(token, pk_iters, rss_rowids_per_segment, owned_per_segment);
}

Status LakePrimaryIndex::parallel_upsert(ThreadPoolToken* token, uint32_t rssid, SegmentPKIterator* segment_pk_iterator,
                                         DeletesMap* new_deletes) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("parallel_upsert on an unloaded lake primary index");
    }
    return index->parallel_upsert(token, rssid, segment_pk_iterator, new_deletes);
}

std::string LakePrimaryIndex::to_string() const {
    return strings::Substitute("LakePrimaryIndex tablet:$0", _tablet_id);
}

Status LakePrimaryIndex::prepare(int64_t version) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("prepare on an unloaded lake primary index");
    }
    index->set_publish_version(version);
    return Status::OK();
}

} // namespace starrocks::lake
