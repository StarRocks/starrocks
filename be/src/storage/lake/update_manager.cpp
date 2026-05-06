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

#include "base/container/lru_cache.h"
#include "base/debug/trace.h"
#include "base/failpoint/fail_point.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "base/utility/pretty_printer.h"
#include "common/config_compaction_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/delta_column_group.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/lake_local_persistent_index.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/lake_primary_key_compaction_conflict_resolver.h"
#include "storage/lake/local_pk_index_manager.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/update_compaction_state.h"
#include "storage/persistent_index_parallel_publish_context.h"
#include "storage/primary_key_encoder.h"
#include "storage/rows_mapper.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/storage_metrics.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_updates.h"
#include "storage/utils.h"

namespace starrocks::lake {

static bool use_cloud_native_pk_index(const TabletMetadata& metadata) {
    return metadata.enable_persistent_index() &&
           metadata.persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE;
}

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

UpdateManager::PkIndexShard& UpdateManager::_get_pk_index_shard(int64_t tabletId) {
    return _pk_index_shards[tabletId & (config::pk_index_map_shard_size - 1)];
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
        bool has_bundle_file_offset = (rs.segments_size() == rs.bundle_file_offsets_size());
        for (int i = 0; i < rs.segments_size(); i++) {
            FileInfo segment_info{.path = rs.segments(i)};
            if (has_bundle_file_offset) {
                segment_info.bundle_file_offset = rs.bundle_file_offsets(i);
            }
            if (LIKELY(has_segment_size)) {
                segment_info.size = rs.segment_size(i);
            }
            if (LIKELY(has_encryption_meta)) {
                segment_info.encryption_meta = rs.segment_encryption_metas(i);
            }
            uint32_t rssid = get_rssid(rs, i);
            _rssid_to_file_info[rssid] = segment_info;
            _rssid_to_rowid[rssid] = rs.id();
        }
    }
}

void RssidFileInfoContainer::add_rssid_to_file(const RowsetMetadataPB& meta, uint32_t rowset_id, uint32_t segment_idx,
                                               const std::map<int, FileInfo>& replace_segments) {
    DCHECK(segment_idx < meta.segments_size());
    uint32_t local_segment_id = get_segment_idx(meta, static_cast<int32_t>(segment_idx));
    if (replace_segments.count(segment_idx) > 0) {
        // partial update
        _rssid_to_file_info[rowset_id + local_segment_id] = replace_segments.at(segment_idx);
        _rssid_to_rowid[rowset_id + local_segment_id] = rowset_id;
    } else {
        bool has_segment_size = (meta.segments_size() == meta.segment_size_size());
        bool has_encryption_meta = (meta.segments_size() == meta.segment_encryption_metas_size());
        bool has_bundle_file_offset = (meta.segments_size() == meta.bundle_file_offsets_size());
        FileInfo segment_info{.path = meta.segments(segment_idx)};
        if (has_bundle_file_offset) {
            segment_info.bundle_file_offset = meta.bundle_file_offsets(segment_idx);
        }
        if (LIKELY(has_segment_size)) {
            segment_info.size = meta.segment_size(segment_idx);
        }
        if (LIKELY(has_encryption_meta)) {
            segment_info.encryption_meta = meta.segment_encryption_metas(segment_idx);
        }
        _rssid_to_file_info[rowset_id + local_segment_id] = segment_info;
        _rssid_to_rowid[rowset_id + local_segment_id] = rowset_id;
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
            StorageMetrics::instance()->primary_key_table_error_state_total.increment(1);
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

StatusOr<TabletMetadataPtr> UpdateManager::flush_pk_memtable(const TabletMetadataPtr& metadata) {
    if (!is_primary_key(*metadata) || !metadata->enable_persistent_index() ||
        metadata->persistent_index_type() != PersistentIndexTypePB::CLOUD_NATIVE) {
        return metadata;
    }
    const int64_t tablet_id = metadata->id();
    const int64_t base_version = metadata->version();

    lock_shard_pk_index_shard(tablet_id);
    DeferOp unlock_shard([&] { unlock_shard_pk_index_shard(tablet_id); });

    // Single mutable deep-copy of metadata. The MetaFileBuilder requires a
    // MutableTabletMetadataPtr, and LakePersistentIndex::commit writes the
    // flushed sstable_meta straight into it — so this same pointer is
    // returned to the caller.
    auto mutable_metadata = std::make_shared<TabletMetadataPB>(*metadata);
    Tablet tablet(_tablet_mgr, tablet_id);
    MetaFileBuilder builder(tablet, mutable_metadata);

    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> write_guard;
    ASSIGN_OR_RETURN(auto* index_entry, prepare_primary_index(metadata, &builder, base_version,
                                                              /*new_version=*/base_version, write_guard));

    // Default = failure cleanup: match PrimaryKeyTxnLogApplier::handle_failure
    // ordering exactly — unload the index first (while the write_guard is
    // still held), then release the guard, then evict from cache so the next
    // attempt rebuilds fresh. Call cancel() after success.
    CancelableDefer failure_cleanup([&] {
        index_entry->value().unload();
        write_guard.reset(nullptr);
        remove_primary_index_cache(index_entry);
    });

    // DML's LakePersistentIndex::commit only flushes memtable when rebuild
    // counts are heavy; reshard needs an unconditional flush so the spliced
    // sstable_meta covers all live data.
    RETURN_IF_ERROR(index_entry->value().sync_flush_persistent_index(
            config::pk_index_memtable_max_wait_flush_timeout_ms * 1000));

    // Reuse the DML publish path: commit → dump_sstable_meta →
    // MetaFileBuilder::finalize_sstable_meta writes the result into
    // mutable_metadata->sstable_meta(). builder.finalize() is NOT called
    // here — this flush does not produce its own metadata version; the
    // returned metadata is consumed by the surrounding reshard publish.
    RETURN_IF_ERROR(index_entry->value().commit(metadata, &builder));

    // Success: dismiss the failure cleanup and release the cache entry.
    // write_guard is released when it goes out of scope at function return.
    failure_cleanup.cancel();
    release_primary_index_cache(index_entry);
    return mutable_metadata;
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
                                                 int64_t base_version, bool batch_apply) {
    FAIL_POINT_TRIGGER_EXECUTE(hook_publish_primary_key_tablet, {
        if (batch_apply) {
            builder->batch_apply_opwrite(op_write, {}, {});
        } else {
            builder->apply_opwrite(op_write, {}, {});
        }
        return Status::OK();
    });
    auto& index = index_entry->value();
    VLOG(2) << strings::Substitute(
            "[publish_pk_tablet][begin] tablet:$0 txn:$1 base_version:$2 new_version:$3 segments:$4 dels:$5 batch:$6",
            tablet->id(), txn_id, base_version, metadata->version(), op_write.rowset().segments_size(),
            op_write.dels_size(), batch_apply);
    // 1. load rowset update data to cache, get upsert and delete list
    const uint32_t rowset_id = metadata->next_rowset_id();
    auto tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
    auto state_entry = _update_state_cache.get_or_create(cache_key(tablet->id(), txn_id));
    state_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    // only use state entry once, remove it when publish finish or fail
    DeferOp remove_state_entry([&] { _update_state_cache.remove(state_entry); });
    auto& state = state_entry->value();
    // Snapshot IO stats before publish to exclude preload IO from trace counters.
    const int64_t io_local_disk_ns_before = state.stats().io_ns_read_local_disk;
    const int64_t io_remote_ns_before = state.stats().io_ns_remote;
    const int64_t io_count_local_disk_before = state.stats().io_count_local_disk;
    const int64_t io_count_remote_before = state.stats().io_count_remote;

    std::vector<FileMetaPB> orphan_files;
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
    RETURN_IF_ERROR(state.prepare(params));
    // Init delvec state.
    // Map from rssid (rowset id + segment offset) to the list of deleted rowids collected during this publish.
    PrimaryIndex::DeletesMap new_deletes;
    // Global segment id offset assigned by builder when batch applying multiple op_write in a single publish.
    uint32_t assigned_global_segments = batch_apply ? builder->assigned_segment_idx() : 0;
    // Number of segments in the incoming rowset of this op_write.
    uint32_t local_segments = op_write.rowset().segments_size();
    std::vector<uint32_t> rowset_segment_ids;
    rowset_segment_ids.reserve(local_segments);
    std::unordered_set<uint32_t> new_rowset_rssids;
    new_rowset_rssids.reserve(local_segments);
    for (uint32_t local_id = 0; local_id < local_segments; ++local_id) {
        uint32_t global_segment_id =
                assigned_global_segments + get_segment_idx(op_write.rowset(), static_cast<int32_t>(local_id));
        rowset_segment_ids.push_back(global_segment_id);
        uint32_t rssid = rowset_id + global_segment_id;
        new_rowset_rssids.insert(rssid);
        new_deletes[rssid] = {};
    }
    // The rssid for delete files equals `rowset_id + op_offset`. Since delete currently happens after upsert,
    // we use the max segment id as the `op_offset` for rebuild. This is a simplification until mixed
    // upsert+delete order in a single transaction is supported.
    // TODO: Support the actual interleaving order of upsert and delete within one transaction.
    uint32_t max_segment_id = 0;
    if (!rowset_segment_ids.empty()) {
        max_segment_id = *std::max_element(rowset_segment_ids.begin(), rowset_segment_ids.end());
    }
    const uint32_t del_rebuild_rssid = rowset_id + max_segment_id;
    // When too many sst files, we need to compact them early.
    int32_t current_fileset_start_idx = index.current_fileset_index();
    AsyncCompactCBPtr async_compact_cb;
    // Check if parallel row-mode partial update is applicable.
    int32_t condition_column = _get_condition_column(op_write, *tablet_schema);
    const bool has_condition_update = (condition_column >= 0);
    // Skip early sst compact when condition update with pregenerate sst files.
    bool skip_early_sst_compact = false;
    const bool is_row_mode_partial_update =
            op_write.has_txn_meta() && op_write.rewrite_segments_size() > 0 && op_write.rowset().num_rows() > 0;
    const bool use_parallel_partial_update = config::enable_pk_index_parallel_execution && is_row_mode_partial_update &&
                                             local_segments > 1 && use_cloud_native_pk_index(*metadata);

    // 2. Process segments in batches. In parallel mode, each batch runs Phase 1 (parallel
    // load+rewrite) then Phase 2 (sequential _do_update), releasing upserts per batch to
    // prevent memory accumulation. In serial mode, batch_size=1 degenerates to original logic.
    const uint32_t batch_size =
            use_parallel_partial_update ? ExecEnv::GetInstance()->lake_partial_update_thread_pool()->max_threads() : 1;

    for (uint32_t batch_start = 0; batch_start < local_segments; batch_start += batch_size) {
        uint32_t batch_end = std::min(batch_start + batch_size, local_segments);

        // Phase 1: parallel load + rewrite for this batch.
        if (use_parallel_partial_update && batch_end - batch_start > 1) {
            TRACE_COUNTER_SCOPE_LATENCY_US("parallel_load_rewrite_seg_latency_us");
            uint32_t batch_count = batch_end - batch_start;
            std::vector<std::map<int, FileInfo>> per_seg_replace(batch_count);
            std::vector<std::vector<FileMetaPB>> per_seg_orphans(batch_count);

            std::mutex status_mutex;
            Status shared_status;
            auto token = ExecEnv::GetInstance()->lake_partial_update_thread_pool()->new_token(
                    ThreadPool::ExecutionMode::CONCURRENT);

            for (uint32_t i = batch_start; i < batch_end; i++) {
                auto func = [&, i]() {
                    uint32_t idx = i - batch_start;
                    auto st = state.load_segment(i, params, base_version, true /*resolve conflict*/,
                                                 false /*no need lock*/);
                    _update_state_cache.update_object_size(state_entry, state.memory_usage());
                    if (st.ok()) {
                        st = state.rewrite_segment(i, txn_id, params, &per_seg_replace[idx], &per_seg_orphans[idx]);
                    }
                    state.release_segment_partial_state(i);
                    _update_state_cache.update_object_size(state_entry, state.memory_usage());

                    std::lock_guard<std::mutex> l(status_mutex);
                    shared_status.update(st);
                };
                auto submit_st = token->submit_func(func);
                if (!submit_st.ok()) {
                    std::lock_guard<std::mutex> l(status_mutex);
                    shared_status.update(submit_st);
                }
            }
            token->wait();
            RETURN_IF_ERROR(shared_status);

            for (uint32_t i = batch_start; i < batch_end; i++) {
                uint32_t idx = i - batch_start;
                replace_segments.merge(per_seg_replace[idx]);
                orphan_files.insert(orphan_files.end(), std::make_move_iterator(per_seg_orphans[idx].begin()),
                                    std::make_move_iterator(per_seg_orphans[idx].end()));
                rssid_fileinfo_container.add_rssid_to_file(op_write.rowset(), metadata->next_rowset_id(), i,
                                                           replace_segments);
            }
            _update_state_cache.update_object_size(state_entry, state.memory_usage());
        }

        // Phase 2: sequential _do_update for this batch (shared by parallel and serial paths).
        for (uint32_t local_id = batch_start; local_id < batch_end; ++local_id) {
            std::shared_ptr<DelVector> dv_generated_during_merge_update;
            uint32_t global_segment_id = rowset_segment_ids[local_id];

            if (!use_parallel_partial_update || batch_end - batch_start <= 1) {
                // Serial path: load + rewrite inline.
                RETURN_IF_ERROR(state.load_segment(local_id, params, base_version, true /*resolve conflict*/,
                                                   false /*no need lock*/));
                _update_state_cache.update_object_size(state_entry, state.memory_usage());
                RETURN_IF_ERROR(state.rewrite_segment(local_id, txn_id, params, &replace_segments, &orphan_files));
                rssid_fileinfo_container.add_rssid_to_file(op_write.rowset(), metadata->next_rowset_id(), local_id,
                                                           replace_segments);
            }

            // PK index update + condition merge.
            TRACE_COUNTER_SCOPE_LATENCY_US("update_index_latency_us");
            DCHECK(state.upserts(local_id) != nullptr);
            if (!has_condition_update) {
                RETURN_IF_ERROR(_do_update(rowset_id, global_segment_id, state.upserts(local_id), index, &new_deletes,
                                           op_write.ssts_size() > 0, use_cloud_native_pk_index(*metadata)));
            } else if (op_write.ssts_size() > 0) {
                RETURN_IF_ERROR(_do_update_with_condition_parallel(params, rowset_id, global_segment_id,
                                                                   condition_column, state.upserts(local_id), index,
                                                                   &new_deletes));
                auto itr = new_deletes.find(rowset_id + global_segment_id);
                if (itr != new_deletes.end() && itr->second.size() > 0) {
                    dv_generated_during_merge_update = std::make_shared<DelVector>();
                    dv_generated_during_merge_update->init(metadata->version(), itr->second.data(), itr->second.size());
                    builder->append_delvec(dv_generated_during_merge_update, rowset_id + global_segment_id);
                }
                skip_early_sst_compact = true;
            } else {
                // NON-SST CONDITION MERGE PATH:
                // When SST files are absent, new-row condition values are read from the freshly
                // ingested segment file on demand. Compare work is parallelized per chunk; the
                // final index.upsert is applied serially after the barrier.
                RETURN_IF_ERROR(_do_update_with_condition(params, rowset_id, global_segment_id, condition_column,
                                                          state.upserts(local_id), index, &new_deletes));
            }
            if (state.auto_increment_deletes(local_id) != nullptr) {
                RETURN_IF_ERROR(index.erase(metadata, *state.auto_increment_deletes(local_id), &new_deletes,
                                            del_rebuild_rssid));
            }
            _index_cache.update_object_size(index_entry, index.memory_usage());
            state.release_segment(local_id);
            _update_state_cache.update_object_size(state_entry, state.memory_usage());
            if (op_write.ssts_size() > 0 && use_cloud_native_pk_index(*metadata)) {
                DelvecPagePB delvec_page_pb = builder->delvec_page(rowset_id + global_segment_id);
                delvec_page_pb.set_version(metadata->version());
                RETURN_IF_ERROR(index.ingest_sst(op_write.ssts(local_id), op_write.sst_ranges(local_id),
                                                 rowset_id + global_segment_id, metadata->version(), delvec_page_pb,
                                                 dv_generated_during_merge_update));
            }
            if (async_compact_cb) {
                TRACE_COUNTER_SCOPE_LATENCY_US("early_sst_compact_wait_us");
                bool succ = true;
                ASSIGN_OR_RETURN(succ, async_compact_cb->wait_for(1000 /* ms timeout */));
                if (succ) {
                    LOG(INFO) << fmt::format("early sst compact finish. tablet {}, txn {}, fileset remain {}, trace {}",
                                             tablet->id(), txn_id,
                                             index.current_fileset_index() - current_fileset_start_idx,
                                             async_compact_cb->trace()->MetricsAsJSON());
                    async_compact_cb = nullptr;
                }
            }
            if (async_compact_cb == nullptr && !skip_early_sst_compact &&
                index.current_fileset_index() - current_fileset_start_idx >=
                        config::pk_index_early_sst_compaction_threshold) {
                TRACE_COUNTER_INCREMENT("early_sst_compact_times", 1);
                ASSIGN_OR_RETURN(async_compact_cb,
                                 index.early_sst_compact(ExecEnv::GetInstance()->parallel_compact_mgr(), _tablet_mgr,
                                                         metadata, current_fileset_start_idx + 1 /* new fileset*/));
            }
        } // end Phase 2 per-segment loop
    }     // end batch loop
    if (async_compact_cb) {
        TRACE_COUNTER_SCOPE_LATENCY_US("early_sst_compact_wait_us");
        RETURN_IF_ERROR(async_compact_cb->wait_for());
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
        if (new_rowset_rssids.contains(rssid)) {
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
                StorageMetrics::instance()->primary_key_table_error_state_total.increment(1);
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

    if (batch_apply) {
        VLOG(1) << strings::Substitute(
                "[publish_pk_tablet][apply_opwrite_batch] tablet:$0 txn:$1 replace_segments:$2 orphan_files:$3",
                tablet->id(), txn_id, replace_segments.size(), orphan_files.size());
        builder->batch_apply_opwrite(op_write, replace_segments, orphan_files);
    } else {
        VLOG(1) << strings::Substitute(
                "[publish_pk_tablet][apply_opwrite_single] tablet:$0 txn:$1 replace_segments:$2 orphan_files:$3",
                tablet->id(), txn_id, replace_segments.size(), orphan_files.size());
        builder->apply_opwrite(op_write, replace_segments, orphan_files);
    }

    RETURN_IF_ERROR(builder->update_num_del_stat(segment_id_to_add_dels));

    TRACE_COUNTER_INCREMENT("rowsetid", rowset_id);
    TRACE_COUNTER_INCREMENT("upserts", op_write.rowset().segments_size());
    TRACE_COUNTER_INCREMENT("deletes", op_write.dels_size());
    TRACE_COUNTER_INCREMENT("new_del", new_del);
    TRACE_COUNTER_INCREMENT("total_del", total_del);
    TRACE_COUNTER_INCREMENT("upsert_rows", op_write.rowset().num_rows());
    TRACE_COUNTER_INCREMENT("base_version", base_version);
    TRACE_COUNTER_INCREMENT("segment_io_local_disk_us",
                            (state.stats().io_ns_read_local_disk - io_local_disk_ns_before) / 1000);
    TRACE_COUNTER_INCREMENT("segment_io_remote_us", (state.stats().io_ns_remote - io_remote_ns_before) / 1000);
    TRACE_COUNTER_INCREMENT("segment_io_count_local_disk",
                            state.stats().io_count_local_disk - io_count_local_disk_before);
    TRACE_COUNTER_INCREMENT("segment_io_count_remote", state.stats().io_count_remote - io_count_remote_before);
    VLOG(1) << strings::Substitute(
            "[publish_pk_tablet][end] tablet:$0 txn:$1 rowset_id:$2 upsert_segments:$3 dels:$4 new_del:$5 total_del:$6 "
            "upsert_rows:$7 base_version:$8 new_version:$9",
            tablet->id(), txn_id, rowset_id, op_write.rowset().segments_size(), op_write.dels_size(), new_del,
            total_del, op_write.rowset().num_rows(), base_version, metadata->version());
    _print_memory_stats();
    return Status::OK();
}

Status UpdateManager::_read_chunk_for_upsert(const TxnLogPB_OpWrite& op_write, const TabletSchemaCSPtr& tschema,
                                             Tablet* tablet, const std::shared_ptr<FileSystem>& fs, uint32_t seg,
                                             const std::vector<uint32_t>& insert_rowids,
                                             const std::vector<uint32_t>& update_cids, ChunkPtr* out_chunk) {
    auto full_schema = ChunkHelper::convert_schema(tschema);
    auto full_chunk = ChunkHelper::new_chunk(full_schema, insert_rowids.size());

    {
        FileInfo info;
        info.path = op_write.rowset().segments(seg);
        if (seg < op_write.rowset().bundle_file_offsets_size()) {
            info.bundle_file_offset = op_write.rowset().bundle_file_offsets(seg);
            info.size = op_write.rowset().segment_size(seg);
        }
        if (seg < op_write.rowset().segment_encryption_metas_size()) {
            info.encryption_meta = op_write.rowset().segment_encryption_metas(seg);
        }

        FileInfo file_info{.path = tablet->segment_location(info.path), .encryption_meta = info.encryption_meta};
        if (info.size.has_value()) file_info.size = info.size;
        if (info.bundle_file_offset.has_value()) file_info.bundle_file_offset = info.bundle_file_offset;

        ASSIGN_OR_RETURN(auto segment, Segment::open(fs, file_info, seg, tschema));
        RandomAccessFileOptions opts;
        if (!file_info.encryption_meta.empty()) {
            ASSIGN_OR_RETURN(auto unwrap, KeyCache::instance().unwrap_encryption_meta(file_info.encryption_meta));
            opts.encryption_info = std::move(unwrap);
        }
        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        ASSIGN_OR_RETURN(auto raf, fs->new_random_access_file_with_bundling(opts, file_info));
        iter_opts.read_file = raf.get();

        for (uint32_t cid : update_cids) {
            const TabletColumn& col = tschema->column(cid);
            ASSIGN_OR_RETURN(auto col_iter, segment->new_column_iterator_or_default(col, nullptr));
            RETURN_IF_ERROR(col_iter->init(iter_opts));
            auto mut_col = full_chunk->get_column_raw_ptr_by_id(cid);
            RETURN_IF_ERROR(col_iter->fetch_values_by_rowid(insert_rowids.data(), insert_rowids.size(), mut_col));
        }
    }

    // Fill in default values for columns not included in update_cids
    std::set<uint32_t> upd_set(update_cids.begin(), update_cids.end());
    for (uint32_t cid = 0; cid < tschema->num_columns(); ++cid) {
        if (upd_set.count(cid) > 0) continue;
        const TabletColumn& tablet_column = tschema->column(cid);
        bool has_default_value = tablet_column.has_default_value();
        std::string default_value = has_default_value ? tablet_column.default_value() : "";

        auto it = op_write.txn_meta().column_to_expr_value().find(tablet_column.name());
        if (it != op_write.txn_meta().column_to_expr_value().end()) {
            has_default_value = true;
            default_value = it->second;
        }
        if (has_default_value) {
            const TypeInfoPtr& type_info = get_type_info(tablet_column);
            std::unique_ptr<DefaultValueColumnIterator> default_value_iter =
                    std::make_unique<DefaultValueColumnIterator>(true, default_value, tablet_column.is_nullable(),
                                                                 type_info, tablet_column.length(),
                                                                 (int)insert_rowids.size());
            ColumnIteratorOptions iter_opts;
            RETURN_IF_ERROR(default_value_iter->init(iter_opts));
            auto mut_col = full_chunk->get_column_raw_ptr_by_id(cid);
            RETURN_IF_ERROR(default_value_iter->fetch_values_by_rowid(nullptr, insert_rowids.size(), mut_col));
        } else {
            auto mut_col = full_chunk->get_column_raw_ptr_by_id(cid);
            mut_col->append_default(insert_rowids.size());
        }
    }

    {
        auto char_indexes = ChunkHelper::get_char_field_indexes(full_schema);
        ChunkHelper::padding_char_columns(char_indexes, full_schema, tschema, full_chunk.get());
    }

    *out_chunk = std::move(full_chunk);
    return Status::OK();
}

Status UpdateManager::_handle_column_upsert_mode(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                                                 const TabletMetadataPtr& metadata, Tablet* tablet,
                                                 LakePrimaryIndex& index, MetaFileBuilder* builder,
                                                 int64_t base_version, uint32_t rowset_id,
                                                 const std::vector<std::vector<uint32_t>>& insert_rowids_by_segment,
                                                 uint32_t* new_del_rebuild_rssid) {
    if (op_write.txn_meta().partial_update_mode() != PartialUpdateMode::COLUMN_UPSERT_MODE) {
        return Status::OK();
    }

    auto tschema = std::make_shared<TabletSchema>(metadata->schema());
    std::vector<uint32_t> pk_cids;
    pk_cids.reserve(tschema->num_key_columns());
    for (size_t i = 0; i < tschema->num_key_columns(); i++) pk_cids.push_back((uint32_t)i);
    Schema pkey_schema = ChunkHelper::convert_schema(tschema, pk_cids);

    std::vector<uint32_t> update_cids(op_write.txn_meta().partial_update_column_ids().begin(),
                                      op_write.txn_meta().partial_update_column_ids().end());

    int32_t ai_cid = -1;
    for (int i = 0; i < tschema->num_columns(); ++i) {
        if (tschema->column(i).is_auto_increment()) {
            ai_cid = i;
            break;
        }
    }
    if (ai_cid >= 0 && std::find(update_cids.begin(), update_cids.end(), (uint32_t)ai_cid) == update_cids.end()) {
        update_cids.push_back((uint32_t)ai_cid);
    }

    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(tablet->metadata_root_location()));

    TxnLogPB_OpWrite new_rows_op;
    uint64_t total_rows = 0;
    uint64_t total_data_size = 0;
    std::map<uint32_t, size_t> segment_id_to_add_dels_new_acc;

    DCHECK_EQ(insert_rowids_by_segment.size(), op_write.rowset().segments_size());

    ASSIGN_OR_RETURN(auto pk_encoding_type, tschema->primary_key_encoding_type_or_error());
    for (uint32_t seg = 0; seg < op_write.rowset().segments_size(); ++seg) {
        // Reuse insert_rowids computed by ColumnModePartialUpdateHandler
        const auto& insert_rowids = insert_rowids_by_segment[seg];
        if (insert_rowids.empty()) {
            continue;
        }
        const size_t batch_size =
                static_cast<size_t>(std::max<int32_t>(1, config::column_mode_partial_update_insert_batch_size));

        SegmentWriterOptions wopts;
        WritableFileOptions fopts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        if (config::enable_transparent_data_encryption) {
            ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
            fopts.encryption_info = pair.info;
            wopts.encryption_meta = std::move(pair.encryption_meta);
        }

        std::string seg_name = gen_segment_filename(txn_id);
        std::string seg_path = tablet->segment_location(seg_name);
        ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(fopts, seg_path));
        SegmentWriter writer(std::move(wfile), /*segment_id*/ 0, tschema, wopts);
        RETURN_IF_ERROR(writer.init());

        // RAII: Clean up incomplete segment file on error
        bool segment_finalized = false;
        DeferOp segment_cleanup([&]() {
            if (!segment_finalized) {
                // If segment was not finalized successfully, delete the incomplete file
                auto st = fs->delete_file(seg_path);
                if (!st.ok()) {
                    LOG(WARNING) << "Failed to delete incomplete segment file: " << seg_path
                                 << ", error: " << st.message();
                }
            }
        });

        MutableColumnPtr pk_column_for_upsert;
        RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column_for_upsert, pk_encoding_type));

        for (size_t batch_start = 0; batch_start < insert_rowids.size(); batch_start += batch_size) {
            size_t batch_end = std::min(batch_start + batch_size, insert_rowids.size());
            std::vector<uint32_t> batch_insert_rowids(insert_rowids.begin() + batch_start,
                                                      insert_rowids.begin() + batch_end);
            ChunkPtr full_chunk;
            RETURN_IF_ERROR(_read_chunk_for_upsert(op_write, tschema, tablet, fs, seg, batch_insert_rowids, update_cids,
                                                   &full_chunk));

            RETURN_IF_ERROR(writer.append_chunk(*full_chunk));
            total_rows += full_chunk->num_rows();

            PrimaryKeyEncoder::encode(pkey_schema, *full_chunk, 0, full_chunk->num_rows(), pk_column_for_upsert.get(),
                                      pk_encoding_type);
        }

        uint64_t seg_file_size = 0, idx_size = 0, footer_pos = 0;
        RETURN_IF_ERROR(writer.finalize(&seg_file_size, &idx_size, &footer_pos));
        segment_finalized = true; // Mark as successfully finalized

        new_rows_op.mutable_rowset()->add_segments(seg_name);
        new_rows_op.mutable_rowset()->add_segment_size(seg_file_size);
        total_data_size += seg_file_size;
        if (config::enable_transparent_data_encryption) {
            new_rows_op.mutable_rowset()->add_segment_encryption_metas(writer.encryption_meta());
        }
        uint32_t segment_idx = new_rows_op.rowset().segments_size() - 1;
        auto* segment_meta = new_rows_op.mutable_rowset()->add_segment_metas();
        writer.write_sort_key_fields_to(segment_meta);
        segment_meta->set_num_rows(writer.num_rows());
        segment_meta->set_segment_idx(segment_idx);

        uint32_t new_segment_id = get_segment_idx(new_rows_op.rowset(), static_cast<int32_t>(segment_idx));
        PrimaryIndex::DeletesMap segment_deletes;
        RETURN_IF_ERROR(index.upsert(rowset_id + new_segment_id, 0, *pk_column_for_upsert, 0,
                                     pk_column_for_upsert->size(), &segment_deletes));

        for (auto& [rssid, del_ids] : segment_deletes) {
            DCHECK(del_ids.empty()) << "del_ids should be empty for new row segments, but got " << del_ids.size()
                                    << " deletes for rssid=" << rssid;
            if (del_ids.empty()) continue;
            DelVectorPtr dv = std::make_shared<DelVector>();
            dv->init(metadata->version(), del_ids.data(), del_ids.size());
            builder->append_delvec(dv, rssid);
            segment_id_to_add_dels_new_acc[rssid] += del_ids.size();
        }
    }

    new_rows_op.mutable_rowset()->set_num_rows(total_rows);
    new_rows_op.mutable_rowset()->set_data_size(total_data_size);
    new_rows_op.mutable_rowset()->set_overlapped(new_rows_op.rowset().segments_size() > 1);
    // append del files from old op to new op
    for (int i = 0; i < op_write.dels_size(); i++) {
        new_rows_op.add_dels(op_write.dels(i));
    }
    for (int i = 0; i < op_write.del_encryption_metas_size(); i++) {
        new_rows_op.add_del_encryption_metas(op_write.del_encryption_metas(i));
    }
    // Carry over the per-del shared flag populated by tablet-split cross-publish.
    // Without this, apply_opwrite(new_rows_op) would drop the flag and the del file
    // would be written into rowset.del_files with shared=false, exposing it to
    // premature deletion by vacuum on sibling split tablets.
    for (int i = 0; i < op_write.shared_dels_size(); i++) {
        new_rows_op.add_shared_dels(op_write.shared_dels(i));
    }
    if (new_rows_op.rowset().segments_size() > 0 || new_rows_op.dels_size() > 0) {
        builder->apply_opwrite(new_rows_op, {}, {});
        if (!segment_id_to_add_dels_new_acc.empty()) {
            (void)builder->update_num_del_stat(segment_id_to_add_dels_new_acc);
            segment_id_to_add_dels_new_acc.clear();
        }
    }
    // set new del_rebuild_rssid via new op
    uint32_t max_segment_id = 0;
    for (int i = 0; i < new_rows_op.rowset().segments_size(); i++) {
        max_segment_id = std::max(max_segment_id, get_segment_idx(new_rows_op.rowset(), i));
    }
    *new_del_rebuild_rssid = rowset_id + max_segment_id;

    return Status::OK();
}

Status UpdateManager::_handle_delete_files(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                                           const TabletMetadataPtr& metadata, Tablet* tablet, LakePrimaryIndex& index,
                                           IndexEntry* index_entry, MetaFileBuilder* builder, int64_t base_version,
                                           uint32_t del_rebuild_rssid, const RowsetUpdateStateParams& params) {
    if (op_write.dels_size() == 0) {
        return Status::OK();
    }

    PrimaryIndex::DeletesMap new_deletes;
    RowsetUpdateState state;
    auto state_entry = _update_state_cache.get_or_create(cache_key(tablet->id(), txn_id));
    state_entry->update_expire_time(MonotonicMillis() + get_cache_expire_ms());
    DeferOp remove_state_entry([&] { _update_state_cache.remove(state_entry); });
    state.init(params);
    for (uint32_t del_id = 0; del_id < op_write.dels_size(); del_id++) {
        RETURN_IF_ERROR(state.load_delete(del_id, params));
        DCHECK(state.deletes(del_id) != nullptr);
        RETURN_IF_ERROR(index.erase(metadata, *state.deletes(del_id), &new_deletes, del_rebuild_rssid));
        _index_cache.update_object_size(index_entry, index.memory_usage());
        state.release_delete(del_id);
    }

    // generate delvec and write to meta
    vector<std::pair<uint32_t, DelVectorPtr>> new_del_vecs;
    new_del_vecs.reserve(new_deletes.size());
    std::map<uint32_t, size_t> segment_id_to_add_dels;
    for (auto& [rssid, del_ids] : new_deletes) {
        if (del_ids.empty()) continue;
        TabletSegmentId tsid;
        tsid.tablet_id = tablet->id();
        tsid.segment_id = rssid;
        DelVectorPtr old_delvec;
        RETURN_IF_ERROR(get_del_vec(tsid, base_version, builder, false, &old_delvec));
        DelVectorPtr dv_new;
        old_delvec->add_dels_as_new_version(del_ids, metadata->version(), &dv_new);
        new_del_vecs.emplace_back(rssid, dv_new);
        segment_id_to_add_dels[rssid] += del_ids.size();
    }
    for (auto&& each : new_del_vecs) {
        builder->append_delvec(each.second, each.first);
    }
    RETURN_IF_ERROR(builder->update_num_del_stat(segment_id_to_add_dels));

    return Status::OK();
}

Status UpdateManager::publish_column_mode_partial_update(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                                                         const TabletMetadataPtr& metadata, Tablet* tablet,
                                                         IndexEntry* index_entry, MetaFileBuilder* builder,
                                                         int64_t base_version) {
    DCHECK(index_entry != nullptr);

    auto tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
    RssidFileInfoContainer rssid_fileinfo_container;
    rssid_fileinfo_container.add_rssid_to_file(*metadata);
    std::vector<std::vector<uint32_t>> insert_rowids_by_segment;

    RowsetUpdateStateParams params{
            .op_write = op_write,
            .tablet_schema = tablet_schema,
            .metadata = metadata,
            .tablet = tablet,
            .container = rssid_fileinfo_container,
    };

    {
        ColumnModePartialUpdateHandler handler(base_version, txn_id, _update_mem_tracker);
        RETURN_IF_ERROR(handler.execute(params, builder, &insert_rowids_by_segment));
    }

    const uint32_t rowset_id = metadata->next_rowset_id();
    uint32_t new_del_rebuild_rssid = rowset_id; // default value if no insert rows

    auto& index = dynamic_cast<LakePrimaryIndex&>(index_entry->value());

    // 1. handle inserted rows: for COLUMN_UPSERT_MODE, build full segments with only inserted rows and append to meta
    RETURN_IF_ERROR(_handle_column_upsert_mode(op_write, txn_id, metadata, tablet, index, builder, base_version,
                                               rowset_id, insert_rowids_by_segment, &new_del_rebuild_rssid));

    // 2. handle delete files and generate delvecs for existing rssids only
    RETURN_IF_ERROR(_handle_delete_files(op_write, txn_id, metadata, tablet, index, index_entry, builder, base_version,
                                         new_del_rebuild_rssid, params));

    return Status::OK();
}

// Update primary index with new rows and collect rows to be deleted.
// This method supports both serial and parallel execution modes.
//
// Parallel Execution:
// When enabled (config::enable_pk_index_parallel_execution && is_cloud_native_index), this method
// uses a thread pool to process segments concurrently, significantly improving performance
// for large tablets during publish operations.
//
// Parameters:
//   rowset_id:            Base RSSID (RowSet Segment ID) for this rowset
//   upsert_idx:           Segment offset within the rowset
//   upsert:               Iterator over segments with encoded primary keys
//   index:                Primary index to update
//   new_deletes:          Output map of segment_id -> row_ids to mark as deleted
//   read_only:            If true, only query index (no updates); used when index files already exist
//   is_cloud_native_index: Whether using cloud-native persistent index
//
// Execution Flow:
//   1. Setup parallel execution context if enabled (allocate thread pool token)
//   2. For each segment:
//      - read_only mode: Call parallel_get() to find existing rows to delete
//      - write mode: Call parallel_upsert() to update index and find deletes
//   3. Wait for all parallel tasks to complete
//   4. Flush memtable if in write mode (batch writes to sstable)
Status UpdateManager::_do_update(uint32_t rowset_id, int32_t upsert_idx, const SegmentPKIteratorPtr& upsert,
                                 LakePrimaryIndex& index, DeletesMap* new_deletes, bool read_only,
                                 bool is_cloud_native_index) {
    TRACE_COUNTER_SCOPE_LATENCY_US("do_update_latency_us");

    // Prepare parallel execution infrastructure if enabled
    std::unique_ptr<ThreadPoolToken> token;

    // Note: Only cloud-native index supports parallel_get/parallel_upsert, local index does not support it
    if (config::enable_pk_index_parallel_execution && is_cloud_native_index) {
        token = ExecEnv::GetInstance()->pk_index_execution_thread_pool()->new_token(
                ThreadPool::ExecutionMode::CONCURRENT);
    }

    if (read_only && is_cloud_native_index) {
        // Note: Only cloud-native index supports readonly execution mode.
        // Query existing rows to delete without modifying the index
        RETURN_IF_ERROR(index.parallel_get(token.get(), upsert.get(), new_deletes));
    } else {
        // Update index and collect rows to delete
        RETURN_IF_ERROR(index.parallel_upsert(token.get(), rowset_id + upsert_idx, upsert.get(), new_deletes));
    }

    return Status::OK();
}

// Handles condition-based merge for a single chunk of primary keys during parallel publish.
// This function is invoked concurrently for different chunks to compare condition column values
// between new and existing rows, deciding which version should be retained.
//
// ALGORITHM:
// 1. Look up existing row locations (old_rowids) for all PKs in current chunk
// 2. Read condition column values from both old and new rows
// 3. Compare values: keep row with larger condition value (e.g., max timestamp)
// 4. Generate deletions for losing rows (either old or new)
//
// CONCURRENCY: This function is called from parallel worker threads. Shared state (context->deletes)
// is protected by context->mutex. Each chunk is processed independently for maximum parallelism.
//
// PERFORMANCE CONSIDERATION: Batch processing chunks reduces lock contention compared to
// row-by-row comparison, while parallel execution scales with CPU cores.
Status UpdateManager::_process_single_chunk_update_with_condition(
        const RowsetUpdateStateParams& params, uint32_t rowset_id, int32_t upsert_idx,
        SegmentPKIterator* segment_pk_iterator, ParallelPublishContext* context,
        const std::pair<ChunkPtr, size_t>& current, const TabletColumn& tablet_column,
        const std::vector<uint32_t>& read_column_ids, LakePrimaryIndex& index) {
    TRACE_COUNTER_INCREMENT("process_condition_update_count", 1);
    // Extract primary key column from current chunk for index lookup
    ASSIGN_OR_RETURN(auto pk_column, segment_pk_iterator->encoded_pk_column(current.first.get()));
    std::vector<uint64_t> old_rowids(pk_column->size());
    RETURN_IF_ERROR(index.get(*pk_column, &old_rowids));
    // Fast path: If no existing rows found for any PK, all new rows win by default
    bool non_old_value = std::all_of(old_rowids.begin(), old_rowids.end(), [](int id) { return -1 == id; });
    if (!non_old_value) {
        // Conflict detected: at least one PK already exists, need to compare condition values
        // STEP 1: Read condition column values from existing (old) rows
        // Group old rowids by RSSID (rowset+segment ID) for efficient batch reads
        std::map<uint32_t, std::vector<uint32_t>> old_rowids_by_rssid;
        size_t num_default = 0;
        vector<uint32_t> idxes;
        RowsetUpdateState::plan_read_by_rssid(old_rowids, &num_default, &old_rowids_by_rssid, &idxes);
        MutableColumns old_columns(1);
        auto old_unordered_column =
                ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        old_columns[0] = old_unordered_column->clone_empty();
        // Batch read condition values from all old row locations
        RETURN_IF_ERROR(get_column_values(params, read_column_ids, num_default > 0, old_rowids_by_rssid, &old_columns));
        // Reorder values to match the order of PKs in current chunk (idxes provides mapping)
        auto old_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        old_column->append_selective(*old_columns[0], idxes.data(), 0, idxes.size());

        // STEP 2: Read condition column values from new rows (from SST files)
        std::map<uint32_t, std::vector<uint32_t>> new_rowids_by_rssid;
        std::vector<uint32_t> rowids;
        rowids.reserve(pk_column->size());
        for (int j = 0; j < pk_column->size(); ++j) {
            // Build absolute rowids: current.second is the base offset for this chunk
            rowids.push_back(j + current.second);
        }
        new_rowids_by_rssid[rowset_id + upsert_idx] = rowids;
        // Read condition values from SST file for new rows
        // LIMITATION: Only single condition column is supported (not composite conditions)
        MutableColumns new_columns(1);
        auto new_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        new_columns[0] = new_column->clone_empty();
        RETURN_IF_ERROR(get_column_values(params, read_column_ids, false, new_rowids_by_rssid, &new_columns));

        // STEP 3: Compare condition values and generate deletion vectors
        // For each PK conflict, compare old vs new condition values to decide winner
        for (int j = 0; j < old_column->size(); ++j) {
            if (num_default > 0 && idxes[j] == 0) {
                // EDGE CASE: Index 0 indicates default value (no old row actually exists for this PK)
                // WHY: plan_read_by_rssid uses index 0 as sentinel for PKs with no old values
                // ACTION: Skip comparison, new row wins by default
            } else {
                // Compare condition column: old_value vs new_value
                // Returns: >0 if old > new, <0 if old < new, 0 if equal
                int r = old_column->compare_at(j, j, *new_columns[0].get(), -1);
                if (r > 0) {
                    // Old value wins (old > new): Delete the new row from current SST file
                    // CRITICAL: Must lock before modifying shared delete map
                    std::lock_guard<std::mutex> lock(*context->mutex);
                    (*context->deletes)[rowset_id + upsert_idx].push_back(j + current.second);
                } else {
                    // New value wins (old <= new): Delete the old row from its original segment
                    // ROWID ENCODING: old_rowid = (rssid << 32) | row_offset
                    // Extract RSSID (high 32 bits) and row offset (low 32 bits) to locate old row
                    std::lock_guard<std::mutex> lock(*context->mutex);
                    uint64_t old_rowid = old_rowids[j];
                    (*context->deletes)[(uint32_t)(old_rowid >> 32)].push_back((uint32_t)(old_rowid & ROWID_MASK));
                }
            }
        }
    }
    return Status::OK();
}

// Performs condition-based merge update with PARALLEL execution for segments with SST files.
// This is the optimized path that leverages pre-materialized condition values in SST files
// to enable parallel chunk-level processing.
//
// PRECONDITION: SST files must exist (verified by caller) containing condition column values
//
// ALGORITHM:
// 1. Iterate through chunks of primary keys from segment
// 2. For each chunk, submit parallel task to compare condition values
// 3. Each task reads old/new condition values and generates appropriate deletions
// 4. Wait for all parallel tasks to complete
//
// PARALLELISM STRATEGY:
// - Chunk-level parallelism: Each chunk processed by independent worker thread
// - Lock-free reads: Condition values read from immutable SST files
// - Fine-grained locking: Only lock when updating shared delete map
//
// PERFORMANCE: 3-5x faster than serial path for large segments (>100K rows)
// WHY: CPU parallelism + reduced lock contention via chunk batching
Status UpdateManager::_do_update_with_condition_parallel(const RowsetUpdateStateParams& params, uint32_t rowset_id,
                                                         int32_t upsert_idx, int32_t condition_column,
                                                         const SegmentPKIteratorPtr& upsert, LakePrimaryIndex& index,
                                                         DeletesMap* new_deletes) {
    RETURN_ERROR_IF_FALSE(condition_column >= 0);
    TRACE_COUNTER_SCOPE_LATENCY_US("do_update_with_condition_and_ingest_latency_us");
    const auto& tablet_column = params.tablet_schema->column(condition_column);
    std::vector<uint32_t> read_column_ids;
    read_column_ids.push_back(condition_column);

    // Obtain thread pool token for parallel execution if enabled
    std::unique_ptr<ThreadPoolToken> token;
    if (config::enable_pk_index_parallel_execution) {
        token = ExecEnv::GetInstance()->pk_index_execution_thread_pool()->new_token(
                ThreadPool::ExecutionMode::CONCURRENT);
    }

    // Setup shared state protected by mutex
    std::mutex mutex; // CRITICAL: Protects concurrent access to deletes map and status
    Status status = Status::OK();

    // Setup context shared across all parallel tasks
    ParallelPublishContext context{.token = token.get(), .mutex = &mutex, .deletes = new_deletes, .status = &status};
    auto* context_ptr = &context;

    // Iterate through all chunks in the segment
    // IMPORTANT: Iteration itself is serial, but chunk processing is parallelized
    for (; !upsert->done(); upsert->next()) {
        auto current = upsert->current(); // Get current chunk (PKs + row offset)

        // Lambda captures chunk data and processes condition merge
        // CAPTURE STRATEGY: Capture context_ptr and current by value to ensure thread safety
        auto condition_merge_func = [&, context_ptr, current]() {
            auto st = _process_single_chunk_update_with_condition(params, rowset_id, upsert_idx, upsert.get(),
                                                                  context_ptr, current, tablet_column, read_column_ids,
                                                                  index);
            if (!st.ok()) {
                // Error handling: Update shared status under lock
                std::lock_guard<std::mutex> lock(*context_ptr->mutex);
                context_ptr->status->update(st);
            }
        };

        if (token) {
            // PARALLEL PATH: Submit chunk processing to thread pool
            // Non-blocking: Immediately continue to next chunk while workers process this one
            auto submit_st = token->submit_func(condition_merge_func);
            if (!submit_st.ok()) {
                std::lock_guard<std::mutex> lock(*context_ptr->mutex);
                context_ptr->status->update(submit_st);
            }
        } else {
            // SERIAL FALLBACK: Process chunk inline when parallelism disabled
            condition_merge_func();
            RETURN_IF_ERROR(status);
        }
    }

    if (token) {
        // Barrier: Wait for all submitted tasks to complete before proceeding
        // IMPORTANT: Ensures all deletions are collected before returning
        token->wait();
    }

    RETURN_IF_ERROR(status);

    return upsert->status();
}

namespace {

// Per-chunk output of the parallel compare phase for _do_update_with_condition (non-SST path).
//
// The compare phase runs concurrently per chunk; its results are consumed serially
// afterwards because PrimaryIndex::upsert is not thread-safe. pk_column is retained
// so the serial merge can upsert winners without re-encoding the chunk.
struct ChunkCondMergeResult {
    MutableColumnPtr pk_column;
    // Absolute row offset of this chunk within the segment (== SegmentPKIterator::current().second).
    size_t chunk_offset = 0;
    // Half-open intervals [begin, end) in chunk-local coordinates identifying new rows
    // that won their condition comparison and must be upserted into the index.
    // Any replaced old rowids are collected by index.upsert into new_deletes.
    std::vector<std::pair<uint32_t, uint32_t>> winner_ranges;
    // Chunk-local indices of new rows that LOST their condition comparison
    // (old row wins); these must be deleted from the current segment.
    std::vector<uint32_t> new_loser_local_ids;
};

} // namespace

// Compare phase for one chunk in the non-SST parallel condition merge.
// Mirrors _process_single_chunk_update_with_condition but writes into a per-chunk
// result instead of mutating the shared index, so it is safe to run concurrently
// and the caller can apply index.upsert serially.
static Status process_single_chunk_update_with_condition_no_sst(
        UpdateManager* mgr, const RowsetUpdateStateParams& params, uint32_t rowset_id, int32_t upsert_idx,
        SegmentPKIterator* segment_pk_iterator, const std::pair<ChunkPtr, size_t>& current,
        const TabletColumn& tablet_column, const std::vector<uint32_t>& read_column_ids, LakePrimaryIndex& index,
        ChunkCondMergeResult* result) {
    TRACE_COUNTER_INCREMENT("process_condition_update_count", 1);
    ASSIGN_OR_RETURN(result->pk_column, segment_pk_iterator->encoded_pk_column(current.first.get()));
    const auto chunk_size = result->pk_column->size();
    if (chunk_size == 0) {
        return Status::OK();
    }
    std::vector<uint64_t> old_rowids(chunk_size);
    RETURN_IF_ERROR(index.get(*result->pk_column, &old_rowids));
    // Fast path: no PKs in this chunk collide with the index → every new row wins.
    bool non_old_value = std::all_of(old_rowids.begin(), old_rowids.end(), [](int64_t id) { return -1 == id; });
    if (non_old_value) {
        result->winner_ranges.emplace_back(0u, static_cast<uint32_t>(chunk_size));
        return Status::OK();
    }

    std::map<uint32_t, std::vector<uint32_t>> old_rowids_by_rssid;
    size_t num_default = 0;
    std::vector<uint32_t> idxes;
    RowsetUpdateState::plan_read_by_rssid(old_rowids, &num_default, &old_rowids_by_rssid, &idxes);
    MutableColumns old_columns(1);
    auto old_unordered_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
    old_columns[0] = old_unordered_column->clone_empty();
    RETURN_IF_ERROR(
            mgr->get_column_values(params, read_column_ids, num_default > 0, old_rowids_by_rssid, &old_columns));
    auto old_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
    old_column->append_selective(*old_columns[0], idxes.data(), 0, idxes.size());

    // Read new-row condition values from the freshly-ingested segment; rowid in this segment
    // equals chunk_offset + local_idx.
    std::map<uint32_t, std::vector<uint32_t>> new_rowids_by_rssid;
    std::vector<uint32_t> rowids;
    rowids.reserve(chunk_size);
    for (size_t j = 0; j < chunk_size; ++j) {
        rowids.push_back(static_cast<uint32_t>(j + current.second));
    }
    new_rowids_by_rssid[rowset_id + upsert_idx] = std::move(rowids);
    // only support condition update on single column
    MutableColumns new_columns(1);
    auto new_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
    new_columns[0] = new_column->clone_empty();
    RETURN_IF_ERROR(mgr->get_column_values(params, read_column_ids, false, new_rowids_by_rssid, &new_columns));

    // Walk the chunk building contiguous "new wins" ranges, splitting on losers.
    uint32_t run_begin = 0;
    uint32_t run_step = 0;
    for (size_t j = 0; j < old_column->size(); ++j) {
        if (num_default > 0 && idxes[j] == 0) {
            // plan_read_by_rssid uses idx 0 as sentinel for "no old row"; new row wins by default.
            run_step++;
        } else {
            int r = old_column->compare_at(j, j, *new_columns[0].get(), -1);
            if (r > 0) {
                // Old wins: close the current winner run (if any) and mark this new row as loser.
                if (run_step > 0) {
                    result->winner_ranges.emplace_back(run_begin, run_begin + run_step);
                }
                result->new_loser_local_ids.push_back(static_cast<uint32_t>(j));
                run_begin = static_cast<uint32_t>(j + 1);
                run_step = 0;
            } else {
                // New wins (old <= new): extend the current run.
                run_step++;
            }
        }
    }
    if (run_step > 0) {
        result->winner_ranges.emplace_back(run_begin, run_begin + run_step);
    }
    return Status::OK();
}

// Performs condition-based merge update for the non-SST path using chunk-level parallelism.
//
// ALGORITHM:
//   1. Iterate segment chunks serially; submit each chunk's compare work to the
//      pk_index_execution thread pool (if parallelism is enabled).
//   2. Each compare task: index.get() → read old/new condition columns → compute
//      winner ranges and loser indices into a per-chunk result. No index writes.
//   3. Barrier (token->wait()).
//   4. Serial merge: for each chunk in order, index.upsert() the winner ranges and
//      append new-loser rowids to new_deletes. The old rowids displaced by winners
//      are collected by index.upsert via its DeletesMap out-param.
//
// WHY the upsert is serial: PrimaryIndex::upsert is documented [not thread-safe]; in
// this path there is no later SST ingest, so winners must be inserted into the index
// here, which forces the write phase to be serial.
Status UpdateManager::_do_update_with_condition(const RowsetUpdateStateParams& params, uint32_t rowset_id,
                                                int32_t upsert_idx, int32_t condition_column,
                                                const SegmentPKIteratorPtr& upsert, LakePrimaryIndex& index,
                                                DeletesMap* new_deletes) {
    RETURN_ERROR_IF_FALSE(condition_column >= 0);
    TRACE_COUNTER_SCOPE_LATENCY_US("do_update_with_condition_latency_us");
    const auto& tablet_column = params.tablet_schema->column(condition_column);
    std::vector<uint32_t> read_column_ids;
    read_column_ids.push_back(condition_column);

    std::unique_ptr<ThreadPoolToken> token;
    if (config::enable_pk_index_parallel_execution) {
        token = ExecEnv::GetInstance()->lake_partial_update_thread_pool()->new_token(
                ThreadPool::ExecutionMode::CONCURRENT);
    }

    std::mutex mutex;
    Status status = Status::OK();
    // Per-chunk results accumulated in iteration order; consumed after the compare barrier.
    std::vector<std::unique_ptr<ChunkCondMergeResult>> chunk_results;

    for (; !upsert->done(); upsert->next()) {
        auto current = upsert->current();
        chunk_results.emplace_back(std::make_unique<ChunkCondMergeResult>());
        auto* result = chunk_results.back().get();
        result->chunk_offset = current.second;

        auto compare_func = [&, result, current]() {
            auto st = process_single_chunk_update_with_condition_no_sst(this, params, rowset_id, upsert_idx,
                                                                        upsert.get(), current, tablet_column,
                                                                        read_column_ids, index, result);
            if (!st.ok()) {
                std::lock_guard<std::mutex> lock(mutex);
                status.update(st);
            }
        };

        if (token) {
            auto submit_st = token->submit_func(compare_func);
            if (!submit_st.ok()) {
                std::lock_guard<std::mutex> lock(mutex);
                status.update(submit_st);
            }
        } else {
            compare_func();
            RETURN_IF_ERROR(status);
        }
    }

    if (token) {
        TRACE_COUNTER_SCOPE_LATENCY_US("condition_update_compare_phase_us");
        token->wait();
    }
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(upsert->status());

    // PHASE 2: apply index.upsert for each chunk's winners.
    //
    // For cloud-native persistent index, every chunk's winning rows are compacted into a single
    // contiguous PK column and submitted via the parallel-publish overload
    // `index.upsert(rssid, rowids, pks, stat, ctx)`. The active-memtable write happens
    // synchronously here (the memtable is not thread-safe), while the SST / inactive-memtable
    // lookup of replaced old values is offloaded to ctx->token. Replaced old rowids are appended
    // to `new_deletes` by the lookup tasks under `upsert_mutex`.
    //
    // WHY compact-per-chunk instead of one task per winner range:
    //   build_persistent_keys() materializes one Slice per row of the *whole* chunk PK column
    //   on the binary/string path, so a per-range slot would cost chunk_size Slices regardless
    //   of range size. Fragmented winner ranges (e.g. alternating winners and losers) would
    //   then accumulate O(num_ranges * chunk_size) live Slices across all in-flight slots, which
    //   can blow up memory for large publishes. Compacting bounds each slot's per-row buffers
    //   to the actual winner count.
    //
    // For local persistent index (non-cloud-native), the parallel-publish overload is not
    // supported, so we fall back to the serial sliced upsert keyed by chunk_offset.
    const uint32_t rssid = rowset_id + upsert_idx;
    const bool use_parallel_upsert = (token != nullptr) && use_cloud_native_pk_index(*params.metadata);
    if (use_parallel_upsert) {
        std::mutex upsert_mutex;
        Status upsert_status = Status::OK();
        ParallelPublishContext upsert_ctx{
                .token = token.get(), .mutex = &upsert_mutex, .deletes = new_deletes, .status = &upsert_status};
        for (const auto& result : chunk_results) {
            // pk_column is guaranteed non-null when the compare task returned OK; if any task
            // had failed we would have bailed at the RETURN_IF_ERROR(status) above the barrier.
            DCHECK(result->pk_column != nullptr);
            if (result->winner_ranges.empty()) {
                continue;
            }
            size_t total_winners = 0;
            for (const auto& range : result->winner_ranges) {
                total_winners += static_cast<size_t>(range.second - range.first);
            }
            if (total_winners == 0) {
                continue;
            }
            // Compact winner indices and absolute rowids; build a winner-only PK column so the
            // submitted lookup task only sees `total_winners` keys.
            std::vector<uint32_t> winner_local_indices;
            winner_local_indices.reserve(total_winners);
            std::vector<uint32_t> winner_rowids;
            winner_rowids.reserve(total_winners);
            const auto chunk_offset = static_cast<uint32_t>(result->chunk_offset);
            for (const auto& range : result->winner_ranges) {
                for (uint32_t i = range.first; i < range.second; ++i) {
                    winner_local_indices.push_back(i);
                    winner_rowids.push_back(chunk_offset + i);
                }
            }
            auto winner_pk_column = result->pk_column->clone_empty();
            winner_pk_column->append_selective(*result->pk_column, winner_local_indices.data(), 0,
                                               winner_local_indices.size());
            // Allocate the slot and move the compacted column into it BEFORE submission so the
            // backing Slice array stays alive for the lookup task.
            upsert_ctx.extend_slots();
            auto* slot = upsert_ctx.slots.back().get();
            slot->pk_column = std::move(winner_pk_column);
            auto st = index.upsert(rssid, winner_rowids, *slot->pk_column, /*stat=*/nullptr, &upsert_ctx);
            if (!st.ok()) {
                std::lock_guard<std::mutex> lock(upsert_mutex);
                upsert_status.update(st);
            }
        }
        {
            TRACE_COUNTER_SCOPE_LATENCY_US("condition_update_upsert_phase_us");
            token->wait();
        }
        RETURN_IF_ERROR(upsert_status);
        // Persist the active-memtable batch built up by the parallel upserts.
        RETURN_IF_ERROR(index.flush_memtable());
    } else {
        // Serial fallback: rowid_start=chunk_offset so the index records rowids matching the
        // segment file layout for each contiguous winner range.
        for (const auto& result : chunk_results) {
            DCHECK(result->pk_column != nullptr);
            for (const auto& range : result->winner_ranges) {
                RETURN_IF_ERROR(index.upsert(rssid, static_cast<uint32_t>(result->chunk_offset), *result->pk_column,
                                             range.first, range.second, new_deletes));
            }
        }
    }

    // Append new-loser rowids to new_deletes. Safe to do here: phase 2 lookup tasks only
    // append old rowids for displaced index entries (whose rssids are different from the
    // freshly-ingested segment's rssid), and we are past the token barrier anyway.
    for (const auto& result : chunk_results) {
        if (result->new_loser_local_ids.empty()) {
            continue;
        }
        auto& seg_deletes = (*new_deletes)[rssid];
        seg_deletes.reserve(seg_deletes.size() + result->new_loser_local_ids.size());
        for (uint32_t local : result->new_loser_local_ids) {
            seg_deletes.push_back(static_cast<uint32_t>(result->chunk_offset) + local);
        }
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

Status UpdateManager::get_rowids_from_pkindex(int64_t tablet_id, int64_t base_version, const MutableColumns& upserts,
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

Status UpdateManager::get_rowids_from_pkindex(int64_t tablet_id, int64_t base_version, const MutableColumnPtr& upsert,
                                              std::vector<uint64_t>* rss_rowids, bool need_lock) {
    Status st;
    st.update(_handle_index_op(tablet_id, base_version, need_lock, [&](LakePrimaryIndex& index) {
        // get rss_rowids for segment's pk
        st.update(index.get(*upsert, rss_rowids));
    }));
    return st;
}

Status UpdateManager::batch_get_rss_rowids_from_pkindex(int64_t tablet_id, int64_t base_version,
                                                        std::vector<SegmentPKIteratorPtr>& pk_iters,
                                                        std::vector<std::vector<uint64_t>>* rss_rowids_per_segment,
                                                        bool need_lock) {
    rss_rowids_per_segment->resize(pk_iters.size());
    Status st;
    st.update(_handle_index_op(tablet_id, base_version, need_lock, [&](LakePrimaryIndex& index) {
        TRACE_COUNTER_INCREMENT("pcu_load_update_state_cnt", pk_iters.size());
        std::unique_ptr<ThreadPoolToken> token;
        if (config::enable_pk_index_parallel_execution) {
            token = ExecEnv::GetInstance()->pk_index_execution_thread_pool()->new_token(
                    ThreadPool::ExecutionMode::CONCURRENT);
        }
        TRACE_COUNTER_SCOPE_LATENCY_US("pcu_prepare_partial_update_states_us");
        st.update(index.batch_parallel_get_rss_rowids(token.get(), pk_iters, rss_rowids_per_segment));
    }));
    return st;
}

static StatusOr<std::shared_ptr<Segment>> get_lake_dcg_segment(GetDeltaColumnContext& ctx, uint32_t ucid,
                                                               int32_t* col_index,
                                                               const TabletSchemaCSPtr& read_tablet_schema) {
    // iterate dcg from new ver to old ver
    for (const auto& dcg : ctx.dcgs) {
        std::pair<int32_t, int32_t> idx = dcg->get_column_idx(ucid);
        if (idx.first < 0) {
            // column not found in this DCG, try next one
            continue;
        }

        auto column_file_result = dcg->column_file_by_idx(parent_name(ctx.segment->file_name()), idx.first);
        if (!column_file_result.ok()) {
            return Status::InternalError(
                    fmt::format("DCG file not found for column {}: {}", ucid, column_file_result.status().to_string()));
        }
        const auto& column_file = column_file_result.value();

        if (ctx.dcg_segments.count(column_file) == 0) {
            auto dcg_segment_result = ctx.segment->new_dcg_segment(*dcg, idx.first, read_tablet_schema);
            if (!dcg_segment_result.ok()) {
                return Status::InternalError(fmt::format("Failed to create DCG segment for column {}: {}", ucid,
                                                         dcg_segment_result.status().to_string()));
            }
            ctx.dcg_segments[column_file] = dcg_segment_result.value();
        }

        if (col_index != nullptr) {
            *col_index = idx.second;
        }
        return ctx.dcg_segments[column_file];
    }
    return Status::NotFound(fmt::format("Column {} not found in any DCG", ucid));
}

static StatusOr<std::unique_ptr<ColumnIterator>> new_lake_dcg_column_iterator(
        GetDeltaColumnContext& ctx, const std::shared_ptr<FileSystem>& fs, ColumnIteratorOptions& iter_opts,
        const TabletColumn& column, const TabletSchemaCSPtr& read_tablet_schema) {
    // build column iter from dcg
    int32_t col_index = 0;
    auto dcg_segment_result = get_lake_dcg_segment(ctx, column.unique_id(), &col_index, read_tablet_schema);
    if (!dcg_segment_result.ok()) {
        return dcg_segment_result.status();
    }

    const auto& dcg_segment = dcg_segment_result.value();
    if (ctx.dcg_read_files.count(dcg_segment->file_name()) == 0) {
        RandomAccessFileOptions ropts;
        if (!dcg_segment->file_info().encryption_meta.empty()) {
            ASSIGN_OR_RETURN(auto info,
                             KeyCache::instance().unwrap_encryption_meta(dcg_segment->file_info().encryption_meta));
            ropts.encryption_info = std::move(info);
        }
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file_with_bundling(ropts, dcg_segment->file_info()));
        ctx.dcg_read_files[dcg_segment->file_name()] = std::move(read_file);
    }
    iter_opts.read_file = ctx.dcg_read_files[dcg_segment->file_name()].get();
    return dcg_segment->new_column_iterator(column, nullptr);
}

Status UpdateManager::get_column_values(const RowsetUpdateStateParams& params, const std::vector<uint32_t>& column_ids,
                                        bool with_default,
                                        const std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                                        MutableColumns* columns, const std::map<string, string>* column_to_expr_value,
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

    bool need_dcg_check = false;
    std::unordered_map<uint32_t, GetDeltaColumnContext> dcg_contexts;
    bool has_any_dcg = false;
    if (params.op_write.has_txn_meta()) {
        // only enable dcg logic when switching from column mode to row mode
        need_dcg_check = !params.metadata->dcg_meta().dcgs().empty();
    }

    if (need_dcg_check) {
        LakeDeltaColumnGroupLoader dcg_loader(params.metadata);
        for (const auto& [rssid, rowids] : rowids_by_rssid) {
            TabletSegmentId tsid;
            tsid.tablet_id = params.tablet->id();
            tsid.segment_id = rssid;

            DeltaColumnGroupList dcgs;
            Status dcg_status = dcg_loader.load(tsid, params.metadata->version(), &dcgs);
            if (dcg_status.ok() && !dcgs.empty()) {
                dcg_contexts[rssid].dcgs = std::move(dcgs);
                has_any_dcg = true;
            }
        }
    }

    auto fetch_values_from_segment = [&](const FileInfo& segment_info, uint32_t segment_id,
                                         const TabletSchemaCSPtr& tablet_schema, const std::vector<uint32_t>& rowids,
                                         const std::vector<uint32_t>& read_column_ids) -> Status {
        FileInfo file_info{.path = params.tablet->segment_location(segment_info.path),
                           .encryption_meta = segment_info.encryption_meta};
        if (segment_info.size.has_value()) {
            file_info.size = segment_info.size;
        }
        if (segment_info.bundle_file_offset.has_value()) {
            file_info.bundle_file_offset = segment_info.bundle_file_offset;
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
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file_with_bundling(opts, file_info));
        iter_opts.read_file = read_file.get();

        GetDeltaColumnContext* dcg_ctx = nullptr;
        if (has_any_dcg && dcg_contexts.count(segment_id) > 0 && !dcg_contexts[segment_id].dcgs.empty()) {
            dcg_ctx = &dcg_contexts[segment_id];
            dcg_ctx->segment = *segment;
        }

        for (auto i = 0; i < read_column_ids.size(); ++i) {
            const TabletColumn& col = tablet_schema->column(read_column_ids[i]);
            std::unique_ptr<ColumnIterator> col_iter = nullptr;

            // try dcg read only if dcg context exists
            if (dcg_ctx != nullptr) {
                auto dcg_col_iter_result = new_lake_dcg_column_iterator(*dcg_ctx, fs, iter_opts, col, tablet_schema);
                if (dcg_col_iter_result.ok()) {
                    col_iter = std::move(dcg_col_iter_result.value());
                } else if (!dcg_col_iter_result.status().is_not_found()) {
                    // NotFound is expected when column doesn't exist in DCG, other errors are real issues
                    return Status::InternalError(fmt::format("Failed to create DCG column iterator for column {}: {}",
                                                             col.name(), dcg_col_iter_result.status().to_string()));
                }
                // If status is NotFound, col_iter remains nullptr and we'll read from original segment
            }

            // read from original segment if no dcg data available
            if (col_iter == nullptr) {
                ASSIGN_OR_RETURN(col_iter, (*segment)->new_column_iterator_or_default(col, nullptr));
                iter_opts.read_file = read_file.get();
            }

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
            ASSIGN_OR_RETURN(fs, FileSystemFactory::CreateSharedFromString(root_path));
        }

        if (params.container.rssid_to_file().count(rssid) == 0) {
            // It may happen when preload partial update state by old tablet meta
            return Status::Cancelled(fmt::format("tablet id {} version {} rowset_segment_id {} no exist",
                                                 params.metadata->id(), params.metadata->version(), rssid));
        }
        // pass the actual segment_id to properly handle DCG reading
        RETURN_IF_ERROR(fetch_values_from_segment(params.container.rssid_to_file().at(rssid), rssid,
                                                  params.tablet_schema, rowids, column_ids));
    }
    if (auto_increment_state != nullptr && with_default) {
        if (fs == nullptr) {
            auto root_path = params.tablet->metadata_root_location();
            ASSIGN_OR_RETURN(fs, FileSystemFactory::CreateSharedFromString(root_path));
        }
        uint32_t segment_id = auto_increment_state->segment_id;
        const std::vector<uint32_t>& rowids = auto_increment_state->rowids;
        const std::vector<uint32_t> auto_increment_col_partial_id(1, auto_increment_state->id);
        FileInfo info;
        info.path = params.op_write.rowset().segments(segment_id);
        if (segment_id < params.op_write.rowset().bundle_file_offsets_size()) {
            // use shared file offset if available
            info.bundle_file_offset = params.op_write.rowset().bundle_file_offsets(segment_id);
            info.size = params.op_write.rowset().segment_size(segment_id);
        }
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
    *pdelvec = std::make_shared<DelVector>();
    // 2. find in delvec file
    return get_del_vec_in_meta(tsid, version, fill_cache, pdelvec->get());
}

// get delvec in meta file
Status UpdateManager::get_del_vec_in_meta(const TabletSegmentId& tsid, int64_t meta_ver, bool fill_cache,
                                          DelVector* delvec) {
    LakeIOOptions lake_io_opts;
    lake_io_opts.fill_data_cache = fill_cache;
    ASSIGN_OR_RETURN(auto metadata, _tablet_mgr->get_tablet_metadata(tsid.tablet_id, meta_ver, fill_cache));
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
        tsid.segment_id = get_rssid(rowset_meta, i);
        auto st = get_del_vec(tsid, version, nullptr, false /* fill cache */, &delvec);
        if (!st.ok()) {
            LOG(WARNING) << "get_rowset_num_deletes: error get del vector " << st;
            continue;
        }
        num_dels += delvec->cardinality();
    }
    return num_dels;
}

size_t UpdateManager::get_rowset_num_deletes(const TabletMetadata& metadata, const RowsetMetadataPB& rowset_meta) {
    size_t num_dels = 0;
    LakeIOOptions lake_io_opts;
    lake_io_opts.fill_data_cache = false;
    for (int i = 0; i < rowset_meta.segments_size(); i++) {
        DelVector delvec;
        uint32_t segment_id = get_rssid(rowset_meta, i);
        auto st = lake::get_del_vec(_tablet_mgr, metadata, segment_id, false /*fill_cache*/, lake_io_opts, &delvec);
        if (!st.ok()) {
            LOG(WARNING) << "get_rowset_num_deletes: error get del vector"
                         << " tablet_id=" << metadata.id() << " metadata_version=" << metadata.version()
                         << " segment_id=" << segment_id << " status=" << st;
            continue;
        }
        num_dels += delvec.cardinality();
    }
    return num_dels;
}

bool UpdateManager::_use_light_publish_primary_compaction(TabletManager* mgr,
                                                          const TxnLogPB_OpCompaction& op_compaction, int64_t tablet_id,
                                                          int64_t txn_id) {
    // Is config enable ?
    if (!config::enable_light_pk_compaction_publish) {
        return false;
    }

    // DESIGN DECISION: Determine if mapper file exists and which storage type
    // WHY: Light publish optimization uses pre-computed rows mapper to avoid re-reading
    // and re-processing compaction input/output rowsets during publish phase.
    // This can reduce publish time from minutes to seconds for large compactions.

    // Priority 1: Check for remote storage lcrm file (new path)
    if (op_compaction.has_lcrm_file()) {
        // WHY: When parallel pk execution is enabled, lcrm_file metadata is stored in txn log.
        // Its presence guarantees the mapper file exists on remote storage (S3/HDFS).
        // No need to check file existence - metadata is the source of truth.
        // BENEFIT: Avoids network I/O to check file existence in S3/HDFS.
        return true;
    }

    // Priority 2: Check for local disk crm file (legacy path)
    // WHY: For backward compatibility with compactions created before remote storage support.
    // These files were created on local disk and need explicit existence check.
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
    auto resolver = std::make_unique<LakePrimaryKeyCompactionConflictResolver>(
            &metadata, &output_rowset, _tablet_mgr, builder, &index, txn_id, base_version, op_compaction.lcrm_file(),
            &segment_id_to_add_dels, &delvecs);
    if (op_compaction.ssts_size() > 0 && use_cloud_native_pk_index(metadata)) {
        RETURN_IF_ERROR(resolver->execute_without_update_index());
    } else {
        RETURN_IF_ERROR(resolver->execute());
    }
    // 3. add delvec to builder
    for (auto&& each : delvecs) {
        builder->append_delvec(each.second, each.first);
    }
    // 4. ingest ssts to index
    DCHECK(op_compaction.ssts_size() == 0 || delvecs.size() == op_compaction.ssts_size())
            << "delvecs.size(): " << delvecs.size() << ", op_compaction.ssts_size(): " << op_compaction.ssts_size();
    for (int i = 0; i < op_compaction.ssts_size() && use_cloud_native_pk_index(metadata); i++) {
        uint32_t rssid = metadata.next_rowset_id() + get_segment_idx(op_compaction.output_rowset(), i);
        DelvecPagePB delvec_page_pb = builder->delvec_page(rssid);
        delvec_page_pb.set_version(metadata.version());
        RETURN_IF_ERROR(index.ingest_sst(op_compaction.ssts(i), op_compaction.sst_ranges(i), rssid, metadata.version(),
                                         delvec_page_pb, delvecs[i].second));
    }
    _index_cache.update_object_size(index_entry, index.memory_usage());
    // 5. update TabletMeta
    RETURN_IF_ERROR(builder->apply_opcompaction(op_compaction, max_rowset_id, tablet_schema->id()));
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
        return builder->apply_opcompaction(
                op_compaction,
                *std::max_element(op_compaction.input_rowsets().begin(), op_compaction.input_rowsets().end()),
                tablet_schema->id());
    });
    if (CompactionUpdateConflictChecker::conflict_check(op_compaction, txn_id, metadata, builder)) {
        // conflict happens
        return Status::OK();
    }
    if (_use_light_publish_primary_compaction(tablet.tablet_mgr(), op_compaction, tablet.id(), txn_id)) {
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
    if (input_rowset == metadata.rowsets().end()) {
        LOG(ERROR) << "cannot find input rowset in tablet metadata, rowset_id: " << max_rowset_id
                   << ", meta : " << metadata.ShortDebugString();
        return Status::InternalError("cannot find input rowset in tablet metadata");
    }
    uint32_t max_src_rssid = max_rowset_id;
    for (int i = 0; i < input_rowset->segments_size(); ++i) {
        max_src_rssid = std::max(max_src_rssid, get_rssid(*input_rowset, i));
    }
    std::map<uint32_t, size_t> segment_id_to_add_dels;

    // 2. update primary index, and generate delete info.
    TRACE_COUNTER_INCREMENT("output_rowsets_size", output_rowset.num_segments());
    for (size_t i = 0; i < output_rowset.num_segments(); i++) {
        RETURN_IF_ERROR(compaction_state.load_segments(&output_rowset, this, tablet_schema, i));
        TRACE_COUNTER_INCREMENT("state_bytes", compaction_state.memory_usage());
        auto& pk_col = compaction_state.pk_cols[i];
        total_rows += pk_col->size();
        uint32_t rssid = rowset_id + get_segment_idx(output_rowset.metadata(), static_cast<int32_t>(i));
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
    RETURN_IF_ERROR(builder->apply_opcompaction(op_compaction, max_rowset_id, tablet_schema->id()));
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

Status UpdateManager::update_primary_index_memory_limit(int32_t update_memory_limit_percent) {
    int64_t byte_limits = GlobalEnv::GetInstance()->process_mem_limit();
    int32_t update_mem_percent = std::max(std::min(100, update_memory_limit_percent), 0);
    _index_cache.set_capacity(byte_limits * update_mem_percent);
    return Status::OK();
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
    scoped_refptr<Trace> trace_guard(new Trace);
    ADOPT_TRACE(trace_guard.get());
    TRACE("start preload_update_state tablet_id=$0 txn_id=$1", tablet->id(), txnlog.txn_id());
    auto start_ts = MonotonicMillis();
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
    auto cost_ms = MonotonicMillis() - start_ts;
    if (cost_ms >= config::lake_publish_version_slow_log_ms) {
        LOG(INFO) << "Slow preload_update_state tablet_id=" << tablet->id() << " txn_id=" << txnlog.txn_id()
                  << " segments=" << segments_size << " cost=" << cost_ms
                  << "ms, trace: " << trace_guard->MetricsAsJSON();
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
    scoped_refptr<Trace> trace_guard(new Trace);
    ADOPT_TRACE(trace_guard.get());
    TRACE("start preload_compaction_state tablet_id=$0 txn_id=$1", tablet.id(), txnlog.txn_id());
    auto start_ts = MonotonicMillis();
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
    auto cost_ms = MonotonicMillis() - start_ts;
    if (cost_ms >= config::lake_publish_version_slow_log_ms) {
        LOG(INFO) << "Slow preload_compaction_state tablet_id=" << tablet.id() << " txn_id=" << txnlog.txn_id()
                  << " segments=" << segments_size << " cost=" << cost_ms
                  << "ms, trace: " << trace_guard->MetricsAsJSON();
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

Status UpdateManager::execute_index_major_compaction(const TabletMetadataPtr& metadata, TxnLogPB* txn_log) {
    if (config::enable_pk_index_parallel_compaction) {
        return LakePersistentIndex::parallel_major_compact(ExecEnv::GetInstance()->parallel_compact_mgr(), _tablet_mgr,
                                                           metadata, txn_log);
    }
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

bool UpdateManager::TEST_primary_index_refcnt(int64_t tablet_id, uint32_t expected_cnt) {
    auto index_entry = _index_cache.get(tablet_id);
    if (index_entry == nullptr) {
        return expected_cnt == 0;
    }
    _index_cache.release(index_entry);
    return index_entry->get_ref() == expected_cnt;
}

int64_t UpdateManager::get_index_memory_size(int64_t tablet_id) const {
    int64_t index_memory_size = 0;
    auto& index_cache = _tablet_mgr->update_mgr()->index_cache();
    auto index_entry = index_cache.get(tablet_id);
    if (index_entry != nullptr) {
        index_memory_size = index_entry->size();
        index_cache.release(index_entry);
    }
    return index_memory_size;
}

} // namespace starrocks::lake
