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

#include "base/debug/trace.h"
#include "base/testutil/sync_point.h"
#include "storage/chunk_helper.h"
#include "storage/lake/lake_local_persistent_index.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/tablet.h"
#include "storage/persistent_index_parallel_publish_context.h"

namespace starrocks::lake {

Status LakePrimaryIndex::lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                                   const MetaFileBuilder* builder) {
    TRACE_COUNTER_SCOPE_LATENCY_US("primary_index_load_latency_us");
    std::lock_guard<std::mutex> lg(_lock);
    if (_loaded && !need_rebuild()) {
        return _status;
    }
    if (need_rebuild()) {
        unload_without_lock();
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
    std::lock_guard<std::mutex> lg(_lock);
    return _loaded && _data_version >= base_version;
}

Status LakePrimaryIndex::_do_lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                       int64_t base_version, const MetaFileBuilder* builder) {
    // 1. create and set key column schema
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
    vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    _set_schema(pkey_schema);

    // Shared-data primary-key tablets support only the cloud-native persistent index. The
    // metadata is normalized to enabled + CLOUD_NATIVE at load time (see
    // normalize_tablet_metadata_after_load), so the in-memory index and the LOCAL persistent
    // index are never used here.
    DCHECK(_persistent_index == nullptr);
    _persistent_index = std::make_shared<LakePersistentIndex>(tablet_mgr, metadata->id());
    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    RETURN_IF_ERROR(lake_persistent_index->init(metadata));
    return lake_persistent_index->load_from_lake_tablet(tablet_mgr, metadata, base_version, builder);
}

Status LakePrimaryIndex::apply_opcompaction(const TabletMetadataPtr& metadata,
                                            const TxnLogPB_OpCompaction& op_compaction) {
    if (_persistent_index == nullptr) {
        return Status::OK();
    }

    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index != nullptr) {
        return lake_persistent_index->apply_opcompaction(metadata, op_compaction);
    } else {
        return Status::InternalError("Persistent index is not a LakePersistentIndex.");
    }
}

Status LakePrimaryIndex::ingest_sst(const FileMetaPB& sst_meta, const PersistentIndexSstableRangePB& sst_range,
                                    uint32_t rssid, int64_t version, const DelvecPagePB& delvec_page,
                                    DelVectorPtr delvec) {
    if (_persistent_index == nullptr) {
        return Status::OK();
    }

    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index != nullptr) {
        return lake_persistent_index->ingest_sst(sst_meta, sst_range, rssid, version, delvec_page, std::move(delvec));
    } else {
        return Status::InternalError("Persistent index is not a LakePersistentIndex.");
    }
}

Status LakePrimaryIndex::commit(const TabletMetadataPtr& metadata, MetaFileBuilder* builder,
                                int64_t generation_version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("primary_index_commit_latency_us");
    if (_persistent_index == nullptr) {
        return Status::OK();
    }

    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index != nullptr) {
        return lake_persistent_index->commit(builder, generation_version);
    } else {
        return Status::InternalError("Persistent index is not a LakePersistentIndex.");
    }
}

Status LakePrimaryIndex::sync_flush_persistent_index(int64_t wait_timeout_us) {
    if (_persistent_index == nullptr) {
        return Status::OK();
    }
    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index == nullptr) {
        return Status::OK();
    }
    return lake_persistent_index->sync_flush_all_memtables(wait_timeout_us);
}

double LakePrimaryIndex::get_local_pk_index_write_amp_score() {
    if (_persistent_index == nullptr) {
        return 0.0;
    }
    auto* local_persistent_index = dynamic_cast<LakeLocalPersistentIndex*>(_persistent_index.get());
    if (local_persistent_index != nullptr) {
        return local_persistent_index->get_write_amp_score();
    }
    return 0.0;
}

void LakePrimaryIndex::set_local_pk_index_write_amp_score(double score) {
    if (_persistent_index == nullptr) {
        return;
    }
    auto* local_persistent_index = dynamic_cast<LakeLocalPersistentIndex*>(_persistent_index.get());
    if (local_persistent_index != nullptr) {
        local_persistent_index->set_write_amp_score(score);
    }
}

static void old_values_to_deletes(const std::vector<uint64_t>& old_values, DeletesMap* deletes) {
    for (uint64_t old : old_values) {
        if (old != NullIndexValue) {
            (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
        }
    }
}

Status LakePrimaryIndex::erase(const TabletMetadataPtr& metadata, const Column& pks, DeletesMap* deletes,
                               uint32_t del_rssid) {
    // No need to setup rebuild point for in-memory index and local persistent index,
    // so keep using previous erase interface.
    if (_persistent_index == nullptr) {
        return PrimaryIndex::erase(pks, deletes);
    }

    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index != nullptr) {
        Buffer<Slice> keys;
        std::vector<uint64_t> old_values(pks.size(), NullIndexValue);
        ASSIGN_OR_RETURN(const Slice* vkeys, build_persistent_keys(pks, _key_size, 0, pks.size(), &keys));
        // Cloud native index needs the delete's rssid as the rebuild point when erasing.
        RETURN_IF_ERROR(lake_persistent_index->erase(pks.size(), vkeys,
                                                     reinterpret_cast<IndexValue*>(old_values.data()), del_rssid));
        old_values_to_deletes(old_values, deletes);
        return Status::OK();
    } else {
        return Status::InternalError("Persistent index is not a LakePersistentIndex.");
    }
}

int32_t LakePrimaryIndex::current_fileset_index() const {
    if (_persistent_index == nullptr) {
        return -1;
    }
    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index != nullptr) {
        return lake_persistent_index->current_fileset_index();
    } else {
        return -1;
    }
}

StatusOr<AsyncCompactCBPtr> LakePrimaryIndex::early_sst_compact(
        lake::LakePersistentIndexParallelCompactMgr* compact_mgr, TabletManager* tablet_mgr,
        const TabletMetadataPtr& metadata, int32_t fileset_start_idx) {
    if (_persistent_index == nullptr) {
        return nullptr;
    }
    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index != nullptr) {
        return lake_persistent_index->early_sst_compact(compact_mgr, tablet_mgr, metadata, fileset_start_idx);
    } else {
        return Status::InternalError("Persistent index is not a LakePersistentIndex.");
    }
}

Status LakePrimaryIndex::flush_memtable(bool force) {
    if (_persistent_index == nullptr) {
        return Status::OK();
    }

    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index != nullptr) {
        return lake_persistent_index->flush_memtable(force);
    }

    return Status::OK();
}

void LakePrimaryIndex::reset_publish_sst_stats() {
    if (_persistent_index == nullptr) return;
    auto* idx = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (idx != nullptr) idx->reset_publish_sst_stats();
}

int32_t LakePrimaryIndex::publish_sst_flush_count() const {
    if (_persistent_index == nullptr) return 0;
    auto* idx = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    return idx != nullptr ? idx->publish_sst_flush_count() : 0;
}

int64_t LakePrimaryIndex::publish_sst_flush_bytes() const {
    if (_persistent_index == nullptr) return 0;
    auto* idx = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    return idx != nullptr ? idx->publish_sst_flush_bytes() : 0;
}

// Query index for existing rows matching primary keys from all segments.
// This is used during read-only publish when index files already exist.
//
// Parameters:
// - token: Thread pool token for parallel execution. If null, executes serially.
// - segment_pk_iterator: Iterator over all segments containing primary keys to query.
// - new_deletes: Output map to store rows that need to be marked as deleted.
//
// Parallel Execution:
// - If token is set, submits each segment as a separate task to the thread pool
// - Otherwise, processes each segment inline (serial mode)
// - Waits for all tasks to complete before returning
//
// The function performs for each segment:
// 1. Get encoded primary keys for the segment
// 2. Query index to find existing row IDs (old_values)
// 3. Add found row IDs to the deletes map (rows to be marked as deleted)
//
// Thread Safety:
// - Each task allocates its own slot to avoid data races during parallel execution
// - Shared state (deletes, status) is protected by mutex when updated
// - Errors are accumulated and checked after all tasks complete
Status LakePrimaryIndex::parallel_get(ThreadPoolToken* token, SegmentPKIterator* segment_pk_iterator,
                                      DeletesMap* new_deletes) {
    // Prepare parallel execution infrastructure if enabled
    std::mutex mutex; // Protects shared state (deletes, status) during parallel execution
    Status status = Status::OK();

    // Setup context shared across all parallel tasks
    ParallelPublishContext context{.token = token, .mutex = &mutex, .deletes = new_deletes, .status = &status};
    auto* context_ptr = &context;

    // Process each segment in the iterator
    for (; !segment_pk_iterator->done(); segment_pk_iterator->next()) {
        auto current = segment_pk_iterator->current();

        // `extend_slots` is not thread-safe, must be called before submitting task
        context.extend_slots(); // Allocate a slot for this task's working data
        auto slot = context.slots.back().get();

        // Define the task to execute (either async in thread pool or inline)
        auto func = [this, context_ptr, current, slot, segment_pk_iterator]() {
            // Error handling: Must not throw or early return, as we need to wait for all tasks
            Status st = Status::OK();

            // Encode primary keys for this segment
            auto pk_column_st = segment_pk_iterator->encoded_pk_column(current.chunk.get());
            DCHECK(context_ptr->slots.size() > 0);

            if (pk_column_st.ok()) {
                // Query index for existing rows with these primary keys
                slot->pk_column = std::move(pk_column_st.value());
                slot->old_values.resize(slot->pk_column->size(), NullIndexValue);
                st = get(*slot->pk_column, &slot->old_values);
            } else {
                st = pk_column_st.status();
            }

            // Update shared state under lock
            std::lock_guard<std::mutex> l(*context_ptr->mutex);
            context_ptr->status->update(st);

            // Collect rows to delete: extract segment ID and row ID from old_values
            // Format: old_value = (segment_id << 32) | row_id
            if (context_ptr->status->ok()) {
                for (unsigned long old : slot->old_values) {
                    if (old != NullIndexValue) {
                        (*context_ptr->deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
                    }
                }
            }
        };

        if (token) {
            // Parallel mode: Submit task to thread pool
            auto st = token->submit_func(func);
            TRACE_COUNTER_INCREMENT("parallel_get_cnt", 1);

            // Record submit errors (actual execution errors will be recorded by the task)
            std::lock_guard<std::mutex> l(*context.mutex);
            context.status->update(st);
        } else {
            // Serial mode: Execute inline
            func();
            RETURN_IF_ERROR(*context.status);
        }
    }
    if (token) {
        TRACE_COUNTER_SCOPE_LATENCY_US("parallel_get_wait_us");
        token->wait(); // Wait for all submitted tasks to complete
    }

    RETURN_IF_ERROR(status); // Check for errors from parallel tasks
    return segment_pk_iterator->status();
}

// Parallel query of PK index to retrieve rss_rowids for all segments at once.
// Submits chunks from all segments to a single shared thread pool token, enabling
// cross-segment parallelism.
Status LakePrimaryIndex::batch_parallel_get_rss_rowids(ThreadPoolToken* token,
                                                       std::vector<SegmentPKIteratorPtr>& pk_iters,
                                                       std::vector<std::vector<uint64_t>>* rss_rowids_per_segment) {
    const uint32_t num_segments = pk_iters.size();
    rss_rowids_per_segment->resize(num_segments);

    struct RssRowidSlot {
        size_t begin_rowid = 0;
        size_t count = 0;
        std::vector<uint64_t> values;
    };

    std::mutex mutex;
    Status status = Status::OK();
    std::vector<std::vector<std::unique_ptr<RssRowidSlot>>> per_segment_slots(num_segments);

    // Iterate all segments' chunks on the main thread and submit them all to the shared pool.
    // begin_rowid is each chunk's logical offset (rows emitted before it within the segment),
    // i.e. its index into this segment's flat result array.
    for (uint32_t seg_idx = 0; seg_idx < num_segments; seg_idx++) {
        auto* pk_iter = pk_iters[seg_idx].get();
        size_t segment_logical_offset = 0;
        for (; !pk_iter->done(); pk_iter->next()) {
            auto current = pk_iter->current();
            auto slot = std::make_unique<RssRowidSlot>();
            slot->begin_rowid = segment_logical_offset;
            slot->count = current.chunk->num_rows();
            segment_logical_offset += slot->count;
            per_segment_slots[seg_idx].push_back(std::move(slot));
            auto* slot_ptr = per_segment_slots[seg_idx].back().get();

            auto func = [this, slot_ptr, current = std::move(current), pk_iter, &mutex, &status]() {
                auto pk_column_st = pk_iter->encoded_pk_column(current.chunk.get());
                Status st;
                if (pk_column_st.ok()) {
                    slot_ptr->values.resize(slot_ptr->count, NullIndexValue);
                    st = get(*pk_column_st.value(), &slot_ptr->values);
                } else {
                    st = pk_column_st.status();
                }
                std::lock_guard<std::mutex> l(mutex);
                status.update(st);
            };

            if (token) {
                auto st = token->submit_func(func);
                TRACE_COUNTER_INCREMENT("batch_parallel_get_rss_rowids_cnt", 1);
                std::lock_guard<std::mutex> l(mutex);
                status.update(st);
            } else {
                func();
                RETURN_IF_ERROR(status);
            }
        }
    }

    if (token) {
        TRACE_COUNTER_SCOPE_LATENCY_US("batch_parallel_get_rss_rowids_wait_us");
        token->wait();
    }
    RETURN_IF_ERROR(status);

    for (uint32_t seg_idx = 0; seg_idx < num_segments; seg_idx++) {
        RETURN_IF_ERROR(pk_iters[seg_idx]->status());
    }

    // Merge per-chunk results into per-segment output vectors.
    for (uint32_t seg_idx = 0; seg_idx < num_segments; seg_idx++) {
        auto& slots = per_segment_slots[seg_idx];
        size_t total = 0;
        if (!slots.empty()) {
            auto& last = slots.back();
            total = last->begin_rowid + last->count;
        }
        auto& output = (*rss_rowids_per_segment)[seg_idx];
        output.resize(total);
        for (auto& slot : slots) {
            memcpy(output.data() + slot->begin_rowid, slot->values.data(), slot->count * sizeof(uint64_t));
        }
    }

    return Status::OK();
}

// Update index with new primary keys from all segments.
// This is used during write operations (non-read-only publish) to insert/update index entries.
//
// Parameters:
// - token: Thread pool token for parallel execution. If null, executes serially.
// - rssid: RowSet Segment ID, identifies the rowset being processed.
// - segment_pk_iterator: Iterator over all segments containing primary keys to upsert.
// - new_deletes: Output map to store rows that need to be marked as deleted.
//
// Parallel Execution:
// - If token is set, submits each segment as a separate task to the thread pool
// - Otherwise, processes each segment inline (serial mode)
// - Waits for all tasks to complete before returning
// - After all tasks finish, flushes accumulated updates to sstable file
//
// Thread Safety:
// - Each parallel task gets its own slot with independent pk_column storage
// - Errors are accumulated in shared status under mutex protection
// - Function returns error status after checking all tasks have completed
//
// Note: Unlike parallel_get which is read-only, this writes to the index memtable
Status LakePrimaryIndex::parallel_upsert(ThreadPoolToken* token, uint32_t rssid, SegmentPKIterator* segment_pk_iterator,
                                         DeletesMap* new_deletes) {
    // Prepare parallel execution infrastructure if enabled
    std::mutex mutex; // Protects shared state (deletes, status) during parallel execution
    Status status = Status::OK();

    // Setup context shared across all parallel tasks
    ParallelPublishContext context{.token = token, .mutex = &mutex, .deletes = new_deletes, .status = &status};

    // Process each segment in the iterator. Each chunk's absolute physical
    // rowid is current.physical_rowid_offset + i_in_chunk (see SegmentPKChunkRef).
    for (; !segment_pk_iterator->done(); segment_pk_iterator->next()) {
        auto current = segment_pk_iterator->current();
        if (token) {
            // Parallel mode: Allocate a slot for this task to store its pk_column
            context.extend_slots();
            auto slot = context.slots.back().get();

            // We can't return error directly, because we need to wait all previous tasks finish.
            // Instead, we accumulate errors in context->status for later checking.
            Status st = Status::OK();
            auto pk_column_st = segment_pk_iterator->encoded_pk_column(current.chunk.get());
            if (pk_column_st.ok()) {
                // Store pk_column in this task's slot to avoid data races
                slot->pk_column = std::move(pk_column_st.value());

                // Submit upsert task to thread pool. Pass nullptr for deletes since we collect
                // them in the context (not used for upsert, only for parallel_get)
                st = upsert(rssid, current.physical_rowid_offset, *slot->pk_column, nullptr /* stat */, &context);
                TRACE_COUNTER_INCREMENT("parallel_upsert_cnt", 1);
            } else {
                st = pk_column_st.status();
            }

            // Update shared status under mutex if error occurred
            if (!st.ok()) {
                std::lock_guard<std::mutex> l(*context.mutex);
                context.status->update(st);
            }
        } else {
            // Serial mode: Execute inline with direct error propagation
            ASSIGN_OR_RETURN(MutableColumnPtr pk_column, segment_pk_iterator->encoded_pk_column(current.chunk.get()));
            RETURN_IF_ERROR(upsert(rssid, current.physical_rowid_offset, *pk_column, context.deletes));
        }
    }
    // Synchronize parallel execution if enabled
    if (token) {
        TRACE_COUNTER_SCOPE_LATENCY_US("parallel_upsert_wait_us");
        token->wait(); // Wait for all submitted tasks to complete

        // Check for errors from parallel tasks
        RETURN_IF_ERROR(status);
        // Flush accumulated updates to sstable file (batch optimization)
        RETURN_IF_ERROR(flush_memtable());
    }
    return segment_pk_iterator->status();
}

} // namespace starrocks::lake
