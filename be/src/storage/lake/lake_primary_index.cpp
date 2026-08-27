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
#include "storage/chunk_helper.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/segment_pk_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/persistent_index_parallel_publish_context.h"

namespace starrocks::lake {

// A cross-published chunk carries this tablet's rows AND its siblings'. SegmentPKChunkRef::owned
// marks which are ours, as a mask over the chunk rather than a filtered copy, so row i keeps its
// source-segment rowid physical_rowid_offset + i. These two turn that mask into the shapes the
// primary index takes, and neither is entered at all on an ordinary publish, where `owned` is empty
// and every row belongs here.

// Absolute source-segment rowids of the owned rows, in chunk order. Must be read off the mask BEFORE
// the column is filtered -- filtering renumbers the survivors and the correspondence is lost.
std::vector<uint32_t> owned_rowids_of(const SegmentPKChunkRef& current) {
    std::vector<uint32_t> rowids;
    rowids.reserve(current.owned.size());
    for (size_t i = 0; i < current.owned.size(); ++i) {
        if (current.owned[i]) {
            rowids.push_back(current.physical_rowid_offset + static_cast<uint32_t>(i));
        }
    }
    return rowids;
}

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
    auto index = std::make_shared<LakePersistentIndex>(tablet_mgr, metadata->id());
    _persistent_index = index;
    RETURN_IF_ERROR(index->init(metadata));
    return index->load_from_lake_tablet(tablet_mgr, metadata, base_version, builder);
}

// The cloud-native index this wrapper delegates to, or nullptr when the index is not loaded.
//
// The downcast is unconditional rather than checked: _do_lake_load is the only place that builds
// _persistent_index for a shared-data tablet and it always builds a LakePersistentIndex, because
// force_cloud_native_pk_persistent_index() normalizes every PK tablet's metadata to
// enabled + CLOUD_NATIVE before any consumer sees it. Read through _persistent_index on every call
// instead of caching the pointer -- unload_without_lock() resets it under _lock.
LakePersistentIndex* LakePrimaryIndex::_lake_index() const {
    DCHECK(_persistent_index == nullptr || dynamic_cast<LakePersistentIndex*>(_persistent_index.get()) != nullptr);
    return static_cast<LakePersistentIndex*>(_persistent_index.get());
}

Status LakePrimaryIndex::apply_opcompaction(const TabletMetadataPtr& metadata,
                                            const TxnLogPB_OpCompaction& op_compaction) {
    auto* index = _lake_index();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->apply_opcompaction(metadata, op_compaction);
}

Status LakePrimaryIndex::ingest_sst(const FileMetaPB& sst_meta, const PersistentIndexSstableRangePB& sst_range,
                                    uint32_t rssid, int64_t version, const DelvecPagePB& delvec_page,
                                    DelVectorPtr delvec) {
    auto* index = _lake_index();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->ingest_sst(sst_meta, sst_range, rssid, version, delvec_page, std::move(delvec));
}

Status LakePrimaryIndex::commit(const TabletMetadataPtr& metadata, MetaFileBuilder* builder,
                                int64_t generation_version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("primary_index_commit_latency_us");
    auto* index = _lake_index();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->commit(builder, generation_version);
}

Status LakePrimaryIndex::sync_flush_persistent_index(int64_t wait_timeout_us) {
    auto* index = _lake_index();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->sync_flush_all_memtables(wait_timeout_us);
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
    auto* index = _lake_index();
    if (index == nullptr) {
        // Index not loaded: fall back to the base in-memory erase, which has no rebuild point.
        return PrimaryIndex::erase(pks, deletes);
    }
    Buffer<Slice> keys;
    std::vector<uint64_t> old_values(pks.size(), NullIndexValue);
    ASSIGN_OR_RETURN(const Slice* vkeys, build_persistent_keys(pks, _key_size, 0, pks.size(), &keys));
    // Cloud native index needs the delete's rssid as the rebuild point when erasing.
    RETURN_IF_ERROR(index->erase(pks.size(), vkeys, reinterpret_cast<IndexValue*>(old_values.data()), del_rssid));
    old_values_to_deletes(old_values, deletes);
    return Status::OK();
}

Status LakePrimaryIndex::bulk_erase(const TabletMetadataPtr& metadata, const Column& pks, DeletesMap* deletes,
                                    uint32_t del_rssid, const FileMetaPB& del_sst_meta,
                                    const PersistentIndexSstableRangePB& del_sst_range, int64_t version) {
    // Unlike erase(), there is no in-memory fallback: a pre-built tombstone sstable can only be
    // ingested by the cloud-native index, so an unloaded index is a broken invariant, not a fallback.
    auto* index = _lake_index();
    if (index == nullptr) {
        return Status::InternalError("bulk_erase requires a cloud-native LakePersistentIndex.");
    }
    Buffer<Slice> keys;
    std::vector<uint64_t> old_values(pks.size(), NullIndexValue);
    ASSIGN_OR_RETURN(const Slice* vkeys, build_persistent_keys(pks, _key_size, 0, pks.size(), &keys));
    RETURN_IF_ERROR(index->bulk_erase(pks.size(), vkeys, reinterpret_cast<IndexValue*>(old_values.data()), del_rssid,
                                      del_sst_meta, del_sst_range, version));
    old_values_to_deletes(old_values, deletes);
    return Status::OK();
}

int32_t LakePrimaryIndex::current_fileset_index() const {
    auto* index = _lake_index();
    return index != nullptr ? index->current_fileset_index() : -1;
}

StatusOr<AsyncCompactCBPtr> LakePrimaryIndex::early_sst_compact(
        lake::LakePersistentIndexParallelCompactMgr* compact_mgr, TabletManager* tablet_mgr,
        const TabletMetadataPtr& metadata, int32_t fileset_start_idx) {
    auto* index = _lake_index();
    if (index == nullptr) {
        return nullptr;
    }
    return index->early_sst_compact(compact_mgr, tablet_mgr, metadata, fileset_start_idx);
}

Status LakePrimaryIndex::flush_memtable(bool force) {
    auto* index = _lake_index();
    if (index == nullptr) {
        return Status::OK();
    }
    return index->flush_memtable(force);
}

void LakePrimaryIndex::reset_publish_sst_stats() {
    auto* index = _lake_index();
    if (index != nullptr) index->reset_publish_sst_stats();
}

int32_t LakePrimaryIndex::publish_sst_flush_count() const {
    auto* index = _lake_index();
    return index != nullptr ? index->publish_sst_flush_count() : 0;
}

int64_t LakePrimaryIndex::publish_sst_flush_bytes() const {
    auto* index = _lake_index();
    return index != nullptr ? index->publish_sst_flush_bytes() : 0;
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
Status LakePrimaryIndex::upsert_owned(uint32_t rssid, const SegmentPKChunkRef& current, ParallelPublishSlot* slot,
                                      ParallelPublishContext* context) {
    DCHECK(!current.owned.empty());
    // Off the mask before filtering: filtering renumbers what survives, and these have to stay the
    // rows' positions in the source segment.
    auto owned_rowids = owned_rowids_of(current);
    slot->pk_column->filter(current.owned);
    if (owned_rowids.empty()) {
        return Status::OK();
    }
    // One call for the whole chunk, never one per contiguous run of owned rows: ownership of a
    // segment ordered by a separate sort key is scattered row by row, so runs degenerate towards one
    // per row and each call redoes the inactive-memtable and SST lookup and the flush check.
    RETURN_IF_ERROR(upsert(rssid, owned_rowids, *slot->pk_column, nullptr /* stat */, context));
    if (context->token == nullptr) {
        // With no token LakePersistentIndex::upsert resolves the replaced locations inline but does
        // not drain them into context->deletes -- in the parallel case that is the submitted lambda's
        // job, so it only happens there.
        old_values_to_deletes(slot->old_values, context->deletes);
    }
    return Status::OK();
}

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
                // Drop the siblings' keys first. Their old locations would otherwise reach
                // old_values_to_deletes below and be marked deleted in THIS child's delvec, and the
                // parent view ORs the children's delvecs -- so a sibling's lookup would erase a row
                // its owner kept. Only the values matter downstream, never their positions, so
                // filtering in place needs no index mapping.
                if (!current.owned.empty()) {
                    slot->pk_column->filter(current.owned);
                }
                slot->old_values.resize(slot->pk_column->size(), NullIndexValue);
                st = slot->pk_column->empty() ? Status::OK() : get(*slot->pk_column, &slot->old_values);
            } else {
                st = pk_column_st.status();
            }

            // Update shared state under lock
            std::lock_guard<std::mutex> l(*context_ptr->mutex);
            context_ptr->status->update(st);

            if (context_ptr->status->ok()) {
                old_values_to_deletes(slot->old_values, context_ptr->deletes);
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
                                                       std::vector<std::unique_ptr<SegmentPKIterator>>& pk_iters,
                                                       std::vector<std::vector<uint64_t>>* rss_rowids_per_segment,
                                                       std::vector<Filter>* owned_per_segment) {
    const uint32_t num_segments = pk_iters.size();
    rss_rowids_per_segment->resize(num_segments);
    if (owned_per_segment != nullptr) {
        owned_per_segment->assign(num_segments, Filter{});
    }

    struct RssRowidSlot {
        size_t begin_rowid = 0;
        size_t count = 0;
        std::vector<uint64_t> values;
        // Moved off the chunk ref before the lookup task runs, so the merge below can concatenate the
        // segment's mask in chunk order. Empty when this chunk carried none.
        Filter owned;
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
            slot->owned = current.owned;
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
        if (owned_per_segment == nullptr) {
            continue;
        }
        // Only materialize a mask for a segment that actually has one; leaving it empty is what tells
        // the consumer "own every row", and is what every non-cross publish produces.
        const bool has_mask =
                std::any_of(slots.begin(), slots.end(), [](const auto& slot) { return !slot->owned.empty(); });
        if (!has_mask) {
            continue;
        }
        auto& owned_output = (*owned_per_segment)[seg_idx];
        owned_output.assign(total, 1);
        for (auto& slot : slots) {
            if (slot->owned.empty()) {
                continue;
            }
            memcpy(owned_output.data() + slot->begin_rowid, slot->owned.data(), slot->count * sizeof(uint8_t));
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

                if (!current.owned.empty()) {
                    st = upsert_owned(rssid, current, slot, &context);
                } else {
                    // Submit upsert task to thread pool. Pass nullptr for deletes since we collect
                    // them in the context (not used for upsert, only for parallel_get)
                    st = upsert(rssid, current.physical_rowid_offset, *slot->pk_column, nullptr /* stat */, &context);
                }
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
            if (current.owned.empty()) {
                // No slot here, unlike the branch below: this overload takes the DeletesMap directly
                // and knows nothing about a context, so it buffers internally. Only the rowid-vector
                // overload needs one -- it was written for the parallel path, where the scratch has
                // to outlive the call in storage the context owns.
                RETURN_IF_ERROR(upsert(rssid, current.physical_rowid_offset, *pk_column, context.deletes));
            } else {
                // A fresh slot per chunk, as in the parallel branch. The scratch inside it is
                // append-only (build_persistent_keys and _build_persistent_values both emplace_back,
                // and old_values is resized without re-initialising what is already there), so
                // reusing one would need explicit clearing; a new slot is simply always empty.
                context.extend_slots();
                auto* slot = context.slots.back().get();
                slot->pk_column = std::move(pk_column);
                RETURN_IF_ERROR(upsert_owned(rssid, current, slot, &context));
            }
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
