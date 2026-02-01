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

#include <bvar/bvar.h>

#include "base/testutil/sync_point.h"
#include "storage/chunk_helper.h"
#include "storage/lake/lake_local_persistent_index.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/local_pk_index_manager.h"
#include "storage/lake/rowset.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/tablet.h"
#include "storage/persistent_index_parallel_publish_context.h"
#include "storage/primary_key_encoder.h"
#include "storage/tablet_meta_manager.h"
#include "util/trace.h"

namespace starrocks::lake {

static bvar::LatencyRecorder g_load_pk_index_latency("lake_load_pk_index");

LakePrimaryIndex::~LakePrimaryIndex() {
    if (!_enable_persistent_index && _persistent_index != nullptr) {
        auto st = LocalPkIndexManager::clear_persistent_index(_tablet_id);
        LOG_IF(WARNING, !st.ok()) << "Fail to clear pk index from local disk: " << st.to_string();
    }
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
    _status = _do_lake_load(tablet_mgr, metadata, base_version, builder);
    TEST_SYNC_POINT_CALLBACK("lake_index_load.1", &_status);
    if (_status.ok()) {
        // update data version when memory index or persistent index load finish.
        _data_version = base_version;
    }
    _tablet_id = metadata->id();
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
    MonotonicStopWatch watch;
    watch.start();
    // 1. create and set key column schema
    std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
    vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    _set_schema(pkey_schema);

    // load persistent index if enable persistent index meta

    if (metadata->enable_persistent_index()) {
        DCHECK(_persistent_index == nullptr);

        switch (metadata->persistent_index_type()) {
        case PersistentIndexTypePB::LOCAL: {
            // Even if `enable_persistent_index` is enabled,
            // it may not take effect if is as compute node without any storage path.
            if (StorageEngine::instance()->get_persistent_index_store(metadata->id()) == nullptr) {
                LOG(WARNING) << "lake_persistent_index_type of LOCAL will not take effect when as cn without any "
                                "storage path";
                return Status::InternalError(
                        "lake_persistent_index_type of LOCAL will not take effect when as cn without any storage "
                        "path");
            }
            std::string path = strings::Substitute(
                    "$0/$1/",
                    StorageEngine::instance()->get_persistent_index_store(metadata->id())->get_persistent_index_path(),
                    metadata->id());

            RETURN_IF_ERROR(StorageEngine::instance()
                                    ->get_persistent_index_store(metadata->id())
                                    ->create_dir_if_path_not_exists(path));
            _persistent_index = std::make_shared<LakeLocalPersistentIndex>(path);
            set_enable_persistent_index(true);
            return dynamic_cast<LakeLocalPersistentIndex*>(_persistent_index.get())
                    ->load_from_lake_tablet(tablet_mgr, metadata, base_version, builder);
        }
        case PersistentIndexTypePB::CLOUD_NATIVE: {
            _persistent_index = std::make_shared<LakePersistentIndex>(tablet_mgr, metadata->id());
            set_enable_persistent_index(true);
            auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
            RETURN_IF_ERROR(lake_persistent_index->init(metadata));
            return lake_persistent_index->load_from_lake_tablet(tablet_mgr, metadata, base_version, builder);
        }
        default:
            return Status::InternalError("Unsupported lake_persistent_index_type " +
                                         PersistentIndexTypePB_Name(metadata->persistent_index_type()));
        }
    }

    OlapReaderStatistics stats;
    MutableColumnPtr pk_column;
    if (pk_columns.size() > 1) {
        // more than one key column
        RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
    }
    vector<uint32_t> rowids;
    rowids.reserve(4096);
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    // 2. scan all rowsets and segments to build primary index
    auto rowsets = Rowset::get_rowsets(tablet_mgr, metadata);
    // NOTICE: primary index will be builded by segment files in metadata, and delvecs.
    // The delvecs we need are stored in delvec file by base_version and current MetaFileBuilder's cache.
    for (auto& rowset : rowsets) {
        auto res = rowset->get_each_segment_iterator_with_delvec(pkey_schema, base_version, builder, &stats);
        if (!res.ok()) {
            return res.status();
        }
        auto& itrs = res.value();
        RETURN_ERROR_IF_FALSE(itrs.size() == rowset->num_segments(), "itrs.size != num_segments");
        for (size_t i = 0; i < itrs.size(); i++) {
            auto itr = itrs[i].get();
            if (itr == nullptr) {
                continue;
            }
            while (true) {
                chunk->reset();
                rowids.clear();
                auto st = itr->get_next(chunk, &rowids);
                if (st.is_end_of_file()) {
                    break;
                } else if (!st.ok()) {
                    return st;
                } else {
                    const Column* pkc = nullptr;
                    if (pk_column) {
                        pk_column->reset_column();
                        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                        pkc = pk_column.get();
                    } else {
                        pkc = chunk->columns()[0].get();
                    }
                    RETURN_IF_ERROR(insert(rowset->id() + i, rowids, *pkc));
                }
            }
            itr->close();
        }
    }
    auto cost_ns = watch.elapsed_time();
    g_load_pk_index_latency << cost_ns / 1000;
    LOG_IF(INFO, cost_ns >= /*10ms=*/10 * 1000 * 1000)
            << "LakePrimaryIndex load cost(ms): " << watch.elapsed_time() / 1000000;
    return Status::OK();
}

Status LakePrimaryIndex::apply_opcompaction(const TabletMetadata& metadata,
                                            const TxnLogPB_OpCompaction& op_compaction) {
    if (!_enable_persistent_index) {
        return Status::OK();
    }

    switch (metadata.persistent_index_type()) {
    case PersistentIndexTypePB::LOCAL: {
        return Status::OK();
    }
    case PersistentIndexTypePB::CLOUD_NATIVE: {
        auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
        if (lake_persistent_index != nullptr) {
            return lake_persistent_index->apply_opcompaction(op_compaction);
        } else {
            return Status::InternalError("Persistent index is not a LakePersistentIndex.");
        }
    }
    default:
        return Status::InternalError("Unsupported lake_persistent_index_type " +
                                     PersistentIndexTypePB_Name(metadata.persistent_index_type()));
    }
    return Status::OK();
}

Status LakePrimaryIndex::ingest_sst(const FileMetaPB& sst_meta, const PersistentIndexSstableRangePB& sst_range,
                                    uint32_t rssid, int64_t version, const DelvecPagePB& delvec_page,
                                    DelVectorPtr delvec) {
    if (!_enable_persistent_index) {
        return Status::OK();
    }

    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index != nullptr) {
        return lake_persistent_index->ingest_sst(sst_meta, sst_range, rssid, version, delvec_page, std::move(delvec));
    } else {
        return Status::InternalError("Persistent index is not a LakePersistentIndex.");
    }
}

Status LakePrimaryIndex::commit(const TabletMetadataPtr& metadata, MetaFileBuilder* builder) {
    TRACE_COUNTER_SCOPE_LATENCY_US("primary_index_commit_latency_us");
    if (!_enable_persistent_index) {
        return Status::OK();
    }

    switch (metadata->persistent_index_type()) {
    case PersistentIndexTypePB::LOCAL: {
        // only take affect in local persistent index
        PersistentIndexMetaPB index_meta;
        DataDir* data_dir = StorageEngine::instance()->get_persistent_index_store(_tablet_id);
        RETURN_IF_ERROR(TabletMetaManager::get_persistent_index_meta(data_dir, _tablet_id, &index_meta));
        RETURN_IF_ERROR(PrimaryIndex::commit(&index_meta));
        RETURN_IF_ERROR(TabletMetaManager::write_persistent_index_meta(data_dir, _tablet_id, index_meta));
        RETURN_IF_ERROR(on_commited());
        set_local_pk_index_write_amp_score(PersistentIndex::major_compaction_score(index_meta));
        // Call `on_commited` here, which will be safe to remove old files.
        // Because if version publishing fails after `on_commited`, index will be rebuild.
        return Status::OK();
    }
    case PersistentIndexTypePB::CLOUD_NATIVE: {
        auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
        if (lake_persistent_index != nullptr) {
            return lake_persistent_index->commit(builder);
        } else {
            return Status::InternalError("Persistent index is not a LakePersistentIndex.");
        }
    }
    default:
        return Status::InternalError("Unsupported lake_persistent_index_type " +
                                     PersistentIndexTypePB_Name(metadata->persistent_index_type()));
    }
    return Status::OK();
}

double LakePrimaryIndex::get_local_pk_index_write_amp_score() {
    if (!_enable_persistent_index) {
        return 0.0;
    }
    auto* local_persistent_index = dynamic_cast<LakeLocalPersistentIndex*>(_persistent_index.get());
    if (local_persistent_index != nullptr) {
        return local_persistent_index->get_write_amp_score();
    }
    return 0.0;
}

void LakePrimaryIndex::set_local_pk_index_write_amp_score(double score) {
    if (!_enable_persistent_index) {
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
                               uint32_t rowset_id) {
    // No need to setup rebuild point for in-memory index and local persistent index,
    // so keep using previous erase interface.
    if (!_enable_persistent_index) {
        return PrimaryIndex::erase(pks, deletes);
    }

    switch (metadata->persistent_index_type()) {
    case PersistentIndexTypePB::LOCAL: {
        return PrimaryIndex::erase(pks, deletes);
    }
    case PersistentIndexTypePB::CLOUD_NATIVE: {
        auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
        if (lake_persistent_index != nullptr) {
            std::vector<Slice> keys;
            std::vector<uint64_t> old_values(pks.size(), NullIndexValue);
            const Slice* vkeys = build_persistent_keys(pks, _key_size, 0, pks.size(), &keys);
            // Cloud native index need to setup rowset id as rebuild point when erase.
            RETURN_IF_ERROR(lake_persistent_index->erase(pks.size(), vkeys,
                                                         reinterpret_cast<IndexValue*>(old_values.data()), rowset_id));
            old_values_to_deletes(old_values, deletes);
            return Status::OK();
        } else {
            return Status::InternalError("Persistent index is not a LakePersistentIndex.");
        }
    }
    default:
        return Status::InternalError("Unsupported lake_persistent_index_type " +
                                     PersistentIndexTypePB_Name(metadata->persistent_index_type()));
    }
}

int32_t LakePrimaryIndex::current_fileset_index() const {
    if (!_enable_persistent_index) {
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
    if (!_enable_persistent_index) {
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
    if (!_enable_persistent_index) {
        return Status::OK();
    }

    auto* lake_persistent_index = dynamic_cast<LakePersistentIndex*>(_persistent_index.get());
    if (lake_persistent_index != nullptr) {
        return lake_persistent_index->flush_memtable(force);
    }

    return Status::OK();
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
            auto pk_column_st = segment_pk_iterator->encoded_pk_column(current.first.get());
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

    // Process each segment in the iterator
    for (; !segment_pk_iterator->done(); segment_pk_iterator->next()) {
        auto current = segment_pk_iterator->current();
        if (token) {
            // Parallel mode: Allocate a slot for this task to store its pk_column
            context.extend_slots();
            auto slot = context.slots.back().get();

            // We can't return error directly, because we need to wait all previous tasks finish.
            // Instead, we accumulate errors in context->status for later checking.
            Status st = Status::OK();
            auto pk_column_st = segment_pk_iterator->encoded_pk_column(current.first.get());
            if (pk_column_st.ok()) {
                // Store pk_column in this task's slot to avoid data races
                slot->pk_column = std::move(pk_column_st.value());

                // Submit upsert task to thread pool. Pass nullptr for deletes since we collect
                // them in the context (not used for upsert, only for parallel_get)
                st = upsert(rssid, current.second, *slot->pk_column, nullptr /* stat */, &context);
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
            ASSIGN_OR_RETURN(MutableColumnPtr pk_column, segment_pk_iterator->encoded_pk_column(current.first.get()));
            RETURN_IF_ERROR(upsert(rssid, current.second, *pk_column, context.deletes));
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
