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
#include "base/utility/defer_op.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/parallel_task_runner.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/segment_pk_iterator.h"
#include "storage/lake/tablet.h"
#include "storage/parallel_upsert_context.h"

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

static void old_values_to_deletes(const std::vector<uint64_t>& old_values, DeletesMap* deletes) {
    for (uint64_t old : old_values) {
        if (old != NullIndexValue) {
            (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
        }
    }
}

Status LakePrimaryIndex::erase(const TabletMetadataPtr& metadata, const Column& pks, DeletesMap* deletes,
                               uint32_t del_rssid) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("erase on an unloaded lake primary index");
    }
    Buffer<Slice> keys;
    std::vector<uint64_t> old_values(pks.size(), NullIndexValue);
    ASSIGN_OR_RETURN(const Slice* vkeys,
                     PrimaryIndex::build_persistent_keys(pks, index->key_size(), 0, pks.size(), &keys));
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
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("bulk_erase requires a cloud-native LakePersistentIndex.");
    }
    Buffer<Slice> keys;
    std::vector<uint64_t> old_values(pks.size(), NullIndexValue);
    ASSIGN_OR_RETURN(const Slice* vkeys,
                     PrimaryIndex::build_persistent_keys(pks, index->key_size(), 0, pks.size(), &keys));
    RETURN_IF_ERROR(index->bulk_erase(pks.size(), vkeys, reinterpret_cast<IndexValue*>(old_values.data()), del_rssid,
                                      del_sst_meta, del_sst_range, version));
    old_values_to_deletes(old_values, deletes);
    return Status::OK();
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

// Upsert only the rows this tablet owns, each keyed to the rowid it has in the SOURCE segment
// rather than its position among the survivors. Cross publish only -- an ordinary publish carries an
// empty mask and takes the plain overload in parallel_upsert.
Status LakePrimaryIndex::upsert_owned(uint32_t rssid, const SegmentPKChunkRef& current, ParallelPublishSlot* slot,
                                      ParallelUpsertContext* context) {
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
    // Whether the replaced rowids reach `context` from here or from the deferred lookup task is
    // LakePersistentIndex::upsert's business, keyed on ParallelUpsertContext::defers_lookup().
    return upsert(rssid, owned_rowids, *slot->pk_column, slot, context);
}

Status LakePrimaryIndex::parallel_get(ThreadPoolToken* token, SegmentPKIterator* segment_pk_iterator,
                                      DeletesMap* new_deletes) {
    ParallelTaskRunner runner(token);
    // Drained into `new_deletes` under this mutex as each task finishes; the tasks themselves touch
    // disjoint slots, so this is the only shared state.
    std::mutex deletes_mutex;
    // One slot per chunk, owned here so the encoded key bytes outlive the task that reads them.
    std::vector<std::unique_ptr<ParallelPublishSlot>> slots;

    for (; !segment_pk_iterator->done(); segment_pk_iterator->next()) {
        auto current = segment_pk_iterator->current();
        slots.push_back(std::make_unique<ParallelPublishSlot>());
        auto* slot = slots.back().get();

        runner.run([this, current, slot, segment_pk_iterator, new_deletes, &deletes_mutex]() -> Status {
            ASSIGN_OR_RETURN(slot->pk_column, segment_pk_iterator->encoded_pk_column(current.chunk.get()));
            // Drop the siblings' keys first. Their old locations would otherwise reach
            // old_values_to_deletes below and be marked deleted in THIS child's delvec, and the
            // parent view ORs the children's delvecs -- so a sibling's lookup would erase a row
            // its owner kept. Only the values matter downstream, never their positions, so
            // filtering in place needs no index mapping.
            if (!current.owned.empty()) {
                slot->pk_column->filter(current.owned);
            }
            if (slot->pk_column->empty()) {
                return Status::OK();
            }
            slot->old_values.resize(slot->pk_column->size(), NullIndexValue);
            RETURN_IF_ERROR(get(*slot->pk_column, &slot->old_values));
            std::lock_guard<std::mutex> l(deletes_mutex);
            old_values_to_deletes(slot->old_values, new_deletes);
            return Status::OK();
        });
        if (token != nullptr) {
            TRACE_COUNTER_INCREMENT("parallel_get_cnt", 1);
        }
    }

    {
        TRACE_COUNTER_SCOPE_LATENCY_US("parallel_get_wait_us");
        RETURN_IF_ERROR(runner.join());
    }
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

    ParallelTaskRunner runner(token);
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

            // Each task writes only its own slot, so there is no shared state to guard.
            runner.run([this, slot_ptr, current = std::move(current), pk_iter]() -> Status {
                ASSIGN_OR_RETURN(auto pk_column, pk_iter->encoded_pk_column(current.chunk.get()));
                slot_ptr->values.resize(slot_ptr->count, NullIndexValue);
                return get(*pk_column, &slot_ptr->values);
            });
            if (token != nullptr) {
                TRACE_COUNTER_INCREMENT("batch_parallel_get_rss_rowids_cnt", 1);
            }
        }
    }

    {
        TRACE_COUNTER_SCOPE_LATENCY_US("batch_parallel_get_rss_rowids_wait_us");
        RETURN_IF_ERROR(runner.join());
    }

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

// Insert or update this rowset's primary keys, collecting the rowids they replace into
// `new_deletes`. Used by every non-read-only publish.
//
// Unlike parallel_get this writes to the index memtable, so the two halves split: the memtable write
// stays on this thread (it is not safe for concurrent writes, and it fixes the upsert's order
// against the deletes around it), and only the read half -- resolving what those keys previously
// mapped to -- is deferred. See LakePersistentIndex::upsert.
Status LakePrimaryIndex::parallel_upsert(ThreadPoolToken* token, uint32_t rssid, SegmentPKIterator* segment_pk_iterator,
                                         DeletesMap* new_deletes) {
    // A null token means no runner in the context, which makes each upsert resolve and flush inline.
    // That is deliberate: a large serial upsert would otherwise grow the memtable unbounded, since
    // nobody would be joining and flushing at the end.
    ParallelTaskRunner runner(token);
    ParallelUpsertContext context(token != nullptr ? &runner : nullptr, new_deletes);
    // One slot per chunk. The slot owns the encoded key bytes the index keeps referencing until the
    // deferred lookup has run, so they all stay alive until the join below.
    std::vector<std::unique_ptr<ParallelPublishSlot>> slots;

    // Every exit from this function has to join first. The deferred lookups hold pointers into
    // `slots` and `context`, and those are destroyed BEFORE `runner` on scope exit -- reverse
    // declaration order -- so the runner destructor's own join would come too late. `context` needs
    // `runner` to exist before it, so the cycle is broken here instead: declared last, this runs
    // first. Covers the RETURN_IF_ERROR paths inside the loop below.
    DeferOp join_before_unwind([&] { (void)runner.join(); });

    // Each chunk's absolute physical rowid is current.physical_rowid_offset + i_in_chunk (see
    // SegmentPKChunkRef).
    for (; !segment_pk_iterator->done(); segment_pk_iterator->next()) {
        auto current = segment_pk_iterator->current();
        slots.push_back(std::make_unique<ParallelPublishSlot>());
        auto* slot = slots.back().get();
        ASSIGN_OR_RETURN(slot->pk_column, segment_pk_iterator->encoded_pk_column(current.chunk.get()));

        if (current.owned.empty()) {
            RETURN_IF_ERROR(upsert(rssid, current.physical_rowid_offset, *slot->pk_column, slot, &context));
        } else {
            RETURN_IF_ERROR(upsert_owned(rssid, current, slot, &context));
        }
        if (token != nullptr) {
            TRACE_COUNTER_INCREMENT("parallel_upsert_cnt", 1);
        }
    }

    if (context.defers_lookup()) {
        {
            TRACE_COUNTER_SCOPE_LATENCY_US("parallel_upsert_wait_us");
            RETURN_IF_ERROR(runner.join());
        }
        // Batched: one flush for the whole rowset instead of one per chunk.
        RETURN_IF_ERROR(flush_memtable());
    }
    return segment_pk_iterator->status();
}

// ---- Surface the publish path shares with the shared-nothing index -----------------------------
//
// These bodies used to live in PrimaryIndex, which marshalled the encoded keys into a Buffer<Slice>
// and then dispatched on whether a persistent index was present. A lake tablet always has one, so
// what is left is the marshalling plus a direct call.

Status LakePrimaryIndex::get(const Column& pks, std::vector<uint64_t>* rowids) const {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("get on an unloaded lake primary index");
    }
    Buffer<Slice> keys;
    ASSIGN_OR_RETURN(const Slice* vkeys,
                     PrimaryIndex::build_persistent_keys(pks, index->key_size(), 0, pks.size(), &keys));
    return index->get(pks.size(), vkeys, reinterpret_cast<IndexValue*>(rowids->data()));
}

Status LakePrimaryIndex::upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, uint32_t idx_begin,
                                uint32_t idx_end, DeletesMap* deletes) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("upsert on an unloaded lake primary index");
    }
    // No runner, so the lookup of the replaced rowids completes inside index->upsert(), which
    // appends them to the context. See ParallelUpsertContext.
    ParallelPublishSlot slot;
    ParallelUpsertContext ctx(/*runner=*/nullptr, deletes);
    const uint32_t n = idx_end - idx_begin;
    slot.values.reserve(n);
    slot.old_values.resize(n, NullIndexValue);
    ASSIGN_OR_RETURN(const Slice* vkeys,
                     PrimaryIndex::build_persistent_keys(pks, index->key_size(), idx_begin, idx_end, &slot.keys));
    const uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
    for (uint32_t i = idx_begin; i < idx_end; i++) {
        slot.values.emplace_back(base + i);
    }
    return index->upsert(n, vkeys, reinterpret_cast<IndexValue*>(slot.values.data()),
                         reinterpret_cast<IndexValue*>(slot.old_values.data()), /*stat=*/nullptr, &ctx);
}

Status LakePrimaryIndex::replace(uint32_t rssid, uint32_t rowid_start, const std::vector<uint32_t>& replace_indexes,
                                 const Column& pks) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("replace on an unloaded lake primary index");
    }
    Buffer<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(pks.size());
    const uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
    for (size_t i = 0; i < pks.size(); i++) {
        values.emplace_back(base + i);
    }
    ASSIGN_OR_RETURN(const Slice* vkeys,
                     PrimaryIndex::build_persistent_keys(pks, index->key_size(), 0, pks.size(), &keys));
    return index->replace(pks.size(), vkeys, reinterpret_cast<IndexValue*>(values.data()), replace_indexes);
}

Status LakePrimaryIndex::try_replace(uint32_t rssid, uint32_t rowid_start, const Column& pks, uint32_t max_src_rssid,
                                     std::vector<uint32_t>* failed) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("try_replace on an unloaded lake primary index");
    }
    Buffer<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(pks.size());
    const uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
    for (size_t i = 0; i < pks.size(); i++) {
        values.emplace_back(base + i);
    }
    ASSIGN_OR_RETURN(const Slice* vkeys,
                     PrimaryIndex::build_persistent_keys(pks, index->key_size(), 0, pks.size(), &keys));
    return index->try_replace(pks.size(), vkeys, reinterpret_cast<IndexValue*>(values.data()), max_src_rssid, failed);
}

Status LakePrimaryIndex::upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, ParallelPublishSlot* slot,
                                ParallelUpsertContext* ctx) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("upsert on an unloaded lake primary index");
    }
    const uint32_t n = pks.size();
    slot->values.reserve(n);
    slot->old_values.resize(n, NullIndexValue);
    ASSIGN_OR_RETURN(const Slice* vkeys,
                     PrimaryIndex::build_persistent_keys(pks, index->key_size(), 0, n, &slot->keys));
    const uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
    for (uint32_t i = 0; i < n; i++) {
        slot->values.emplace_back(base + i);
    }
    return index->upsert(n, vkeys, reinterpret_cast<IndexValue*>(slot->values.data()),
                         reinterpret_cast<IndexValue*>(slot->old_values.data()), /*stat=*/nullptr, ctx);
}

Status LakePrimaryIndex::upsert(uint32_t rssid, const std::vector<uint32_t>& rowids, const Column& pks,
                                ParallelPublishSlot* slot, ParallelUpsertContext* ctx) {
    auto* index = _index.get();
    if (index == nullptr) {
        return Status::InternalError("upsert on an unloaded lake primary index");
    }
    const uint32_t n = pks.size();
    DCHECK_EQ(rowids.size(), n);
    slot->values.reserve(n);
    slot->old_values.resize(n, NullIndexValue);
    ASSIGN_OR_RETURN(const Slice* vkeys,
                     PrimaryIndex::build_persistent_keys(pks, index->key_size(), 0, n, &slot->keys));
    const uint64_t base = ((uint64_t)rssid) << 32;
    for (uint32_t i = 0; i < n; i++) {
        slot->values.emplace_back(base + rowids[i]);
    }
    return index->upsert(n, vkeys, reinterpret_cast<IndexValue*>(slot->values.data()),
                         reinterpret_cast<IndexValue*>(slot->old_values.data()), /*stat=*/nullptr, ctx);
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
