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

#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cache/scan/shared_buffered_input_stream.h"
#include "common/global_types.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_read_order_ctx.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/group_reader.h"
#include "storage/primitive/range.h"

namespace starrocks::parquet {

class LazyMaterializationContext;
class ReadRangePlanner;

class ColumnMaterializer {
public:
    using ColumnReaderMap = std::unordered_map<SlotId, std::unique_ptr<ColumnReader>>;

    ColumnMaterializer(const GroupReaderParam& param, ColumnReaderMap* column_readers);

    ReadRangePlanner* read_range_planner() const { return _read_range_planner.get(); }
    HdfsScanStats* stats() const { return _param.stats; }

    void clear_classification();
    void add_active_column(int col_idx);
    void add_lazy_column(int col_idx);
    void add_active_slot(SlotId slot_id) { _active_slot_ids.push_back(slot_id); }
    void promote_lazy_to_active();
    void rebuild_read_order_ctx();

    // Classify physical read_cols as active/lazy and populate dict-filter /
    // post-read conjunct buckets.  _deferred_source_slots_ are variant-backed
    // slots whose conjuncts are deferred until after projection.
    void classify_columns(const std::unordered_set<SlotId>& deferred_source_slots, bool* out_has_reserved_field_filter);

    const std::vector<int>& active_column_indices() const { return _active_column_indices; }
    const std::vector<int>& lazy_column_indices() const { return _lazy_column_indices; }
    std::vector<int>& mutable_active_column_indices() { return _active_column_indices; }
    std::vector<int>& mutable_lazy_column_indices() { return _lazy_column_indices; }
    std::vector<SlotId>& mutable_active_slot_ids() { return _active_slot_ids; }
    const std::vector<SlotId>& active_slot_ids() const { return _active_slot_ids; }
    const std::vector<SlotId>& lazy_slot_ids() const { return _lazy_slot_ids; }
    ColumnReadOrderCtx* read_order_ctx() const { return _column_read_order_ctx.get(); }
    size_t min_round_cost() const {
        return _column_read_order_ctx == nullptr ? 0 : _column_read_order_ctx->get_min_round_cost();
    }

    void add_dict_filter_column(int col_idx, std::vector<std::string>& sub_field_path);
    void add_post_read_conjunct(SlotId slot_id, ExprContext* ctx);
    bool has_predicate_filter() const { return !_dict_column_indices.empty() || !_post_read_conjuncts_by_slot.empty(); }
    size_t dict_filter_column_count() const { return _dict_column_indices.size(); }
    const std::vector<int>& dict_column_indices() const { return _dict_column_indices; }
    const std::unordered_map<int, std::vector<std::vector<std::string>>>& dict_column_sub_field_paths() const {
        return _dict_column_sub_field_paths;
    }
    const std::unordered_map<SlotId, std::vector<ExprContext*>>& post_read_conjuncts_by_slot() const {
        return _post_read_conjuncts_by_slot;
    }

    // Read chunk lifecycle (owned by this materializer)
    Status init_read_chunk();
    const ChunkPtr& read_chunk() const { return _read_chunk; }
    // Mutable access for unit tests that bypass init_read_chunk().
    ChunkPtr& mutable_read_chunk() { return _read_chunk; }
    void reset_read_chunk() {
        if (_read_chunk) _read_chunk->reset();
        _slot_cache.clear();
    }

    ChunkPtr create_active_chunk() const;
    ChunkPtr create_lazy_chunk() const;

    Status read_slot(SlotId slot_id, const Range<uint64_t>& range, const Filter* filter, ChunkPtr* chunk);

    Status read_range(const std::vector<int>& read_columns, const Range<uint64_t>& range, const Filter* filter,
                      ChunkPtr* chunk, bool ignore_reserved_field = false);
    Status read_active_range(const Range<uint64_t>& range, const Filter* filter, ChunkPtr* chunk);
    Status read_lazy_range(const Range<uint64_t>& range, const Filter* filter, ChunkPtr* chunk);
    // read_active_range_round_by_round accepts an optional LazyMaterializationContext*
    // as a forward-looking parameter for Phase 6 expression-driven materialization.
    StatusOr<size_t> read_active_range_round_by_round(const Range<uint64_t>& range, Filter* filter, ChunkPtr* chunk,
                                                      LazyMaterializationContext* lazy_ctx = nullptr);

    Status rewrite_dict_conjuncts_to_predicate(bool* is_group_filtered);
    Status filter_dict_column(SlotId slot_id, ColumnPtr& column, Filter* filter,
                              const std::vector<std::string>& sub_field_path, const size_t& layer);
    StatusOr<size_t> eval_slot_conjuncts(const std::vector<ExprContext*>& ctxs, SlotId slot_id, ChunkPtr* chunk,
                                         Filter* filter);

    Status fill_dst_column(SlotId slot_id, ColumnPtr& dst, ColumnPtr& src);

    // Per-slot cache scoped to the current get_next() range.  Populated by
    // read_slot(); cleared by reset_read_chunk().  Avoids repeated Parquet
    // I/O when the same slot is referenced by multiple expression branches.
    struct SlotCacheEntry {
        ColumnPtr values;
    };
    const SlotCacheEntry* get_slot_cache(SlotId slot_id) const {
        auto it = _slot_cache.find(slot_id);
        return it != _slot_cache.end() ? &it->second : nullptr;
    }

    // On-demand single-slot materialization.  Reads the slot through its
    // ColumnReader into _read_chunk and populates _slot_cache.  Returns OK
    // (no-op) if the slot is already cached for the current range.
    Status materialize_slot(SlotId slot_id, const Range<uint64_t>& range, const Filter* filter);

    // Post-filter lazy-column backfill.
    // chunk_filter has full_range.span_size() entries and is used to apply the
    // correct filter to any lazy columns that were already triggered (via
    // LazyMaterializationContext) during predicate evaluation.
    Status read_lazy_columns(const Range<uint64_t>& full_range, const Range<uint64_t>& post_filter_range,
                             const Filter& post_filter, const Filter& chunk_filter, bool has_filter,
                             ChunkPtr& active_chunk);
    // Emit physical + reserved-field columns into dst. Skips slots listed in skip_slots.
    Status emit_physical_columns(ChunkPtr& active_chunk, ChunkPtr* dst,
                                 const std::unordered_set<SlotId>* skip_slots = nullptr);

    bool lazy_column_needed() const { return _lazy_column_needed; }
    void set_lazy_column_needed(bool v) { _lazy_column_needed = v; }

    void collect_io_ranges(std::vector<SharedBufferedInputStream::IORange>* ranges, int64_t* end,
                           ColumnIOTypeFlags types);

private:
    const GroupReaderParam& _param;
    ColumnReaderMap* _column_readers = nullptr;

    // Physical read column classification:
    // - _active_column_indices and _lazy_column_indices partition physical
    //   entries from GroupReaderParam::read_cols, excluding variant virtual
    //   projection slots that do not have their own physical reader.
    // - Active columns are read before predicate evaluation can finish; lazy
    //   columns are read only after row filtering has produced the final row set.
    // - *_slot_ids mirror those column-index sets for Chunk construction and
    //   may additionally include hidden variant source slots. Therefore slot-id
    //   lists are not a strict projection of read_cols.
    std::vector<int> _active_column_indices;
    std::vector<int> _lazy_column_indices;
    std::vector<SlotId> _active_slot_ids;
    std::vector<SlotId> _lazy_slot_ids;

    // Predicate classification refines active columns:
    // - _dict_column_indices is a subset of _active_column_indices. These
    //   columns have one or more conjuncts that ColumnReader can evaluate on
    //   dictionary values before full decode; subfield paths record which part
    //   of a complex value the dict predicate targets.
    // - _post_read_conjuncts_by_slot contains remaining conjuncts keyed by the
    //   slot they need. A slot may be a physical read_col, a reserved field, or
    //   a promoted variant virtual slot. These conjuncts run immediately after
    //   their slot's column is read into the working chunk.
    // - A conjunct belongs to exactly one of the dict-filter bucket or the
    //   post-read bucket; slots without predicates appear in neither bucket.
    std::vector<int> _dict_column_indices;
    std::unordered_map<int, std::vector<std::vector<std::string>>> _dict_column_sub_field_paths;
    std::unordered_map<SlotId, std::vector<ExprContext*>> _post_read_conjuncts_by_slot;

    ChunkPtr create_read_chunk(const std::vector<SlotId>& slot_ids, bool include_reserved_fields) const;

    bool _try_use_dict_filter(int col_idx, const GroupReaderParam::Column& column, ExprContext* ctx,
                              std::vector<std::string>& sub_field_path);

    std::unique_ptr<ColumnReadOrderCtx> _column_read_order_ctx;

    ChunkPtr _read_chunk;

    // Per-range slot cache: populated by read_slot(), cleared by reset_read_chunk().
    std::unordered_map<SlotId, SlotCacheEntry> _slot_cache;

    bool _lazy_column_needed = false;

    std::unique_ptr<ReadRangePlanner> _read_range_planner;
};

} // namespace starrocks::parquet
