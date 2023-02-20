// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/chunks_sorter_full_sort.h"

#include "column/column_helper.h"
#include "exec/vectorized/sorting/merge.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "exprs/expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

ChunksSorterFullSort::ChunksSorterFullSort(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                                           const std::vector<bool>* is_asc_order,
                                           const std::vector<bool>* is_null_first, const std::string& sort_keys,
                                           int64_t max_buffered_rows, int64_t max_buffered_bytes,
                                           const std::vector<SlotId>& eager_materialized_slots)
        : ChunksSorter(state, sort_exprs, is_asc_order, is_null_first, sort_keys, false),
          max_buffered_rows(static_cast<size_t>(max_buffered_rows)),
          max_buffered_bytes(max_buffered_bytes),
          _eager_materialized_slots(eager_materialized_slots.begin(), eager_materialized_slots.end()) {}

ChunksSorterFullSort::~ChunksSorterFullSort() = default;
void ChunksSorterFullSort::setup_runtime(starrocks::RuntimeProfile* profile, MemTracker* parent_mem_tracker) {
    ChunksSorter::setup_runtime(profile, parent_mem_tracker);
    _runtime_profile = profile;
    _parent_mem_tracker = parent_mem_tracker;
    _object_pool = std::make_unique<ObjectPool>();
    _runtime_profile->add_info_string("MaxBufferedRows", strings::Substitute("$0", max_buffered_rows));
    _runtime_profile->add_info_string("MaxBufferedBytes", strings::Substitute("$0", max_buffered_bytes));
    _profiler = _object_pool->add(new ChunksSorterFullSortProfiler(profile, parent_mem_tracker));
}
Status ChunksSorterFullSort::update(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_merge_unsorted(state, chunk));
    RETURN_IF_ERROR(_partial_sort(state, false));

    return Status::OK();
}

// Accumulate unsorted input chunks into a larger chunk
Status ChunksSorterFullSort::_merge_unsorted(RuntimeState* state, const ChunkPtr& chunk) {
    SCOPED_TIMER(_build_timer);
    _staging_unsorted_chunks.push_back(std::move(chunk));
    _staging_unsorted_rows += chunk->num_rows();
    _staging_unsorted_bytes += chunk->bytes_usage();
    return Status::OK();
}

template <typename BinaryColumnType>
static void reserve_memory(Column* dst_col, const std::vector<ChunkPtr>& src_chunks, size_t col_idx) {
    auto* binary_dst_col = down_cast<BinaryColumnType*>(dst_col);
    size_t total_num_bytes = 0;
    for (const auto& src_chk : src_chunks) {
        const auto* src_data_col = ColumnHelper::get_data_column(src_chk->get_column_by_index(col_idx).get());
        const auto* src_binary_col = down_cast<const BinaryColumnType*>(src_data_col);
        total_num_bytes += src_binary_col->get_bytes().size();
    }
    binary_dst_col->get_bytes().reserve(total_num_bytes);
}

static void concat_chunks(ChunkPtr& dst_chunk, const std::vector<ChunkPtr>& src_chunks, size_t num_rows) {
    DCHECK(!src_chunks.empty());
    dst_chunk = src_chunks.front()->clone_empty(num_rows);
    const auto num_columns = dst_chunk->num_columns();
    for (auto i = 0; i < num_columns; ++i) {
        auto dst_col = dst_chunk->get_column_by_index(i);
        auto* dst_data_col = ColumnHelper::get_data_column(dst_col.get());
        if (dst_data_col->is_binary()) {
            reserve_memory<BinaryColumn>(dst_data_col, src_chunks, i);
        } else if (dst_col->is_large_binary()) {
            reserve_memory<LargeBinaryColumn>(dst_data_col, src_chunks, i);
        }
    }
    for (const auto& src_chk : src_chunks) {
        dst_chunk->append(*src_chk);
    }
}
// Sort the large chunk
Status ChunksSorterFullSort::_partial_sort(RuntimeState* state, bool done) {
    if (!_staging_unsorted_rows) {
        return Status::OK();
    }
    bool reach_limit = _staging_unsorted_rows >= max_buffered_rows || _staging_unsorted_bytes >= max_buffered_bytes;
    if (done || reach_limit) {
        _max_num_rows = std::max<int>(_max_num_rows, _staging_unsorted_rows);
        _profiler->input_required_memory->update(_staging_unsorted_bytes);
        concat_chunks(_unsorted_chunk, _staging_unsorted_chunks, _staging_unsorted_rows);
        RETURN_IF_ERROR(_unsorted_chunk->upgrade_if_overflow());

        SCOPED_TIMER(_sort_timer);
        DataSegment segment(_sort_exprs, _unsorted_chunk);
        _sort_permutation.resize(0);
        RETURN_IF_ERROR(
                sort_and_tie_columns(state->cancelled_ref(), segment.order_by_columns, _sort_desc, &_sort_permutation));
        auto sorted_chunk = _unsorted_chunk->clone_empty_with_slot(_unsorted_chunk->num_rows());
        materialize_by_permutation(sorted_chunk.get(), {_unsorted_chunk}, _sort_permutation);
        RETURN_IF_ERROR(sorted_chunk->upgrade_if_overflow());

        _sorted_chunks.emplace_back(std::move(sorted_chunk));
        _total_rows += _unsorted_chunk->num_rows();
        _unsorted_chunk->reset();
        _staging_unsorted_rows = 0;
        _staging_unsorted_bytes = 0;
        _staging_unsorted_chunks.clear();
    }

    return Status::OK();
}

Status ChunksSorterFullSort::_merge_sorted(RuntimeState* state) {
    SCOPED_TIMER(_merge_timer);
    _profiler->num_sorted_runs->set((int64_t)_sorted_chunks.size());
    if (_eager_materialized_slots.empty() || _sorted_chunks.size() < 3) {
        _eager_materialized_slots.clear();
        _runtime_profile->add_info_string("LazyMaterialization", "false");
        RETURN_IF_ERROR(merge_sorted_chunks(_sort_desc, _sort_exprs, _sorted_chunks, &_merged_runs));
    } else {
        _runtime_profile->add_info_string("LazyMaterialization", "true");
        _split_lazy_and_eager_chunks();
        _assign_ordinals();
        RETURN_IF_ERROR(merge_sorted_chunks(_sort_desc, _sort_exprs, _eager_materialized_chunks, &_merged_runs));
    }

    return Status::OK();
}

void ChunksSorterFullSort::_assign_ordinals() {
    _chunk_idx_bits = (int)std::ceil(std::log2(_eager_materialized_chunks.size()));
    _chunk_idx_bits = std::max(1, _chunk_idx_bits);
    _offset_in_chunk_bits = (int)std::ceil(std::log2(_max_num_rows));
    _offset_in_chunk_bits = std::max(1, _offset_in_chunk_bits);
    auto use_64bit_ordinal = (_chunk_idx_bits + _offset_in_chunk_bits) > 32;
    _runtime_profile->add_info_string("LazyMaterializationUse64BitOrdinal",
                                      strings::Substitute("$0", use_64bit_ordinal));
    if (use_64bit_ordinal) {
        _assign_ordinals_tmpl<uint64_t>();
    } else {
        _assign_ordinals_tmpl<uint32_t>();
    }
}
TYPE_GUARD(OrdinalGuard, type_is_ordinal, uint32_t, uint64_t);

template <typename T, typename = OrdinalGuard<T>>
using OrdinalColumn = FixedLengthColumn<T>;

template <typename T>
void ChunksSorterFullSort::_assign_ordinals_tmpl() {
    static_assert(type_is_ordinal<T>, "T must be uint32_t or uint64_t");
    size_t chunk_idx = 0;
    for (auto& partial_sort_chunk : _eager_materialized_chunks) {
        if (partial_sort_chunk->is_empty()) {
            ++chunk_idx;
            continue;
        }
        size_t num_rows = partial_sort_chunk->num_rows();
        auto ordinal_column = OrdinalColumn<T>::create();
        auto& ordinal_data = down_cast<OrdinalColumn<T>*>(ordinal_column.get())->get_data();
        raw::make_room(&ordinal_data, num_rows);
        for (T off = 0; off < num_rows; ++off) {
            ordinal_data[off] = static_cast<T>((chunk_idx << _offset_in_chunk_bits) | off);
        }
        partial_sort_chunk->append_column(ordinal_column, ORDINAL_COLUMN_SLOT_ID);
        ++chunk_idx;
    }
}

void ChunksSorterFullSort::_split_lazy_and_eager_chunks() {
    _eager_materialized_chunks.reserve(_sorted_chunks.size());
    _lazy_materialized_chunks.reserve(_sorted_chunks.size());
    auto& slot_id_to_column_id = _sorted_chunks[0]->get_slot_id_to_index_map();
    _column_id_to_slot_id.resize(slot_id_to_column_id.size());
    for (auto it : _sorted_chunks[0]->get_slot_id_to_index_map()) {
        _column_id_to_slot_id[it.second] = it.first;
    }
    for (auto& chunk : _sorted_chunks) {
        auto eager_chunk = std::make_unique<Chunk>();
        auto lazy_chunk = std::make_unique<Chunk>();
        for (auto column_id = 0; column_id < chunk->num_columns(); ++column_id) {
            auto slot_id = _column_id_to_slot_id[column_id];
            auto column = chunk->columns()[column_id];
            if (_eager_materialized_slots.count(slot_id)) {
                eager_chunk->append_column(column, slot_id);
            } else {
                lazy_chunk->append_column(column, slot_id);
            }
        }
        _eager_materialized_chunks.push_back(std::move(eager_chunk));
        _lazy_materialized_chunks.push_back(std::move(lazy_chunk));
    }
    _sorted_chunks.clear();
}

ChunkPtr ChunksSorterFullSort::_lazy_materialize(const ChunkPtr& chunk) {
    auto use_64bit_ordinal = (_chunk_idx_bits + _offset_in_chunk_bits) > 32;
    if (use_64bit_ordinal) {
        return _lazy_materialize_tmpl<uint64_t>(chunk);
    } else {
        return _lazy_materialize_tmpl<uint32_t>(chunk);
    }
}

template <typename T>
ChunkPtr ChunksSorterFullSort::_lazy_materialize_tmpl(const ChunkPtr& sorted_eager_chunk) {
    static_assert(type_is_ordinal<T>, "T must be uint32_t or uint64_t");
    const auto num_rows = sorted_eager_chunk->num_rows();
    auto sorted_lazy_chunk = _lazy_materialized_chunks[0]->clone_empty(num_rows);
    auto ordinal_column = sorted_eager_chunk->get_column_by_slot_id(ORDINAL_COLUMN_SLOT_ID);
    auto& ordinal_data = down_cast<OrdinalColumn<T>*>(ordinal_column.get())->get_data();
    T _offset_in_chunk_mask = static_cast<T>((1L << _offset_in_chunk_bits) - 1);
    for (auto i = 0; i < num_rows; ++i) {
        T ordinal = ordinal_data[i];
        T chunk_idx = ordinal >> _offset_in_chunk_bits;
        T off_in_chunk = ordinal & _offset_in_chunk_mask;
        sorted_lazy_chunk->append(*_lazy_materialized_chunks[chunk_idx], off_in_chunk, 1);
    }
    auto final_chunk = std::make_shared<Chunk>();
    for (auto slot_id : _column_id_to_slot_id) {
        if (_eager_materialized_slots.count(slot_id)) {
            final_chunk->append_column(sorted_eager_chunk->get_column_by_slot_id(slot_id), slot_id);
        } else {
            final_chunk->append_column(sorted_lazy_chunk->get_column_by_slot_id(slot_id), slot_id);
        }
    }
    return final_chunk;
}

Status ChunksSorterFullSort::do_done(RuntimeState* state) {
    RETURN_IF_ERROR(_partial_sort(state, true));
    _sort_permutation = {};
    RETURN_IF_ERROR(_merge_sorted(state));

    return Status::OK();
}

Status ChunksSorterFullSort::get_next(ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_output_timer);
    if (_merged_runs.num_chunks() == 0) {
        *chunk = nullptr;
        *eos = true;
        return Status::OK();
    }
    size_t chunk_size = _state->chunk_size();
    SortedRun& run = _merged_runs.front();
    *chunk = run.steal_chunk(chunk_size);
    if (*chunk != nullptr) {
        if (!_eager_materialized_slots.empty()) {
            *chunk = _lazy_materialize(*chunk);
        }
        RETURN_IF_ERROR((*chunk)->downgrade());
    }
    if (run.empty()) {
        _merged_runs.pop_front();
    }
    *eos = false;
    return Status::OK();
}

size_t ChunksSorterFullSort::get_output_rows() const {
    return _merged_runs.num_rows();
}

int64_t ChunksSorterFullSort::mem_usage() const {
    return _merged_runs.mem_usage();
}

} // namespace starrocks::vectorized
