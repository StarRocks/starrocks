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

#include "chunks_sorter_full_sort.h"

#include "column/vectorized_fwd.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/new_sort.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "exprs/expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

ChunksSorterFullSort::ChunksSorterFullSort(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                                           const std::vector<bool>* is_asc_order,
                                           const std::vector<bool>* is_null_first, const std::string& sort_keys)
        : ChunksSorter(state, sort_exprs, is_asc_order, is_null_first, sort_keys, false) {}

ChunksSorterFullSort::~ChunksSorterFullSort() = default;

void ChunksSorterFullSort::setup_runtime(RuntimeState* state, RuntimeProfile* profile, MemTracker* parent_mem_tracker) {
    ChunksSorter::setup_runtime(state, profile, parent_mem_tracker);
    _runtime_profile = profile;
    _parent_mem_tracker = parent_mem_tracker;
    _object_pool = std::make_unique<ObjectPool>();
    _profiler = _object_pool->add(new ChunksSorterFullSortProfiler(profile, parent_mem_tracker));
}

Status ChunksSorterFullSort::update(RuntimeState* state, const ChunkPtr& chunk) {
    SCOPED_TIMER(_build_timer);
    _staging_unsorted_chunks.push_back(chunk);
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
    // Columns like FixedLengthColumn have already reserved memory when invoke Chunk::clone_empty(num_rows).
    dst_chunk = src_chunks.front()->clone_empty(num_rows);
    const auto num_columns = dst_chunk->num_columns();
    for (auto i = 0; i < num_columns; ++i) {
        auto dst_col = dst_chunk->get_column_by_index(i);
        auto* dst_data_col = ColumnHelper::get_data_column(dst_col.get());
        // Reserve memory room for bytes array in BinaryColumn here.
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
Status ChunksSorterFullSort::_partial_sort(RuntimeState* state) {
    if (!_staging_unsorted_rows) {
        return Status::OK();
    }

    COUNTER_UPDATE(_profiler->input_required_memory, _staging_unsorted_bytes);
    concat_chunks(_unsorted_chunk, _staging_unsorted_chunks, _staging_unsorted_rows);
    _staging_unsorted_chunks.clear();
    RETURN_IF_ERROR(_unsorted_chunk->upgrade_if_overflow());

    SCOPED_TIMER(_sort_timer);
    DataSegment segment(_sort_exprs, _unsorted_chunk);
    std::unique_ptr<InlinedData> inlined_data_ptr{};
    RETURN_IF_ERROR(inline_data(segment.order_by_columns, inlined_data_ptr));
    auto& inlined_data = *inlined_data_ptr;
    RETURN_IF_ERROR(sort(state->cancelled_ref(), segment.order_by_columns, inlined_data, _sort_desc));
    const size_t input_size = _unsorted_chunk->num_rows();
    const size_t output_chunk_size = state->chunk_size();
    const size_t chunk_count = (input_size + output_chunk_size - 1) / output_chunk_size;
    _sorted_chunks.resize(chunk_count);

    auto order_by_indexes = get_order_by_indexes(_unsorted_chunk->columns(), segment.order_by_columns);
    for (size_t i = 0; i < chunk_count; ++i) {
        const size_t range_begin = i * output_chunk_size;
        const size_t range_end = std::min(input_size, (i + 1) * output_chunk_size);
        ChunkUniquePtr sorted_chunk = _unsorted_chunk->clone_empty(range_end - range_begin);
        RETURN_IF_ERROR(materialize(*sorted_chunk, _unsorted_chunk, order_by_indexes, inlined_data,
                                    SortRange{range_begin, range_end}));
        _sorted_chunks.at(chunk_count - 1 - i) = std::move(sorted_chunk);
    }

    _total_rows += _unsorted_chunk->num_rows();
    _unsorted_chunk.reset();
    _staging_unsorted_rows = 0;
    _staging_unsorted_bytes = 0;

    return Status::OK();
}

Status ChunksSorterFullSort::do_done(RuntimeState* state) {
    RETURN_IF_ERROR(_partial_sort(state));
    _unsorted_chunk.reset();

    return Status::OK();
}

Status ChunksSorterFullSort::get_next(ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_output_timer);
    if (_sorted_chunks.empty()) {
        *chunk = nullptr;
        *eos = true;
        return Status::OK();
    }
    *chunk = std::move(_sorted_chunks.back());
    _sorted_chunks.pop_back();
    DCHECK(*chunk != nullptr);
    RETURN_IF_ERROR((*chunk)->downgrade());
    *eos = false;
    return Status::OK();
}

size_t ChunksSorterFullSort::get_output_rows() const {
    size_t num_rows = 0;
    for (size_t i = 0; i < _sorted_chunks.size(); ++i) {
        num_rows += _sorted_chunks.at(i)->num_rows();
    }
    return num_rows;
}

int64_t ChunksSorterFullSort::mem_usage() const {
    int64_t mem_usage = 0;
    for (size_t i = 0; i < _sorted_chunks.size(); ++i) {
        mem_usage += _sorted_chunks.at(i)->memory_usage();
    }
    return mem_usage;
}

} // namespace starrocks
