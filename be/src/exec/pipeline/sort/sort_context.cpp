// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/sort/sort_context.h"

#include "column/vectorized_fwd.h"
#include "exec/vectorized/sorting/merge.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/chunk_cursor.h"

namespace starrocks::pipeline {

using vectorized::Permutation;
using vectorized::Columns;
using vectorized::SortedRuns;

void SortContext::close(RuntimeState* state) {
    _chunks_sorter_partitions.clear();
}

void SortContext::add_partition_chunks_sorter(std::shared_ptr<ChunksSorter> chunks_sorter) {
    _chunks_sorter_partitions.push_back(chunks_sorter);
}

void SortContext::finish_partition(uint64_t partition_rows) {
    _total_rows.fetch_add(partition_rows, std::memory_order_relaxed);
    _num_partition_finished.fetch_add(1, std::memory_order_release);
}

bool SortContext::is_partition_sort_finished() const {
    return _num_partition_finished.load(std::memory_order_acquire) == _num_partition_sinkers;
}

bool SortContext::is_output_finished() const {
    return is_partition_sort_finished() && _merger_inited && _required_rows == 0;
}

StatusOr<ChunkPtr> SortContext::pull_chunk() {
    _init_merger();

    while (_required_rows > 0 && !_merger.is_eos()) {
        if (_current_chunk.empty()) {
            auto chunk = _merger.try_get_next();
            // Input cursor maye short circuit
            if (!chunk) {
                _required_rows = 0;
                return nullptr;
            }
            _current_chunk.reset(std::move(chunk));
        }

        // Skip some rows before return it
        if (_offset > 0) {
            _offset -= _current_chunk.skip(_offset);
            if (_current_chunk.empty()) {
                continue;
            }
        }

        // Return required rows and cutoff the big chunk
        size_t required_rows = std::min<size_t>(_required_rows, _state->chunk_size());
        ChunkPtr res = _current_chunk.cutoff(required_rows);
        _required_rows -= res->num_rows();
        return res;
    }
    return nullptr;
}

Status SortContext::_init_merger() {
    if (_merger_inited) {
        return Status::OK();
    }

    // Keep all the data if topn type is RANK or DENSE_RANK
    _required_rows = _total_rows;
    if (_topn_type == TTopNType::ROW_NUMBER) {
        _required_rows = ((_limit < 0) ? _total_rows.load() : std::min<int64_t>(_limit + _offset, _total_rows));
    }

    std::vector<SortedRuns> partial_sorted_runs;
    for (int i = 0; i < _num_partition_sinkers; ++i) {
        auto& partition_sorter = _chunks_sorter_partitions[i];
        partial_sorted_runs.push_back(partition_sorter->get_sorted_runs());
    }
    _partial_cursors.reserve(_num_partition_sinkers);
    for (int i = 0; i < _num_partition_sinkers; i++) {
        vectorized::ChunkProvider provider = [i, this](vectorized::ChunkUniquePtr* out_chunk, bool* eos) -> bool {
            // data ready
            if (out_chunk == nullptr || eos == nullptr) {
                return true;
            }
            auto& partition_sorter = _chunks_sorter_partitions[i];
            ChunkPtr chunk;
            Status st = partition_sorter->get_next(&chunk, eos);
            if (!st.ok() || *eos || chunk == nullptr) {
                return false;
            }
            *out_chunk = chunk->clone_unique();
            return true;
        };
        _partial_cursors.push_back(
                std::make_unique<vectorized::SimpleChunkSortCursor>(std::move(provider), &_sort_exprs));
    }

    RETURN_IF_ERROR(_merger.init(_sort_desc, std::move(_partial_cursors)));
    CHECK(_merger.is_data_ready()) << "data must be ready";
    _merger_inited = true;

    return Status::OK();
}

SortContextFactory::SortContextFactory(RuntimeState* state, const TTopNType::type topn_type, bool is_merging,
                                       int64_t offset, int64_t limit, int32_t num_right_sinkers,
                                       const std::vector<ExprContext*>& sort_exprs,
                                       const std::vector<bool>& is_asc_order, const std::vector<bool>& is_null_first)
        : _state(state),
          _topn_type(topn_type),
          _is_merging(is_merging),
          _sort_contexts(is_merging ? 1 : num_right_sinkers),
          _offset(offset),
          _limit(limit),
          _num_right_sinkers(num_right_sinkers),
          _sort_exprs(sort_exprs),
          _sort_descs(is_asc_order, is_null_first) {}

SortContextPtr SortContextFactory::create(int32_t idx) {
    size_t actual_idx = _is_merging ? 0 : idx;
    int32_t num_sinkers = _is_merging ? _num_right_sinkers : 1;

    DCHECK_LE(actual_idx, _sort_contexts.size());
    if (!_sort_contexts[actual_idx]) {
        _sort_contexts[actual_idx] = std::make_shared<SortContext>(_state, _topn_type, _offset, _limit, num_sinkers,
                                                                   _sort_exprs, _sort_descs);
    }
    return _sort_contexts[actual_idx];
}
} // namespace starrocks::pipeline
