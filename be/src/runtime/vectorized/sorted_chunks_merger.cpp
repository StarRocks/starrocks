// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "sorted_chunks_merger.h"

#include "exec/sort_exec_exprs.h"

namespace starrocks::vectorized {

SortedChunksMerger::SortedChunksMerger() {}

SortedChunksMerger::~SortedChunksMerger() {}

Status SortedChunksMerger::init(const ChunkSuppliers& suppliers, const std::vector<ExprContext*>* sort_exprs,
                                const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first) {
    if (suppliers.size() == 1) {
        _single_supplier = suppliers[0];
    } else {
        _cursors.reserve(suppliers.size());
        _min_heap.reserve(suppliers.size());
        for (auto& supplier : suppliers) {
            _cursors.emplace_back(std::make_unique<ChunkCursor>(supplier, sort_exprs, is_asc, is_null_first));
            ChunkCursor* cursor = _cursors.rbegin()->get();
            cursor->next();
            if (cursor->is_valid()) {
                _min_heap.push_back(cursor);
            }
        }
        std::make_heap(_min_heap.begin(), _min_heap.end(), _cursor_cmp_greater);
    }
    return Status::OK();
}

void SortedChunksMerger::set_profile(RuntimeProfile* profile) {
    _total_timer = ADD_TIMER(profile, "MergeSortedChunks");
}

Status SortedChunksMerger::get_next(ChunkPtr* chunk, bool* eos) {
    ScopedTimer<MonotonicStopWatch> timer(_total_timer);

    DCHECK(chunk != nullptr);
    if (_min_heap.empty() && !_single_supplier) {
        *eos = true;
        *chunk = nullptr;
        return Status::OK();
    }

    // single source
    if (_single_supplier) {
        Chunk* tmp_chunk = nullptr;
        Status status = _single_supplier(&tmp_chunk);
        *eos = (tmp_chunk == nullptr);
        (*chunk).reset(tmp_chunk);
        return status;
    }

    // multiple sources
    *eos = false;
    ChunkCursor* cursor = _min_heap[0];
    *chunk = cursor->clone_empty_chunk(config::vector_chunk_size);

    ChunkPtr current_chunk = cursor->get_current_chunk();
    std::vector<uint32_t> selective_values; // for append_selective call
    selective_values.reserve(config::vector_chunk_size);
    selective_values.push_back(cursor->get_current_position_in_chunk());
    size_t row_number = 1;

    std::pop_heap(_min_heap.begin(), _min_heap.end(), _cursor_cmp_greater);
    cursor->next();
    if (cursor->is_valid()) {
        std::push_heap(_min_heap.begin(), _min_heap.end(), _cursor_cmp_greater);
    } else {
        _min_heap.pop_back();
    }

    while (row_number < config::vector_chunk_size && !_min_heap.empty()) {
        cursor = _min_heap[0];
        const auto& ptr = cursor->get_current_chunk();
        if (current_chunk == ptr) {
            selective_values.push_back(cursor->get_current_position_in_chunk());
        } else {
            (*chunk)->append_selective(*current_chunk, selective_values.data(), 0, selective_values.size());
            current_chunk = ptr;
            selective_values.clear();
            selective_values.push_back(cursor->get_current_position_in_chunk());
        }

        std::pop_heap(_min_heap.begin(), _min_heap.end(), _cursor_cmp_greater);
        cursor->next();
        if (cursor->is_valid()) {
            std::push_heap(_min_heap.begin(), _min_heap.end(), _cursor_cmp_greater);
        } else {
            _min_heap.pop_back();
        }

        ++row_number;
    }

    (*chunk)->append_selective(*current_chunk, selective_values.data(), 0, selective_values.size());
    (*chunk)->set_num_rows(row_number); // set constant column in chunk with right size.

    return Status::OK();
}

} // namespace starrocks::vectorized