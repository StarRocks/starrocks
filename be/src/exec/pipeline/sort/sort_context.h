// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <algorithm>
#include <atomic>
#include <memory>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/sorting/sorting.h"

namespace starrocks::pipeline {

class SortContext;
using SortContextPtr = std::shared_ptr<SortContext>;
using vectorized::ChunkPtr;
using vectorized::ChunksSorter;

class SortContext final : public ContextWithDependency {
public:
    explicit SortContext(RuntimeState* state, int64_t limit, const int32_t num_right_sinkers,
                         const std::vector<ExprContext*> sort_exprs, const std::vector<bool>& is_asc_order,
                         const std::vector<bool>& is_null_first)
            : _state(state),
              _limit(limit),
              _num_partition_sinkers(num_right_sinkers),
              _sort_exprs(sort_exprs),
              _sort_desc(is_asc_order, is_null_first) {
        _chunks_sorter_partions.reserve(num_right_sinkers);
    }

    void close(RuntimeState* state) override {}

    void add_partition_chunks_sorter(std::shared_ptr<ChunksSorter> chunks_sorter) {
        _chunks_sorter_partions.push_back(chunks_sorter);
    }

    void finish_partition(uint64_t partition_rows) {
        _total_rows.fetch_add(partition_rows, std::memory_order_relaxed);
        _num_partition_finished.fetch_add(1, std::memory_order_release);
    }

    bool is_partition_sort_finished() const {
        return _num_partition_finished.load(std::memory_order_acquire) == _num_partition_sinkers;
    }

    bool is_output_finished() const { return _is_merge_finish; }

    ChunkPtr pull_chunk();

private:
    void _merge_inputs();

    RuntimeState* _state;
    const int64_t _limit;
    const int32_t _num_partition_sinkers;
    const std::vector<ExprContext*> _sort_exprs;
    const vectorized::SortDescs _sort_desc;

    std::atomic<int64_t> _total_rows = 0; // size of all chunks from all partitions.
    std::atomic<int32_t> _num_partition_finished = 0;

    std::vector<std::shared_ptr<ChunksSorter>> _chunks_sorter_partions; // Partial sorters
    bool _is_merge_finish = false;
    ChunkPtr _merged_chunk;
};

class SortContextFactory {
public:
    SortContextFactory(RuntimeState* state, bool is_merging, int64_t limit, int32_t num_right_sinkers,
                       const std::vector<ExprContext*>& sort_exprs, const std::vector<bool>& _is_asc_order,
                       const std::vector<bool>& is_null_first);

    SortContextPtr create(int32_t idx);

private:
    RuntimeState* _state;
    // _is_merging is true means to merge multiple output streams of PartitionSortSinkOperators into a common
    // LocalMergeSortSourceOperator that will produce a total order output stream.
    // _is_merging is false means to pipe each output stream of PartitionSortSinkOperators to an independent
    // LocalMergeSortSourceOperator respectively for scenarios of AnalyticNode with partition by.
    const bool _is_merging;
    std::vector<SortContextPtr> _sort_contexts;
    const int64_t _limit;
    const int32_t _num_right_sinkers;
    const std::vector<ExprContext*> _sort_exprs;
    const std::vector<bool> _is_asc_order;
    const std::vector<bool> _is_null_first;
};

} // namespace starrocks::pipeline
