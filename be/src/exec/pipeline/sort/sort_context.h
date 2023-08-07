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

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/chunks_sorter.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sorting.h"
#include "exprs/runtime_filter_bank.h"

namespace starrocks::pipeline {

class SortContext;
using SortContextPtr = std::shared_ptr<SortContext>;

class SortContext final : public ContextWithDependency {
public:
    explicit SortContext(RuntimeState* state, const TTopNType::type topn_type, int64_t offset, int64_t limit,
                         const std::vector<ExprContext*>& sort_exprs, const SortDescs& sort_descs,
                         const std::vector<RuntimeFilterBuildDescriptor*>& build_runtime_filters)
            : _state(state),
              _topn_type(topn_type),
              _offset(offset),
              _limit(limit),
              _sort_exprs(sort_exprs),
              _sort_desc(sort_descs),
              _build_runtime_filters(build_runtime_filters) {}
    ~SortContext() override = default;

    void close(RuntimeState* state) override;

    void incr_sinker() { ++_num_partition_sinkers; }
    const std::vector<RuntimeFilterBuildDescriptor*>& build_runtime_filters() { return _build_runtime_filters; }
    void add_partition_chunks_sorter(const std::shared_ptr<ChunksSorter>& chunks_sorter);
    ChunksSorter* get_chunks_sorter(int32_t driver_sequence) {
        DCHECK_LT(driver_sequence, _chunks_sorter_partitions.size());
        return _chunks_sorter_partitions[driver_sequence].get();
    }
    TTopNType::type topn_type() const { return _topn_type; }
    int64_t offset() const { return _offset; }
    int64_t limit() const { return _limit; }
    const std::vector<ExprContext*>& sort_exprs() const { return _sort_exprs; }
    const SortDescs& sort_descs() const { return _sort_desc; }

    void finish_partition(uint64_t partition_rows);
    bool is_partition_sort_finished() const;
    bool is_output_finished() const;
    bool is_partition_ready() const;
    void cancel();

    StatusOr<ChunkPtr> pull_chunk();

    void set_runtime_filter_collector(RuntimeFilterHub* hub, int32_t plan_node_id,
                                      std::unique_ptr<RuntimeFilterCollector>&& collector);

private:
    Status _init_merger();

    RuntimeState* _state;
    const TTopNType::type _topn_type;
    int64_t _offset;
    const int64_t _limit;
    int32_t _num_partition_sinkers = 0;
    const std::vector<ExprContext*>& _sort_exprs;
    const SortDescs _sort_desc;

    std::atomic<int64_t> _total_rows = 0; // size of all chunks from all partitions.
    std::atomic<int32_t> _num_partition_finished = 0;

    std::vector<std::shared_ptr<ChunksSorter>> _chunks_sorter_partitions; // Partial sorters
    std::vector<std::unique_ptr<SimpleChunkSortCursor>> _partial_cursors;
    MergeCursorsCascade _merger;
    ChunkSlice _current_chunk;
    int64_t _required_rows = 0;
    bool _merger_inited = false;
    const std::vector<RuntimeFilterBuildDescriptor*>& _build_runtime_filters;
    // used for set runtime filter collector
    std::once_flag _set_collector_flag;
};

class SortContextFactory {
public:
    SortContextFactory(RuntimeState* state, const TTopNType::type topn_type, bool is_merging,
                       std::vector<ExprContext*> sort_exprs, const std::vector<bool>& is_asc_order,
                       const std::vector<bool>& is_null_first, const std::vector<TExpr>& partition_exprs,
                       int64_t offset, int64_t limit, const std::string& sort_keys,
                       const std::vector<OrderByType>& order_by_types,
                       const std::vector<RuntimeFilterBuildDescriptor*>& build_runtime_filters);

    SortContextPtr create(int32_t idx);

private:
    RuntimeState* _state;
    const TTopNType::type _topn_type;
    // _is_merging is true means to merge multiple output streams of PartitionSortSinkOperators into a common
    // LocalMergeSortSourceOperator that will produce a total order output stream.
    // _is_merging is false means to pipe each output stream of PartitionSortSinkOperators to an independent
    // LocalMergeSortSourceOperator respectively for scenarios of AnalyticNode with partition by.
    const bool _is_merging;
    std::unordered_map<int32_t, SortContextPtr> _sort_contexts;
    const int64_t _offset;
    const int64_t _limit;
    const std::vector<ExprContext*> _sort_exprs;
    const SortDescs _sort_descs;
    const std::vector<RuntimeFilterBuildDescriptor*>& _build_runtime_filters;
};

} // namespace starrocks::pipeline
