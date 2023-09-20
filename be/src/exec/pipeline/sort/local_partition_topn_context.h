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

#include <queue>

#include "exec/chunks_sorter.h"
#include "exec/partition/chunks_partitioner.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class RuntimeFilterBuildDescriptor;
}

namespace starrocks::pipeline {

class LocalPartitionTopnContext;
class LocalPartitionTopnContextFactory;

using LocalPartitionTopnContextPtr = std::shared_ptr<LocalPartitionTopnContext>;
using LocalPartitionTopnContextFactoryPtr = std::shared_ptr<LocalPartitionTopnContextFactory>;

// LocalPartitionTopnContext is the bridge of each pair of LocalPartitionTopn{Sink/Source}Operators
// The purpose of LocalPartitionTopn{Sink/Source}Operator is to reduce the amount of data,
// so the output chunks are still remain unordered
//                                   ┌────► topn─────┐
//                                   │               │
// (unordered)                       │               │                  (unordered)
// inputChunks ───────► partitioner ─┼────► topn ────┼─► gather ─────► outputChunks
//                                   │               │
//                                   │               │
//                                   └────► topn ────┘
class LocalPartitionTopnContext {
public:
    LocalPartitionTopnContext(const std::vector<TExpr>& t_partition_exprs, const std::vector<ExprContext*>& sort_exprs,
                              std::vector<bool> is_asc_order, std::vector<bool> is_null_first, std::string sort_keys,
                              int64_t offset, int64_t partition_limit, const TTopNType::type topn_type);

    [[nodiscard]] Status prepare(RuntimeState* state);

    // Add one chunk to partitioner
    [[nodiscard]] Status push_one_chunk_to_partitioner(RuntimeState* state, const ChunkPtr& chunk);

    // Notify that there is no further input for partitiner
    void sink_complete();

    // Pull chunks form partitioner of each partition to correspondent sorter
    [[nodiscard]] Status transfer_all_chunks_from_partitioner_to_sorters(RuntimeState* state);

    // Return true if at least one of the sorters has remaining data
    bool has_output();

    // Return true if sink completed and all the data in the chunks_sorters has been pulled out
    bool is_finished();

    // Pull one chunk from sorters or passthrough_buffer
    [[nodiscard]] StatusOr<ChunkPtr> pull_one_chunk();

    bool is_passthrough() const { return _chunks_partitioner->is_passthrough(); }

    size_t num_partitions() const { return _partition_num; }

private:
    // Pull one chunk from one of the sorters
    // The output chunk stream is unordered
    [[nodiscard]] StatusOr<ChunkPtr> pull_one_chunk_from_sorters();

    const std::vector<TExpr>& _t_partition_exprs;
    std::vector<ExprContext*> _partition_exprs;
    std::vector<PartitionColumnType> _partition_types;
    bool _has_nullable_key = false;

    // No more input chunks if after _is_sink_complete is set to true
    bool _is_sink_complete = false;

    ChunksPartitionerPtr _chunks_partitioner;
    bool _is_transfered = false;
    size_t _partition_num = 0;

    // Every partition holds a chunks_sorter
    ChunksSorters _chunks_sorters;
    const std::vector<ExprContext*>& _sort_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    const std::string _sort_keys;
    int64_t _offset;
    int64_t _partition_limit;
    const TTopNType::type _topn_type;

    int32_t _sorter_index = 0;
};

using LocalPartitionTopnContextPtr = std::shared_ptr<LocalPartitionTopnContext>;

class LocalPartitionTopnContextFactory {
public:
    LocalPartitionTopnContextFactory(RuntimeState* state, const TTopNType::type topn_type, bool is_merging,
                                     const std::vector<ExprContext*>& sort_exprs, std::vector<bool> is_asc_order,
                                     std::vector<bool> is_null_first, const std::vector<TExpr>& t_partition_exprs,
                                     int64_t offset, int64_t limit, std::string sort_keys,
                                     const std::vector<OrderByType>& order_by_types,
                                     const std::vector<RuntimeFilterBuildDescriptor*>& rfs);

    [[nodiscard]] Status prepare(RuntimeState* state);
    LocalPartitionTopnContext* create(int32_t driver_sequence);

private:
    std::unordered_map<int32_t, LocalPartitionTopnContextPtr> _ctxs;

    const TTopNType::type _topn_type;

    ChunksSorters _chunks_sorters;
    const std::vector<ExprContext*>& _sort_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    const std::vector<TExpr>& _t_partition_exprs;
    int64_t _offset;
    int64_t _partition_limit;
    const std::string _sort_keys;
};
} // namespace starrocks::pipeline
