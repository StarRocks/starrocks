// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <queue>

#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/partition/chunks_partitioner.h"
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

    Status prepare(RuntimeState* state);

    // Add one chunk to partitioner
    Status push_one_chunk_to_partitioner(RuntimeState* state, const vectorized::ChunkPtr& chunk);

    // Notify that there is no further input for partitiner
    void sink_complete();

    // Pull chunks form partitioner of each partition to correspondent sorter
    Status transfer_all_chunks_from_partitioner_to_sorters(RuntimeState* state);

    // Return true if at least one of the sorters has remaining data
    bool has_output();

    // Return true if sink completed and all the data in the chunks_sorters has been pulled out
    bool is_finished();

    // Pull one chunk from sorters or downgrade_buffer
    StatusOr<vectorized::ChunkPtr> pull_one_chunk();

private:
    // Pull one chunk from one of the sorters
    // The output chunk stream is unordered
    StatusOr<vectorized::ChunkPtr> pull_one_chunk_from_sorters();

    const std::vector<TExpr>& _t_partition_exprs;
    std::vector<ExprContext*> _partition_exprs;
    std::vector<vectorized::PartitionColumnType> _partition_types;
    bool _has_nullable_key = false;

    // No more input chunks if after _is_sink_complete is set to true
    bool _is_sink_complete = false;

    vectorized::ChunksPartitionerPtr _chunks_partitioner;
    bool _is_transfered = false;

    // Every partition holds a chunks_sorter
    vectorized::ChunksSorters _chunks_sorters;
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
                                     size_t num_right_sinkers, const std::vector<ExprContext*>& sort_exprs,
                                     std::vector<bool> is_asc_order, std::vector<bool> is_null_first,
                                     const std::vector<TExpr>& t_partition_exprs, int64_t offset, int64_t limit,
                                     std::string sort_keys, const std::vector<OrderByType>& order_by_types);

    Status prepare(RuntimeState* state);
    LocalPartitionTopnContext* create(int32_t driver_sequence);

private:
    std::vector<LocalPartitionTopnContextPtr> _ctxs;

    const TTopNType::type _topn_type;

    vectorized::ChunksSorters _chunks_sorters;
    const std::vector<ExprContext*>& _sort_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    const std::vector<TExpr>& _t_partition_exprs;
    int64_t _offset;
    int64_t _partition_limit;
    const std::string _sort_keys;
};
} // namespace starrocks::pipeline
