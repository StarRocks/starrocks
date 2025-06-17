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

#include "exec/analytor.h"
#include "exec/chunks_sorter.h"
#include "exec/partition/chunks_partitioner.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class RuntimeFilterBuildDescriptor;

struct FunctionTypes;
} // namespace starrocks

namespace starrocks::pipeline {
class LocalPartitionTopnContext;
class LocalPartitionTopnContextFactory;

using LocalPartitionTopnContextPtr = std::shared_ptr<LocalPartitionTopnContext>;
using LocalPartitionTopnContextFactoryPtr = std::shared_ptr<LocalPartitionTopnContextFactory>;

using AggDataPtr = uint8_t*;

struct PreAggState {
    PreAggState(const std::vector<TExpr>& t_pre_agg_exprs, const std::vector<TSlotId>& t_pre_agg_output_slot_id)
            : _t_pre_agg_exprs(t_pre_agg_exprs), _t_pre_agg_output_slot_id(t_pre_agg_output_slot_id) {}

    bool _is_first_chunk_of_current_sorter = true;
    const std::vector<TExpr>& _t_pre_agg_exprs;
    const std::vector<TSlotId>& _t_pre_agg_output_slot_id;

    // The offset of the n-th aggregate function in a row of aggregate functions.
    std::vector<size_t> _agg_states_offsets;
    // The total size of the row for the aggregate function state.
    size_t _agg_states_total_size = 0;
    // The max align size for all aggregate state
    size_t _max_agg_state_align_size = 1;
    // The followings are aggregate function information:
    std::vector<FunctionContext*> _agg_fn_ctxs;
    std::vector<const AggregateFunction*> _agg_functions;
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    std::vector<Columns> _agg_input_columns;
    //raw pointers in order to get multi-column values
    std::vector<std::vector<const Column*>> _agg_input_raw_columns;
    std::vector<FunctionTypes> _agg_fn_types;
    // every partition has one Agg State
    std::vector<ManagedFunctionStatesPtr<PreAggState>> _managed_fn_states;
};

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
    friend class ManagedFunctionStates<LocalPartitionTopnContext>;

public:
    LocalPartitionTopnContext(const std::vector<TExpr>& t_partition_exprs, bool has_nullable_key, bool enable_pre_agg,
                              const std::vector<TExpr>& t_pre_agg_exprs,
                              const std::vector<TSlotId>& t_pre_agg_output_slot_id,
                              const std::vector<ExprContext*>& sort_exprs, std::vector<bool> is_asc_order,
                              std::vector<bool> is_null_first, std::string sort_keys, int64_t offset,
                              int64_t partition_limit, const TTopNType::type topn_type);

    Status prepare(RuntimeState* state, RuntimeProfile* runtime_profile);

    // Add one chunk to partitioner
    Status push_one_chunk_to_partitioner(RuntimeState* state, const ChunkPtr& chunk);

    // Notify that there is no further input for partitiner
    void sink_complete();

    // Pull chunks form partitioner of each partition to correspondent sorter
    Status transfer_all_chunks_from_partitioner_to_sorters(RuntimeState* state);

    // Return true if at least one of the sorters has remaining data
    bool has_output();

    // Return true if sink completed and all the data in the chunks_sorters has been pulled out
    bool is_finished();

    // Pull one chunk from sorters or passthrough_buffer
    StatusOr<ChunkPtr> pull_one_chunk();

    bool is_passthrough() const { return _chunks_partitioner->is_passthrough(); }

    void set_passthrough() { _chunks_partitioner->set_passthrough(true); }

    bool is_full() const { return _chunks_partitioner->is_passthrough_buffer_full(); }

    size_t num_partitions() const { return _partition_num; }

    PipeObservable& observable() { return _observable; }

private:
    // Pull one chunk from one of the sorters
    // The output chunk stream is unordered
    StatusOr<ChunkPtr> pull_one_chunk_from_sorters();

    // prepare all stuff for pre agg
    Status prepare_pre_agg(RuntimeState* state);

    // calculate agg for the ‘partition_idx' partition
    Status compute_agg_state(Chunk* chunk, size_t partition_idx);

    // when call chunk_sorter->get_next(), if this is first chunk it has, is_first_chunk is true
    void output_agg_result(Chunk* chunk, bool eos, bool is_first_chunk);

    Status output_agg_streaming(Chunk* chunk);

    Status _evaluate_agg_input_columns(Chunk* chunk);

    Columns _create_agg_result_columns(size_t num_rows);
    const std::vector<TExpr>& _t_partition_exprs;
    std::vector<ExprContext*> _partition_exprs;
    std::vector<PartitionColumnType> _partition_types;
    bool _has_nullable_key = false;

    // only set when _enable_pre_agg=true
    bool _enable_pre_agg;
    std::unique_ptr<PreAggState> _pre_agg;
    // bool _is_first_chunk_of_current_sorter = true;
    // const std::vector<TExpr>& _t_pre_agg_exprs;
    // const std::vector<TSlotId>& _t_pre_agg_output_slot_id;

    // // The offset of the n-th aggregate function in a row of aggregate functions.
    // std::vector<size_t> _agg_states_offsets;
    // // The total size of the row for the aggregate function state.
    // size_t _agg_states_total_size = 0;
    // // The max align size for all aggregate state
    // size_t _max_agg_state_align_size = 1;
    // // The followings are aggregate function information:
    // std::vector<FunctionContext*> _agg_fn_ctxs;
    // std::vector<const AggregateFunction*> _agg_functions;
    // std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    // std::vector<Columns> _agg_input_columns;
    // //raw pointers in order to get multi-column values
    // std::vector<std::vector<const Column*>> _agg_input_raw_columns;
    // std::vector<FunctionTypes> _agg_fn_types;
    // // every partition has one Agg State
    // std::vector<ManagedFunctionStatesPtr<LocalPartitionTopnContext>> _managed_fn_states;

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

    std::unique_ptr<MemPool> _mem_pool = nullptr;

    PipeObservable _observable;
};

using LocalPartitionTopnContextPtr = std::shared_ptr<LocalPartitionTopnContext>;

class LocalPartitionTopnContextFactory {
public:
    LocalPartitionTopnContextFactory(RuntimeState* state, const TTopNType::type topn_type, bool is_merging,
                                     const std::vector<ExprContext*>& sort_exprs, std::vector<bool> is_asc_order,
                                     std::vector<bool> is_null_first, const std::vector<TExpr>& t_partition_exprs,
                                     bool enable_pre_agg, const std::vector<TExpr>& t_pre_agg_exprs,
                                     const std::vector<TSlotId>& t_pre_agg_output_slot_id, int64_t offset,
                                     int64_t limit, std::string sort_keys,
                                     const std::vector<OrderByType>& order_by_types, bool has_outer_join_child,
                                     const std::vector<RuntimeFilterBuildDescriptor*>& rfs);

    Status prepare(RuntimeState* state);
    LocalPartitionTopnContext* create(int32_t driver_sequence);

private:
    std::unordered_map<int32_t, LocalPartitionTopnContextPtr> _ctxs;

    const TTopNType::type _topn_type;

    ChunksSorters _chunks_sorters;
    const std::vector<ExprContext*>& _sort_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    const std::vector<TExpr>& _t_partition_exprs;
    bool enable_pre_agg;
    const std::vector<TExpr>& _t_pre_agg_exprs;
    const std::vector<TSlotId>& _t_pre_agg_output_slot_id;
    int64_t _offset;
    int64_t _partition_limit;
    const std::string _sort_keys;
    bool _has_outer_join_child;
};
} // namespace starrocks::pipeline
