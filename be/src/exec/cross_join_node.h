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

#include "column/vectorized_fwd.h"
#include "exec/exec_node.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class CrossJoinNode final : public ExecNode {
public:
    CrossJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    ~CrossJoinNode() override {
        if (runtime_state() != nullptr) {
            (void)close(runtime_state());
        }
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status get_next_internal(RuntimeState* state, ChunkPtr* chunk, bool* eos,
                             ScopedTimer<MonotonicStopWatch>& probe_timer);
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    void close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

    // rewrite conjuncts as RuntimeFilter according to could_rewrite.
    // now we only support rewrites with chunk rows of 1.
    //
    // eg: if input chunk is [col3: 1, col4: 2]
    // slot1 > if (slot3 > 1, col3, col4) will be rewrited as slot1 > 4
    //
    // TODO: support multi rows rewrite
    static StatusOr<std::list<ExprContext*>> rewrite_runtime_filter(
            ObjectPool* pool, const std::vector<RuntimeFilterBuildDescriptor*>& rf_descs, Chunk* chunk,
            const std::vector<ExprContext*>& ctxs);

private:
    Status _build(RuntimeState* state);
    Status _get_next_probe_chunk(RuntimeState* state);

    template <class BuildFactory, class ProbeFactory>
    std::vector<std::shared_ptr<pipeline::OperatorFactory>> _decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context);

    // append cross-joined rows into chunk

    void _copy_joined_rows_with_index_base_build(ChunkPtr& chunk, size_t row_count, size_t probe_index,
                                                 size_t build_index);
    void _copy_joined_rows_with_index_base_probe(ChunkPtr& chunk, size_t row_count, size_t probe_index,
                                                 size_t build_index);

    void _copy_probe_rows_with_index_base_build(ColumnPtr& dest_col, ColumnPtr& src_col, size_t start_row,
                                                size_t copy_number);
    void _copy_probe_rows_with_index_base_probe(ColumnPtr& dest_col, ColumnPtr& src_col, size_t start_row,
                                                size_t copy_number);

    void _copy_build_rows_with_index_base_build(ColumnPtr& dest_col, ColumnPtr& src_col, size_t start_row,
                                                size_t row_count);
    void _copy_build_rows_with_index_base_probe(ColumnPtr& dest_col, ColumnPtr& src_col, size_t start_row,
                                                size_t row_count);

    void _init_row_desc();
    void _init_chunk(ChunkPtr* chunk);

    TJoinOp::type _join_op = TJoinOp::type::CROSS_JOIN;
    std::vector<ExprContext*> _join_conjuncts;
    std::string _sql_join_conjuncts;

    // previsou saved chunk.
    ChunkPtr _pre_output_chunk = nullptr;
    // used as right table's chunk.
    // _build_chunk include all rows of right table.
    ChunkPtr _build_chunk = nullptr;
    // total rows of right table.
    size_t _number_of_build_rows = 0;
    // total rows of right table by chunk_size.
    size_t _build_chunks_size = 0;
    // used when scan rows in [0, _build_chunks_size) rows.
    size_t _build_chunks_index = 0;
    // used when scan rows in [_build_chunks_size, _number_of_built_rows) rows.
    size_t _build_rows_index = 0;

    // used as left table's chunk.
    // _probe_chunk about one chunk_size(maybe 4096) of left table.
    ChunkPtr _probe_chunk = nullptr;
    // used when scan chunks in right table based on one row of left table.
    //  _probe_chunk_index is a local index in _probe_chunk.
    size_t _probe_chunk_index = 0;
    // used when scan rows in right table.
    // _probe_rows_index is a local index in _probe_chunk.
    // And is used when _probe_chunk_index == _probe_chunk->num_rows().
    size_t _probe_rows_index = 0;

    bool _eos = false;
    bool _need_create_tuple_columns = true;

    Buffer<SlotDescriptor*> _col_types;
    Buffer<TupleId> _output_build_tuple_ids;
    Buffer<TupleId> _output_probe_tuple_ids;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;

    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _probe_timer = nullptr;
    RuntimeProfile::Counter* _build_rows_counter = nullptr;
    RuntimeProfile::Counter* _probe_rows_counter = nullptr;

    std::vector<uint32_t> _buf_selective;

    std::vector<RuntimeFilterBuildDescriptor*> _build_runtime_filters;
};

} // namespace starrocks
