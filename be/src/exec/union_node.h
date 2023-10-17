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

#include "exec/exec_node.h"
#include "exec/pipeline/set/union_passthrough_operator.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class UnionNode final : public ExecNode {
public:
    UnionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~UnionNode() override;

    [[nodiscard]] Status init(const TPlanNode& tnode, RuntimeState* state) override;
    [[nodiscard]] Status prepare(RuntimeState* state) override;
    [[nodiscard]] Status open(RuntimeState* state) override;
    [[nodiscard]] Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    void close(RuntimeState* state) override;

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;

private:
    void _convert_pass_through_slot_map(const std::map<SlotId, SlotId>& slot_map);

    [[nodiscard]] Status _get_next_passthrough(RuntimeState* state, ChunkPtr* chunk);
    [[nodiscard]] Status _get_next_materialize(RuntimeState* state, ChunkPtr* chunk);
    [[nodiscard]] Status _get_next_const(RuntimeState* state, ChunkPtr* chunk);

    void _move_passthrough_chunk(ChunkPtr& src_chunk, ChunkPtr& dest_chunk);
    [[nodiscard]] Status _move_materialize_chunk(ChunkPtr& src_chunk, ChunkPtr& dest_chunk);
    [[nodiscard]] Status _move_const_chunk(ChunkPtr& dest_chunk);

    static void _clone_column(ChunkPtr& dest_chunk, const ColumnPtr& src_column, const SlotDescriptor* dest_slot,
                              size_t row_count);

    static void _move_column(ChunkPtr& dest_chunk, ColumnPtr& src_column, const SlotDescriptor* dest_slot,
                             size_t row_count);

    bool _has_more_passthrough() const { return _child_idx < _first_materialized_child_idx; }

    bool _has_more_materialized() const {
        return _first_materialized_child_idx != _children.size() && _child_idx < _children.size();
    }

    bool _has_more_const(RuntimeState* state) const {
        return state->per_fragment_instance_idx() == 0 && _const_expr_list_idx < _const_expr_lists.size();
    }

    std::vector<std::vector<ExprContext*>> _const_expr_lists;
    std::vector<std::vector<ExprContext*>> _child_expr_lists;

    // the map from slot id of output chunk to slot id of child chunk
    // There may be multiple DestSlotId mapped to the same SrcSlotId,
    // so here we have to decide whether you can MoveColumn according to this situation
    std::vector<pipeline::UnionPassthroughOperator::SlotMap> _pass_through_slot_maps;

    size_t _child_idx = 0;
    const int _first_materialized_child_idx = 0;
    int _const_expr_list_idx = 0;

    bool _child_eos = false;
    const int _tuple_id = 0;
    const TupleDescriptor* _tuple_desc = nullptr;
};

} // namespace starrocks
