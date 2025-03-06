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

#include "exec/union_node.h"

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/project_operator.h"
#include "exec/pipeline/set/union_const_source_operator.h"
#include "exec/pipeline/set/union_passthrough_operator.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"

namespace starrocks {
UnionNode::UnionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _first_materialized_child_idx(tnode.union_node.first_materialized_child_idx),
          _tuple_id(tnode.union_node.tuple_id) {}

UnionNode::~UnionNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status UnionNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    const auto& const_expr_lists = tnode.union_node.const_expr_lists;
    for (const auto& exprs : const_expr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, exprs, &ctxs, state));
        _const_expr_lists.push_back(ctxs);
    }

    const auto& result_expr_lists = tnode.union_node.result_expr_lists;
    for (const auto& exprs : result_expr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, exprs, &ctxs, state));
        _child_expr_lists.push_back(ctxs);
    }

    if (tnode.union_node.__isset.pass_through_slot_maps) {
        for (const auto& item : tnode.union_node.pass_through_slot_maps) {
            _convert_pass_through_slot_map(item);
        }
    }

    return Status::OK();
}

void UnionNode::_convert_pass_through_slot_map(const std::map<SlotId, SlotId>& slot_map) {
    std::map<SlotId, size_t> tmp_map;
    for (const auto& [key, value] : slot_map) {
        if (tmp_map.find(value) == tmp_map.end()) {
            tmp_map[value] = 1;
        } else {
            tmp_map[value]++;
        }
    }

    pipeline::UnionPassthroughOperator::SlotMap tuple_slot_map;
    for (const auto& [key, value] : slot_map) {
        tuple_slot_map[key] = {value, tmp_map[value]};
    }
    _pass_through_slot_maps.emplace_back(tuple_slot_map);
}

Status UnionNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    for (const vector<ExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(Expr::prepare(exprs, state));
    }

    for (auto& _child_expr_list : _child_expr_lists) {
        RETURN_IF_ERROR(Expr::prepare(_child_expr_list, state));
    }

    return Status::OK();
}

Status UnionNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(ExecNode::open(state));

    for (const vector<ExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(Expr::open(exprs, state));
    }

    for (const vector<ExprContext*>& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(Expr::open(exprs, state));
    }

    if (!_children.empty()) {
        RETURN_IF_ERROR(child(_child_idx)->open(state));
    }

    return Status::OK();
}

Status UnionNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    while (true) {
        //@TODO: There seems to be no difference between passthrough and materialize, can be
        // remove the passthrough handle
        if (_has_more_passthrough()) {
            RETURN_IF_ERROR(_get_next_passthrough(state, chunk));
            if (!_child_eos) {
                *eos = false;
                break;
            }
        } else if (_has_more_materialized()) {
            RETURN_IF_ERROR(_get_next_materialize(state, chunk));
            if (!_child_eos) {
                *eos = false;
                break;
            }
        } else if (_has_more_const(state)) {
            RETURN_IF_ERROR(_get_next_const(state, chunk));
            *eos = false;
            break;
        } else {
            *eos = true;
            break;
        }
    }

    if (*eos) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    } else {
        _num_rows_returned += (*chunk)->num_rows();
    }

    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        DCHECK_GE((*chunk)->num_rows(), num_rows_over);
        (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
    }

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

void UnionNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    for (auto& exprs : _child_expr_lists) {
        Expr::close(exprs, state);
    }
    for (auto& exprs : _const_expr_lists) {
        Expr::close(exprs, state);
    }
    ExecNode::close(state);
}

Status UnionNode::_get_next_passthrough(RuntimeState* state, ChunkPtr* chunk) {
    (*chunk) = std::make_shared<Chunk>();
    ChunkPtr tmp_chunk = nullptr;

    if (_child_eos) {
        RETURN_IF_ERROR(child(_child_idx)->open(state));
        _child_eos = false;
    }

    while (true) {
        RETURN_IF_ERROR(child(_child_idx)->get_next(state, &tmp_chunk, &_child_eos));
        if (_child_eos) {
            child(_child_idx)->close(state);
            _child_idx++;
            break;
        }

        if (tmp_chunk->num_rows() <= 0) {
            continue;
        }

        _move_passthrough_chunk(tmp_chunk, *chunk);
        break;
    }

    return Status::OK();
}

Status UnionNode::_get_next_materialize(RuntimeState* state, ChunkPtr* chunk) {
    (*chunk) = std::make_shared<Chunk>();
    ChunkPtr tmp_chunk = nullptr;
    if (_child_eos) {
        RETURN_IF_ERROR(child(_child_idx)->open(state));
        _child_eos = false;
    }

    while (true) {
        RETURN_IF_ERROR(child(_child_idx)->get_next(state, &tmp_chunk, &_child_eos));
        if (_child_eos) {
            child(_child_idx)->close(state);
            _child_idx++;
            break;
        }

        if (tmp_chunk->num_rows() <= 0) {
            continue;
        }

        RETURN_IF_ERROR(_move_materialize_chunk(tmp_chunk, *chunk));
        break;
    }

    return Status::OK();
}

Status UnionNode::_get_next_const(RuntimeState* state, ChunkPtr* chunk) {
    *chunk = std::make_shared<Chunk>();

    RETURN_IF_ERROR(_move_const_chunk(*chunk));

    _const_expr_list_idx++;
    return Status::OK();
}

void UnionNode::_move_passthrough_chunk(ChunkPtr& src_chunk, ChunkPtr& dest_chunk) {
    const auto& tuple_descs = child(_child_idx)->row_desc().tuple_descriptors();

    if (!_pass_through_slot_maps.empty()) {
        for (auto* dest_slot : _tuple_desc->slots()) {
            auto slot_item = _pass_through_slot_maps[_child_idx][dest_slot->id()];
            ColumnPtr& column = src_chunk->get_column_by_slot_id(slot_item.slot_id);
            // There may be multiple DestSlotId mapped to the same SrcSlotId,
            // so here we have to decide whether you can MoveColumn according to this situation
            if (slot_item.ref_count <= 1) {
                _move_column(dest_chunk, column, dest_slot, src_chunk->num_rows());
            } else {
                _clone_column(dest_chunk, column, dest_slot, src_chunk->num_rows());
            }
        }
    } else {
        // For backward compatibility
        // TODO(kks): when StarRocks 2.0 release, we could remove this branch.
        size_t index = 0;
        // When pass through, the child tuple size must be 1;
        for (auto* src_slot : tuple_descs[0]->slots()) {
            auto* dest_slot = _tuple_desc->slots()[index++];
            ColumnPtr& column = src_chunk->get_column_by_slot_id(src_slot->id());
            _move_column(dest_chunk, column, dest_slot, src_chunk->num_rows());
        }
    }
}

Status UnionNode::_move_materialize_chunk(ChunkPtr& src_chunk, ChunkPtr& dest_chunk) {
    for (size_t i = 0; i < _child_expr_lists[_child_idx].size(); i++) {
        auto* dest_slot = _tuple_desc->slots()[i];
        ASSIGN_OR_RETURN(ColumnPtr column, _child_expr_lists[_child_idx][i]->evaluate(src_chunk.get()));
        _move_column(dest_chunk, column, dest_slot, src_chunk->num_rows());
    }
    RETURN_IF_HAS_ERROR(_child_expr_lists[_child_idx]);
    return Status::OK();
}

Status UnionNode::_move_const_chunk(ChunkPtr& dest_chunk) {
    for (size_t i = 0; i < _const_expr_lists[_const_expr_list_idx].size(); i++) {
        ASSIGN_OR_RETURN(ColumnPtr column, _const_expr_lists[_const_expr_list_idx][i]->evaluate(nullptr));
        auto* dest_slot = _tuple_desc->slots()[i];

        _move_column(dest_chunk, column, dest_slot, 1);
    }

    RETURN_IF_HAS_ERROR(_const_expr_lists[_const_expr_list_idx]);
    return Status::OK();
}

void UnionNode::_clone_column(ChunkPtr& dest_chunk, const ColumnPtr& src_column, const SlotDescriptor* dest_slot,
                              size_t row_count) {
    if (src_column->is_nullable() || !dest_slot->is_nullable()) {
        dest_chunk->append_column(src_column->clone(), dest_slot->id());
    } else {
        ColumnPtr nullable_column = NullableColumn::create(src_column->clone(), NullColumn::create(row_count, 0));
        dest_chunk->append_column(nullable_column, dest_slot->id());
    }
}

void UnionNode::_move_column(ChunkPtr& dest_chunk, ColumnPtr& src_column, const SlotDescriptor* dest_slot,
                             size_t row_count) {
    if (src_column->is_nullable()) {
        if (src_column->is_constant()) {
            auto nullable_column = ColumnHelper::create_column(dest_slot->type(), true);
            nullable_column->reserve(row_count);
            nullable_column->append_nulls(row_count);
            dest_chunk->append_column(std::move(nullable_column), dest_slot->id());
        } else {
            dest_chunk->append_column(src_column, dest_slot->id());
        }
    } else {
        if (src_column->is_constant()) {
            auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(src_column);
            // Note: we must create a new column every time here,
            // because VectorizedLiteral always return a same shared_ptr and we will modify it later.
            MutableColumnPtr new_column = ColumnHelper::create_column(dest_slot->type(), dest_slot->is_nullable());
            new_column->append(*const_column->data_column(), 0, 1);
            new_column->assign(row_count, 0);
            dest_chunk->append_column(std::move(new_column), dest_slot->id());
        } else {
            if (dest_slot->is_nullable()) {
                MutableColumnPtr nullable_column = NullableColumn::create(std::move(src_column)->as_mutable_ptr(),
                                                                          NullColumn::create(row_count, 0));
                dest_chunk->append_column(std::move(nullable_column), dest_slot->id());
            } else {
                dest_chunk->append_column(std::move(src_column), dest_slot->id());
            }
        }
    }
}

pipeline::OpFactories UnionNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    std::vector<OpFactories> operators_list;
    operators_list.reserve(_children.size() + 1);
    const auto num_operators_generated = _children.size() + !_const_expr_lists.empty();
    auto&& rc_rf_probe_collector =
            std::make_shared<RcRfProbeCollector>(num_operators_generated, std::move(this->runtime_filter_collector()));
    size_t i = 0;
    // UnionPassthroughOperator is used for the passthrough sub-node.
    for (; i < _first_materialized_child_idx; i++) {
        auto child_ops = child(i)->decompose_to_pipeline(context);
        child_ops = context->maybe_interpolate_grouped_exchange(_id, child_ops);
        operators_list.emplace_back(child_ops);

        UnionPassthroughOperator::SlotMap* dst2src_slot_map = nullptr;
        if (!_pass_through_slot_maps.empty()) {
            dst2src_slot_map = &_pass_through_slot_maps[i];
        }

        const auto& dst_tuple_desc =
                context->fragment_context()->runtime_state()->desc_tbl().get_tuple_descriptor(_tuple_id);
        const auto& dst_slots = dst_tuple_desc->slots();

        // When pass through, the child tuple size must be 1;
        const auto& tuple_descs = child(i)->row_desc().tuple_descriptors();
        const auto& src_slots = tuple_descs[0]->slots();

        auto union_passthrough_op = std::make_shared<UnionPassthroughOperatorFactory>(
                context->next_operator_id(), id(), dst2src_slot_map, dst_slots, src_slots);
        operators_list[i].emplace_back(std::move(union_passthrough_op));
        // Initialize OperatorFactory's fields involving runtime filters.
        this->init_runtime_filter_for_operator(operators_list[i].back().get(), context, rc_rf_probe_collector);
    }

    // ProjectOperatorFactory is used for the materialized sub-node.
    for (; i < _children.size(); i++) {
        auto child_ops = child(i)->decompose_to_pipeline(context);
        child_ops = context->maybe_interpolate_grouped_exchange(_id, child_ops);
        operators_list.emplace_back(child_ops);

        const auto& dst_tuple_desc =
                context->fragment_context()->runtime_state()->desc_tbl().get_tuple_descriptor(_tuple_id);
        size_t columns_count = dst_tuple_desc->slots().size();

        std::vector<int32_t> dst_column_ids;
        std::vector<bool> dst_column_is_nullables;
        dst_column_ids.reserve(columns_count);
        dst_column_is_nullables.reserve(columns_count);
        for (auto* dst_slot : dst_tuple_desc->slots()) {
            dst_column_ids.emplace_back(dst_slot->id());
            dst_column_is_nullables.emplace_back(dst_slot->is_nullable());
        }

        auto project_op = std::make_shared<ProjectOperatorFactory>(
                context->next_operator_id(), id(), std::move(dst_column_ids), std::move(_child_expr_lists[i]),
                std::move(dst_column_is_nullables), std::vector<int32_t>(), std::vector<ExprContext*>());
        operators_list[i].emplace_back(std::move(project_op));
        // Initialize OperatorFactory's fields involving runtime filters.
        this->init_runtime_filter_for_operator(operators_list[i].back().get(), context, rc_rf_probe_collector);
    }

    // UnionConstSourceOperatorFactory is used for the const sub exprs.
    if (!_const_expr_lists.empty()) {
        operators_list.emplace_back();

        const auto& dst_tuple_desc =
                context->fragment_context()->runtime_state()->desc_tbl().get_tuple_descriptor(_tuple_id);
        const auto& dst_slots = dst_tuple_desc->slots();
        auto union_const_source_op = std::make_shared<UnionConstSourceOperatorFactory>(
                context->next_operator_id(), id(), dst_slots, _const_expr_lists);

        // Each _const_expr_list is project to one row.
        // Divide _const_expr_lists into several drivers, each of which is going to evaluate
        // at least *runtime_state()->chunk_size()* _const_expr_list.
        size_t parallelism = std::min(
                context->degree_of_parallelism(),
                (_const_expr_lists.size() + runtime_state()->chunk_size() - 1) / runtime_state()->chunk_size());
        union_const_source_op->set_degree_of_parallelism(parallelism);

        operators_list[i].emplace_back(std::move(union_const_source_op));
        // Initialize OperatorFactory's fields involving runtime filters.
        this->init_runtime_filter_for_operator(operators_list[i].back().get(), context, rc_rf_probe_collector);
    }

    if (limit() != -1) {
        for (size_t i = 0; i < operators_list.size(); ++i) {
            operators_list[i].emplace_back(
                    std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
        }
    }

    auto final_operators = context->maybe_gather_pipelines_to_one(runtime_state(), id(), operators_list);
    if (limit() != -1) {
        final_operators.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return final_operators;
}

} // namespace starrocks
