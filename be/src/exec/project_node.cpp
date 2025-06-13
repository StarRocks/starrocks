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

#include "exec/project_node.h"

#include <cstring>
#include <memory>
#include <set>
#include <vector>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/status.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/project_operator.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

ProjectNode::ProjectNode(starrocks::ObjectPool* pool, const starrocks::TPlanNode& node,
                         const starrocks::DescriptorTbl& desc)
        : ExecNode(pool, node, desc) {}

ProjectNode::~ProjectNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status ProjectNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    size_t column_size = tnode.project_node.slot_map.size();
    _expr_ctxs.reserve(column_size);
    _slot_ids.reserve(column_size);
    _type_is_nullable.reserve(column_size);

    std::map<SlotId, bool> slot_null_mapping;
    for (auto const& slot : row_desc().tuple_descriptors()[0]->slots()) {
        slot_null_mapping[slot->id()] = slot->is_nullable();
    }

    for (auto const& [key, val] : tnode.project_node.slot_map) {
        _slot_ids.emplace_back(key);
        ExprContext* context;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, val, &context, state, true));
        _expr_ctxs.emplace_back(context);
        _type_is_nullable.emplace_back(slot_null_mapping[key]);
    }

    size_t common_sub_column_size = tnode.project_node.common_slot_map.size();
    _common_sub_expr_ctxs.reserve(common_sub_column_size);
    _common_sub_slot_ids.reserve(common_sub_column_size);

    for (auto const& [key, val] : tnode.project_node.common_slot_map) {
        ExprContext* context;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, val, &context, state, true));
        _common_sub_slot_ids.emplace_back(key);
        _common_sub_expr_ctxs.emplace_back(context);
    }

    return Status::OK();
}

Status ProjectNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));

    RETURN_IF_ERROR(Expr::prepare(_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_common_sub_expr_ctxs, state));

    _expr_compute_timer = ADD_TIMER(runtime_profile(), "ExprComputeTime");
    _common_sub_expr_compute_timer = ADD_TIMER(runtime_profile(), "CommonSubExprComputeTime");

    return Status::OK();
}

Status ProjectNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(_children[0]->open(state));

    DictOptimizeParser::set_output_slot_id(&_common_sub_expr_ctxs, _common_sub_slot_ids);
    DictOptimizeParser::set_output_slot_id(&_expr_ctxs, _slot_ids);

    RETURN_IF_ERROR(Expr::open(_common_sub_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_expr_ctxs, state));
    return Status::OK();
}

Status ProjectNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit()) {
        *chunk = nullptr;
        *eos = true;
        return Status::OK();
    }

    *eos = false;
    do {
        RETURN_IF_ERROR(_children[0]->get_next(state, chunk, eos));
    } while (!(*eos) && ((*chunk)->num_rows() == 0));

    TRY_CATCH_ALLOC_SCOPE_START()

    if (*eos) {
        *chunk = nullptr;
        return Status::OK();
    }

    {
        SCOPED_TIMER(_common_sub_expr_compute_timer);
        for (size_t i = 0; i < _common_sub_slot_ids.size(); ++i) {
            ASSIGN_OR_RETURN(auto col, _common_sub_expr_ctxs[i]->evaluate((*chunk).get()));
            (*chunk)->append_column(std::move(col), _common_sub_slot_ids[i]);
        }
        RETURN_IF_HAS_ERROR(_common_sub_expr_ctxs);
    }

    // ToDo(kks): we could reuse result columns, if the parent node isn't sort node
    Columns result_columns(_slot_ids.size());
    {
        SCOPED_TIMER(_expr_compute_timer);
        for (size_t i = 0; i < _slot_ids.size(); ++i) {
            ASSIGN_OR_RETURN(result_columns[i], _expr_ctxs[i]->evaluate((*chunk).get()));
            result_columns[i] =
                    ColumnHelper::align_return_type(std::move(result_columns[i]), _expr_ctxs[i]->root()->type(),
                                                    (*chunk)->num_rows(), _type_is_nullable[i]);
        }
        RETURN_IF_HAS_ERROR(_expr_ctxs);
    }

    ChunkPtr result_chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < result_columns.size(); ++i) {
        result_chunk->append_column(result_columns[i], _slot_ids[i]);
    }

    *chunk = std::move(result_chunk);
    eval_join_runtime_filters(chunk);

    _num_rows_returned += (*chunk)->num_rows();

    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
        DCHECK_CHUNK(*chunk);
        return Status::OK();
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    DCHECK_CHUNK(*chunk);

    TRY_CATCH_ALLOC_SCOPE_END()
    return Status::OK();
}

Status ProjectNode::reset(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::reset(state));
    return Status::OK();
}

void ProjectNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }

    Expr::close(_expr_ctxs, state);
    Expr::close(_common_sub_expr_ctxs, state);

    ExecNode::close(state);
}

void ProjectNode::push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs) {
    for (const auto& ctx : (*expr_ctxs)) {
        if (!ctx->root()->is_bound(_tuple_ids)) {
            continue;
        }

        if (!ctx->root()->get_child(0)->is_slotref()) {
            continue;
        }

        auto column = down_cast<ColumnRef*>(ctx->root()->get_child(0));

        for (int i = 0; i < _slot_ids.size(); ++i) {
            if (_slot_ids[i] == column->slot_id() && _expr_ctxs[i]->root()->is_slotref()) {
                auto ref = down_cast<ColumnRef*>(_expr_ctxs[i]->root());
                column->set_slot_id(ref->slot_id());
                column->set_tuple_id(ref->tuple_id());
                break;
            }
        }
    }

    ExecNode::push_down_predicate(state, expr_ctxs);
}

void ProjectNode::push_down_tuple_slot_mappings(RuntimeState* state,
                                                const std::vector<TupleSlotMapping>& parent_mappings) {
    _tuple_slot_mappings = parent_mappings;

    DCHECK(_tuple_ids.size() == 1);
    for (int i = 0; i < _slot_ids.size(); ++i) {
        if (_expr_ctxs[i]->root()->is_slotref()) {
            DCHECK(nullptr != dynamic_cast<ColumnRef*>(_expr_ctxs[i]->root()));
            auto ref = ((ColumnRef*)_expr_ctxs[i]->root());
            _tuple_slot_mappings.emplace_back(ref->tuple_id(), ref->slot_id(), _tuple_ids[0], _slot_ids[i]);
        }
    }

    for (auto& child : _children) {
        child->push_down_tuple_slot_mappings(state, _tuple_slot_mappings);
    }
}

void ProjectNode::push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) {
    // accept runtime filters from parent if possible.
    _runtime_filter_collector.push_down(state, id(), collector, _tuple_ids, _local_rf_waiting_set);

    // check to see if runtime filters can be rewritten
    auto& descriptors = _runtime_filter_collector.descriptors();
    RuntimeFilterProbeCollector rewritten_collector;

    auto iter = descriptors.begin();
    while (iter != descriptors.end()) {
        RuntimeFilterProbeDescriptor* rf_desc = iter->second;
        if (!rf_desc->can_push_down_runtime_filter()) {
            ++iter;
            continue;
        }
        SlotId slot_id;
        // bound to this tuple and probe expr is slot ref.
        if (!rf_desc->is_bound(_tuple_ids) || !rf_desc->is_probe_slot_ref(&slot_id)) {
            ++iter;
            continue;
        }
        bool match = false;
        for (int i = 0; i < _slot_ids.size(); i++) {
            if (_slot_ids[i] == slot_id) {
                // replace with new probe expr
                ExprContext* new_probe_expr_ctx = _expr_ctxs[i];
                rf_desc->replace_probe_expr_ctx(state, row_desc(), new_probe_expr_ctx);
                match = true;
                break;
            }
        }
        if (match) {
            rewritten_collector.add_descriptor(rf_desc);
            iter = descriptors.erase(iter);
        } else {
            ++iter;
        }
    }

    if (!rewritten_collector.empty()) {
        // push down rewritten runtime filters to children
        push_down_join_runtime_filter_to_children(state, &rewritten_collector);
        rewritten_collector.close(state);
    }
}

pipeline::OpFactories ProjectNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators = _children[0]->decompose_to_pipeline(context);
    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));

    operators.emplace_back(std::make_shared<ProjectOperatorFactory>(
            context->next_operator_id(), id(), std::move(_slot_ids), std::move(_expr_ctxs),
            std::move(_type_is_nullable), std::move(_common_sub_slot_ids), std::move(_common_sub_expr_ctxs)));
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(operators.back().get(), context, rc_rf_probe_collector);
    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return operators;
}

} // namespace starrocks
