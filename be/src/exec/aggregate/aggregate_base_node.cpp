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

#include "exec/aggregate/aggregate_base_node.h"

#include "exec/aggregator.h"
#include "exec_primitive/runtime_filter/runtime_filter_descriptor.h"
#include "exprs/expr_factory.h"

namespace starrocks {

AggregateBaseNode::AggregateBaseNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs), _tnode(tnode) {}

AggregateBaseNode::~AggregateBaseNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status AggregateBaseNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(
            ExprFactory::create_expr_trees(_pool, tnode.agg_node.grouping_exprs, &_group_by_expr_ctxs, state, true));
    for (auto& expr : _group_by_expr_ctxs) {
        auto& type_desc = expr->root()->type();
        if (!type_desc.support_groupby()) {
            return Status::NotSupported(fmt::format("group by type {} is not supported", type_desc.debug_string()));
        }
    }
    if (tnode.agg_node.__isset.build_runtime_filters) {
        for (const auto& desc : tnode.agg_node.build_runtime_filters) {
            auto* rf_desc = _pool->add(new RuntimeFilterBuildDescriptor());
            RETURN_IF_ERROR(rf_desc->init(_pool, desc, state));
            _build_runtime_filters.emplace_back(rf_desc);
        }
    }
    return Status::OK();
}

void AggregateBaseNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    if (_aggregator != nullptr) {
        if (_aggregator->is_hash_set()) {
            _mem_tracker->set(_aggregator->hash_set_memory_usage());
        } else {
            _mem_tracker->set(_aggregator->hash_map_memory_usage());
        }
        _num_rows_returned = _aggregator->num_rows_returned();
        _aggregator->close(state);
        _aggregator.reset();
    }
    ExecNode::close(state);
}

void AggregateBaseNode::push_down_tuple_slot_mappings(RuntimeState* state,
                                                      const std::vector<TupleSlotMapping>& parent_mappings) {
    _tuple_slot_mappings = parent_mappings;

    DCHECK(_tuple_ids.size() == 1);
    for (auto& expr_ctx : _group_by_expr_ctxs) {
        if (expr_ctx->root()->is_slotref()) {
            auto ref = dynamic_cast<ColumnRef*>(expr_ctx->root());
            DCHECK(ref != nullptr);
            _tuple_slot_mappings.emplace_back(ref->tuple_id(), ref->slot_id(), _tuple_ids[0], ref->slot_id());
        }
    }

    for (auto& child : _children) {
        child->push_down_tuple_slot_mappings(state, _tuple_slot_mappings);
    }
}

} // namespace starrocks
