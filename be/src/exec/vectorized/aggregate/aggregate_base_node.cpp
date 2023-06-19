// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/aggregate/aggregate_base_node.h"

#include "exprs/anyval_util.h"
#include "gutil/strings/substitute.h"

namespace starrocks::vectorized {

AggregateBaseNode::AggregateBaseNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _tnode(tnode) {}

AggregateBaseNode::~AggregateBaseNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status AggregateBaseNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.agg_node.grouping_exprs, &_group_by_expr_ctxs));
    for (auto& expr : _group_by_expr_ctxs) {
        auto& type_desc = expr->root()->type();
        if (!type_desc.support_groupby()) {
            return Status::NotSupported(fmt::format("group by type {} is not supported", type_desc.debug_string()));
        }
    }
    return Status::OK();
}

Status AggregateBaseNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
<<<<<<< HEAD:be/src/exec/vectorized/aggregate/aggregate_base_node.cpp
    _aggregator = std::make_shared<Aggregator>(_tnode);
    return _aggregator->prepare(state, _pool, runtime_profile(), _mem_tracker.get());
=======
    auto params = convert_to_aggregator_params(_tnode);

    // Avoid partial-prepared Aggregator, which is dangerous to close
    auto aggregator = std::make_shared<Aggregator>(std::move(params));
    RETURN_IF_ERROR(aggregator->prepare(state, _pool, runtime_profile()));
    _aggregator = std::move(aggregator);
    return Status::OK();
>>>>>>> 2f9f17539 ([BugFix] fix access partial-prepared Aggregator (#25561)):be/src/exec/aggregate/aggregate_base_node.cpp
}

Status AggregateBaseNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    if (_aggregator != nullptr) {
        _num_rows_returned = _aggregator->num_rows_returned();
        _aggregator->close(state);
        _aggregator.reset();
    }
    return ExecNode::close(state);
}

void AggregateBaseNode::push_down_join_runtime_filter(RuntimeState* state,
                                                      vectorized::RuntimeFilterProbeCollector* collector) {
    // accept runtime filters from parent if possible.
    _runtime_filter_collector.push_down(collector, _tuple_ids, _local_rf_waiting_set);

    // check to see if runtime filters can be rewritten
    auto& descriptors = _runtime_filter_collector.descriptors();
    RuntimeFilterProbeCollector pushdown_collector;

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
        for (ExprContext* group_expr_ctx : _group_by_expr_ctxs) {
            if (group_expr_ctx->root()->is_slotref()) {
                auto* slot = down_cast<ColumnRef*>(group_expr_ctx->root());
                if (slot->slot_id() == slot_id) {
                    match = true;
                    break;
                }
            }
        }

        if (match) {
            pushdown_collector.add_descriptor(rf_desc);
            iter = descriptors.erase(iter);
        } else {
            ++iter;
        }
    }

    // push down rewritten runtime filters to children
    if (!pushdown_collector.empty()) {
        push_down_join_runtime_filter_to_children(state, &pushdown_collector);
        pushdown_collector.close(state);
    }
}

} // namespace starrocks::vectorized
