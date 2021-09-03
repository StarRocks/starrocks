// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "aggregate_base_node.h"

namespace starrocks::vectorized {

Status AggregateBaseNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return AggregateBaseNode::_init(tnode, state);
}

Status AggregateBaseNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    RETURN_IF_ERROR(AggregateBaseNode::_prepare(state));

    RETURN_IF_ERROR(Expr::prepare(_group_by_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));
    for (const auto& ctx : _agg_expr_ctxs) {
        RETURN_IF_ERROR(Expr::prepare(ctx, state, child(0)->row_desc(), expr_mem_tracker()));
    }

    return Status::OK();
}

Status AggregateBaseNode::close(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBaseNode::_close(state));
    return ExecNode::close(state);
}

Status AggregateBaseNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::InternalError("not support rowBatch version of get_next");
}

void AggregateBaseNode::push_down_join_runtime_filter(RuntimeState* state,
                                                      vectorized::RuntimeFilterProbeCollector* collector) {
    // accept runtime filters from parent if possible.
    _runtime_filter_collector.push_down(collector, _tuple_ids);

    // check to see if runtime filters can be rewritten
    auto& descriptors = _runtime_filter_collector.descriptors();
    RuntimeFilterProbeCollector pushdown_collector;

    auto iter = descriptors.begin();
    while (iter != descriptors.end()) {
        RuntimeFilterProbeDescriptor* rf_desc = iter->second;
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
};

} // namespace starrocks::vectorized