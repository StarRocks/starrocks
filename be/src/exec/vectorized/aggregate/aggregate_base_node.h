// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/exec_node.h"
#include "exec/vectorized/aggregate/aggregate_base.h"

namespace starrocks::vectorized {

class AggregateBaseNode : public ExecNode, public AggregateBase {
public:
    AggregateBaseNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs), AggregateBase(tnode) {}
    ~AggregateBaseNode() = default;

    MemTracker* get_memtracker0() override { return ExecNode::mem_tracker(); }
    int64_t get_num_rows_returned0() const override { return _num_rows_returned; }
    int64_t get_prev_num_rows_returned0() const override { return _children[0]->rows_returned(); }
    int64_t increase_num_rows_returned0(int increment) override {
        _num_rows_returned += increment;
        return _num_rows_returned;
    }
    RuntimeProfile::Counter* get_rows_returned_counter0() override { return _rows_returned_counter; }
    int64_t get_limit0() override { return ExecNode::limit(); }
    bool reached_limit0() override { return ExecNode::reached_limit(); }
    ObjectPool* get_obj_pool0() override { return _pool; }
    bool is_closed0() override { return ExecNode::is_closed(); }
    RuntimeProfile* get_runtime_profile0() override { return ExecNode::runtime_profile(); }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    void push_down_join_runtime_filter(RuntimeState* state,
                                       vectorized::RuntimeFilterProbeCollector* collector) override;
};
} // namespace starrocks::vectorized