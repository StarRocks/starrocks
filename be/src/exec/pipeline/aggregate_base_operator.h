// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/vectorized/aggregate/aggregate_base.h"
#include "runtime/types.h"

namespace starrocks {

class ObjectPool;

namespace pipeline {

class AggregateBaseOperator : public Operator, public vectorized::AggregateBase {
public:
    AggregateBaseOperator(int32_t id, std::string name, int32_t plan_node_id, const TPlanNode& tnode);

    ~AggregateBaseOperator() override = default;

    MemTracker* get_memtracker0() override { return Operator::get_memtracker(); }
    int64_t get_num_rows_returned0() const override { return _num_rows_returned; }
    int64_t get_prev_num_rows_returned0() const override {
        // TODO(hcf)
        return -1;
    }
    int64_t increase_num_rows_returned0(int increment) override {
        _num_rows_returned += increment;
        return _num_rows_returned;
    }
    RuntimeProfile::Counter* get_rows_returned_counter0() override { return _rows_returned_counter; }
    int64_t get_limit0() override {
        // TODO(hcf)
        return _limit;
    }
    bool reached_limit0() override { return _limit != -1 && _num_rows_returned >= _limit; }
    ObjectPool* get_obj_pool0() override { return _pool; }
    bool is_closed0() override {
        // TODO(hcf)
        return false;
    }
    RuntimeProfile* get_runtime_profile0() override { return Operator::get_runtime_profile(); }

    Status prepare(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

protected:
    const TPlanNode _tnode;
    ObjectPool* _pool;
    RuntimeProfile::Counter* _rows_returned_counter;
    int _limit = -1;
    int64_t _num_rows_returned = 0;
};

class AggregateBaseOperatorFactory : public OperatorFactory {
public:
    AggregateBaseOperatorFactory(int32_t id, int32_t plan_node_id, const TPlanNode& tnode)
            : OperatorFactory(id, plan_node_id), _tnode(tnode) {}

    ~AggregateBaseOperatorFactory() override = default;

protected:
    const TPlanNode _tnode;
};
} // namespace pipeline
} // namespace starrocks