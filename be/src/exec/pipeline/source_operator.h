// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/chunk_source.h"
#include "exec/pipeline/operator.h"

namespace starrocks {
namespace pipeline {
class SourceOperator;
using SourceOperatorPtr = std::shared_ptr<SourceOperator>;

class SourceOperator : public Operator {
public:
    SourceOperator(int32_t id, std::string name, int32_t plan_node_id) : Operator(id, name, plan_node_id) {}
    ~SourceOperator() override = default;

    bool need_input() const override { return false; }
    virtual bool pending_finish() { return false; }
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override {
        return Status::InternalError("Shouldn't push chunk to source operator");
    }

    virtual void add_morsel_queue(MorselQueue* morsel_queue) { _morsel_queue = morsel_queue; };

protected:
    MorselQueue* _morsel_queue;
    ChunkSourcePtr _chunk_source;
};

class SourceOperatorFactory : public OperatorFactory {
public:
    SourceOperatorFactory(int32_t id, int32_t plan_node_id) : OperatorFactory(id, plan_node_id) {}
    bool is_source() const override { return true; }
    virtual bool with_morsels() const { return false; }
    virtual void set_num_driver_instances(size_t num_driver_instances) {
        this->_num_driver_instances = num_driver_instances;
    }
    virtual size_t num_driver_instances() const { return _num_driver_instances; }

protected:
    size_t _num_driver_instances = 1;
};
} // namespace pipeline
} // namespace starrocks
