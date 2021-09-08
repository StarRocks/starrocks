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
} // namespace pipeline
} // namespace starrocks
