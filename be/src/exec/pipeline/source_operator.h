// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

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
    // pending_finish returns whether this source operator still has pending i/o task which executed in i/o threads
    // and has reference to the object outside (such as desc_tbl) owned by FragmentContext.
    // It can ONLY be called after calling finish().
    // When a driver's sink operator is finished, the driver should wait for pending i/o task completion.
    // Otherwise, pending tasks shall reference to destructed objects in FragmentContext,
    // since FragmentContext is unregistered prematurely after all the drivers are finalized.
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
    SourceOperatorFactory(int32_t id, const std::string& name, int32_t plan_node_id)
            : OperatorFactory(id, name, plan_node_id) {}
    bool is_source() const override { return true; }
    // with_morsels returning true means that the SourceOperator needs attach to MorselQueue, only
    // ScanOperator needs to do so.
    virtual bool with_morsels() const { return false; }
    // Set the DOP(degree of parallelism) of the SourceOperator, SourceOperator's DOP determine the Pipeline's DOP.
    virtual void set_degree_of_parallelism(size_t degree_of_parallelism) {
        this->_degree_of_parallelism = degree_of_parallelism;
    }
    virtual size_t degree_of_parallelism() const { return _degree_of_parallelism; }

protected:
    size_t _degree_of_parallelism = 1;
};
} // namespace pipeline
} // namespace starrocks
