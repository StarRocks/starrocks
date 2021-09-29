// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "common/statusor.h"

namespace starrocks {
class Expr;
class ExprContext;
class MemTracker;
class RuntimeProfile;
class RuntimeState;
namespace pipeline {
class Operator;
using OperatorPtr = std::shared_ptr<Operator>;
using Operators = std::vector<OperatorPtr>;

class Operator {
public:
    Operator(int32_t id, const std::string& name, int32_t plan_node_id);
    virtual ~Operator() = default;

    virtual Status prepare(RuntimeState* state);

    virtual Status close(RuntimeState* state);

    // Whether we could pull chunk from this operator
    virtual bool has_output() const = 0;

    // Whether we could push chunk to this operator
    virtual bool need_input() const = 0;

    // Is this operator completely finished processing and no more
    // output chunks will be produced
    virtual bool is_finished() const = 0;

    // Notifies the operator that no more input chunk will be added.
    // The operator should finish processing.
    // The method should be idempotent, because it may be triggered
    // multiple times in the entire life cycle
    virtual void finish(RuntimeState* state) = 0;

    // Pull chunk from this operator
    // Use shared_ptr, because in some cases (local broadcast exchange),
    // the chunk need to be shared
    virtual StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) = 0;

    // Push chunk to this operator
    virtual Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) = 0;

    int32_t get_id() const { return _id; }

    int32_t get_plan_node_id() const { return _plan_node_id; }

    MemTracker* get_memtracker() const { return _mem_tracker.get(); }

    RuntimeProfile* get_runtime_profile() const { return _runtime_profile.get(); }

    std::string get_name() const { return _name + "_" + std::to_string(_id); }

protected:
    int32_t _id = 0;
    std::string _name;
    // Which plan node this operator belongs to
    int32_t _plan_node_id = -1;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    std::shared_ptr<MemTracker> _mem_tracker;
};

class OperatorFactory {
public:
    OperatorFactory(int32_t id, int32_t plan_node_id) : _id(id), _plan_node_id(plan_node_id) {}
    virtual ~OperatorFactory() = default;
    // Create the operator for the specific sequence driver
    // For some operators, when share some status, need to know the the degree_of_parallelism
    virtual OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) = 0;
    virtual bool is_source() const { return false; }
    int32_t plan_node_id() const { return _plan_node_id; }
    virtual Status prepare(RuntimeState* state, MemTracker* mem_tracker) { return Status::OK(); }
    virtual void close(RuntimeState* state) {}

protected:
    int32_t _id = 0;
    int32_t _plan_node_id = -1;
};

using OpFactoryPtr = std::shared_ptr<OperatorFactory>;
using OpFactories = std::vector<OpFactoryPtr>;

} // namespace pipeline
} // namespace starrocks
