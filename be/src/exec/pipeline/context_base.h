// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "common/status.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// The complex ExecNode is usually decomposed to multiple operators,
// which share some variables in a context, and have dependency relationships between them.
// (For example, HashJoinNode is composed to HashJoinBuildOperator and HashJoinProbeOperator,
// which share a HashJoiner context. And HashJoinProbeOperator depends on HashJoinBuildOperator.)
// Assume OpA depends on OpB.
// However, OpA can be finished earlier than OpB, if it's successor operator is finished early.
// Therefore, there are two problems which needs to be considered:
// 1. OpB should perceive that OpA has been finished early.
// 2. Context object can be released only if both OpA and OpB are closed,
//    because the close order of OpA and OpB are uncertain.
class ContextBase {
public:
    ContextBase() = default;
    ~ContextBase() = default;

    ContextBase(const ContextBase&) = delete;
    ContextBase(ContextBase&&) = delete;
    ContextBase& operator=(ContextBase&&) = delete;
    ContextBase& operator=(const ContextBase&) = delete;

    virtual Status close(RuntimeState* state) = 0;

    // create_one_operator and close_one_operator are used to close context
    // when the last running related operator is closed.
    // - create_one_operator is called by operator::constructor() at the preparation stage.
    // - close_one_operator is called by operator::close() at the close stage.
    // It is the guaranteed by the dispatcher queue that the increment operations
    // by create_one_operator() are visible to close_one_operator(), so we needn't barrier here.
    void create_one_operator() {
        _num_running_operators.fetch_add(1, std::memory_order_relaxed);
    }

    // Called by operator::close. Close the context when the last running operator is closed.
    Status close_one_operator(RuntimeState* state) {
        if (_num_running_operators.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            return close(state);
        }
        return Status::OK();
    }

    // When the output operator is finished, the context can be finished regardless of other running operators.
    void set_finished() { _is_finished.store(true, std::memory_order_release); }

    // Predicate whether the context is finished, which is used to notify non-output operators finished early.
    bool is_finished() const { return _is_finished.load(std::memory_order_acquire); }

private:
    std::atomic<int32_t> _num_running_operators = 0;
    std::atomic<bool> _is_finished = false;
};

} // namespace starrocks::pipeline