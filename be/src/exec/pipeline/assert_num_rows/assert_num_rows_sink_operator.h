// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/assert_num_rows/assert_num_rows_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {
namespace pipeline {
class AssertNumRowsSinkOperator final : public Operator {
public:
    AssertNumRowsSinkOperator(int32_t id, int32_t plan_node_id,
                              const std::shared_ptr<AssertNumRowsContext>& assert_num_rowscontext)
            : Operator(id, "assert_num_rows_sink_sink", plan_node_id),
              _assert_num_rows_context(assert_num_rowscontext) {}

    ~AssertNumRowsSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished; }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    void finish(RuntimeState* state) override;

private:
    const std::shared_ptr<AssertNumRowsContext>& _assert_num_rows_context;

    bool _is_finished = false;
};

class AssertNumRowsSinkOperatorFactory final : public OperatorFactory {
public:
    AssertNumRowsSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                     std::shared_ptr<AssertNumRowsContext> assert_num_rows_context)
            : OperatorFactory(id, "assert_num_rows_sink_sink", plan_node_id),
              _assert_num_rows_context(assert_num_rows_context) {}

    ~AssertNumRowsSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        auto ope = std::make_shared<AssertNumRowsSinkOperator>(_id, _plan_node_id, _assert_num_rows_context);
        return ope;
    }

    Status prepare(RuntimeState* state, MemTracker* mem_tracker) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<AssertNumRowsContext> _assert_num_rows_context;
};

} // namespace pipeline
} // namespace starrocks
