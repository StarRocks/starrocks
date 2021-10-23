// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/assert_num_rows/assert_num_rows_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class ExecNode;

namespace vectorized {
class ChunksSorter;
}

namespace pipeline {
class AssertNumRowsSourceOperator final : public SourceOperator {
public:
    AssertNumRowsSourceOperator(int32_t id, int32_t plan_node_id, const int64_t& desired_num_rows,
                                const std::string& subquery_string, const TAssertion::type& assertion,
                                const std::shared_ptr<AssertNumRowsContext>& assert_num_rowscontext)
            : SourceOperator(id, "assert_num_rows_source_sink", plan_node_id),
              _desired_num_rows(desired_num_rows),
              _subquery_string(subquery_string),
              _assertion(std::move(assertion)),
              _assert_num_rows_context(assert_num_rowscontext),
              _has_assert(false) {}

    ~AssertNumRowsSourceOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    void finish(RuntimeState* state) override;

private:
    const int64_t& _desired_num_rows;
    const std::string& _subquery_string;
    const TAssertion::type& _assertion;
    const std::shared_ptr<AssertNumRowsContext>& _assert_num_rows_context;
    bool _has_assert;

    bool _is_finished = false;
};

class AssertNumRowsSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AssertNumRowsSourceOperatorFactory(int32_t id, int32_t plan_node_id, int64_t desired_num_rows,
                                       const std::string& subquery_string, TAssertion::type&& assertion,
                                       std::shared_ptr<AssertNumRowsContext>&& assert_num_rows_context)
            : SourceOperatorFactory(id, "assert_num_rows_source_sink", plan_node_id),
              _desired_num_rows(desired_num_rows),
              _subquery_string(subquery_string),
              _assertion(std::move(assertion)),
              _assert_num_rows_context(std::move(assert_num_rows_context)) {}

    ~AssertNumRowsSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        auto ope = std::make_shared<AssertNumRowsSourceOperator>(
                _id, _plan_node_id, _desired_num_rows, _subquery_string, _assertion, _assert_num_rows_context);
        return ope;
    }

    Status prepare(RuntimeState* state, MemTracker* mem_tracker) override;
    void close(RuntimeState* state) override;

private:
    int64_t _desired_num_rows;
    std::string _subquery_string;
    TAssertion::type _assertion;
    std::shared_ptr<AssertNumRowsContext> _assert_num_rows_context;
};

} // namespace pipeline
} // namespace starrocks
