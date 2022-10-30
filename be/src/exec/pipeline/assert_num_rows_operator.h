// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
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
class AssertNumRowsOperator final : public Operator {
public:
    AssertNumRowsOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                          const int64_t& desired_num_rows, const std::string& subquery_string,
                          const TAssertion::type assertion)
            : Operator(factory, id, "assert_num_rows_sink", plan_node_id, driver_sequence),
              _desired_num_rows(desired_num_rows),
              _subquery_string(subquery_string),
              _assertion(assertion),
              _has_assert(false) {}

    ~AssertNumRowsOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    bool need_input() const override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;
    bool is_finished() const override;

private:
    const int64_t& _desired_num_rows;
    const std::string& _subquery_string;
    const TAssertion::type _assertion;
    bool _has_assert;

    int64_t _actual_num_rows = 0;
    vectorized::ChunkPtr _cur_chunk = nullptr;

    bool _input_finished = false;
};

class AssertNumRowsOperatorFactory final : public OperatorFactory {
public:
    AssertNumRowsOperatorFactory(int32_t id, int32_t plan_node_id, int64_t desired_num_rows,
                                 const std::string& subquery_string, TAssertion::type assertion)
            : OperatorFactory(id, "assert_num_rows_sink", plan_node_id),
              _desired_num_rows(desired_num_rows),
              _subquery_string(subquery_string),
              _assertion(assertion) {}

    ~AssertNumRowsOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AssertNumRowsOperator>(this, _id, _plan_node_id, driver_sequence, _desired_num_rows,
                                                       _subquery_string, _assertion);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    int64_t _desired_num_rows;
    std::string _subquery_string;
    TAssertion::type _assertion;
};

} // namespace pipeline
} // namespace starrocks
