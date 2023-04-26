// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <utility>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class ExecNode;
class ChunksSorter;

namespace pipeline {
class AssertNumRowsOperator final : public Operator {
public:
    AssertNumRowsOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                          const int64_t& desired_num_rows, const std::string& subquery_string,
                          const TAssertion::type assertion)
            : Operator(factory, id, "assert_num_rows_sink", plan_node_id, driver_sequence),
              _desired_num_rows(desired_num_rows),
              _subquery_string(subquery_string),
              _assertion(assertion) {}

    ~AssertNumRowsOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    bool need_input() const override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;
    bool is_finished() const override;

private:
    const int64_t& _desired_num_rows;
    const std::string& _subquery_string;
    const TAssertion::type _assertion;

    int64_t _actual_num_rows = 0;
    ChunkPtr _cur_chunk = nullptr;

    bool _input_finished = false;
};

class AssertNumRowsOperatorFactory final : public OperatorFactory {
public:
    AssertNumRowsOperatorFactory(int32_t id, int32_t plan_node_id, int64_t desired_num_rows,
                                 std::string subquery_string, TAssertion::type assertion)
            : OperatorFactory(id, "assert_num_rows_sink", plan_node_id),
              _desired_num_rows(desired_num_rows),
              _subquery_string(std::move(subquery_string)),
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
