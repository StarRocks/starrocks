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

#include <string>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {
class ExprContext;

namespace pipeline {

class SchemaTableSinkOperator final : public Operator {
public:
    SchemaTableSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                            std::vector<ExprContext*> output_expr_ctxs, std::string table_name)
            : Operator(factory, id, "schema_table_sink", plan_node_id, false, driver_sequence),
              _output_expr_ctxs(std::move(output_expr_ctxs)),
              _table_name(std::move(table_name)) {}

    ~SchemaTableSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    bool pending_finish() const override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    static Status write_be_configs_table(Columns& columns);
    std::vector<ExprContext*> _output_expr_ctxs;
    bool _is_finished = false;
    std::string _table_name;
};

class SchemaTableSinkOperatorFactory final : public OperatorFactory {
public:
    SchemaTableSinkOperatorFactory(int32_t id, std::vector<TExpr> t_output_expr,
                                   const TSchemaTableSink& t_schema_table_sink, FragmentContext* const fragment_ctx);

    ~SchemaTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<SchemaTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _output_expr_ctxs,
                                                         _table_name);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::string _table_name;
};

} // namespace pipeline
} // namespace starrocks
