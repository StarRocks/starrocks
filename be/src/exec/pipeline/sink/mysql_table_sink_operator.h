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

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {
class ExprContext;

namespace pipeline {

class MysqlTableSinkIOBuffer;

class MysqlTableSinkOperator final : public Operator {
public:
    MysqlTableSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                           std::shared_ptr<MysqlTableSinkIOBuffer> mysql_table_sink_buffer)
            : Operator(factory, id, "mysql_table_sink", plan_node_id, driver_sequence),
              _mysql_table_sink_buffer(std::move(std::move(mysql_table_sink_buffer))) {}

    ~MysqlTableSinkOperator() override = default;

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
    std::shared_ptr<MysqlTableSinkIOBuffer> _mysql_table_sink_buffer;
};

class MysqlTableSinkOperatorFactory final : public OperatorFactory {
public:
    MysqlTableSinkOperatorFactory(int32_t id, const TMysqlTableSink& t_mysql_table_sink,
                                  std::vector<TExpr> t_output_expr, int32_t num_sinkers, FragmentContext* fragment_ctx);

    ~MysqlTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MysqlTableSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                        _mysql_table_sink_buffer);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    TMysqlTableSink _t_mysql_table_sink;
    int32_t _num_sinkers;

    std::shared_ptr<MysqlTableSinkIOBuffer> _mysql_table_sink_buffer;
    FragmentContext* _fragment_ctx = nullptr;
};
} // namespace pipeline
} // namespace starrocks
