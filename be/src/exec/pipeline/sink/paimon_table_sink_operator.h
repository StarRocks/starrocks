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

#include "common/logging.h"
#include "exec/jni_writer.h"
#include "exec/paimon_writer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "gen_cpp/DataSinks_types.h"

namespace starrocks::pipeline {

class PaimonTableSinkOperator final : public Operator {
public:
    PaimonTableSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                            PaimonTableDescriptor* paimon_table, int32_t driver_sequence,
                            std::vector<ExprContext*> partition_expr_ctxs, std::vector<ExprContext*> bucket_expr_ctxs,
                            std::vector<ExprContext*> output_expr_ctxs, std::vector<std::string> data_column_names,
                            std::vector<std::string> data_column_types, bool use_native_writer)
            : Operator(factory, id, "paimon_table_sink", plan_node_id, false, driver_sequence),
              _paimon_table(paimon_table),
              _partition_expr(std::move(partition_expr_ctxs)),
              _bucket_expr(std::move(bucket_expr_ctxs)),
              _output_expr(std::move(output_expr_ctxs)),
              _data_column_names(std::move(data_column_names)),
              _data_column_types(std::move(data_column_types)),
              _use_native_writer(use_native_writer) {}

    ~PaimonTableSinkOperator() override = default;

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

    Status do_commit(RuntimeState* state);

    void add_paimon_commit_info(const std::string& paimon_commit_info, RuntimeState* state);

private:
#define RETURN_AND_SET_IF_ERROR(stmt)                                                                 \
    do {                                                                                              \
        auto&& status__ = (stmt);                                                                     \
        if (UNLIKELY(!status__.ok())) {                                                               \
            _error = true;                                                                            \
            return to_status(status__).clone_and_append_context(__FILE__, __LINE__, AS_STRING(stmt)); \
        }                                                                                             \
    } while (false)

    Status init_profile();

    std::string _location;
    std::string _file_format;
    TCompressionType::type _compression_codec;
    TCloudConfiguration _cloud_conf;
    PaimonTableDescriptor* _paimon_table;

    std::vector<ExprContext*> _partition_expr;
    std::vector<ExprContext*> _bucket_expr;
    std::vector<ExprContext*> _output_expr;
    std::atomic<bool> _is_finished = false;
    bool _is_static_partition_insert = false;
    std::vector<std::string> _partition_column_names;
    std::vector<std::string> _data_column_names;
    std::vector<std::string> _data_column_types;
    bool _use_native_writer = false;

    RuntimeProfile::Counter* _init_timer = nullptr;
    RuntimeProfile::Counter* _write_timer = nullptr;
    RuntimeProfile::Counter* _convert_timer = nullptr;
    RuntimeProfile::Counter* _commit_timer = nullptr;

    bool _closed = false;
    int _num_chunk = 0;
    std::atomic_bool _error{false};
    std::unique_ptr<PaimonWriter> _writer = nullptr;

    std::unique_ptr<PaimonWriter> create_paimon_writer();
    std::unique_ptr<PaimonWriter> create_paimon_jni_writer();
};

class PaimonTableSinkOperatorFactory final : public OperatorFactory {
public:
    PaimonTableSinkOperatorFactory(int32_t id, FragmentContext* fragment_ctx, PaimonTableDescriptor* paimon_table,
                                   const TPaimonTableSink& t_paimon_table_sink, vector<TExpr> t_output_expr,
                                   std::vector<ExprContext*> partition_expr_ctxs,
                                   std::vector<ExprContext*> bucket_expr_ctxs,
                                   std::vector<ExprContext*> output_expr_ctxs,
                                   std::vector<std::string> data_column_names,
                                   std::vector<std::string> data_column_types, bool use_native_writer);

    ~PaimonTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::vector<ExprContext*> _partition_expr_ctxs;
    std::vector<ExprContext*> _bucket_expr_ctxs;
    PaimonTableDescriptor* _paimon_table;
    std::vector<std::string> _data_column_names;
    std::vector<std::string> _data_column_types;
    bool _use_native_writer = false;
};

} // namespace starrocks::pipeline
