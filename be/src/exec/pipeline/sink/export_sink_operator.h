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
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/file_result_writer.h"

namespace starrocks {

class RowDescriptor;

namespace pipeline {

class ExportSinkIOBuffer;
class FragmentContext;

class ExportSinkOperator final : public Operator {
public:
    ExportSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                       std::shared_ptr<ExportSinkIOBuffer> export_sink_buffer, std::atomic<int32_t>& num_sinkers)
            : Operator(factory, id, "export_sink", plan_node_id, false, driver_sequence),
              _export_sink_buffer(std::move(std::move(export_sink_buffer))),
              _num_sinkers(num_sinkers) {}

    ~ExportSinkOperator() override = default;

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
    std::shared_ptr<ExportSinkIOBuffer> _export_sink_buffer;
    std::atomic<int32_t>& _num_sinkers;
    bool _is_audit_report_done = true;
};

class ExportSinkOperatorFactory final : public OperatorFactory {
public:
    ExportSinkOperatorFactory(int32_t id, const TExportSink& t_export_sink, std::vector<TExpr> t_output_expr,
                              int32_t num_sinkers, FragmentContext* fragment_ctx)
            : OperatorFactory(id, "export_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
              _t_export_sink(t_export_sink),
              _t_output_expr(std::move(std::move(t_output_expr))),
              _total_num_sinkers(num_sinkers),
              _fragment_ctx(fragment_ctx) {}

    ~ExportSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        _increment_num_sinkers_no_barrier();
        return std::make_shared<ExportSinkOperator>(this, _id, _plan_node_id, driver_sequence, _export_sink_buffer,
                                                    _num_sinkers);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    void _increment_num_sinkers_no_barrier() { _num_sinkers.fetch_add(1, std::memory_order_relaxed); }

    TExportSink _t_export_sink;

    const std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;

    const int32_t _total_num_sinkers;
    std::atomic<int32_t> _num_sinkers = 0;
    std::shared_ptr<ExportSinkIOBuffer> _export_sink_buffer;

    FragmentContext* _fragment_ctx = nullptr;
};

} // namespace pipeline
} // namespace starrocks
