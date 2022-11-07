// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

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
                       std::shared_ptr<ExportSinkIOBuffer> export_sink_buffer)
            : Operator(factory, id, "export_sink", plan_node_id, driver_sequence),
              _export_sink_buffer(export_sink_buffer) {}

    ~ExportSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    bool pending_finish() const override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    std::shared_ptr<ExportSinkIOBuffer> _export_sink_buffer;
};

class ExportSinkOperatorFactory final : public OperatorFactory {
public:
    ExportSinkOperatorFactory(int32_t id, const TExportSink& t_export_sink, std::vector<TExpr> t_output_expr,
                              int32_t num_sinkers, FragmentContext* fragment_ctx)
            : OperatorFactory(id, "export_sink", Operator::s_pseudo_plan_node_id_for_export_sink),
              _t_export_sink(t_export_sink),
              _t_output_expr(t_output_expr),
              _num_sinkers(num_sinkers),
              _fragment_ctx(fragment_ctx) {}

    ~ExportSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<ExportSinkOperator>(this, _id, _plan_node_id, driver_sequence, _export_sink_buffer);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    TExportSink _t_export_sink;

    const std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;

    int32_t _num_sinkers;
    std::shared_ptr<ExportSinkIOBuffer> _export_sink_buffer;

    FragmentContext* _fragment_ctx = nullptr;
};

} // namespace pipeline
} // namespace starrocks
