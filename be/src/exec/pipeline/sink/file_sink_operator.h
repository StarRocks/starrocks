// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/file_result_writer.h"

namespace starrocks {
class BufferControlBlock;
class ExprContext;

namespace pipeline {

class FileSinkIOBuffer;

class FileSinkOperator final : public Operator {
public:
    FileSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                     std::shared_ptr<FileSinkIOBuffer> file_sink_buffer)
            : Operator(factory, id, "file_sink", plan_node_id, driver_sequence), _file_sink_buffer(file_sink_buffer) {}

    ~FileSinkOperator() override = default;

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
    std::shared_ptr<FileSinkIOBuffer> _file_sink_buffer;
};

class FileSinkOperatorFactory final : public OperatorFactory {
public:
    FileSinkOperatorFactory(int32_t id, std::vector<TExpr> t_output_expr, std::shared_ptr<ResultFileOptions> file_opts,
                            int32_t num_sinkers, FragmentContext* const fragment_ctx);

    ~FileSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<FileSinkOperator>(this, _id, _plan_node_id, driver_sequence, _file_sink_buffer);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::shared_ptr<ResultFileOptions> _file_opts;
    int32_t _num_sinkers;

    std::shared_ptr<FileSinkIOBuffer> _file_sink_buffer;
    RuntimeProfile* _profile = nullptr;
    FragmentContext* const _fragment_ctx;
};
} // namespace pipeline
} // namespace starrocks