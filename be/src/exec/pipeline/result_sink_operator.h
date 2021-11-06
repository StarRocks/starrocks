// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "exec/pipeline/operator.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/mysql_result_writer.h"
namespace starrocks {
class BufferControlBlock;
class ExprContext;
class ResultWriter;

namespace pipeline {
class ResultSinkOperator final : public Operator {
public:
    ResultSinkOperator(int32_t id, int32_t plan_node_id, TResultSinkType::type sink_type,
                       const std::vector<ExprContext*>& output_expr_ctxs,
                       const std::shared_ptr<BufferControlBlock>& sender, const size_t& result_sinks_num,
                       std::atomic<size_t>& closed_result_sinks_num, std::atomic<int64_t>& written_rows_num)
            : Operator(id, "result_sink", plan_node_id),
              _sink_type(sink_type),
              _output_expr_ctxs(output_expr_ctxs),
              _sender(sender),
              _result_sinks_num(result_sinks_num),
              _closed_result_sinks_num(closed_result_sinks_num),
              _written_rows_num(written_rows_num) {}

    ~ResultSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    // Result sink will send result chunk to BufferControlBlock directly,
    // Then FE will pull result from BufferControlBlock
    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished && !_fetch_data_result && _sender; }

    void finish(RuntimeState* state) override { _is_finished = true; }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    TResultSinkType::type _sink_type;
    std::vector<ExprContext*> _output_expr_ctxs;

    /// The following four fields are shared by all the ResultSinkOperators
    /// created by the same ResultSinkOperatorFactory.
    const std::shared_ptr<BufferControlBlock>& _sender;
    const size_t& _result_sinks_num;
    std::atomic<size_t>& _closed_result_sinks_num;
    std::atomic<int64_t>& _written_rows_num;

    std::shared_ptr<ResultWriter> _writer;
    mutable TFetchDataResultPtr _fetch_data_result;

    std::unique_ptr<RuntimeProfile> _profile = nullptr;

    mutable Status _last_error;
    bool _is_finished = false;
};

class ResultSinkOperatorFactory final : public OperatorFactory {
public:
    ResultSinkOperatorFactory(int32_t id, int32_t plan_node_id, TResultSinkType::type sink_type,
                              std::vector<TExpr> t_output_expr)
            : OperatorFactory(id, "result_sink", plan_node_id),
              _sink_type(sink_type),
              _t_output_expr(std::move(t_output_expr)) {}

    ~ResultSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        ++_result_sinks_num;
        return std::make_shared<ResultSinkOperator>(_id, _plan_node_id, _sink_type, _output_expr_ctxs, _sender,
                                                    _result_sinks_num, _closed_result_sinks_num, _written_rows_num);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    TResultSinkType::type _sink_type;
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;

    /// The followings are shared by all the ResultSinkOperators created by this ResultSinkOperatorFactory.
    // A fragment_instance_id can only have ONE sender, because result_mgr saves the mapping from fragment_instance_id
    // to sender. Therefore, sender is created in this factory and shared by all the ResultSinkOperator instances.
    std::shared_ptr<BufferControlBlock> _sender;
    size_t _result_sinks_num = 0;
    std::atomic<size_t> _closed_result_sinks_num = 0;
    std::atomic<int64_t> _written_rows_num = 0;
};

} // namespace pipeline
} // namespace starrocks
