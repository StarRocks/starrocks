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
#include "runtime/mysql_result_writer.h"

namespace starrocks {
class BufferControlBlock;
class ExprContext;
class ResultWriter;

namespace pipeline {
class ResultSinkOperator final : public Operator {
public:
    ResultSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                       TResultSinkType::type sink_type, bool is_binary_format, TResultSinkFormatType::type format_type,
                       std::vector<ExprContext*> output_expr_ctxs, const std::shared_ptr<BufferControlBlock>& sender,
                       std::atomic<int32_t>& num_sinks, std::atomic<int64_t>& num_written_rows,
                       FragmentContext* const fragment_ctx)
            : Operator(factory, id, "result_sink", plan_node_id, false, driver_sequence),
              _sink_type(sink_type),
              _is_binary_format(is_binary_format),
              _format_type(format_type),
              _output_expr_ctxs(std::move(output_expr_ctxs)),
              _sender(sender),
              _num_sinkers(num_sinks),
              _num_written_rows(num_written_rows),
              _fragment_ctx(fragment_ctx) {}

    ~ResultSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    // Result sink will send result chunk to BufferControlBlock directly,
    // Then FE will pull result from BufferControlBlock
    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished && _fetch_data_result.empty(); }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    TResultSinkType::type _sink_type;
    bool _is_binary_format;
    TResultSinkFormatType::type _format_type;
    std::vector<ExprContext*> _output_expr_ctxs;

    /// The following three fields are shared by all the ResultSinkOperators
    /// created by the same ResultSinkOperatorFactory.
    const std::shared_ptr<BufferControlBlock>& _sender;
    std::atomic<int32_t>& _num_sinkers;
    std::atomic<int64_t>& _num_written_rows;

    std::shared_ptr<ResultWriter> _writer;
    mutable TFetchDataResultPtrs _fetch_data_result;

    std::unique_ptr<RuntimeProfile> _profile = nullptr;

    mutable Status _last_error;
    bool _is_finished = false;

    FragmentContext* const _fragment_ctx;
};

class ResultSinkOperatorFactory final : public OperatorFactory {
public:
    ResultSinkOperatorFactory(int32_t id, TResultSinkType::type sink_type, bool is_binary_format,
                              TResultSinkFormatType::type format_type, std::vector<TExpr> t_output_expr,
                              FragmentContext* const fragment_ctx)
            : OperatorFactory(id, "result_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
              _sink_type(sink_type),
              _is_binary_format(is_binary_format),
              _format_type(format_type),
              _t_output_expr(std::move(t_output_expr)),
              _fragment_ctx(fragment_ctx) {}

    ~ResultSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        // _num_sinkers is incremented when creating a ResultSinkOperator instance here at the preparing
        // phase of FragmentExecutor, and decremented and read when closing ResultSinkOperator. The visibility
        // of increasing _num_sinkers to ResultSinkOperator::close is guaranteed by pipeline driver queue,
        // so it doesn't need memory barrier here.
        _increment_num_sinkers_no_barrier();
        return std::make_shared<ResultSinkOperator>(this, _id, _plan_node_id, driver_sequence, _sink_type,
                                                    _is_binary_format, _format_type, _output_expr_ctxs, _sender,
                                                    _num_sinkers, _num_written_rows, _fragment_ctx);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    void _increment_num_sinkers_no_barrier() { _num_sinkers.fetch_add(1, std::memory_order_relaxed); }

    TResultSinkType::type _sink_type;
    bool _is_binary_format;
    TResultSinkFormatType::type _format_type;
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;

    /// The followings are shared by all the ResultSinkOperators created by this ResultSinkOperatorFactory.
    // A fragment_instance_id can only have ONE sender, because result_mgr saves the mapping from fragment_instance_id
    // to sender. Therefore, sender is created in this factory and shared by all the ResultSinkOperator instances.
    std::shared_ptr<BufferControlBlock> _sender;
    std::atomic<int32_t> _num_sinkers = 0;
    std::atomic<int64_t> _num_written_rows = 0;

    FragmentContext* const _fragment_ctx;
};

} // namespace pipeline
} // namespace starrocks
