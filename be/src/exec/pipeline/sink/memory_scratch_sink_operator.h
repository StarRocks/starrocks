// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/result_queue_mgr.h"
#include "util/blocking_queue.hpp"

namespace arrow {
class MemoryPool;
class RecordBatch;
class Schema;
} // namespace arrow

namespace starrocks {
class ExprContext;

namespace pipeline {

class MemoryScratchSinkOperator final : public Operator {
public:
    MemoryScratchSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                              const std::vector<ExprContext*>& output_expr_ctxs,
                              const std::shared_ptr<arrow::Schema>& arrow_schema, const BlockQueueSharedPtr& queue)
            : Operator(factory, id, "memory_scratch_sink", plan_node_id, driver_sequence),
              _output_expr_ctxs(output_expr_ctxs),
              _arrow_schema(arrow_schema),
              _queue(queue) {}

    ~MemoryScratchSinkOperator() override = default;

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
    void try_to_put_sentinel();

    std::vector<ExprContext*> _output_expr_ctxs;
    std::shared_ptr<arrow::Schema> _arrow_schema;
    BlockQueueSharedPtr _queue;
    mutable std::shared_ptr<arrow::RecordBatch> _pending_result;
    bool _is_finished = false;
    bool _has_put_sentinel = false;
};

class MemoryScratchSinkOperatorFactory final : public OperatorFactory {
public:
    MemoryScratchSinkOperatorFactory(int32_t id, const RowDescriptor& row_desc, std::vector<TExpr> t_output_expr,
                                     FragmentContext* const fragment_ctx);

    ~MemoryScratchSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MemoryScratchSinkOperator>(this, _id, _plan_node_id, driver_sequence, _output_expr_ctxs,
                                                           _arrow_schema, _queue);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    void _prepare_id_to_col_name_map();

    const RowDescriptor _row_desc;
    std::shared_ptr<arrow::Schema> _arrow_schema;
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;

    BlockQueueSharedPtr _queue;
    std::unordered_map<int64_t, std::string> _id_to_col_name;

    FragmentContext* const _fragment_ctx;
};

} // namespace pipeline
} // namespace starrocks