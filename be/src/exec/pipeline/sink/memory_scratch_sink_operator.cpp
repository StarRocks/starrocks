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

#include "exec/pipeline/sink/memory_scratch_sink_operator.h"

#include "util/arrow/row_batch.h"
#include "util/arrow/starrocks_column_to_arrow.h"

namespace starrocks::pipeline {

Status MemoryScratchSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void MemoryScratchSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool MemoryScratchSinkOperator::need_input() const {
    if (_pending_result == nullptr) {
        return true;
    }
    if (_queue->try_put(_pending_result)) {
        _pending_result.reset();
        return true;
    }
    return false;
}

bool MemoryScratchSinkOperator::is_finished() const {
    return _is_finished;
}

Status MemoryScratchSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

bool MemoryScratchSinkOperator::pending_finish() const {
    // After set_finishing, there may be data that has not been sent.
    // We need to ensure that all remaining data are put into the queue.
    const_cast<MemoryScratchSinkOperator*>(this)->try_to_put_sentinel();
    return !(_is_finished && _pending_result == nullptr && _has_put_sentinel);
}

Status MemoryScratchSinkOperator::set_cancelled(RuntimeState* state) {
    // because we introduced pending_finish, once cancel occurs, some states need to be changed so that the pending_finish can end immediately
    _pending_result.reset();
    _is_finished = true;
    _has_put_sentinel = true;
    return Status::OK();
}

StatusOr<ChunkPtr> MemoryScratchSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from memory scratch sink operator");
}

Status MemoryScratchSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return Status::OK();
    }
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(convert_chunk_to_arrow_batch(chunk.get(), _output_expr_ctxs, _arrow_schema,
                                                 arrow::default_memory_pool(), &result));

    if (!_queue->try_put(result)) {
        DCHECK(_pending_result == nullptr);
        _pending_result.swap(result);
    }
    return Status::OK();
}

void MemoryScratchSinkOperator::try_to_put_sentinel() {
    if (_pending_result != nullptr) {
        if (!_queue->try_put(_pending_result)) {
            return;
        }
        _pending_result.reset();
    }
    // the result queue uses nullptr as a sentinel to indicate that no new data is generated.
    if (!_has_put_sentinel && _queue->try_put(nullptr)) {
        _has_put_sentinel = true;
    }
}

MemoryScratchSinkOperatorFactory::MemoryScratchSinkOperatorFactory(int32_t id, const RowDescriptor& row_desc,
                                                                   std::vector<TExpr> t_output_expr,
                                                                   FragmentContext* const fragment_ctx)
        : OperatorFactory(id, "memory_scratch_sink", Operator::s_pseudo_plan_node_id_for_memory_scratch_sink),
          _row_desc(row_desc),
          _t_output_expr(std::move(t_output_expr)) {}

Status MemoryScratchSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    _prepare_id_to_col_name_map();
    RETURN_IF_ERROR(convert_to_arrow_schema(_row_desc, _id_to_col_name, &_arrow_schema, _output_expr_ctxs));

    TUniqueId fragment_instance_id = state->fragment_instance_id();
    state->exec_env()->result_queue_mgr()->create_queue(fragment_instance_id, &_queue);
    return Status::OK();
}

void MemoryScratchSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

void MemoryScratchSinkOperatorFactory::_prepare_id_to_col_name_map() {
    for (auto* tuple_desc : _row_desc.tuple_descriptors()) {
        auto& slots = tuple_desc->slots();
        int64_t tuple_id = tuple_desc->id();
        for (auto slot : slots) {
            int64_t slot_id = slot->id();
            int64_t id = tuple_id << 32 | slot_id;
            _id_to_col_name.emplace(id, slot->col_name());
        }
    }
}

} // namespace starrocks::pipeline
