// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/memory_scratch_sink.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/memory_scratch_sink.h"

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>

#include <sstream>

#include "common/logging.h"
#include "exprs/expr.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/starrocks_column_to_arrow.h"

namespace starrocks {

MemoryScratchSink::MemoryScratchSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_output_expr,
                                     const TMemoryScratchSink& sink)
        : _row_desc(row_desc), _t_output_expr(t_output_expr) {}

MemoryScratchSink::~MemoryScratchSink() = default;

void MemoryScratchSink::_prepare_id_to_col_name_map() {
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

Status MemoryScratchSink::prepare_exprs(RuntimeState* state) {
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));

    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    // Prepare id_to_col_name map
    _prepare_id_to_col_name_map();
    // generate the arrow schema
    RETURN_IF_ERROR(convert_to_arrow_schema(_row_desc, _id_to_col_name, &_arrow_schema, _output_expr_ctxs));
    return Status::OK();
}

Status MemoryScratchSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // prepare output_expr
    RETURN_IF_ERROR(prepare_exprs(state));
    // create queue
    TUniqueId fragment_instance_id = state->fragment_instance_id();
    state->exec_env()->result_queue_mgr()->create_queue(fragment_instance_id, &_queue);
    std::stringstream title;
    title << "MemoryScratchSink (frag_id=" << fragment_instance_id << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));

    return Status::OK();
}

Status MemoryScratchSink::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    // Same as ResultSinkOperator, The memory of the output result set should not be counted in the query memory,
    // otherwise it will cause memory statistics errors.
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return Status::OK();
    }
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(convert_chunk_to_arrow_batch(chunk, _output_expr_ctxs, _arrow_schema, arrow::default_memory_pool(),
                                                 &result));
    _queue->blocking_put(result);
    return Status::OK();
}

Status MemoryScratchSink::open(RuntimeState* state) {
    return Expr::open(_output_expr_ctxs, state);
}

Status MemoryScratchSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    // put sentinel
    if (_queue != nullptr) {
        _queue->blocking_put(nullptr);
    }
    Expr::close(_output_expr_ctxs, state);
    _closed = true;
    return Status::OK();
}

const RowDescriptor MemoryScratchSink::get_row_desc() {
    return _row_desc;
}

} // namespace starrocks
