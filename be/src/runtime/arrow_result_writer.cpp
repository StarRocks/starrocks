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

#include "runtime/arrow_result_writer.h"

#include <arrow/record_batch.h>
#include <bvar/recorder.h>
#include <column/column_helper.h>
#include <util/arrow/row_batch.h>
#include <util/arrow/starrocks_column_to_arrow.h>

#include "column/const_column.h"
#include "exprs/cast_expr.h"
#include "exprs/expr.h"
#include "rapidjson/writer.h"
#include "runtime/buffer_control_block.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"

namespace starrocks {

// Constructor for ArrowResultWriter
// This class is responsible for converting StarRocks' internal Chunk
// into Arrow RecordBatch format and writing it into a result sink.
//
// Parameters:
// - sinker: the result sink (BufferControlBlock) to which Arrow batches will be written
// - output_expr_ctxs: a list of output expression contexts to be evaluated on the Chunk
// - parent_profile: the parent runtime profile for performance tracking
// - row_desc: the row descriptor (schema) of the output
ArrowResultWriter::ArrowResultWriter(BufferControlBlock* sinker, std::vector<ExprContext*>& output_expr_ctxs,
                                     const std::vector<std::string>& output_column_names,
                                     RuntimeProfile* parent_profile, const RowDescriptor& row_desc)
        : BufferControlResultWriter(sinker, parent_profile),
          _output_expr_ctxs(output_expr_ctxs),
          _output_column_names(output_column_names),
          _row_desc(row_desc) {}

// ┌────────────────────────────────────────────────────────────┐
// │ init(): Initialize ArrowResultWriter                       │
// └────────────────────────────────────────────────────────────┘
// [1] Init performance timer
// [2] Check sinker is not null
// [3] Build column ID → name map
// [4] Convert RowDescriptor → Arrow Schema
// [5] Register Arrow Schema to ResultMgr
Status ArrowResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    std::unordered_map<int64_t, std::string> temp_id_to_col_name;

    RETURN_IF_ERROR(convert_to_arrow_schema(_row_desc, temp_id_to_col_name, &_arrow_schema, _output_expr_ctxs,
                                            &_output_column_names, state->arrow_flight_sql_version()));

    state->exec_env()->result_mgr()->set_arrow_schema(state->fragment_instance_id(), _arrow_schema);

    return Status::OK();
}

void ArrowResultWriter::_init_profile() {
    _append_chunk_timer = ADD_TIMER(_parent_profile, "AppendChunkTime");
}

Status ArrowResultWriter::append_chunk(Chunk* chunk) {
    return Status::OK();
}

Status ArrowResultWriter::close() {
    VLOG_ROW << "[Flight] ArrowResultWriter::close() called";
    return Status::OK();
}

// ┌────────────────────────────────────────────────────────────┐
// │ process_chunk(): Convert Chunk → Arrow → Write to sinker   │
// └────────────────────────────────────────────────────────────┘
// [1] Start timer
// [2] Convert Chunk → Arrow::RecordBatch
// [3] Write Arrow batch to sinker (BufferControlBlock)
StatusOr<TFetchDataResultPtrs> ArrowResultWriter::process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(convert_chunk_to_arrow_batch(chunk, _output_expr_ctxs, _arrow_schema, arrow::default_memory_pool(),
                                                 &result));
    RETURN_IF_ERROR(_sinker->add_arrow_batch(result));

    _written_rows += chunk->num_rows();

    return TFetchDataResultPtrs{};
}

} // namespace starrocks
