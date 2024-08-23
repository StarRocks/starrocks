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

#include "column/chunk.h"
#include "column/const_column.h"
#include "exprs/cast_expr.h"
#include "exprs/expr.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/buffer_control_block.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "types/logical_type.h"

namespace starrocks {

ArrowResultWriter::ArrowResultWriter(BufferControlBlock* sinker, std::vector<ExprContext*>& output_expr_ctxs,
                                     RuntimeProfile* parent_profile,const RowDescriptor& row_desc)
        : _sinker(sinker), _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile), _row_desc(row_desc) {}

Status ArrowResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    _prepare_id_to_col_name_map();
    RETURN_IF_ERROR(convert_to_arrow_schema(_row_desc, _id_to_col_name, &_arrow_schema, _output_expr_ctxs));

    state->exec_env()->result_mgr()->set_arrow_schema(state->fragment_instance_id(), _arrow_schema);

    return Status::OK();
}

void ArrowResultWriter::_init_profile() {
    _append_chunk_timer = ADD_TIMER(_parent_profile, "AppendChunkTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendChunkTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultRendTime", "AppendChunkTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

Status ArrowResultWriter::append_chunk(Chunk* chunk) {
    return Status::OK();
}

Status ArrowResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

StatusOr<TFetchDataResultPtrs> ArrowResultWriter::process_chunk(Chunk* chunk) {
    std::shared_ptr<arrow::RecordBatch> result;
    RETURN_IF_ERROR(convert_chunk_to_arrow_batch(chunk, _output_expr_ctxs, _arrow_schema, arrow::default_memory_pool(),
                                                 &result));
    Status  status = _sinker->add_arrow_batch(result);
    return TFetchDataResultPtrs{};
}

StatusOr<bool> ArrowResultWriter::try_add_batch(TFetchDataResultPtrs& results) {
    return true;
}

void ArrowResultWriter::_prepare_id_to_col_name_map() {
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

} // namespace starrocks
