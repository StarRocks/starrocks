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

#include "runtime/variable_result_writer.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/expr.h"
#include "gen_cpp/Data_types.h"
#include "runtime/buffer_control_block.h"
#include "types/logical_type.h"
#include "util/thrift_util.h"

namespace starrocks {

VariableResultWriter::VariableResultWriter(BufferControlBlock* sinker,
                                           const std::vector<ExprContext*>& output_expr_ctxs,
                                           starrocks::RuntimeProfile* parent_profile)
        : BufferControlResultWriter(sinker, parent_profile), _output_expr_ctxs(output_expr_ctxs) {}

VariableResultWriter::~VariableResultWriter() = default;

Status VariableResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is nullptr.");
    }
    return Status::OK();
}

Status VariableResultWriter::append_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    auto process_status = _process_chunk(chunk);
    if (!process_status.ok() || process_status.value() == nullptr) {
        return process_status.status();
    }
    auto result = std::move(process_status.value());
    size_t num_rows = result->result_batch.rows.size();
    Status status = _sinker->add_batch(result);

    if (status.ok()) {
        _written_rows += num_rows;
        return status;
    }

    LOG(WARNING) << "Append user variable result to sink failed, status : " << status.to_string();
    return status;
}

StatusOr<TFetchDataResultPtrs> VariableResultWriter::process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    TFetchDataResultPtrs results;
    auto process_status = _process_chunk(chunk);
    if (!process_status.ok()) {
        return process_status.status();
    }
    if (process_status.value() != nullptr) {
        results.push_back(std::move(process_status.value()));
    }
    return results;
}

StatusOr<TFetchDataResultPtr> VariableResultWriter::_process_chunk(Chunk* chunk) {
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return nullptr;
    }

    int num_columns = _output_expr_ctxs.size();
    Columns result_columns;
    result_columns.reserve(num_columns);
    for (int i = 0; i < num_columns; ++i) {
        ASSIGN_OR_RETURN(auto col, _output_expr_ctxs[i]->evaluate(chunk));
        result_columns.emplace_back(std::move(col));
    }

    std::unique_ptr<TFetchDataResult> result(new (std::nothrow) TFetchDataResult());
    if (!result) {
        return Status::MemoryAllocFailed("memory allocate failed");
    }

    auto* variable = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(result_columns[0].get()));
    std::vector<TVariableData> var_list;

    int num_rows = chunk->num_rows();
    var_list.resize(num_rows);
    if (!result_columns[0]->is_null(0)) {
        var_list[0].__set_isNull(false);
        var_list[0].__set_result(variable->get_slice(0).to_string());
    } else {
        var_list[0].__set_isNull(true);
    }
    result->result_batch.rows.resize(num_rows);

    ThriftSerializer serializer(true, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&var_list[i], &result->result_batch.rows[i]));
    }
    return result;
}

} // namespace starrocks
