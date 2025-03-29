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

#include "runtime/customized_result_writer.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/expr.h"
#include "gen_cpp/Row_types.h"
#include "runtime/buffer_control_block.h"
#include "types/logical_type.h"
#include "util/faststring.h"
#include "util/thrift_util.h"

namespace starrocks {

CustomizedResultWriter::CustomizedResultWriter(BufferControlBlock* sinker,
                                               const std::vector<ExprContext*>& output_expr_ctxs,
                                               starrocks::RuntimeProfile* parent_profile)
        : BufferControlResultWriter(sinker, parent_profile), _output_expr_ctxs(output_expr_ctxs) {}

CustomizedResultWriter::~CustomizedResultWriter() = default;

Status CustomizedResultWriter::init(RuntimeState* state) {
    _pack_funcs.resize(_output_expr_ctxs.size(), nullptr);
    for (auto i = 0; i < _output_expr_ctxs.size(); ++i) {
        ASSIGN_OR_RETURN(_pack_funcs[i], get_pack(_output_expr_ctxs[i]->root()->type().type));
    }
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is nullptr.");
    }
    return Status::OK();
}

Status CustomizedResultWriter::append_chunk(Chunk* chunk) {
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

    return status;
}

StatusOr<TFetchDataResultPtrs> CustomizedResultWriter::process_chunk(Chunk* chunk) {
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

StatusOr<TFetchDataResultPtr> CustomizedResultWriter::_process_chunk(Chunk* chunk) {
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return nullptr;
    }

    int num_columns = _output_expr_ctxs.size();

    Columns result_columns;
    result_columns.reserve(num_columns);

    for (int i = 0; i < num_columns; ++i) {
        ASSIGN_OR_RETURN(auto col, _output_expr_ctxs[i]->evaluate(chunk));
        col = _output_expr_ctxs[i]->root()->type().type == TYPE_TIME
                      ? ColumnHelper::convert_time_column_from_double_to_str(col)
                      : col;
        result_columns.emplace_back(std::move(col));
    }

    std::unique_ptr<TFetchDataResult> result(new (std::nothrow) TFetchDataResult());
    if (!result) {
        return Status::MemoryAllocFailed("memory allocate failed");
    }

    const auto num_rows = chunk->num_rows();
    auto& rows = result->result_batch.rows;
    rows.resize(num_rows);
    faststring packed_data;
    // reserve 20480 bytes for initial allocation, it is a moderate size, in most of the situations,
    // it is enough to hold a packed row.
    packed_data.reserve(20480);
    ThriftSerializer compact_serializer(true, 20480);
    for (auto i = 0; i < num_rows; ++i) {
        TRowFormat row;
        std::string null_bits(num_columns, '\0');
        auto* nulls = null_bits.data();
        for (auto c = 0; c < num_columns; ++c) {
            if (!result_columns[c]->is_null(i)) {
                nulls[c] = 1;
                _pack_funcs[c](&packed_data, result_columns[c]->get(i));
            }
        }
        row.__isset.null_bits = true;
        row.null_bits = std::move(null_bits);
        row.__isset.packed_data = true;
        row.packed_data = packed_data.ToString();
        packed_data.resize(0);
        std::string str_row;
        RETURN_IF_ERROR(compact_serializer.serialize(&row, &str_row));
        rows[i] = std::move(str_row);
    }
    return result;
}

} // namespace starrocks
