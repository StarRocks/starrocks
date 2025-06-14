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

#include "runtime/http_result_writer.h"

#include <column/column_helper.h>

#include "column/chunk.h"
#include "column/const_column.h"
#include "exprs/cast_expr.h"
#include "exprs/expr.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/buffer_control_block.h"
#include "runtime/current_thread.h"
#include "types/logical_type.h"

namespace starrocks {

HttpResultWriter::HttpResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                                   RuntimeProfile* parent_profile, TResultSinkFormatType::type format_type)
        : BufferControlResultWriter(sinker, parent_profile),
          _output_expr_ctxs(output_expr_ctxs),
          _format_type(format_type) {}

Status HttpResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    return Status::OK();
}

// transform one row into json format
Status HttpResultWriter::_transform_row_to_json(const Columns& result_columns, int idx) {
    size_t num_columns = result_columns.size();

    _row_str.append("{\"data\":[");
    for (size_t i = 0; i < num_columns; ++i) {
        std::string row;
        ASSIGN_OR_RETURN(row, cast_type_to_json_str(result_columns[i], idx));
        _row_str.append(row);
        if (i != num_columns - 1) {
            _row_str.append(",");
        }
    }
    _row_str.append("]}\n");
    return Status::OK();
}

Status HttpResultWriter::append_chunk(Chunk* chunk) {
    return Status::NotSupported("HttpResultWriter doesn't support non-pipeline engine");
}

StatusOr<TFetchDataResultPtrs> HttpResultWriter::process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    int num_rows = chunk->num_rows();
    std::vector<TFetchDataResultPtr> results;

    Columns result_columns;
    // Step 1: compute expr
    int num_columns = _output_expr_ctxs.size();
    result_columns.reserve(num_columns);

    for (int i = 0; i < num_columns; ++i) {
        ASSIGN_OR_RETURN(ColumnPtr column, _output_expr_ctxs[i]->evaluate(chunk));
        column = _output_expr_ctxs[i]->root()->type().type == TYPE_TIME
                         ? ColumnHelper::convert_time_column_from_double_to_str(column)
                         : column;
        result_columns.emplace_back(std::move(column));
    }

    // Step 2: convert chunk to http json row format row by row
    {
        TRY_CATCH_ALLOC_SCOPE_START()
        _row_str.reserve(128);
        size_t current_bytes = 0;
        int current_rows = 0;
        SCOPED_TIMER(_convert_tuple_timer);
        auto result = std::make_unique<TFetchDataResult>();
        auto& result_rows = result->result_batch.rows;
        result_rows.resize(num_rows);

        for (int i = 0; i < num_rows; ++i) {
            switch (_format_type) {
            case TResultSinkFormatType::type::JSON:
                RETURN_IF_ERROR(_transform_row_to_json(result_columns, i));
                break;
            case TResultSinkFormatType::type::OTHERS:
                return Status::NotSupported("HttpResultWriter only support json format right now");
            }
            size_t len = _row_str.size();

            if (UNLIKELY(current_bytes + len >= _max_row_buffer_size)) {
                result_rows.resize(current_rows);
                results.emplace_back(std::move(result));

                result = std::make_unique<TFetchDataResult>();
                result_rows = result->result_batch.rows;
                result_rows.resize(num_rows - i);

                current_bytes = 0;
                current_rows = 0;
            }

            // VLOG_ROW << "written row:" << row_str;
            result_rows[current_rows] = std::move(_row_str);
            _row_str.clear();

            _row_str.reserve(len * 1.1);

            current_bytes += len;
            current_rows += 1;
        }
        if (current_rows > 0) {
            result_rows.resize(current_rows);
            results.emplace_back(std::move(result));
        }
        TRY_CATCH_ALLOC_SCOPE_END()
    }
    return results;
}

} // namespace starrocks
