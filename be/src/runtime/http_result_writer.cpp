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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mysql_result_writer.cpp

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
                                   RuntimeProfile* parent_profile)
        : _sinker(sinker), _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile) {}

Status HttpResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    return Status::OK();
}

void HttpResultWriter::_init_profile() {
    _append_chunk_timer = ADD_TIMER(_parent_profile, "AppendChunkTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendChunkTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultRendTime", "AppendChunkTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

Status HttpResultWriter::append_chunk(Chunk* chunk) {
    return Status::NotSupported("HttpResultWriter doesn't support non-pipeline engine");
}

Status HttpResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
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
        row_str.reserve(128);
        size_t current_bytes = 0;
        int current_rows = 0;
        SCOPED_TIMER(_convert_tuple_timer);
        auto result = std::make_unique<TFetchDataResult>();
        auto& result_rows = result->result_batch.rows;
        result_rows.resize(num_rows);

        for (int i = 0; i < num_rows; ++i) {
            row_str.append("{\"data\":[");
            for (auto& result_column : result_columns) {
                std::string row = cast_type_to_json_str(result_column, i).value();
                row_str.append(row);
                if (result_column != result_columns[num_columns - 1]) {
                    row_str.append(",");
                }
            }
            row_str.append("]}\n");

            size_t len = row_str.size();

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
            result_rows[current_rows].swap(reinterpret_cast<std::string&>(row_str));
            row_str.clear();

            row_str.reserve(len * 1.1);

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

StatusOr<bool> HttpResultWriter::try_add_batch(TFetchDataResultPtrs& results) {
    SCOPED_TIMER(_result_send_timer);
    size_t num_rows = 0;
    for (auto& result : results) {
        num_rows += result->result_batch.rows.size();
    }

    auto status = _sinker->try_add_batch(results);
    if (status.ok()) {
        // success in add result to ResultQueue of _sinker
        if (status.value()) {
            _written_rows += num_rows;
            results.clear();
        }
    } else {
        results.clear();
        LOG(WARNING) << "Append result batch to sink failed: status=" << status.status().to_string();
    }
    return status;
}

} // namespace starrocks
