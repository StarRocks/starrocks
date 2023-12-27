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

#include "runtime/mysql_result_writer.h"

#include <column/column_helper.h>

#include "column/chunk.h"
#include "column/const_column.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/current_thread.h"
#include "types/logical_type.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

MysqlResultWriter::MysqlResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                                     bool is_binary_format, RuntimeProfile* parent_profile)
        : _sinker(sinker),
          _output_expr_ctxs(output_expr_ctxs),
          _row_buffer(nullptr),
          _is_binary_format(is_binary_format),
          _parent_profile(parent_profile) {}

MysqlResultWriter::~MysqlResultWriter() {
    delete _row_buffer;
}

Status MysqlResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    _row_buffer = new (std::nothrow) MysqlRowBuffer(_is_binary_format);

    if (nullptr == _row_buffer) {
        return Status::InternalError("no memory to alloc.");
    }

    return Status::OK();
}

void MysqlResultWriter::_init_profile() {
    _append_chunk_timer = ADD_TIMER(_parent_profile, "AppendChunkTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendChunkTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultRendTime", "AppendChunkTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

Status MysqlResultWriter::append_chunk(Chunk* chunk) {
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return Status::OK();
    }
    auto num_rows = chunk->num_rows();
    auto status = _process_chunk(chunk);
    if (!status.ok()) {
        return status.status();
    }

    TFetchDataResultPtr result = std::move(status.value());
    auto* fetch_data = result.release();
    SCOPED_TIMER(_result_send_timer);
    // Note: this method will delete result pointer if status is OK
    // TODO(kks): use std::unique_ptr instead of raw pointer
    auto add_status = _sinker->add_batch(fetch_data);
    if (add_status.ok()) {
        _written_rows += static_cast<int64_t>(num_rows);
        return add_status;
    } else {
        LOG(WARNING) << "append result batch to sink failed.";
    }

    delete fetch_data;
    return add_status;
}

Status MysqlResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

StatusOr<TFetchDataResultPtr> MysqlResultWriter::_process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    int num_rows = chunk->num_rows();
    auto result = std::make_unique<TFetchDataResult>();
    auto& result_rows = result->result_batch.rows;
    result_rows.resize(num_rows);

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

    // Step 2: convert chunk to mysql row format row by row
    {
        _row_buffer->reserve(128);
        SCOPED_TIMER(_convert_tuple_timer);
        for (int i = 0; i < num_rows; ++i) {
            DCHECK_EQ(0, _row_buffer->length());
            if (_is_binary_format) {
                _row_buffer->start_binary_row(num_columns);
            };
            for (auto& result_column : result_columns) {
                result_column->put_mysql_row_buffer(_row_buffer, i);
            }
            size_t len = _row_buffer->length();
            _row_buffer->move_content(&result_rows[i]);
            _row_buffer->reserve(len * 1.1);
        }
    }
    return result;
}

StatusOr<TFetchDataResultPtrs> MysqlResultWriter::process_chunk(Chunk* chunk) {
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

    // Step 2: convert chunk to mysql row format row by row
    {
        TRY_CATCH_ALLOC_SCOPE_START()
        _row_buffer->reserve(128);
        size_t current_bytes = 0;
        int current_rows = 0;
        SCOPED_TIMER(_convert_tuple_timer);
        auto result = std::make_unique<TFetchDataResult>();
        auto& result_rows = result->result_batch.rows;
        result_rows.resize(num_rows);

        for (int i = 0; i < num_rows; ++i) {
            DCHECK_EQ(0, _row_buffer->length());
            if (_is_binary_format) {
                _row_buffer->start_binary_row(num_columns);
            }
            for (auto& result_column : result_columns) {
                result_column->put_mysql_row_buffer(_row_buffer, i);
            }
            size_t len = _row_buffer->length();

            if (UNLIKELY(current_bytes + len >= _max_row_buffer_size)) {
                result_rows.resize(current_rows);
                results.emplace_back(std::move(result));

                result = std::make_unique<TFetchDataResult>();
                result_rows = result->result_batch.rows;
                result_rows.resize(num_rows - i);

                current_bytes = 0;
                current_rows = 0;
            }
            _row_buffer->move_content(&result_rows[current_rows]);
            _row_buffer->reserve(len * 1.1);

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

StatusOr<bool> MysqlResultWriter::try_add_batch(TFetchDataResultPtrs& results) {
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
