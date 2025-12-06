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

#include <optional>

#include "column/chunk.h"
#include "column/const_column.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/buffer_control_result_writer.h"
#include "runtime/current_thread.h"
#include "runtime/mysql_column_writer.h"
#include "types/logical_type.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

namespace {

struct ColumnSerializeContext {
    ColumnSerializeContext(MysqlColumnViewer viewer_, MysqlSerializeFn serializer_, const TypeDescriptor& type_desc_)
            : viewer(std::move(viewer_)), serializer(serializer_), type_desc(type_desc_), use_viewer(true) {}

    explicit ColumnSerializeContext(ColumnPtr column_)
            : fallback_column(std::move(column_)), serializer(nullptr), use_viewer(false) {}

    std::optional<MysqlColumnViewer> viewer;
    ColumnPtr fallback_column;
    MysqlSerializeFn serializer;
    TypeDescriptor type_desc;
    bool use_viewer;
};

} // namespace

MysqlResultWriter::MysqlResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                                     bool is_binary_format, RuntimeProfile* parent_profile)
        : BufferControlResultWriter(sinker, parent_profile),
          _output_expr_ctxs(output_expr_ctxs),
          _row_buffer(nullptr),
          _is_binary_format(is_binary_format) {}

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
    BufferControlResultWriter::_init_profile();
    _expr_eval_timer = ADD_CHILD_TIMER(_parent_profile, "ExprEvalTime", "AppendChunkTime");
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

StatusOr<TFetchDataResultPtr> MysqlResultWriter::_process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    int num_rows = chunk->num_rows();
    auto result = std::make_unique<TFetchDataResult>();
    auto& result_rows = result->result_batch.rows;
    result_rows.resize(num_rows);

    Columns result_columns;
    int num_columns = _output_expr_ctxs.size();
    result_columns.reserve(num_columns);

    std::vector<ColumnSerializeContext> column_ctxs;
    column_ctxs.reserve(num_columns);

    {
        SCOPED_TIMER(_expr_eval_timer);
        for (int i = 0; i < num_columns; ++i) {
            ASSIGN_OR_RETURN(ColumnPtr column, _output_expr_ctxs[i]->evaluate(chunk));
            const auto& root_type = _output_expr_ctxs[i]->root()->type();
            TypeDescriptor serializer_type = root_type;
            LogicalType physical_type = root_type.type;
            if (root_type.type == TYPE_TIME) {
                column = ColumnHelper::convert_time_column_from_double_to_str(column);
                physical_type = TYPE_VARCHAR;
                serializer_type.type = TYPE_VARCHAR;
            }

            result_columns.emplace_back(std::move(column));
            auto& stored_column = result_columns.back();

            if (is_scalar_logical_type(physical_type)) {
                auto viewer = type_dispatch_basic(physical_type, MysqlColumnViewerBuilder(), stored_column);
                auto serializer = get_mysql_serializer(physical_type);
                column_ctxs.emplace_back(std::move(viewer), serializer, serializer_type);
            } else {
                column_ctxs.emplace_back(stored_column);
            }
        }
    }

    {
        _row_buffer->reserve(128);
        SCOPED_TIMER(_convert_tuple_timer);
        size_t max_row_len = 0;
        for (int i = 0; i < num_rows; ++i) {
            DCHECK_EQ(0, _row_buffer->length());
            if (_is_binary_format) {
                _row_buffer->start_binary_row(num_columns);
            }
            for (int col = 0; col < num_columns; ++col) {
                auto& ctx = column_ctxs[col];
                if (ctx.use_viewer) {
                    ctx.serializer(ctx.viewer.value(), ctx.type_desc, _row_buffer, i, _is_binary_format);
                } else {
                    if (_is_binary_format && !ctx.fallback_column->is_nullable()) {
                        _row_buffer->update_field_pos();
                    }
                    ctx.fallback_column->put_mysql_row_buffer(_row_buffer, i, _is_binary_format);
                }
            }
            size_t len = _row_buffer->length();
            _row_buffer->move_content(&result_rows[i]);
            if (len > max_row_len) {
                max_row_len = len;
            }
            if (max_row_len > 0) {
                _row_buffer->reserve(max_row_len);
            }
        }
    }
    return result;
}

StatusOr<TFetchDataResultPtrs> MysqlResultWriter::process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    int num_rows = chunk->num_rows();
    std::vector<TFetchDataResultPtr> results;

    Columns result_columns;
    int num_columns = _output_expr_ctxs.size();
    result_columns.reserve(num_columns);

    std::vector<ColumnSerializeContext> column_ctxs;
    column_ctxs.reserve(num_columns);

    {
        SCOPED_TIMER(_expr_eval_timer);
        for (int i = 0; i < num_columns; ++i) {
            ASSIGN_OR_RETURN(ColumnPtr column, _output_expr_ctxs[i]->evaluate(chunk));
            const auto& root_type = _output_expr_ctxs[i]->root()->type();
            TypeDescriptor serializer_type = root_type;
            LogicalType physical_type = root_type.type;
            if (root_type.type == TYPE_TIME) {
                column = ColumnHelper::convert_time_column_from_double_to_str(column);
                physical_type = TYPE_VARCHAR;
                serializer_type.type = TYPE_VARCHAR;
            }

            result_columns.emplace_back(std::move(column));
            auto& stored_column = result_columns.back();

            if (is_scalar_logical_type(physical_type)) {
                auto viewer = type_dispatch_basic(physical_type, MysqlColumnViewerBuilder(), stored_column);
                auto serializer = get_mysql_serializer(physical_type);
                column_ctxs.emplace_back(std::move(viewer), serializer, serializer_type);
            } else {
                column_ctxs.emplace_back(stored_column);
            }
        }
    }

    {
        TRY_CATCH_ALLOC_SCOPE_START()
        _row_buffer->reserve(128);
        size_t current_bytes = 0;
        int current_rows = 0;
        size_t max_row_len = 0;
        SCOPED_TIMER(_convert_tuple_timer);
        auto result = std::make_unique<TFetchDataResult>();
        auto& result_rows = result->result_batch.rows;
        result_rows.resize(num_rows);

        for (int i = 0; i < num_rows; ++i) {
            DCHECK_EQ(0, _row_buffer->length());
            if (_is_binary_format) {
                _row_buffer->start_binary_row(num_columns);
            }
            for (int col = 0; col < num_columns; ++col) {
                auto& ctx = column_ctxs[col];
                if (ctx.use_viewer) {
                    ctx.serializer(ctx.viewer.value(), ctx.type_desc, _row_buffer, i, _is_binary_format);
                } else {
                    if (_is_binary_format && !ctx.fallback_column->is_nullable()) {
                        _row_buffer->update_field_pos();
                    }
                    ctx.fallback_column->put_mysql_row_buffer(_row_buffer, i, _is_binary_format);
                }
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
            if (len > max_row_len) {
                max_row_len = len;
            }
            if (max_row_len > 0) {
                _row_buffer->reserve(max_row_len);
            }

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
