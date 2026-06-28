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

#include "exec/data_sinks/mysql_result_writer.h"

#include <column/column_helper.h>

#include <optional>

#include "column/chunk.h"
#include "column/mysql_row_buffer.h"
#include "common/statusor.h"
#include "compute_env/result/buffer_control_block.h"
#include "exec/data_sinks/buffer_control_result_writer.h"
#include "exec/data_sinks/mysql_column_writer.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

namespace {

MysqlRowBufferOptions make_mysql_row_buffer_options(const TQueryOptions& query_options) {
    MysqlRowBufferOptions options;
    if (query_options.__isset.binary_encoding_format) {
        switch (query_options.binary_encoding_format) {
        case TBinaryEncodingFormat::RAW:
            options.binary_encoding_format = MysqlRowBufferOptions::BinaryEncodingFormat::RAW;
            break;
        case TBinaryEncodingFormat::BASE64:
            options.binary_encoding_format = MysqlRowBufferOptions::BinaryEncodingFormat::BASE64;
            break;
        case TBinaryEncodingFormat::HEX:
        default:
            options.binary_encoding_format = MysqlRowBufferOptions::BinaryEncodingFormat::HEX;
            break;
        }
    }
    if (query_options.__isset.binary_encoding_level) {
        switch (query_options.binary_encoding_level) {
        case TBinaryEncodingLevel::ALL:
            options.binary_encoding_level = MysqlRowBufferOptions::BinaryEncodingLevel::ALL;
            break;
        case TBinaryEncodingLevel::NESTED:
        default:
            options.binary_encoding_level = MysqlRowBufferOptions::BinaryEncodingLevel::NESTED;
            break;
        }
    }
    return options;
}

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

inline void serialize_row(MysqlRowBuffer* buf, const std::vector<ColumnSerializeContext>& column_ctxs, int row_idx,
                          bool is_binary_format) {
    for (const auto& ctx : column_ctxs) {
        if (ctx.use_viewer) {
            ctx.serializer(ctx.viewer.value(), ctx.type_desc, buf, row_idx, is_binary_format);
        } else {
            if (is_binary_format && !ctx.fallback_column->is_nullable()) {
                buf->update_field_pos();
            }
            ctx.fallback_column->put_mysql_row_buffer(buf, row_idx, is_binary_format);
        }
    }
}

Status build_column_contexts(const std::vector<ExprContext*>& output_expr_ctxs, Chunk* chunk,
                             std::vector<ColumnSerializeContext>* column_ctxs, Columns* result_columns) {
    int num_columns = output_expr_ctxs.size();
    result_columns->reserve(num_columns);
    column_ctxs->reserve(num_columns);

    for (int i = 0; i < num_columns; ++i) {
        ASSIGN_OR_RETURN(ColumnPtr column, output_expr_ctxs[i]->evaluate(chunk));
        const auto& root_type = output_expr_ctxs[i]->root()->type();
        TypeDescriptor serializer_type = root_type;
        LogicalType physical_type = root_type.type;
        if (root_type.type == TYPE_TIME) {
            column = ColumnHelper::convert_time_column_from_double_to_str(column);
            physical_type = TYPE_VARCHAR;
            serializer_type.type = TYPE_VARCHAR;
        }
        // Mark BinaryColumns that carry VARBINARY data so push_binary is used inside nested types.
        ColumnHelper::mark_binary_columns(column, root_type);

        result_columns->emplace_back(std::move(column));
        auto& stored_column = result_columns->back();

        auto viewer_opt = type_dispatch_filter(physical_type, std::optional<MysqlColumnViewer>{},
                                               MysqlColumnViewerBuilder(), stored_column);
        if (viewer_opt.has_value()) {
            auto serializer = get_mysql_serializer(physical_type);
            column_ctxs->emplace_back(std::move(*viewer_opt), serializer, serializer_type);
        } else {
            column_ctxs->emplace_back(stored_column);
        }
    }
    return Status::OK();
}

} // namespace

MysqlResultWriter::MysqlResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                                     bool is_binary_format, RuntimeProfile* parent_profile)
        : BufferControlResultWriter(sinker, parent_profile),
          _output_expr_ctxs(output_expr_ctxs),

          _is_binary_format(is_binary_format) {}

MysqlResultWriter::~MysqlResultWriter() {
    delete _row_buffer;
}

Status MysqlResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    _row_buffer =
            new (std::nothrow) MysqlRowBuffer(_is_binary_format, make_mysql_row_buffer_options(state->query_options()));

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
    int num_columns = _output_expr_ctxs.size();
    auto result = std::make_unique<TFetchDataResult>();
    auto& result_rows = result->result_batch.rows;
    result_rows.resize(num_rows);

    Columns result_columns;
    std::vector<ColumnSerializeContext> column_ctxs;
    {
        SCOPED_TIMER(_expr_eval_timer);
        RETURN_IF_ERROR(build_column_contexts(_output_expr_ctxs, chunk, &column_ctxs, &result_columns));
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
            serialize_row(_row_buffer, column_ctxs, i, _is_binary_format);
            size_t len = _row_buffer->length();
            _row_buffer->move_content(&result_rows[i]);
            if (len > max_row_len) {
                max_row_len = len;
            }
            // move_content swaps the buffer's storage into result_rows[i], leaving the
            // buffer at zero capacity. Reserve every row so subsequent pushes don't
            // reallocate inside the row; +10% headroom to absorb size variance.
            if (max_row_len > 0) {
                _row_buffer->reserve(max_row_len + max_row_len / 10);
            }
        }
    }
    return result;
}

StatusOr<TFetchDataResultPtrs> MysqlResultWriter::process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    int num_rows = chunk->num_rows();
    int num_columns = _output_expr_ctxs.size();
    std::vector<TFetchDataResultPtr> results;

    Columns result_columns;
    std::vector<ColumnSerializeContext> column_ctxs;
    {
        SCOPED_TIMER(_expr_eval_timer);
        RETURN_IF_ERROR(build_column_contexts(_output_expr_ctxs, chunk, &column_ctxs, &result_columns));
    }

    {
        TRY_CATCH_ALLOC_SCOPE_START()
        _row_buffer->reserve(128);
        size_t current_bytes = 0;
        int current_rows = 0;
        size_t max_row_len = 0;
        SCOPED_TIMER(_convert_tuple_timer);
        auto result = std::make_unique<TFetchDataResult>();
        auto* result_rows = &result->result_batch.rows;
        result_rows->resize(num_rows);

        for (int i = 0; i < num_rows; ++i) {
            DCHECK_EQ(0, _row_buffer->length());
            if (_is_binary_format) {
                _row_buffer->start_binary_row(num_columns);
            }
            serialize_row(_row_buffer, column_ctxs, i, _is_binary_format);
            size_t len = _row_buffer->length();

            if (UNLIKELY(current_bytes + len >= _max_row_buffer_size)) {
                result_rows->resize(current_rows);
                results.emplace_back(std::move(result));

                result = std::make_unique<TFetchDataResult>();
                result_rows = &result->result_batch.rows;
                result_rows->resize(num_rows - i);

                current_bytes = 0;
                current_rows = 0;
            }
            _row_buffer->move_content(&(*result_rows)[current_rows]);
            if (len > max_row_len) {
                max_row_len = len;
            }
            // move_content swaps the buffer's storage into result_rows[current_rows],
            // leaving the buffer at zero capacity. Reserve every row so subsequent
            // pushes don't reallocate inside the row; +10% headroom to absorb size
            // variance.
            if (max_row_len > 0) {
                _row_buffer->reserve(max_row_len + max_row_len / 10);
            }

            current_bytes += len;
            current_rows += 1;
        }
        if (current_rows > 0) {
            result_rows->resize(current_rows);
            results.emplace_back(std::move(result));
        }
        TRY_CATCH_ALLOC_SCOPE_END()
    }
    return results;
}

} // namespace starrocks
