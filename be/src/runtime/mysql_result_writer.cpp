// This file is made available under Elastic License 2.0.
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

#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "exprs/expr.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/buffer_control_block.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/date_func.h"
#include "util/mysql_row_buffer.h"
#include "util/types.h"

namespace starrocks {

MysqlResultWriter::MysqlResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                                     RuntimeProfile* parent_profile)
        : _sinker(sinker), _output_expr_ctxs(output_expr_ctxs), _row_buffer(nullptr), _parent_profile(parent_profile) {}

MysqlResultWriter::~MysqlResultWriter() {
    delete _row_buffer;
}

Status MysqlResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    _row_buffer = new (std::nothrow) MysqlRowBuffer();

    if (nullptr == _row_buffer) {
        return Status::InternalError("no memory to alloc.");
    }

    return Status::OK();
}

void MysqlResultWriter::_init_profile() {
    _append_row_batch_timer = ADD_TIMER(_parent_profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultRendTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

Status MysqlResultWriter::_add_one_row(TupleRow* row) {
    SCOPED_TIMER(_convert_tuple_timer);
    _row_buffer->reset();
    int num_columns = _output_expr_ctxs.size();

    for (int i = 0; i < num_columns; ++i) {
        void* item = _output_expr_ctxs[i]->get_value(row);

        if (nullptr == item) {
            _row_buffer->push_null();
            continue;
        }

        switch (_output_expr_ctxs[i]->root()->type().type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            _row_buffer->push_tinyint(*static_cast<int8_t*>(item));
            break;

        case TYPE_SMALLINT:
            _row_buffer->push_smallint(*static_cast<int16_t*>(item));
            break;

        case TYPE_INT:
            _row_buffer->push_int(*static_cast<int32_t*>(item));
            break;

        case TYPE_BIGINT:
            _row_buffer->push_bigint(*static_cast<int64_t*>(item));
            break;

        case TYPE_LARGEINT:
            _row_buffer->push_largeint(reinterpret_cast<const PackedInt128*>(item)->value);
            break;

        case TYPE_FLOAT:
            _row_buffer->push_float(*static_cast<float*>(item));
            break;

        case TYPE_DOUBLE:
            _row_buffer->push_double(*static_cast<double*>(item));
            break;

        case TYPE_TIME: {
            double time = *static_cast<double*>(item);
            std::string time_str = time_str_from_double(time);
            _row_buffer->push_string(time_str.c_str(), time_str.size());
            break;
        }

        case TYPE_DATE:
        case TYPE_DATETIME: {
            char buf[64];
            const DateTimeValue* time_val = (const DateTimeValue*)(item);
            // TODO(zhaochun), this function has core risk
            char* pos = time_val->to_string(buf);
            _row_buffer->push_string(buf, pos - buf - 1);
            break;
        }

        case TYPE_HLL:
        case TYPE_OBJECT:
        case TYPE_PERCENTILE: {
            _row_buffer->push_null();
            break;
        }

        case TYPE_VARCHAR:
        case TYPE_CHAR: {
            const StringValue* string_val = (const StringValue*)(item);

            if (string_val->ptr == nullptr) {
                if (string_val->len == 0) {
                    // 0x01 is a magic num, not usefull actually, just for present ""
                    char* tmp_val = reinterpret_cast<char*>(0x01);
                    _row_buffer->push_string(tmp_val, string_val->len);
                } else {
                    _row_buffer->push_null();
                }
            } else {
                _row_buffer->push_string(string_val->ptr, string_val->len);
            }

            break;
        }

        case TYPE_DECIMAL: {
            const DecimalValue* decimal_val = reinterpret_cast<const DecimalValue*>(item);
            std::string decimal_str;
            int output_scale = _output_expr_ctxs[i]->root()->output_scale();

            if (output_scale > 0 && output_scale <= 30) {
                decimal_str = decimal_val->to_string(output_scale);
            } else {
                decimal_str = decimal_val->to_string();
            }

            _row_buffer->push_string(decimal_str.c_str(), decimal_str.length());
            break;
        }

        case TYPE_DECIMALV2: {
            DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(item)->value);
            std::string decimal_str;
            int output_scale = _output_expr_ctxs[i]->root()->output_scale();

            if (output_scale > 0 && output_scale <= 30) {
                decimal_str = decimal_val.to_string(output_scale);
            } else {
                decimal_str = decimal_val.to_string();
            }

            _row_buffer->push_string(decimal_str.c_str(), decimal_str.length());
            break;
        }

        case TYPE_DECIMAL32: {
            auto precision = _output_expr_ctxs[i]->root()->type().precision;
            auto scale = _output_expr_ctxs[i]->root()->type().scale;
            auto& value = *static_cast<int32_t*>(item);
            auto s = DecimalV3Cast::to_string<int32_t>(value, precision, scale);
            _row_buffer->push_string(s.c_str(), s.length());
            break;
        }

        case TYPE_DECIMAL64: {
            auto precision = _output_expr_ctxs[i]->root()->type().precision;
            auto scale = _output_expr_ctxs[i]->root()->type().scale;
            auto& value = *static_cast<int64_t*>(item);
            auto s = DecimalV3Cast::to_string<int64_t>(value, precision, scale);
            _row_buffer->push_string(s.c_str(), s.length());
            break;
        }

        case TYPE_DECIMAL128: {
            auto precision = _output_expr_ctxs[i]->root()->type().precision;
            auto scale = _output_expr_ctxs[i]->root()->type().scale;
            auto& value = *static_cast<int128_t*>(item);
            auto s = DecimalV3Cast::to_string<int128_t>(value, precision, scale);
            _row_buffer->push_string(s.c_str(), s.length());
            break;
        }

        default:
            LOG(WARNING) << "can't convert this type to mysql type. type = " << _output_expr_ctxs[i]->root()->type();
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status MysqlResultWriter::append_row_batch(const RowBatch* batch) {
    SCOPED_TIMER(_append_row_batch_timer);
    if (nullptr == batch || 0 == batch->num_rows()) {
        return Status::OK();
    }

    Status status;
    // convert one batch
    TFetchDataResult* result = new (std::nothrow) TFetchDataResult();
    int num_rows = batch->num_rows();
    result->result_batch.rows.resize(num_rows);

    for (int i = 0; status.ok() && i < num_rows; ++i) {
        TupleRow* row = batch->get_row(i);
        status = _add_one_row(row);

        if (status.ok()) {
            _row_buffer->move_content(&result->result_batch.rows[i]);
        } else {
            LOG(WARNING) << "convert row to mysql result failed.";
            break;
        }
    }

    if (status.ok()) {
        SCOPED_TIMER(_result_send_timer);
        // push this batch to back
        status = _sinker->add_batch(result);

        if (status.ok()) {
            result = nullptr;
            _written_rows += num_rows;
        } else {
            LOG(WARNING) << "append result batch to sink failed.";
        }
    }

    delete result;
    result = nullptr;

    return status;
}

Status MysqlResultWriter::append_chunk(vectorized::Chunk* chunk) {
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return Status::OK();
    }
    auto num_rows = chunk->num_rows();
    auto status = process_chunk(chunk);
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

StatusOr<TFetchDataResultPtr> MysqlResultWriter::process_chunk(vectorized::Chunk* chunk) {
    SCOPED_TIMER(_append_row_batch_timer);
    int num_rows = chunk->num_rows();
    auto result = std::make_unique<TFetchDataResult>();
    auto& result_rows = result->result_batch.rows;
    result_rows.resize(num_rows);

    vectorized::Columns result_columns;
    // Step 1: compute expr
    int num_columns = _output_expr_ctxs.size();
    result_columns.reserve(num_columns);

    using vectorized::DoubleColumn;
    using vectorized::BinaryColumn;
    using vectorized::NullableColumn;

    auto get_binary_column = [](DoubleColumn* data_column, size_t size) -> ColumnPtr {
        auto new_data_column = BinaryColumn::create();
        new_data_column->reserve(size);

        for (int row = 0; row < size; ++row) {
            auto time = data_column->get_data()[row];
            std::string time_str = time_str_from_double(time);
            new_data_column->append(time_str);
        }

        return new_data_column;
    };

    for (int i = 0; i < num_columns; ++i) {
        ColumnPtr column = _output_expr_ctxs[i]->evaluate(chunk);
        auto size = column->size();
        if (_output_expr_ctxs[i]->root()->type().type == TYPE_TIME) {
            if (column->only_null()) {
                // not handle
            } else if (column->is_nullable()) {
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                auto* data_column = down_cast<DoubleColumn*>(nullable_column->mutable_data_column());
                column = NullableColumn::create(get_binary_column(data_column, size), nullable_column->null_column());
            } else if (column->is_constant()) {
                auto* const_column = down_cast<vectorized::ConstColumn*>(column.get());
                string time_str = time_str_from_double(const_column->get(i).get_double());
                column = vectorized::ColumnHelper::create_const_column<TYPE_VARCHAR>(time_str, size);
            } else {
                auto* data_column = down_cast<DoubleColumn*>(column.get());
                column = get_binary_column(data_column, size);
            }
        }
        result_columns.emplace_back(std::move(column));
    }

    // Step 2: convert chunk to mysql row format row by row
    {
        _row_buffer->reserve(128);
        SCOPED_TIMER(_convert_tuple_timer);
        for (int i = 0; i < num_rows; ++i) {
            DCHECK_EQ(0, _row_buffer->length());
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

StatusOr<bool> MysqlResultWriter::try_add_batch(TFetchDataResultPtr& result) {
    SCOPED_TIMER(_result_send_timer);
    auto* fetch_data = result.release();
    auto num_rows = fetch_data->result_batch.rows.size();
    auto status = _sinker->try_add_batch(fetch_data);

    if (status.ok()) {
        // success in add result to ResultQueue of _sinker
        if (status.value()) {
            _written_rows += num_rows;
        } else {
            // the result is given back to chunk
            result.reset(fetch_data);
        }
    } else {
        delete fetch_data;
        if (!status.ok()) {
            LOG(WARNING) << "Append result batch to sink failed: status=" << status.status().to_string();
        }
    }
    return status;
}

} // namespace starrocks
