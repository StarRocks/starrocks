// This file_builder is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "plain_text_builder.h"

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

const size_t PlainTextBuilder::OUTSTREAM_BUFFER_SIZE_BYTES = 1024 * 1024;

Status PlainTextBuilder::add_chunk(vectorized::Chunk* chunk) {
    int num_rows = chunk->num_rows();
    auto result = std::make_unique<TFetchDataResult>();
    auto& result_rows = result->result_batch.rows;
    result_rows.resize(num_rows);

    vectorized::Columns result_columns;
    // Step 1: compute expr
    auto num_columns = _output_expr_ctxs.size();
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
        for (int i = 0; i < num_rows; ++i) {
            for (auto& result_column : result_columns) {
                result_column->put_string_stream(&_plain_text_outstream, i);
                if (i < num_columns - 1) {
                    _plain_text_outstream << _column_format.terminated_by;
                }
            }
            _plain_text_outstream << _line_format.terminated_by;

            // write one line to file_builder
            return _flush_plain_text_outstream(false);
        }
    }

    // Step 3
    _flush_plain_text_outstream(true);
    return Status::OK();
}

uint64_t PlainTextBuilder::file_size() {

}

Status PlainTextBuilder::_flush_plain_text_outstream(bool eos) {
    size_t pos = _plain_text_outstream.tellp();
    if (pos == 0 || (pos < OUTSTREAM_BUFFER_SIZE_BYTES && !eos)) {
        return Status::OK();
    }

    const std::string& buf = _plain_text_outstream.str();
    _current_written_bytes += buf.size();
    RETURN_IF_ERROR(_writable_file->append({reinterpret_cast<const uint8_t*>(buf.c_str()), buf.size()}));

    // clear the stream
    _plain_text_outstream.str("");
    _plain_text_outstream.clear();

    return Status::OK();
}

} // namespace starrocks
