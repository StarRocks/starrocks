// This file_builder is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "plain_text_builder.h"

#include "column/column_helper.h"
#include "column/const_column.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/primitive_type.h"
#include "util/date_func.h"
#include "util/mysql_row_buffer.h"
#include "util/types.h"

namespace starrocks {

const char* PlainTextBuilder::NULL_IN_CSV = "\\N";
const size_t PlainTextBuilder::OUTSTREAM_BUFFER_SIZE_BYTES = 1024 * 1024;

Status PlainTextBuilder::add_chunk(vectorized::Chunk* chunk) {
    auto num_rows = chunk->num_rows();
    vectorized::Columns result_columns;
    auto num_columns = _output_expr_ctxs.size();
    result_columns.reserve(num_columns);
    for (int i = 0; i < num_columns; ++i) {
        ColumnPtr column = _output_expr_ctxs[i]->evaluate(chunk);
        column = _output_expr_ctxs[i]->root()->type().type == TYPE_TIME
                         ? vectorized::ColumnHelper::convert_time_column_from_double_to_str(column.get())
                         : column;
        result_columns.emplace_back(std::move(column));
    }

    for (int i = 0; i < num_rows; ++i) {
        for (auto& result_column : result_columns) {
            _plain_text_outstream << result_column->to_string(i, NULL_IN_CSV);
            if (i < num_columns - 1) {
                _plain_text_outstream << _options.column_terminated_by;
            }
        }
        _plain_text_outstream << _options.line_terminated_by;

        _flush_plain_text_outstream(false);
    }

    return Status::OK();
}

uint64_t PlainTextBuilder::file_size() {
    return _current_written_bytes;
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

Status PlainTextBuilder::finish() {
    return _flush_plain_text_outstream(true);
}

} // namespace starrocks
