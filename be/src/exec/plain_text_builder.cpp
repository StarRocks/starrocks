// This file_builder is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "plain_text_builder.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream.h"
#include "formats/csv/output_stream_file.h"
#include "util/date_func.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

const size_t PlainTextBuilder::OUTSTREAM_BUFFER_SIZE_BYTES = 1024 * 1024;

PlainTextBuilder::PlainTextBuilder(PlainTextBuilderOptions options, std::unique_ptr<WritableFile> writable_file,
                                   const std::vector<ExprContext*>& output_expr_ctxs)
        : _options(std::move(options)),
          _output_expr_ctxs(output_expr_ctxs),
          _output_stream(std::make_unique<vectorized::csv::OutputStreamFile>(std::move(writable_file),
                                                                             OUTSTREAM_BUFFER_SIZE_BYTES)),
          _init(false) {}

Status PlainTextBuilder::init() {
    if (_init) {
        return Status::OK();
    }
    _converters.reserve(_output_expr_ctxs.size());
    for (auto* ctx : _output_expr_ctxs) {
        const auto& type = ctx->root()->type();
<<<<<<< HEAD
        auto conv = vectorized::csv::get_converter(type, ctx->root()->is_nullable());

=======
        // in some cases, the nullable property between the column in the chunk and _output_expr_ctxs
        // may not be consistent.
        // for example: order by limit + left outer join
        // select t1.k1, t1.k2, count(distinct t1.k3) as k33 from t1 left join t2 on t1.k1 = t2.k1
        // group by t1.k1,t1.k2 order by k33
        // so we use nullable converter, and process whether the column is nullable in the nullable converter.
        auto conv = csv::get_converter(type, true);
>>>>>>> 498c39a71f ([BugFix] Fix csv converter and chunk column inconsistent in select into outfile (#48052))
        if (conv == nullptr) {
            return Status::InternalError("No CSV converter for type " + type.debug_string());
        }
        _converters.emplace_back(std::move(conv));
    }
    _init = true;

    return Status::OK();
}

Status PlainTextBuilder::add_chunk(vectorized::Chunk* chunk) {
    RETURN_IF_ERROR(init());

    const size_t num_rows = chunk->num_rows();
    const size_t num_cols = _output_expr_ctxs.size();
    if (num_cols != _converters.size()) {
        auto err = strings::Substitute("Unmatched number of columns expected=$0 real=$1", _converters.size(), num_cols);
        return Status::InternalError(err);
    }
<<<<<<< HEAD
    std::vector<const vectorized::Column*> columns_raw_ptr;
    columns_raw_ptr.reserve(num_cols);
=======
    Columns columns;
    columns.reserve(num_cols);
>>>>>>> 498c39a71f ([BugFix] Fix csv converter and chunk column inconsistent in select into outfile (#48052))
    for (int i = 0; i < num_cols; i++) {
        auto root = _output_expr_ctxs[i]->root();
        if (!root->is_slotref()) {
            return Status::InternalError("Not slot ref column");
        }
<<<<<<< HEAD
        auto column_ref = ((vectorized::ColumnRef*)root);
        columns_raw_ptr.emplace_back(chunk->get_column_by_slot_id(column_ref->slot_id()).get());
=======
        auto column_ref = ((ColumnRef*)root);
        auto col = chunk->get_column_by_slot_id(column_ref->slot_id());
        if (col == nullptr) {
            return Status::InternalError(strings::Substitute("Column not found by slot id %0", column_ref->slot_id()));
        }
        col = ColumnHelper::unfold_const_column(column_ref->type(), num_rows, col);
        columns.emplace_back(col);
>>>>>>> 498c39a71f ([BugFix] Fix csv converter and chunk column inconsistent in select into outfile (#48052))
    }

    const std::string& row_delimiter = _options.line_terminated_by;
    const std::string& column_delimiter = _options.column_terminated_by;

    vectorized::csv::Converter::Options opts;
    auto* os = _output_stream.get();
    for (size_t row = 0; row < num_rows; row++) {
        for (size_t col = 0; col < num_cols; col++) {
            auto& col_ptr = columns[col];
            RETURN_IF_ERROR(_converters[col]->write_string(os, *col_ptr, row, opts));
            RETURN_IF_ERROR(os->write((col == num_cols - 1) ? row_delimiter : column_delimiter));
        }
    }
    return Status::OK();
}

std::size_t PlainTextBuilder::file_size() {
    DCHECK(_output_stream != nullptr);
    return _output_stream->size();
}

Status PlainTextBuilder::finish() {
    DCHECK(_output_stream != nullptr);
    return _output_stream->finalize();
}

} // namespace starrocks
