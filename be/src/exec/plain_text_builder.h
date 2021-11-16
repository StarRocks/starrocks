// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include "file_builder.h"

namespace starrocks {

class ExprContext;
class FileWriter;
class RowBatch;

struct PlainTextBuilderOptions {
    int character;
    std::string column_terminated_by;
    std::string column_optionally_enclosed_by;
    std::string column_escaped_by;
    std::string line_starting_by;
    std::string line_terminated_by;
};

class PlainTextBuilder : public FileBuilder {
public:
    PlainTextBuilder(WritableFile* writable_file, const std::vector<ExprContext*>& output_expr_ctxs,
                     PlainTextBuilderOptions options)
            : _options(std::move(options)), _output_expr_ctxs(output_expr_ctxs), _writable_file(writable_file) {}
    ~PlainTextBuilder() override = default;

    Status add_chunk(vectorized::Chunk* chunk) override;

    uint64_t file_size() override;

    Status finish() override {}

private:
    const PlainTextBuilderOptions _options;

    const std::vector<ExprContext*>& _output_expr_ctxs;

    WritableFile* _writable_file;

    // Used to buffer the export data of plain text
    // TODO(cmy): I simply use a stringstrteam to buffer the data, to avoid calling
    // file writer's write() for every single row.
    // But this cannot solve the problem of a row of data that is too large.
    // For exampel: bitmap_to_string() may return large volumn of data.
    // And the speed is relative low, in my test, is about 6.5MB/s.
    std::stringstream _plain_text_outstream;
    static const size_t OUTSTREAM_BUFFER_SIZE_BYTES;

    // current written bytes, used for split data
    int64_t _current_written_bytes = 0;

    // if buffer exceed the limit, write the data buffered in _plain_text_outstream via file_writer
    // if eos, write the data even if buffer is not full.
    Status _flush_plain_text_outstream(bool eos);
};

} // namespace starrocks
