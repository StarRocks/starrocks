// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include "exec/file_builder.h"

namespace starrocks {

namespace vectorized::csv {
class Converter;
class OutputStream;
} // namespace vectorized::csv

class ExprContext;
class FileWriter;

struct PlainTextBuilderOptions {
    std::string column_terminated_by;
    std::string line_terminated_by;
};

class PlainTextBuilder final : public FileBuilder {
public:
    PlainTextBuilder(PlainTextBuilderOptions options, std::unique_ptr<WritableFile> writable_file,
                     const std::vector<ExprContext*>& output_expr_ctxs);
    ~PlainTextBuilder() override = default;

    Status add_chunk(vectorized::Chunk* chunk) override;

    std::size_t file_size() override;

    Status finish() override;

private:
    const static size_t OUTSTREAM_BUFFER_SIZE_BYTES;
    const PlainTextBuilderOptions _options;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    std::unique_ptr<vectorized::csv::OutputStream> _output_stream;
    std::vector<std::unique_ptr<vectorized::csv::Converter>> _converters;
    bool _init;

    Status init();
};

} // namespace starrocks
