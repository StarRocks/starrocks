//
// Created by Letian Jiang on 2024/1/15.
//

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include "exec/file_builder.h"
#include "exec/pipeline/sink/rolling_file_writer.h"

namespace starrocks {
namespace csv {
    class Converter;
    class OutputStream;
} // namespace csv

class ExprContext;
class FileWriter;

struct PlainTextBuilderOptions {
    std::string column_terminated_by;
    std::string line_terminated_by;
};

// class CSVFileWriter final : public pipeline::FileWriter {
// public:
//     CSVFileWriter(PlainTextBuilderOptions options, std::unique_ptr<WritableFile> writable_file,
//                      const std::vector<ExprContext*>& output_expr_ctxs);
//     ~CSVFileWriter() override = default;
//
//     int64_t getWrittenBytes() override;
//
//     std::future<Status> write(ChunkPtr chunk) override;
//
//     std::future<CommitResult> commit() override;
//
//     void commitAsync(std::function<void(CommitResult)> callback) override;
//
//     void rollback() override;
//
//     void close() override;
//
//     FileMetrics metrics() override;
// private:
//     const static size_t OUTSTREAM_BUFFER_SIZE_BYTES;
//     const PlainTextBuilderOptions _options;
//     const std::vector<ExprContext*>& _output_expr_ctxs;
//     std::unique_ptr<csv::OutputStream> _output_stream;
//     std::vector<std::unique_ptr<csv::Converter>> _converters;
//     bool _init;
// };
} // namespace starrocks
