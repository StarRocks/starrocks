//
// Created by Letian Jiang on 2024/1/13.
//

#include "file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/orc/orc_chunk_writer.h"
#include "formats/orc/orc_file_writer.h"

namespace starrocks::formats {

FileWriterFactory::FileWriterFactory(std::shared_ptr<FileSystem> fs, FileWriter::FileFormat format,
                                     std::shared_ptr<FileWriter::FileWriterOptions> options,
                                     const std::vector<std::string>& column_names, const std::vector<ExprContext*>& output_exprs,
                                     PriorityThreadPool* executors)
        : _options(options),
          _fs(std::move(fs)),
          _format(format),
          _column_names(column_names),
          _output_exprs(output_exprs),
          _executors(executors) {}

// TODO: pass rollback action in ctor
StatusOr<std::shared_ptr<FileWriter>> FileWriterFactory::create(const std::string& path) const {
    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(path));
    switch (_format) {
        case FileWriter::FileFormat::PARQUET: {
            auto output_stream = std::make_unique<parquet::ParquetOutputStream>(std::move(file));
            auto options = std::dynamic_pointer_cast<ParquetFileWriter::ParquetWriterOptions>(_options);
            return std::make_shared<ParquetFileWriter>(std::move(output_stream), _column_names, _output_exprs,
                                                       options, _executors);
        }
        case FileWriter::FileFormat::ORC: {
            auto output_stream = std::make_unique<OrcOutputStream>(std::move(file));
            auto options = std::dynamic_pointer_cast<ORCFileWriter::ORCWriterOptions>(_options);
            return std::make_shared<ORCFileWriter>(std::move(output_stream), _column_names, _output_exprs, options,
                                                   _executors);
        }
        default: {
            return Status::NotSupported("unsupported file format");
        }
    }
}

}

