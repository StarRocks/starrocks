//
// Created by Letian Jiang on 2024/1/16.
//

#include "connector_chunk_sink.h"

#include <formats/parquet/file_writer.h>
#include <future>
#include <fmt/format.h>
#include <formats/orc/orc_chunk_writer.h>
#include <util/url_coding.h>

#include "formats/orc/orc_file_writer.h"

namespace starrocks::pipeline {

StatusOr<std::unique_ptr<FileWriter>> FileWriterFactory::create(std::string path) const {
    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(path));
    switch (_format) {
        case FileWriter::FileFormat::PARQUET: {
            auto output_stream = std::make_unique<parquet::ParquetOutputStream>(std::move(file));
            auto options = std::dynamic_pointer_cast<parquet::ParquetFileWriter::ParquetWriterOptions>(_options);
            return std::make_unique<parquet::ParquetFileWriter>(output_stream, _column_names, _output_exprs, options, _executors);
        }
        case FileWriter::FileFormat::ORC: {
            auto output_stream = std::make_unique<OrcOutputStream>(std::move(file));
            auto options = std::dynamic_pointer_cast<ORCFileWriter::ORCWriterOptions>(_options);
            return std::make_unique<ORCFileWriter>(output_stream, _column_names, _output_exprs, options, _executors);
        }
        default: {
            return Status::NotSupported("unsupported file format");
        }
    }
}

FilesChunkSink::FilesChunkSink(const std::vector<std::string>& partition_columns,
    const std::vector<ExprContext*>& partition_exprs, std::unique_ptr<LocationProvider> location_provider,
    std::unique_ptr<FileWriterFactory> file_writer_factory, int64_t max_file_size) : _partition_column_names(partition_columns), _partition_exprs(partition_exprs), _location_provider(std::move(location_provider)), _file_writer_factory(std::move(file_writer_factory)), _max_file_size(max_file_size) {}

// requires that input chunk belongs to a single partition (see LocalKeyPartitionExchange)
Status<ConnectorChunkSink::Futures> FilesChunkSink::add(ChunkPtr chunk) {
    std::string partition;
    if (_partition_exprs.empty()) {
        partition = DEFAULT_PARTITION;
    } else {
        ASSIGN_OR_RETURN(partition, HiveUtils::make_partition_name(_partition_column_names, _partition_exprs, chunk));
    }

    // create writer if not found
    if (_partition_writers[partition] == nullptr) {
        auto path = _location_provider->get(partition);
        ASSIGN_OR_RETURN(_partition_writers[partition], _file_writer_factory->create(path));
    }

    Futures futures;
    auto& writer = _partition_writers[partition];
    if (writer->get_written_bytes() > _max_file_size) {
        // TODO(me): how to handle the ownership of to commit file writer? use shared_ptr?
        auto f = writer->commit();
        futures.commit_file_future.push_back(std::move(f));
        futures.file_writers.push_back(std::move(writer));
    }

    auto f = writer->write(chunk);
    futures.add_chunk_future.push_back(std::move(f));
    return futures;
}

ConnectorChunkSink::Futures FilesChunkSink::finish() {
    Futures futures;
    for (auto& [_, writer] : _partition_writers) {
        auto f = writer->commit();
        futures.commit_file_future.push_back(std::move(f));
        futures.file_writers.push_back(std::move(writer));
    }
    return futures;
}

StatusOr<std::string> HiveUtils::make_partition_name(const std::vector<std::string>& column_names, const std::vector<ExprContext*>& exprs, ChunkPtr chunk) {
    DCHECK_EQ(column_names.size(), exprs.size());
    std::stringstream ss;
    for (size_t i = 0; i < exprs.size(); i++) {
        ASSIGN_OR_RETURN(auto column, exprs[i]->evaluate(chunk.get()));
        auto type = exprs[i]->root()->type();
        ASSIGN_OR_RETURN(auto value, column_value(type, column));
        ss << column_names[i] << "=" << value << "/";
    }
    return ss.str();
}

// TODO(letian-jiang): translate org.apache.hadoop.hive.common.FileUtils#makePartName
StatusOr<std::string> HiveUtils::column_value(const TypeDescriptor& type_desc, const ColumnPtr& column) {
    DCHECK_GT(column->size(), 0);
    auto datum = column->get(0);
    if (datum.is_null()) {
        return "null";
    }

    switch (type_desc.type) {
        case TYPE_BOOLEAN: {
            return datum.get_uint8() ? "true" : "false";
        }
        case TYPE_TINYINT: {
            return std::to_string(datum.get_int8());
        }
        case TYPE_SMALLINT: {
            return std::to_string(datum.get_int16());
        }
        case TYPE_INT: {
            return std::to_string(datum.get_int32());
        }
        case TYPE_BIGINT: {
            return std::to_string(datum.get_int64());
        }
        case TYPE_DATE: {
            return datum.get_date().to_string();
        }
        case TYPE_DATETIME: {
            return url_encode(datum.get_timestamp().to_string());
        }
        case TYPE_CHAR: {
            std::string origin_str = datum.get_slice().to_string();
            if (origin_str.length() < type_desc.len) {
                origin_str.append(type_desc.len - origin_str.length(), ' ');
            }
            return url_encode(origin_str);
        }
        case TYPE_VARCHAR: {
            return url_encode(datum.get_slice().to_string());
        }
        default: {
            return Status::InvalidArgument("unsupported partition column type" + type_desc.debug_string());
        }
    }
}

}


