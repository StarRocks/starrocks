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

#include "file_chunk_sink.h"

#include <future>

#include "column/datum.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/expr.h"
#include "formats/csv/csv_file_writer.h"
#include "formats/orc/orc_file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/utils.h"
#include "util/url_coding.h"
#include "utils.h"
#include "connector/sink_memory_manager.h"
#include "connector/async_io_poller.h"

namespace starrocks::connector {

FileChunkSink::FileChunkSink(std::vector<std::string> partition_columns,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                             std::unique_ptr<LocationProvider> location_provider,
                             std::unique_ptr<formats::FileWriterFactory> file_writer_factory, int64_t max_file_size,
                             RuntimeState* state)
        : ConnectorChunkSink(),
          _partition_column_names(std::move(partition_columns)),
          _partition_column_evaluators(std::move(partition_column_evaluators)),
          _location_provider(std::move(location_provider)),
          _file_writer_factory(std::move(file_writer_factory)),
          _max_file_size(max_file_size),
          _state(state) {
    _op_mem_mgr->init(&_writer_stream_pairs, std::bind_front(&FileChunkSink::callback_on_success, this));
}

Status FileChunkSink::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_partition_column_evaluators));
    RETURN_IF_ERROR(_file_writer_factory->init());
    return Status::OK();
}

// requires that input chunk belongs to a single partition (see LocalKeyPartitionExchange)
Status FileChunkSink::add(ChunkPtr chunk) {
    std::string partition = DEFAULT_PARTITION;
    bool partitioned = !_partition_column_names.empty();
    if (partitioned) {
        ASSIGN_OR_RETURN(partition, HiveUtils::make_partition_name_nullable(_partition_column_names,
                                                                            _partition_column_evaluators, chunk.get()));
    }

    auto it = _writer_stream_pairs.find(partition);
    if (it != _writer_stream_pairs.end()) {
        Writer* writer = it->second.first.get();
        if (writer->get_written_bytes() >= _max_file_size) {
            callback_on_success(writer->commit());
            _writer_stream_pairs.erase(it);
            auto path = partitioned ? _location_provider->get(partition) : _location_provider->get();
            ASSIGN_OR_RETURN(auto new_writer_and_stream, _file_writer_factory->createAsync(path));
            std::unique_ptr<Writer> new_writer =  std::move(new_writer_and_stream.writer);
            std::unique_ptr<Stream> new_stream = std::move(new_writer_and_stream.stream);
            RETURN_IF_ERROR(new_writer->init());
            RETURN_IF_ERROR(new_writer->write(chunk));
            _writer_stream_pairs[partition] = std::make_pair(std::move(new_writer), new_stream.get());
            _io_poller->enqueue(std::move(new_stream));
        } else {
            RETURN_IF_ERROR(writer->write(chunk));
        }
    } else {
        auto path = partitioned ? _location_provider->get(partition) : _location_provider->get();
        ASSIGN_OR_RETURN(auto new_writer_and_stream, _file_writer_factory->createAsync(path));
        std::unique_ptr<Writer> new_writer =  std::move(new_writer_and_stream.writer);
        std::unique_ptr<Stream> new_stream = std::move(new_writer_and_stream.stream);
        RETURN_IF_ERROR(new_writer->init());
        RETURN_IF_ERROR(new_writer->write(chunk));
        _writer_stream_pairs[partition] = std::make_pair(std::move(new_writer), new_stream.get());
        _io_poller->enqueue(std::move(new_stream));
    }

    return Status::OK();
}

Status FileChunkSink::finish() {
    for (auto& [_, writer_and_stream] : _writer_stream_pairs) {
        callback_on_success(writer_and_stream.first->commit());
    }
    return Status::OK();
}

void FileChunkSink::callback_on_success(const formats::FileWriter::CommitResult& result) {
    if (!result.io_status.ok()) {
        return;
    }

    _state->update_num_rows_load_sink(result.file_statistics.record_count);
}

StatusOr<std::unique_ptr<ConnectorChunkSink>> FileChunkSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<FileChunkSinkContext>(context);
    auto runtime_state = ctx->fragment_context->runtime_state();
    auto fs = FileSystem::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_conf)).value();
    auto column_evaluators = ColumnEvaluator::clone(ctx->column_evaluators);
    auto location_provider = std::make_unique<connector::LocationProvider>(
            ctx->path, print_id(ctx->fragment_context->query_id()), runtime_state->be_number(), driver_id,
            boost::to_lower_copy(ctx->format));

    std::unique_ptr<formats::FileWriterFactory> file_writer_factory;
    if (boost::iequals(ctx->format, formats::PARQUET)) {
        file_writer_factory = std::make_unique<formats::ParquetFileWriterFactory>(
                std::move(fs), ctx->compression_type, ctx->options, ctx->column_names, std::move(column_evaluators),
                std::nullopt, ctx->executor, runtime_state);
    } else if (boost::iequals(ctx->format, formats::ORC)) {
        file_writer_factory = std::make_unique<formats::ORCFileWriterFactory>(
                std::move(fs), ctx->compression_type, ctx->options, ctx->column_names, std::move(column_evaluators),
                ctx->executor, runtime_state);
    } else if (boost::iequals(ctx->format, formats::CSV)) {
        file_writer_factory = std::make_unique<formats::CSVFileWriterFactory>(
                std::move(fs), ctx->compression_type, ctx->options, ctx->column_names, std::move(column_evaluators),
                ctx->executor, runtime_state);
    } else {
        file_writer_factory = std::make_unique<formats::UnknownFileWriterFactory>(ctx->format);
    }

    std::vector<std::string> partition_columns;
    std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators;
    for (auto idx : ctx->partition_column_indices) {
        partition_columns.push_back(ctx->column_names[idx]);
        partition_column_evaluators.push_back(ctx->column_evaluators[idx]->clone());
    }
    return std::make_unique<connector::FileChunkSink>(partition_columns, std::move(partition_column_evaluators),
                                                      std::move(location_provider), std::move(file_writer_factory),
                                                      ctx->max_file_size, runtime_state);
}

} // namespace starrocks::connector
