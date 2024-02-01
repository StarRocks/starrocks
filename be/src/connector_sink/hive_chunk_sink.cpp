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

#include "hive_chunk_sink.h"

#include <future>

#include "column/datum.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/expr.h"
#include "formats/orc/orc_file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/utils.h"
#include "util/url_coding.h"
#include "utils.h"

namespace starrocks::connector {

HiveChunkSink::HiveChunkSink(const std::vector<std::string>& partition_columns,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                             std::unique_ptr<LocationProvider> location_provider,
                             std::unique_ptr<formats::FileWriterFactory> file_writer_factory, int64_t max_file_size,
                             RuntimeState* state)
        : _partition_column_names(partition_columns),
          _partition_column_evaluators(std::move(partition_column_evaluators)),
          _location_provider(std::move(location_provider)),
          _file_writer_factory(std::move(file_writer_factory)),
          _max_file_size(max_file_size),
          _state(state) {}

Status HiveChunkSink::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_partition_column_evaluators));
    return Status::OK();
}

// requires that input chunk belongs to a single partition (see LocalKeyPartitionExchange)
StatusOr<ConnectorChunkSink::Futures> HiveChunkSink::add(ChunkPtr chunk) {
    std::string partition;
    if (_partition_column_names.empty()) {
        partition = DEFAULT_PARTITION;
    } else {
        ASSIGN_OR_RETURN(partition,
                         HiveUtils::make_partition_name(_partition_column_names, _partition_column_evaluators, chunk));
    }

    // create writer if not found
    if (_partition_writers[partition] == nullptr) {
        auto path = _partition_column_names.empty() ? _location_provider->get() : _location_provider->get(partition);
        ASSIGN_OR_RETURN(_partition_writers[partition], _file_writer_factory->create(path));
        RETURN_IF_ERROR(_partition_writers[partition]->init());
    }

    Futures futures;
    auto& writer = _partition_writers[partition];
    if (writer->get_written_bytes() > _max_file_size) {
        auto f = writer->commit();
        futures.commit_file_future.push_back(std::move(f));
        auto path = _partition_column_names.empty() ? _location_provider->get() : _location_provider->get(partition);
        ASSIGN_OR_RETURN(_partition_writers[partition], _file_writer_factory->create(path));
    }

    auto f = writer->write(chunk);
    futures.add_chunk_future.push_back(std::move(f));
    return futures;
}

ConnectorChunkSink::Futures HiveChunkSink::finish() {
    Futures futures;
    for (auto& [_, writer] : _partition_writers) {
        auto f = writer->commit();
        futures.commit_file_future.push_back(std::move(f));
    }
    return futures;
}

std::function<void(const formats::FileWriter::CommitResult& result)> HiveChunkSink::callback_on_success() {
    return [state = _state](const formats::FileWriter::CommitResult& result) {
        DCHECK(result.io_status.ok());
        state->update_num_rows_load_sink(result.file_metrics.record_count);

        THiveFileInfo hive_file_info;
        hive_file_info.__set_file_name(PathUtils::get_filename(result.location));
        hive_file_info.__set_partition_path(PathUtils::get_parent_path(result.location));
        hive_file_info.__set_record_count(result.file_metrics.record_count);
        hive_file_info.__set_file_size_in_bytes(result.file_metrics.file_size);
        TSinkCommitInfo commit_info;
        commit_info.__set_hive_file_info(hive_file_info);
        state->add_sink_commit_info(commit_info);
    };
}

std::unique_ptr<ConnectorChunkSink> HiveChunkSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<HiveChunkSinkContext>(context);
    auto runtime_state = ctx->fragment_context->runtime_state();
    auto fs = FileSystem::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_conf)).value();
    auto data_column_evaluators = ColumnEvaluator::clone(ctx->data_column_evaluators);
    auto location_provider = std::make_unique<connector::LocationProvider>(
            ctx->path, print_id(ctx->fragment_context->query_id()), runtime_state->be_number(), driver_id,
            boost::to_lower_copy(ctx->format));

    std::unique_ptr<formats::FileWriterFactory> file_writer_factory;
    if (boost::iequals(ctx->format, formats::PARQUET)) {
        file_writer_factory = std::make_unique<formats::ParquetFileWriterFactory>(
                std::move(fs), ctx->format, ctx->options, ctx->data_column_names, std::move(data_column_evaluators),
                std::nullopt, ctx->executor);
    } else if (boost::iequals(ctx->format, formats::ORC)) {
        file_writer_factory = std::make_unique<formats::ORCFileWriterFactory>(
                std::move(fs), ctx->format, ctx->options, ctx->data_column_names, std::move(data_column_evaluators),
                ctx->executor);
    } else {
        CHECK(false) << "unreachable";
        __builtin_unreachable();
    }

    auto partition_column_evaluators = ColumnEvaluator::clone(ctx->partition_column_evaluators);
    return std::make_unique<connector::HiveChunkSink>(ctx->partition_column_names, std::move(partition_column_evaluators),
                                                      std::move(location_provider), std::move(file_writer_factory),
                                                      ctx->max_file_size, runtime_state);
}

} // namespace starrocks::connector
