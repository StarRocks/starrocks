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

#include "exec/pipeline/fragment_context.h"
#include "exprs/expr.h"
#include "formats/csv/csv_file_writer.h"
#include "formats/orc/orc_file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/utils.h"
#include "util/url_coding.h"
#include "utils.h"

namespace starrocks::connector {

HiveChunkSink::HiveChunkSink(std::vector<std::string> partition_columns,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                             std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory,
                             RuntimeState* state)
        : ConnectorChunkSink(std::move(partition_columns), std::move(partition_column_evaluators),
                             std::move(partition_chunk_writer_factory), state, false) {}

void HiveChunkSink::callback_on_commit(const CommitResult& result) {
    _rollback_actions.push_back(std::move(result.rollback_action));
    if (result.io_status.ok()) {
        _state->update_num_rows_load_sink(result.file_statistics.record_count);
        THiveFileInfo hive_file_info;
        hive_file_info.__set_file_name(PathUtils::get_filename(result.location));
        hive_file_info.__set_partition_path(PathUtils::get_parent_path(result.location));
        hive_file_info.__set_record_count(result.file_statistics.record_count);
        hive_file_info.__set_file_size_in_bytes(result.file_statistics.file_size);
        TSinkCommitInfo commit_info;
        commit_info.__set_hive_file_info(hive_file_info);
        _state->add_sink_commit_info(commit_info);
    }
}

StatusOr<std::unique_ptr<ConnectorChunkSink>> HiveChunkSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<HiveChunkSinkContext>(context);
    auto runtime_state = ctx->fragment_context->runtime_state();
    auto fs = FileSystem::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_conf)).value(); // must succeed
    auto data_column_evaluators = ColumnEvaluator::clone(ctx->data_column_evaluators);
    auto location_provider = std::make_shared<connector::LocationProvider>(
            ctx->path, print_id(ctx->fragment_context->query_id()), runtime_state->be_number(), driver_id,
            boost::to_lower_copy(ctx->format));

    std::shared_ptr<formats::FileWriterFactory> file_writer_factory;
    if (boost::iequals(ctx->format, formats::PARQUET)) {
        // ensure hive compatibility since hive 3 and lower version accepts specific encoding
        ctx->options[formats::ParquetWriterOptions::USE_LEGACY_DECIMAL_ENCODING] = "true";
        ctx->options[formats::ParquetWriterOptions::USE_INT96_TIMESTAMP_ENCODING] = "true";
        file_writer_factory = std::make_shared<formats::ParquetFileWriterFactory>(
                std::move(fs), ctx->compression_type, ctx->options, ctx->data_column_names,
                std::move(data_column_evaluators), std::nullopt, ctx->executor, runtime_state);
    } else if (boost::iequals(ctx->format, formats::ORC)) {
        file_writer_factory = std::make_shared<formats::ORCFileWriterFactory>(
                std::move(fs), ctx->compression_type, ctx->options, ctx->data_column_names,
                std::move(data_column_evaluators), ctx->executor, runtime_state);
    } else if (boost::iequals(ctx->format, formats::TEXTFILE)) {
        file_writer_factory = std::make_shared<formats::CSVFileWriterFactory>(
                std::move(fs), ctx->compression_type, ctx->options, ctx->data_column_names,
                std::move(data_column_evaluators), ctx->executor, runtime_state);
    } else {
        file_writer_factory = std::make_shared<formats::UnknownFileWriterFactory>(ctx->format);
    }

    std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory;
    // Disable the load spill for hive sink temperarily
    if (/* config::enable_connector_sink_spill */ false) {
        auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
                SpillPartitionChunkWriterContext{{file_writer_factory, location_provider, ctx->max_file_size,
                                                  ctx->partition_column_names.empty()},
                                                 ctx->fragment_context,
                                                 nullptr,
                                                 nullptr});
        partition_chunk_writer_factory = std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    } else {
        auto partition_chunk_writer_ctx = std::make_shared<BufferPartitionChunkWriterContext>(
                BufferPartitionChunkWriterContext{{file_writer_factory, location_provider, ctx->max_file_size,
                                                   ctx->partition_column_names.empty()}});
        partition_chunk_writer_factory =
                std::make_unique<BufferPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    }

    auto partition_column_evaluators = ColumnEvaluator::clone(ctx->partition_column_evaluators);
    return std::make_unique<connector::HiveChunkSink>(ctx->partition_column_names,
                                                      std::move(partition_column_evaluators),
                                                      std::move(partition_chunk_writer_factory), runtime_state);
}

} // namespace starrocks::connector
