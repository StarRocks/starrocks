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

#include "iceberg_chunk_sink.h"

#include <future>

#include "column/datum.h"
#include "connector/async_flush_stream_poller.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/expr.h"
#include "formats/orc/orc_file_writer.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/utils.h"
#include "util/url_coding.h"
#include "utils.h"

namespace starrocks::connector {

IcebergChunkSink::IcebergChunkSink(std::vector<std::string> partition_columns, std::vector<std::string> transform_exprs,
                                   std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                                   std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory,
                                   RuntimeState* state)
        : ConnectorChunkSink(std::move(partition_columns), std::move(partition_column_evaluators),
                             std::move(partition_chunk_writer_factory), state, true),
          _transform_exprs(std::move(transform_exprs)) {}

void IcebergChunkSink::callback_on_commit(const CommitResult& result) {
    push_rollback_action(std::move(result.rollback_action));
    if (result.io_status.ok()) {
        _state->update_num_rows_load_sink(result.file_statistics.record_count);

        TIcebergColumnStats iceberg_column_stats;
        if (result.file_statistics.column_sizes.has_value()) {
            iceberg_column_stats.__set_column_sizes(result.file_statistics.column_sizes.value());
        }
        if (result.file_statistics.value_counts.has_value()) {
            iceberg_column_stats.__set_value_counts(result.file_statistics.value_counts.value());
        }
        if (result.file_statistics.null_value_counts.has_value()) {
            iceberg_column_stats.__set_null_value_counts(result.file_statistics.null_value_counts.value());
        }
        if (result.file_statistics.lower_bounds.has_value()) {
            iceberg_column_stats.__set_lower_bounds(result.file_statistics.lower_bounds.value());
        }
        if (result.file_statistics.upper_bounds.has_value()) {
            iceberg_column_stats.__set_upper_bounds(result.file_statistics.upper_bounds.value());
        }

        TIcebergDataFile iceberg_data_file;
        iceberg_data_file.__set_column_stats(iceberg_column_stats);
        iceberg_data_file.__set_partition_path(PathUtils::get_parent_path(result.location));
        iceberg_data_file.__set_path(result.location);
        iceberg_data_file.__set_format(result.format);
        iceberg_data_file.__set_record_count(result.file_statistics.record_count);
        iceberg_data_file.__set_file_size_in_bytes(result.file_statistics.file_size);
        iceberg_data_file.__set_partition_null_fingerprint(result.extra_data);

        if (result.file_statistics.split_offsets.has_value()) {
            iceberg_data_file.__set_split_offsets(result.file_statistics.split_offsets.value());
        }

        TSinkCommitInfo commit_info;
        commit_info.__set_iceberg_data_file(iceberg_data_file);
        _state->add_sink_commit_info(commit_info);
    }
}

StatusOr<std::unique_ptr<ConnectorChunkSink>> IcebergChunkSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<IcebergChunkSinkContext>(context);
    auto runtime_state = ctx->fragment_context->runtime_state();
    std::shared_ptr<FileSystem> fs = FileSystem::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_conf)).value();
    auto column_evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>(
            ColumnEvaluator::clone(ctx->column_evaluators));
    auto location_provider = std::make_shared<connector::LocationProvider>(
            ctx->path, print_id(ctx->fragment_context->query_id()), runtime_state->be_number(), driver_id,
            boost::to_lower_copy(ctx->format));

    std::vector<std::string>& partition_columns = ctx->partition_column_names;
    std::vector<std::string>& transform_exprs = ctx->transform_exprs;
    auto partition_evaluators = ColumnEvaluator::clone(ctx->partition_evaluators);
    std::shared_ptr<formats::FileWriterFactory> file_writer_factory;
    if (boost::iequals(ctx->format, formats::PARQUET)) {
        file_writer_factory = std::make_shared<formats::ParquetFileWriterFactory>(
                fs, ctx->compression_type, ctx->options, ctx->column_names, column_evaluators, ctx->parquet_field_ids,
                ctx->executor, runtime_state);
    } else {
        file_writer_factory = std::make_shared<formats::UnknownFileWriterFactory>(ctx->format);
    }

    std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory;
    if (config::enable_connector_sink_spill) {
        auto partition_chunk_writer_ctx =
                std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
                        {file_writer_factory, location_provider, ctx->max_file_size, partition_columns.empty()},
                        fs,
                        ctx->fragment_context,
                        runtime_state->desc_tbl().get_tuple_descriptor(ctx->tuple_desc_id),
                        column_evaluators,
                        ctx->sort_ordering});
        partition_chunk_writer_factory = std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    } else {
        auto partition_chunk_writer_ctx =
                std::make_shared<BufferPartitionChunkWriterContext>(BufferPartitionChunkWriterContext{
                        {file_writer_factory, location_provider, ctx->max_file_size, partition_columns.empty()}});
        partition_chunk_writer_factory =
                std::make_unique<BufferPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    }

    return std::make_unique<connector::IcebergChunkSink>(partition_columns, transform_exprs,
                                                         std::move(partition_evaluators),
                                                         std::move(partition_chunk_writer_factory), runtime_state);
}

Status IcebergChunkSink::add(Chunk* chunk) {
    std::string partition = DEFAULT_PARTITION;
    bool partitioned = !_partition_column_names.empty();
    std::vector<int8_t> partition_field_null_list;
    if (partitioned) {
        ASSIGN_OR_RETURN(partition, HiveUtils::iceberg_make_partition_name(
                                            _partition_column_names, _partition_column_evaluators,
                                            dynamic_cast<IcebergChunkSink*>(this)->transform_expr(), chunk,
                                            _support_null_partition, partition_field_null_list));
    }

    RETURN_IF_ERROR(ConnectorChunkSink::write_partition_chunk(partition, partition_field_null_list, chunk));
    return Status::OK();
}

} // namespace starrocks::connector
