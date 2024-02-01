//
// Created by Letian Jiang on 2024/1/29.
//

#include "iceberg_chunk_sink.h"

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

IcebergChunkSink::IcebergChunkSink(const std::vector<std::string>& partition_columns,
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

Status IcebergChunkSink::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_partition_column_evaluators));
    return Status::OK();
}

// requires that input chunk belongs to a single partition (see LocalKeyPartitionExchange)
StatusOr<ConnectorChunkSink::Futures> IcebergChunkSink::add(ChunkPtr chunk) {
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

ConnectorChunkSink::Futures IcebergChunkSink::finish() {
    Futures futures;
    for (auto& [_, writer] : _partition_writers) {
        auto f = writer->commit();
        futures.commit_file_future.push_back(std::move(f));
    }
    return futures;
}

std::function<void(const formats::FileWriter::CommitResult& result)> IcebergChunkSink::callback_on_success() {
    return [state = _state](const formats::FileWriter::CommitResult& result) {
        DCHECK(result.io_status.ok());
        state->update_num_rows_load_sink(result.file_metrics.record_count);

        TIcebergColumnStats iceberg_column_stats;
        if (result.file_metrics.column_sizes.has_value()) {
            iceberg_column_stats.__set_column_sizes(result.file_metrics.column_sizes.value());
        }
        if (result.file_metrics.value_counts.has_value()) {
            iceberg_column_stats.__set_value_counts(result.file_metrics.value_counts.value());
        }
        if (result.file_metrics.null_value_counts.has_value()) {
            iceberg_column_stats.__set_null_value_counts(result.file_metrics.null_value_counts.value());
        }
        if (result.file_metrics.lower_bounds.has_value()) {
            iceberg_column_stats.__set_lower_bounds(result.file_metrics.lower_bounds.value());
        }
        if (result.file_metrics.upper_bounds.has_value()) {
            iceberg_column_stats.__set_upper_bounds(result.file_metrics.upper_bounds.value());
        }

        TIcebergDataFile iceberg_data_file;
        iceberg_data_file.__set_column_stats(iceberg_column_stats);
        iceberg_data_file.__set_partition_path(PathUtils::get_parent_path(result.location));
        iceberg_data_file.__set_path(result.location);
        iceberg_data_file.__set_format(result.format);
        iceberg_data_file.__set_record_count(result.file_metrics.record_count);
        iceberg_data_file.__set_file_size_in_bytes(result.file_metrics.file_size);

        if (result.file_metrics.split_offsets.has_value()) {
            iceberg_data_file.__set_split_offsets(result.file_metrics.split_offsets.value());
        }

        TSinkCommitInfo commit_info;
        commit_info.__set_iceberg_data_file(iceberg_data_file);
        state->add_sink_commit_info(commit_info);
    };
}

std::unique_ptr<ConnectorChunkSink> IcebergChunkSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<IcebergChunkSinkContext>(context);
    auto runtime_state = ctx->fragment_context->runtime_state();
    auto fs = FileSystem::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_conf)).value();
    auto column_evaluators = ColumnEvaluator::clone(ctx->column_evaluators);
    auto location_provider = std::make_unique<connector::LocationProvider>(
            ctx->path, print_id(ctx->fragment_context->query_id()), runtime_state->be_number(), driver_id,
            boost::to_lower_copy(ctx->format));

    std::unique_ptr<formats::FileWriterFactory> file_writer_factory;
    CHECK(boost::iequals(ctx->format, formats::PARQUET)) << "iceberg sink only supports parquet";
    file_writer_factory = std::make_unique<formats::ParquetFileWriterFactory>(
            std::move(fs), ctx->format, ctx->options, ctx->column_names, std::move(column_evaluators), ctx->parquet_field_ids,
            ctx->executor);

    std::vector<std::string> partition_columns;
    std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators;
    for (auto idx : ctx->partition_column_indices) {
        partition_columns.push_back(ctx->column_names[idx]);
        partition_column_evaluators.push_back(ctx->column_evaluators[idx]->clone());
    }
    return std::make_unique<connector::IcebergChunkSink>(partition_columns, std::move(partition_column_evaluators),
                                                      std::move(location_provider), std::move(file_writer_factory),
                                                      ctx->max_file_size, runtime_state);
}

} // namespace starrocks::connector
