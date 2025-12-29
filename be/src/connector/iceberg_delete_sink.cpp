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

#include "connector/iceberg_delete_sink.h"

#include <fmt/format.h>

#include <algorithm>
#include <future>

#include "column/column_helper.h"
#include "column/datum.h"
#include "connector/async_flush_stream_poller.h"
#include "connector/partition_chunk_writer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/sorting/sorting.h"
#include "exprs/expr.h"
#include "formats/column_evaluator.h"
#include "formats/parquet/parquet_file_writer.h"
#include "formats/utils.h"
#include "gutil/strings/fastmem.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"
#include "util/url_coding.h"
#include "utils.h"

namespace starrocks::connector {

// IcebergDeleteSink implementation
IcebergDeleteSink::IcebergDeleteSink(std::vector<std::string> partition_columns,
                                     std::vector<std::string> transform_exprs,
                                     std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                                     std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory,
                                     RuntimeState* state,
                                     const std::unordered_map<std::string, TExprNode>& column_slot_map)
        : ConnectorChunkSink(std::move(partition_columns), std::move(partition_column_evaluators),
                             std::move(partition_chunk_writer_factory), state, true),
          _transform_exprs(std::move(transform_exprs)),
          _column_slot_map(column_slot_map),
          _state(state) {}

// Callback for handling commit results
void IcebergDeleteSink::callback_on_commit(const CommitResult& result) {
    // TODO: Implement proper commit result handling for delete files
}

// Adds a chunk of data to the delete sink.
// Groups rows by file_path and writes separate delete files for each source data file.
//
// Parameters:
//   chunk - Input chunk containing columns: _file (file_path) and _pos (row_position)
//
// Returns Status::OK() on success, or an error if processing fails.
Status IcebergDeleteSink::add(const ChunkPtr& chunk) {
    int num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    std::string partition = DEFAULT_PARTITION;
    bool is_partitioned = !_partition_column_names.empty();
    std::vector<int8_t> partition_field_null_list;

    // Compute partition name if table is partitioned
    if (is_partitioned) {
        ASSIGN_OR_RETURN(partition, HiveUtils::iceberg_make_partition_name(
                                            _partition_column_names, _partition_column_evaluators, _transform_exprs,
                                            chunk.get(), _support_null_partition, partition_field_null_list));
    }

    // Find file_path column slot_id from the mapping
    auto file_path_it = _column_slot_map.find("_file");
    if (file_path_it == _column_slot_map.end()) {
        return Status::InternalError("Could not find _file column in column_slot_map");
    }
    SlotId file_path_slot_id = file_path_it->second.slot_ref.slot_id;

    // Get file_path column using slot_id
    ColumnPtr file_path_column = chunk->get_column_by_slot_id(file_path_slot_id);
    if (file_path_column == nullptr) {
        return Status::InternalError(
                fmt::format("Could not find file_path column with slot_id {} in chunk", file_path_slot_id));
    }

    // Get underlying binary column (handles nullable columns)
    const BinaryColumn* binary_column = ColumnHelper::get_binary_column(file_path_column.get());

    // Group rows by file_path for file-level delete files
    std::unordered_map<std::string, std::vector<uint32_t>> file_path_to_indices;
    for (int i = 0; i < num_rows; ++i) {
        // Skip NULL file_path values (they don't reference any file)
        if (file_path_column->is_null(i)) {
            LOG(WARNING) << "Skipping row " << i << " with NULL file_path";
            continue;
        }
        std::string file_path = binary_column->get_slice(i).to_string();
        file_path_to_indices[std::move(file_path)].push_back(i);
    }

    // Write separate delete files for each file_path
    for (auto& [file_path, indices] : file_path_to_indices) {
        // Create chunk with only rows for this file_path
        ChunkPtr file_chunk = chunk->clone_empty_with_slot();
        file_chunk->append_selective(*chunk, indices.data(), 0, indices.size());

        // Write using file-level writer for this (partition, file_path)
        RETURN_IF_ERROR(write_file_level_chunk(partition, partition_field_null_list, file_chunk, file_path));
    }

    return Status::OK();
}

// Finishes writing all delete files.
// Flushes and finalizes all file-level writers.
//
// Returns Status::OK() on success, or an error if finishing fails.
Status IcebergDeleteSink::finish() {
    // Flush all file-level writers
    for (auto& [key, writer] : _file_writers) {
        RETURN_IF_ERROR(writer->flush());
    }
    // Wait for all flushes to complete
    for (auto& [key, writer] : _file_writers) {
        RETURN_IF_ERROR(writer->wait_flush());
    }
    // Finish all writers
    for (auto& [key, writer] : _file_writers) {
        RETURN_IF_ERROR(writer->finish());
    }
    return Status::OK();
}

// Checks if all delete file writes are finished.
//
// Returns true if all writers are finished, false otherwise.
bool IcebergDeleteSink::is_finished() {
    for (auto& [key, writer] : _file_writers) {
        if (!writer->is_finished()) {
            return false;
        }
    }
    return true;
}

// Creates an IcebergDeleteSink instance for writing position delete files.
// Validates the context and creates necessary writers for delete file generation.
//
// Parameters:
//   context - The sink context containing configuration (must be IcebergDeleteSinkContext)
//   driver_id - The driver ID for this sink instance
//
// Returns the created sink on success, or an error if creation fails.
StatusOr<std::unique_ptr<ConnectorChunkSink>> IcebergDeleteSinkProvider::create_chunk_sink(
        std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) {
    auto ctx = std::dynamic_pointer_cast<IcebergDeleteSinkContext>(context);
    if (ctx == nullptr) {
        return Status::InternalError("IcebergDeleteSinkProvider: context is not IcebergDeleteSinkContext");
    }

    auto runtime_state = ctx->fragment_context->runtime_state();

    TupleDescriptor* tuple_desc = runtime_state->desc_tbl().get_tuple_descriptor(ctx->tuple_desc_id);
    if (tuple_desc == nullptr) {
        return Status::InternalError(fmt::format("Failed to find tuple descriptor with id {}", ctx->tuple_desc_id));
    }
    DCHECK(tuple_desc->slots().size() == ctx->output_exprs.size());

    // Verify we found the required columns
    if (ctx->column_slot_map.find("_file") == ctx->column_slot_map.end()) {
        return Status::InternalError("Could not find _file column in column_slot_map");
    }
    if (ctx->column_slot_map.find("_pos") == ctx->column_slot_map.end()) {
        return Status::InternalError("Could not find _pos column in column_slot_map");
    }

    // Create filesystem
    std::shared_ptr<FileSystem> fs =
            FileSystem::CreateUniqueFromString(ctx->path, FSOptions(&ctx->cloud_configuration)).value();

    // For delete files, we only need file_path and row_position columns
    std::vector<std::string> column_names = {"file_path", "pos"};
    // We only need evaluators for the two columns: file_path and pos
    std::vector<std::unique_ptr<ColumnEvaluator>> column_evaluators_vec;
    column_evaluators_vec.push_back(ctx->column_evaluators[0]->clone());
    column_evaluators_vec.push_back(ctx->column_evaluators[1]->clone());
    auto column_evaluators =
            std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>(std::move(column_evaluators_vec));

    // Create location provider for delete files
    auto location_provider = std::make_shared<connector::LocationProvider>(
            ctx->path, print_id(ctx->fragment_context->query_id()), runtime_state->be_number(), driver_id, "parquet");

    std::vector<formats::FileColumnId> file_column_ids(column_names.size());
    // file_path column (index 0)
    file_column_ids[0].field_id = INT32_MAX - 101;
    // pos column (index 1)
    file_column_ids[1].field_id = INT32_MAX - 102;

    // Create Parquet writer factory for delete files
    auto file_writer_factory = std::make_shared<formats::ParquetFileWriterFactory>(
            fs, ctx->compression_type, ctx->options, column_names, column_evaluators, file_column_ids, ctx->executor,
            runtime_state);

    // Initialize sort ordering for position delete files (required by Iceberg spec)
    // Sort by: file_path ASC, then pos ASC
    std::shared_ptr<SortOrdering> sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0, 1};                    // file_path, pos
    sort_ordering->sort_descs.descs.emplace_back(true, false); // file_path: ASC, nulls last
    sort_ordering->sort_descs.descs.emplace_back(true, false); // pos: ASC, nulls last

    // Create partition chunk writer factory
    std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory;

    // Create a custom tuple descriptor with only file_path and pos columns
    // Use DescriptorTbl::create to properly create tuple and slot descriptors
    TSlotDescriptorBuilder slot_builder;
    TTupleDescriptorBuilder tuple_builder;

    // Add file_path column (VARCHAR)
    tuple_builder.add_slot(slot_builder.id(1)
                                   .type(TYPE_VARCHAR)
                                   .nullable(true)
                                   .is_materialized(true)
                                   .column_name("file_path")
                                   .build());

    // Add pos column (BIGINT)
    tuple_builder.add_slot(
            slot_builder.id(2).type(TYPE_BIGINT).nullable(true).is_materialized(true).column_name("pos").build());

    // Create descriptor table and tuple
    TDescriptorTableBuilder desc_tbl_builder;
    tuple_builder.build(&desc_tbl_builder);
    TDescriptorTable t_desc_tbl = desc_tbl_builder.desc_tbl();

    DescriptorTbl* desc_tbl = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(runtime_state, runtime_state->obj_pool(), t_desc_tbl, &desc_tbl,
                                          config::vector_chunk_size));

    // Extract the tuple descriptor we just created (it will be the first one)
    TupleDescriptor* delete_tuple_desc = desc_tbl->get_tuple_descriptor(0);
    DCHECK(delete_tuple_desc != nullptr);
    DCHECK_EQ(delete_tuple_desc->slots().size(), 2);

    auto writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(SpillPartitionChunkWriterContext{
            {file_writer_factory, location_provider, ctx->max_file_size, ctx->partition_column_names.empty()},
            fs,
            ctx->fragment_context,
            delete_tuple_desc,
            column_evaluators,
            sort_ordering});
    partition_chunk_writer_factory = std::make_unique<SpillPartitionChunkWriterFactory>(writer_ctx);

    // Create the delete sink
    return std::make_unique<IcebergDeleteSink>(
            ctx->partition_column_names, ctx->transform_exprs, ColumnEvaluator::clone(ctx->partition_evaluators),
            std::move(partition_chunk_writer_factory), runtime_state, ctx->column_slot_map);
}

// Writes a chunk to the file-level delete file for a specific source data file.
// Creates a writer for (partition, file_path) if one doesn't exist.
//
// Parameters:
//   partition - The partition name
//   partition_field_null_list - List indicating which partition fields are NULL
//   chunk - The chunk to write (contains rows for one specific source file)
//   file_path - The path of the source data file these deletes apply to
//
// Returns Status::OK() on success, or an error if writing fails.
Status IcebergDeleteSink::write_file_level_chunk(const std::string& partition,
                                                 const std::vector<int8_t>& partition_field_null_list,
                                                 const ChunkPtr& chunk, const std::string& file_path) {
    // Key: (partition, file_path)
    auto key = std::make_pair(partition, file_path);

    // Check if writer already exists for this file
    auto it = _file_writers.find(key);
    if (it != _file_writers.end()) {
        return it->second->write(chunk);
    }

    // Create new writer for this (partition, file_path)
    auto writer = _partition_chunk_writer_factory->create(partition, partition_field_null_list);

    // Set up callbacks
    auto commit_callback = [this](const CommitResult& r) { this->callback_on_commit(r); };
    auto error_handler = [this](const Status& s) { this->set_status(s); };
    writer->set_commit_callback(commit_callback);
    writer->set_error_handler(error_handler);
    writer->set_io_poller(_io_poller);

    // Initialize and cache the writer
    RETURN_IF_ERROR(writer->init());

    _file_writers[key] = writer;
    return writer->write(chunk);
}

} // namespace starrocks::connector