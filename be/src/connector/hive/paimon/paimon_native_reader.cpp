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

#include "connector/hive/paimon/paimon_native_reader.h"

#include <arrow/c/bridge.h>
#include <fmt/format.h>
#include <paimon/defs.h>
#include <paimon/read_context.h>
#include <paimon/table/source/split.h>
#include <paimon/table/source/table_read.h>

#include <algorithm>
#include <string_view>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "connector/hive/paimon/paimon_evaluator.h"
#include "connector/hive/paimon/paimon_file_system.h"
#include "connector/hive/paimon/paimon_memory_pool.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "formats/arrow/arrow_column_converter.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {

constexpr int64_t kPaimonReadBatchSize = 10000;
constexpr int32_t kPaimonRowToBatchThreadNumMax = 256;
constexpr int32_t kPaimonRemoteReadBufferBlockCount = 64;
constexpr int64_t kPaimonParquetCacheHoleSizeLimit = 4L * 1024 * 1024;
constexpr int64_t kPaimonParquetCacheRangeSizeLimit = 32L * 1024 * 1024;
constexpr int64_t kPaimonParquetBitmapCoalesceHoleSizeLimit = 32;
constexpr std::string_view kPaimonParquetBitmapRefiningStrategy = "coalesce";

} // namespace

PaimonNativeReader::PaimonNativeReader(const HdfsScannerContext& scanner_ctx, RuntimeState* runtime_state,
                                       FormatScannerStats* app_stats)
        : _scanner_ctx(scanner_ctx),
          _runtime_state(runtime_state),
          _app_stats(app_stats),
          _max_chunk_size(runtime_state->chunk_size() > 0 ? runtime_state->chunk_size() : 4096),
          _memory_pool(std::make_shared<TrackedPaimonMemoryPool>(runtime_state->query_mem_tracker_ptr().get())) {
    _convert_context.timezone = runtime_state->timezone();
    _convert_context.current_file = scanner_ctx.file_path;
}

PaimonNativeReader::~PaimonNativeReader() {
    close();
}

Status PaimonNativeReader::open() {
    SCOPED_RAW_TIMER(&_app_stats->reader_init_ns);
    const THdfsScanRange& scan_range = *_scanner_ctx.scan_range;
    const auto* table_descriptor = dynamic_cast<const PaimonTableDescriptor*>(_scanner_ctx.hive_table);
    std::string table_path;
    if (table_descriptor != nullptr && !table_descriptor->get_paimon_table_path().empty()) {
        table_path = table_descriptor->get_paimon_table_path();
    } else if (scan_range.__isset.paimon_table_path) {
        table_path = scan_range.paimon_table_path;
    }
    if (table_path.empty()) {
        return Status::InvalidArgument("Paimon native scan range is missing paimon_table_path");
    }
    if (!scan_range.__isset.paimon_split_info_binary || scan_range.paimon_split_info_binary.empty()) {
        return Status::InvalidArgument("Paimon native scan range is missing paimon_split_info_binary");
    }
    if (_scanner_ctx.fs == nullptr) {
        return Status::InternalError("Paimon native scanner has no StarRocks file system");
    }

    const auto& query_options = _runtime_state->query_options();
    const int32_t remote_read_buffer_block_count =
            query_options.__isset.paimon_datacache_remote_read_buffer_block_count
                    ? query_options.paimon_datacache_remote_read_buffer_block_count
                    : kPaimonRemoteReadBufferBlockCount;
    const int32_t row_to_batch_thread_num = query_options.__isset.paimon_native_reader_row_to_batch_thread_num
                                                    ? query_options.paimon_native_reader_row_to_batch_thread_num
                                                    : 1;
    const int64_t parquet_cache_hole_size_limit = query_options.__isset.paimon_parquet_read_cache_hole_size_limit
                                                          ? query_options.paimon_parquet_read_cache_hole_size_limit
                                                          : kPaimonParquetCacheHoleSizeLimit;
    const int64_t parquet_cache_range_size_limit = query_options.__isset.paimon_parquet_read_cache_range_size_limit
                                                           ? query_options.paimon_parquet_read_cache_range_size_limit
                                                           : kPaimonParquetCacheRangeSizeLimit;
    if (row_to_batch_thread_num <= 0 || row_to_batch_thread_num > kPaimonRowToBatchThreadNumMax) {
        return Status::InvalidArgument("paimon_native_reader_row_to_batch_thread_num must be between 1 and 256");
    }
    if (parquet_cache_range_size_limit <= parquet_cache_hole_size_limit) {
        return Status::InvalidArgument(
                "paimon_parquet_read_cache_range_size_limit must be greater than "
                "paimon_parquet_read_cache_hole_size_limit");
    }
    _paimon_file_system = std::make_shared<PaimonFileSystem>(_scanner_ctx.fs, _scanner_ctx.datacache_options,
                                                             remote_read_buffer_block_count);

    const auto& materialized_columns = _scanner_ctx.format_scan_context.materialized_columns;
    std::vector<std::string> field_names;
    std::vector<SlotDescriptor*> read_slots;
    field_names.reserve(materialized_columns.size());
    read_slots.reserve(materialized_columns.size());
    _convert_functions.reserve(materialized_columns.size());
    _cast_exprs.resize(materialized_columns.size(), nullptr);
    for (const auto& materialized_column : materialized_columns) {
        field_names.emplace_back(materialized_column.name());
        read_slots.emplace_back(materialized_column.slot_desc);
        _convert_functions.emplace_back(std::make_unique<ConvertFuncTree>());
    }

    paimon::ReadContextBuilder context_builder(table_path);
    if (!field_names.empty()) {
        context_builder.SetReadFieldNames(field_names);
    }
    if (table_descriptor != nullptr && !table_descriptor->get_paimon_table_schema_json().empty()) {
        context_builder.SetTableSchema(std::string(table_descriptor->get_paimon_table_schema_json()));
    }

    std::vector<Expr*> conjuncts;
    for (const auto& entry : _scanner_ctx.format_scan_context.conjunct_ctxs_by_slot) {
        for (ExprContext* conjunct_context : entry.second) {
            conjuncts.emplace_back(conjunct_context->root());
        }
    }
    if (!conjuncts.empty()) {
        PaimonEvaluator evaluator(read_slots);
        auto predicate = evaluator.evaluate(&conjuncts);
        if (predicate != nullptr) {
            context_builder.SetPredicate(predicate);
        }
    }
    context_builder.AddOption(paimon::Options::READ_BATCH_SIZE, std::to_string(kPaimonReadBatchSize));
    context_builder.AddOption("parquet.read.cache-option.hole-size-limit",
                              std::to_string(parquet_cache_hole_size_limit));
    context_builder.AddOption("parquet.read.cache-option.range-size-limit",
                              std::to_string(parquet_cache_range_size_limit));
    context_builder.AddOption("parquet.read.bitmap.row-range-refining-strategy",
                              query_options.__isset.paimon_parquet_read_bitmap_row_range_refining_strategy
                                      ? query_options.paimon_parquet_read_bitmap_row_range_refining_strategy
                                      : std::string(kPaimonParquetBitmapRefiningStrategy));
    context_builder.AddOption("parquet.read.bitmap.coalesce-hole-size-limit",
                              std::to_string(query_options.__isset.paimon_parquet_read_bitmap_coalesce_hole_size_limit
                                                     ? query_options.paimon_parquet_read_bitmap_coalesce_hole_size_limit
                                                     : kPaimonParquetBitmapCoalesceHoleSizeLimit));
    context_builder.EnablePrefetch(query_options.__isset.paimon_native_reader_enable_prefetch
                                           ? query_options.paimon_native_reader_enable_prefetch
                                           : false);
    context_builder.EnableMultiThreadRowToBatch(
            query_options.__isset.paimon_native_reader_enable_multi_thread_row_to_batch
                    ? query_options.paimon_native_reader_enable_multi_thread_row_to_batch
                    : false);
    context_builder.SetRowToBatchThreadNumber(static_cast<uint32_t>(row_to_batch_thread_num));
    context_builder.WithMemoryPool(_memory_pool);
    context_builder.WithFileSystem(_paimon_file_system);

    auto context_result = context_builder.Finish();
    if (!context_result.ok()) {
        return Status::InternalError(
                fmt::format("failed to create Paimon read context: {}", context_result.status().ToString()));
    }

    auto split_result = paimon::Split::Deserialize(scan_range.paimon_split_info_binary.data(),
                                                   scan_range.paimon_split_info_binary.size(), _memory_pool);
    if (!split_result.ok()) {
        return Status::InternalError(
                fmt::format("failed to deserialize Paimon split: {}", split_result.status().ToString()));
    }

    auto table_read_result = paimon::TableRead::Create(std::move(context_result).value());
    if (!table_read_result.ok()) {
        return Status::InternalError(
                fmt::format("failed to create Paimon table reader: {}", table_read_result.status().ToString()));
    }
    auto reader_result = table_read_result.value()->CreateReader(split_result.value());
    if (!reader_result.ok()) {
        return Status::InternalError(
                fmt::format("failed to create Paimon split reader: {}", reader_result.status().ToString()));
    }
    _reader = std::move(reader_result).value();
    _read_chunk = std::make_shared<Chunk>();
    _chunk_filter.reserve(_max_chunk_size);
    return Status::OK();
}

Status PaimonNativeReader::get_next(ChunkPtr* chunk) {
    _read_chunk->reset();
    _chunk_filter.clear();
    _chunk_start_idx = 0;

    while (_chunk_start_idx < _max_chunk_size) {
        if (_batch_is_exhausted()) {
            RETURN_IF_CANCELLED(_runtime_state);
            RETURN_IF_ERROR(_next_batch());
            if (_scanner_eof) {
                break;
            }
        }
        RETURN_IF_ERROR(_append_batch_to_chunk());
    }

    if (_chunk_start_idx == 0 && _scanner_eof) {
        return Status::EndOfFile("Paimon reader reached end of split");
    }
    return _fill_output_chunk(chunk);
}

void PaimonNativeReader::close() noexcept {
    if (_closed) {
        return;
    }
    _closed = true;

    // Imported Arrow batches can retain release callbacks backed by the reader's
    // arena, so release them before closing the BatchReader.
    _arrow_batch.reset();
    if (_reader != nullptr) {
        _reader->Close();
        _reader_metrics = _reader->GetReaderMetrics();
        _reader.reset();
    }
}

Status PaimonNativeReader::_next_batch() {
    SCOPED_RAW_TIMER(&_app_stats->column_read_ns);
    SCOPED_RAW_TIMER(&_app_stats->io_ns);
    ++_app_stats->io_count;
    _arrow_batch.reset();
    _batch_start_idx = 0;

    auto batch_result = _reader->NextBatch();
    if (!batch_result.ok()) {
        return Status::InternalError(
                fmt::format("Paimon reader failed to read next batch: {}", batch_result.status().ToString()));
    }
    auto batch = std::move(batch_result).value();
    if (paimon::BatchReader::IsEofBatch(batch)) {
        _scanner_eof = true;
        return Status::OK();
    }

    auto arrow_result = arrow::ImportRecordBatch(batch.first.get(), batch.second.get());
    if (!arrow_result.ok()) {
        return Status::InternalError(
                fmt::format("failed to import Paimon Arrow batch: {}", arrow_result.status().ToString()));
    }
    _arrow_batch = std::move(arrow_result).ValueOrDie();
    if (!_converters_initialized) {
        RETURN_IF_ERROR(_initialize_converters());
    }
    return Status::OK();
}

Status PaimonNativeReader::_initialize_converters() {
    DCHECK(_arrow_batch != nullptr);
    DCHECK_EQ(_read_chunk->num_columns(), 0);
    const auto& materialized_columns = _scanner_ctx.format_scan_context.materialized_columns;
    for (size_t i = 0; i < materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = materialized_columns[i].slot_desc;
        MutableColumnPtr column;
        const auto arrow_column = _arrow_batch->GetColumnByName(std::string(materialized_columns[i].name()));
        if (arrow_column == nullptr) {
            column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            _cast_exprs[i] = _pool.add(new ColumnRef(slot_desc));
        } else {
            RETURN_IF_ERROR(create_arrow_column(arrow_column->type().get(), slot_desc, &column,
                                                _convert_functions[i].get(), &_cast_exprs[i], _pool, true));
        }
        column->reserve(_max_chunk_size);
        _read_chunk->append_column(std::move(column), slot_desc->id());
    }
    _converters_initialized = true;
    return Status::OK();
}

Status PaimonNativeReader::_append_batch_to_chunk() {
    DCHECK(_arrow_batch != nullptr);
    SCOPED_RAW_TIMER(&_app_stats->column_convert_ns);
    const size_t num_rows =
            std::min<int64_t>(_max_chunk_size - _chunk_start_idx, _arrow_batch->num_rows() - _batch_start_idx);
    _chunk_filter.resize(_chunk_filter.size() + num_rows, 1);

    const auto& materialized_columns = _scanner_ctx.format_scan_context.materialized_columns;
    for (size_t i = 0; i < materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = materialized_columns[i].slot_desc;
        _convert_context.set_current_column(slot_desc->col_name(), slot_desc->type());
        Column* column = _read_chunk->get_column_raw_ptr_by_slot_id(slot_desc->id());
        const auto arrow_column = _arrow_batch->GetColumnByName(std::string(materialized_columns[i].name()));
        if (arrow_column == nullptr) {
            if (!column->append_nulls(num_rows)) {
                return Status::InvalidArgument(
                        fmt::format("Paimon batch is missing non-nullable column {}", slot_desc->col_name()));
            }
            continue;
        }
        RETURN_IF_ERROR(convert_arrow_array_to_column(_convert_functions[i].get(), num_rows, arrow_column.get(), column,
                                                      _batch_start_idx, _chunk_start_idx, &_chunk_filter,
                                                      &_convert_context));
    }

    _batch_start_idx += num_rows;
    _chunk_start_idx += num_rows;
    _app_stats->raw_rows_read += num_rows;
    return Status::OK();
}

Status PaimonNativeReader::_fill_output_chunk(ChunkPtr* chunk) {
    const auto& materialized_columns = _scanner_ctx.format_scan_context.materialized_columns;
    const size_t rows_before_filter = materialized_columns.empty() ? _chunk_start_idx : _read_chunk->num_rows();
    size_t row_count = rows_before_filter;
    if (_read_chunk->num_columns() > 0) {
        _read_chunk->filter(_chunk_filter);
        row_count = _read_chunk->num_rows();
    }
    _app_stats->late_materialize_skip_rows += rows_before_filter - row_count;

    ChunkPtr output = std::make_shared<Chunk>();
    for (size_t i = 0; i < materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = materialized_columns[i].slot_desc;
        auto output_column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        output_column->reserve(row_count);
        output->append_column(std::move(output_column), slot_desc->id());

        ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, _read_chunk.get()));
        column = ColumnHelper::unfold_const_column(slot_desc->type(), row_count, column);
        // A passthrough ColumnRef aliases the reusable staging column. Move its
        // data into the output so the next reset cannot clear an in-flight chunk.
        output->get_column_by_slot_id(slot_desc->id())->swap_column(*column);
    }
    output->set_num_rows(row_count);
    *chunk = std::move(output);
    return Status::OK();
}

bool PaimonNativeReader::_batch_is_exhausted() const {
    return _scanner_eof || _arrow_batch == nullptr || _batch_start_idx >= _arrow_batch->num_rows();
}

} // namespace starrocks
