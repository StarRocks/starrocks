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

#include "connector/hive/paimon/paimon_scanner.h"

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
constexpr int64_t kPaimonParquetCacheHoleSizeLimit = 4L * 1024 * 1024;
constexpr int64_t kPaimonParquetCacheRangeSizeLimit = 32L * 1024 * 1024;
constexpr int64_t kPaimonParquetBitmapCoalesceHoleSizeLimit = 32;
constexpr std::string_view kPaimonParquetBitmapRefiningStrategy = "coalesce";

void update_paimon_io_profile(RuntimeProfile* profile, const PaimonFileSystemStats::Snapshot& io_stats) {
    const std::string paimon_fs_section = "PaimonFileSystem";
    ADD_COUNTER(profile, paimon_fs_section, TUnit::NONE);
    auto update_counter = [&](const char* name, TUnit::type unit, int64_t value) {
        auto* counter = ADD_CHILD_COUNTER(profile, name, unit, paimon_fs_section);
        COUNTER_UPDATE(counter, value);
    };
    update_counter("PaimonFSAppIOCount", TUnit::UNIT, io_stats.app_io_count());
    update_counter("PaimonFSAppIOBytes", TUnit::BYTES, io_stats.app_io_bytes());
    update_counter("PaimonFSAppIOTime", TUnit::TIME_NS, io_stats.app_io_ns());
    update_counter("PaimonFSIOCount", TUnit::UNIT, io_stats.fs_io_count);
    update_counter("PaimonFSIOBytes", TUnit::BYTES, io_stats.fs_io_bytes);
    update_counter("PaimonFSIOTime", TUnit::TIME_NS, io_stats.fs_io_ns);
    update_counter("PaimonFSSequentialReadCount", TUnit::UNIT, io_stats.sequential_read_count);
    update_counter("PaimonFSSequentialReadBytes", TUnit::BYTES, io_stats.sequential_read_bytes);
    update_counter("PaimonFSSequentialReadTime", TUnit::TIME_NS, io_stats.sequential_read_ns);
    update_counter("PaimonFSPositionalReadCount", TUnit::UNIT, io_stats.positional_read_count);
    update_counter("PaimonFSPositionalReadBytes", TUnit::BYTES, io_stats.positional_read_bytes);
    update_counter("PaimonFSPositionalReadTime", TUnit::TIME_NS, io_stats.positional_read_ns);
    update_counter("PaimonFSAsyncReadCount", TUnit::UNIT, io_stats.async_read_count);
    update_counter("PaimonFSAsyncReadBytes", TUnit::BYTES, io_stats.async_read_bytes);
    update_counter("PaimonFSAsyncReadTime", TUnit::TIME_NS, io_stats.async_read_ns);
}

} // namespace

PaimonScanner::~PaimonScanner() {
    _close_reader();
}

Status PaimonScanner::do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) {
    _max_chunk_size = runtime_state->chunk_size() > 0 ? runtime_state->chunk_size() : 4096;
    _convert_context.timezone = runtime_state->timezone();
    _convert_context.current_file = scanner_ctx.file_path;
    return Status::OK();
}

Status PaimonScanner::do_open(RuntimeState* runtime_state) {
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    const THdfsScanRange& scan_range = *_scanner_ctx->scan_range;
    const auto* table_descriptor = dynamic_cast<const PaimonTableDescriptor*>(_scanner_ctx->hive_table);
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
    if (_scanner_ctx->fs == nullptr) {
        return Status::InternalError("Paimon native scanner has no StarRocks file system");
    }

    const auto& query_options = runtime_state->query_options();
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

    _memory_pool = std::make_shared<TrackedPaimonMemoryPool>(runtime_state->query_mem_tracker_ptr().get());
    _paimon_file_system = std::make_shared<PaimonFileSystem>(_scanner_ctx->fs, _scanner_ctx->datacache_options);

    const auto& materialized_columns = _scanner_ctx->format_scan_context.materialized_columns;
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
    for (const auto& entry : _scanner_ctx->format_scan_context.conjunct_ctxs_by_slot) {
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

Status PaimonScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    _read_chunk->reset();
    _chunk_filter.clear();
    _chunk_start_idx = 0;
    while (_chunk_start_idx < _max_chunk_size) {
        if (_batch_is_exhausted()) {
            RETURN_IF_CANCELLED(runtime_state);
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
    return _finish_chunk(chunk);
}

void PaimonScanner::do_prepare_close() noexcept {
    _close_reader();
}

void PaimonScanner::do_close(RuntimeState*) noexcept {
    _close_reader();
    _read_chunk.reset();
    _convert_functions.clear();
    _cast_exprs.clear();
    _chunk_filter.clear();
    _reader_metrics.reset();
    _paimon_file_system.reset();
    _memory_pool.reset();
    _pool.clear();
}

void PaimonScanner::_close_reader() noexcept {
    if (_reader_closed) {
        return;
    }
    _reader_closed = true;
    _arrow_batch.reset();
    if (_reader != nullptr) {
        _reader->Close();
        _reader_metrics = _reader->GetReaderMetrics();
        _reader.reset();
    }
}

void PaimonScanner::do_update_counter(HdfsScannerProfile* profile) {
    RuntimeProfile* runtime_profile = profile->runtime_profile;
    auto metrics = _reader != nullptr ? _reader->GetReaderMetrics() : _reader_metrics;
    if (metrics != nullptr) {
        const std::string paimon_section = "PaimonNativeReader";
        ADD_COUNTER(runtime_profile, paimon_section, TUnit::NONE);
        for (const auto& [key, value] : metrics->GetAllCounters()) {
            TUnit::type unit = TUnit::UNIT;
            int64_t counter_value = static_cast<int64_t>(value);
            if (key.find("bytes") != std::string::npos) {
                unit = TUnit::BYTES;
            } else if (key.find("latency") != std::string::npos) {
                unit = TUnit::TIME_NS;
                counter_value *= 1000;
            }
            auto* counter = ADD_CHILD_COUNTER(runtime_profile, key, unit, paimon_section);
            COUNTER_UPDATE(counter, counter_value);
        }
    }
    if (_paimon_file_system == nullptr) {
        return;
    }
    const auto fs_stats = _paimon_file_system->get_stats();
    update_paimon_io_profile(runtime_profile, fs_stats);
    if (!_paimon_file_system->datacache_enabled()) {
        return;
    }

    const auto& stats = fs_stats.datacache;
    COUNTER_UPDATE(profile->datacache_read_counter, stats.read_block_cache_count);
    COUNTER_UPDATE(profile->datacache_read_bytes, stats.read_block_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_mem_bytes, stats.read_mem_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_disk_bytes, stats.read_disk_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_timer, stats.read_block_cache_ns);
    COUNTER_UPDATE(profile->datacache_read_peer_bytes, stats.read_peer_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_peer_counter, stats.read_peer_cache_count);
    COUNTER_UPDATE(profile->datacache_read_peer_timer, stats.read_peer_cache_ns);
    COUNTER_UPDATE(profile->datacache_skip_read_counter, stats.skip_read_cache_count);
    COUNTER_UPDATE(profile->datacache_skip_read_bytes, stats.skip_read_cache_bytes);
    COUNTER_UPDATE(profile->datacache_skip_read_peer_counter, stats.skip_read_peer_cache_count);
    COUNTER_UPDATE(profile->datacache_skip_read_peer_bytes, stats.skip_read_peer_cache_bytes);
    COUNTER_UPDATE(profile->datacache_write_counter, stats.write_block_cache_count);
    COUNTER_UPDATE(profile->datacache_write_bytes, stats.write_block_cache_bytes);
    COUNTER_UPDATE(profile->datacache_write_timer, stats.write_block_cache_ns);
    COUNTER_UPDATE(profile->datacache_write_fail_counter, stats.write_cache_fail_count);
    COUNTER_UPDATE(profile->datacache_write_fail_bytes, stats.write_cache_fail_bytes);
    COUNTER_UPDATE(profile->datacache_skip_write_counter, stats.skip_write_cache_count);
    COUNTER_UPDATE(profile->datacache_skip_write_bytes, stats.skip_write_cache_bytes);
    COUNTER_UPDATE(profile->datacache_read_block_buffer_counter, stats.read_block_buffer_count);
    COUNTER_UPDATE(profile->datacache_read_block_buffer_bytes, stats.read_block_buffer_bytes);

    const auto& shared_stats = fs_stats.shared_buffered;
    COUNTER_UPDATE(profile->shared_buffered_shared_io_count, shared_stats.shared_io_count);
    COUNTER_UPDATE(profile->shared_buffered_shared_io_bytes, shared_stats.shared_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_shared_align_io_bytes, shared_stats.shared_align_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_shared_io_timer, shared_stats.shared_io_timer);
    COUNTER_UPDATE(profile->shared_buffered_direct_io_count, shared_stats.direct_io_count);
    COUNTER_UPDATE(profile->shared_buffered_direct_io_bytes, shared_stats.direct_io_bytes);
    COUNTER_UPDATE(profile->shared_buffered_direct_io_timer, shared_stats.direct_io_timer);
}

Status PaimonScanner::_next_batch() {
    SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
    SCOPED_RAW_TIMER(&_app_stats.io_ns);
    ++_app_stats.io_count;
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

Status PaimonScanner::_initialize_converters() {
    DCHECK(_arrow_batch != nullptr);
    DCHECK_EQ(_read_chunk->num_columns(), 0);
    const auto& materialized_columns = _scanner_ctx->format_scan_context.materialized_columns;
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

Status PaimonScanner::_append_batch_to_chunk() {
    DCHECK(_arrow_batch != nullptr);
    SCOPED_RAW_TIMER(&_app_stats.column_convert_ns);
    const size_t num_rows =
            std::min<int64_t>(_max_chunk_size - _chunk_start_idx, _arrow_batch->num_rows() - _batch_start_idx);
    _chunk_filter.resize(_chunk_filter.size() + num_rows, 1);
    const auto& materialized_columns = _scanner_ctx->format_scan_context.materialized_columns;
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
    _app_stats.raw_rows_read += num_rows;
    return Status::OK();
}

Status PaimonScanner::_finish_chunk(ChunkPtr* chunk) {
    const auto& materialized_columns = _scanner_ctx->format_scan_context.materialized_columns;
    const size_t row_count = _read_chunk->filter(_chunk_filter);
    _app_stats.late_materialize_skip_rows += _chunk_start_idx - row_count;

    ChunkPtr output = _read_chunk->clone_empty_with_slot(_max_chunk_size);
    {
        SCOPED_RAW_TIMER(&_app_stats.cast_chunk_ns);
        for (size_t i = 0; i < materialized_columns.size(); ++i) {
            SlotDescriptor* slot_desc = materialized_columns[i].slot_desc;
            ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, _read_chunk.get()));
            column = ColumnHelper::unfold_const_column(slot_desc->type(), row_count, column);
            output->get_column_by_slot_id(slot_desc->id())->swap_column(*column);
        }
    }
    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.append_side_columns_to_chunk(&output, row_count));
    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.evaluate_all_predicates(&output));
    *chunk = std::move(output);
    return Status::OK();
}

bool PaimonScanner::_batch_is_exhausted() const {
    return _scanner_eof || _arrow_batch == nullptr || _batch_start_idx >= _arrow_batch->num_rows();
}

} // namespace starrocks
