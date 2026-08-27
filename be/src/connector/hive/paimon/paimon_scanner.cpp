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

#include "column/arrow/type_to_arrow_converter.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "connector/hive/paimon/paimon_file_system.h"
#include "connector/hive/paimon/paimon_predicate_converter.h"
#include "connector/hive/paimon/tracked_paimon_memory_pool.h"
#include "exprs/expr_context.h"
#include "formats/arrow/arrow_column_converter.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {

constexpr int64_t kPaimonReadBatchSize = 10000;
constexpr int64_t kPaimonParquetCacheHoleSizeLimit = 4L * 1024 * 1024;
constexpr int64_t kPaimonParquetCacheRangeSizeLimit = 32L * 1024 * 1024;
constexpr int64_t kPaimonParquetBitmapCoalesceHoleSizeLimit = 32;
constexpr std::string_view kPaimonParquetBitmapRefiningStrategy = "coalesce";
constexpr bool kPaimonEnablePrefetch = true;
constexpr bool kPaimonEnableMultiThreadRowToBatch = true;
constexpr uint32_t kPaimonRowToBatchThreadNum = 3;

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

PaimonScanner::~PaimonScanner() = default;

Status PaimonScanner::do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) {
    return Status::OK();
}

Status PaimonScanner::do_open(RuntimeState* runtime_state) {
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    _memory_pool = std::make_shared<TrackedPaimonMemoryPool>(runtime_state->query_mem_tracker_ptr().get());
    _max_chunk_size = runtime_state->chunk_size() ? runtime_state->chunk_size() : 4096;
    const THdfsScanRange& scan_range = *_scanner_ctx->scan_range;
    const auto* table_descriptor = dynamic_cast<const PaimonTableDescriptor*>(_scanner_ctx->hive_table);
    if (table_descriptor == nullptr || table_descriptor->get_paimon_table_path().empty()) {
        return Status::InvalidArgument("Paimon native scan is missing table path in PaimonTableDescriptor");
    }
    // Keep timestamp conversion aligned with the JNI reader, which converts
    // timezone-aware timestamps with the catalog's time zone (see jni_scanner.cpp).
    _convert_context.timezone = std::string(table_descriptor->get_time_zone());
    const std::string table_path(table_descriptor->get_paimon_table_path());
    if (!scan_range.__isset.paimon_split_info_binary || scan_range.paimon_split_info_binary.empty()) {
        return Status::InvalidArgument("Paimon native scan range is missing paimon_split_info_binary");
    }
    if (_scanner_ctx->fs == nullptr) {
        return Status::InternalError("Paimon native scanner has no StarRocks file system");
    }

    _paimon_file_system = std::make_shared<PaimonFileSystem>(_scanner_ctx->fs, _scanner_ctx->datacache_options);

    const auto& materialized_columns = _scanner_ctx->format_scan_context.materialized_columns;
    std::vector<std::string> selected_field_names;
    selected_field_names.reserve(materialized_columns.size());
    _convert_functions.reserve(materialized_columns.size());
    _cast_exprs.resize(materialized_columns.size(), nullptr);
    for (const auto& materialized_column : materialized_columns) {
        selected_field_names.emplace_back(materialized_column.name());
        _convert_functions.emplace_back(std::make_unique<ConvertFuncTree>());
    }

    paimon::ReadContextBuilder context_builder(table_path);
    context_builder.SetReadFieldNames(selected_field_names);
    if (table_descriptor->get_paimon_table_schema_json().empty()) {
        return Status::InvalidArgument("Paimon native scan table schema is empty");
    }
    context_builder.SetTableSchema(std::string(table_descriptor->get_paimon_table_schema_json()));

    std::vector<Expr*> conjuncts;
    for (const auto& entry : _scanner_ctx->format_scan_context.conjunct_ctxs_by_slot) {
        for (ExprContext* conjunct_context : entry.second) {
            conjuncts.emplace_back(conjunct_context->root());
        }
    }
    if (!conjuncts.empty()) {
        PaimonPredicateConverter converter(_scanner_ctx->tuple_desc->slots());
        auto predicate = converter.convert(&conjuncts);
        if (predicate != nullptr) {
            context_builder.SetPredicate(predicate);
        }
    }

    context_builder.AddOption(paimon::Options::READ_BATCH_SIZE, std::to_string(kPaimonReadBatchSize));
    // These option keys are defined in paimon-cpp's internal parquet_format_defs.h, which is not
    // part of its installed public headers, so they have to be spelled out as string literals here.
    context_builder.AddOption("parquet.read.cache-option.hole-size-limit",
                              std::to_string(kPaimonParquetCacheHoleSizeLimit));
    context_builder.AddOption("parquet.read.cache-option.range-size-limit",
                              std::to_string(kPaimonParquetCacheRangeSizeLimit));
    context_builder.AddOption("parquet.read.bitmap.row-range-refining-strategy",
                              std::string(kPaimonParquetBitmapRefiningStrategy));
    context_builder.AddOption("parquet.read.bitmap.coalesce-hole-size-limit",
                              std::to_string(kPaimonParquetBitmapCoalesceHoleSizeLimit));
    context_builder.EnablePrefetch(kPaimonEnablePrefetch);
    context_builder.EnableMultiThreadRowToBatch(kPaimonEnableMultiThreadRowToBatch);
    context_builder.SetRowToBatchThreadNumber(kPaimonRowToBatchThreadNum);
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
    _read_chunk_template = std::make_shared<Chunk>();
    for (size_t i = 0; i < materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = materialized_columns[i].slot_desc;
        std::shared_ptr<arrow::DataType> arrow_type;
        if (slot_desc->type().type == TYPE_DATE) {
            arrow_type = arrow::date32();
        } else if (slot_desc->type().type == TYPE_DATETIME) {
            arrow_type = arrow::timestamp(arrow::TimeUnit::MICRO);
        } else {
            RETURN_IF_ERROR(convert_to_arrow_type(slot_desc->type(), &arrow_type));
        }
        MutableColumnPtr column;
        RETURN_IF_ERROR(create_arrow_column(arrow_type.get(), slot_desc, &column, _convert_functions[i].get(),
                                            &_cast_exprs[i], _pool, true));
        _read_chunk_template->append_column(std::move(column), slot_desc->id());
    }
    _arrow_record_batch_start_idx = 0;
    _scanner_eof = false;
    return Status::OK();
}

Status PaimonScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_scanner_eof) {
        return Status::EndOfFile("no data");
    }
    if (_arrow_record_batch_is_exhausted()) {
        // Nothing is buffered yet, so an EndOfFile here propagates to the caller as-is.
        RETURN_IF_ERROR(_next_arrow_record_batch());
    }
    // Downstream operators keep returned chunks and their columns alive across calls, so
    // every call materializes into a freshly cloned chunk; _read_chunk_template never holds data.
    *chunk = _read_chunk_template->clone_empty_with_slot(_max_chunk_size);
    // Emit min(_max_chunk_size, remaining rows of the current batch) rows; a chunk
    // smaller than _max_chunk_size at a batch tail is fine.
    RETURN_IF_ERROR(_append_arrow_record_batch_to_chunk(*chunk));
    RETURN_IF_ERROR(_fill_dst_chunk(*chunk));
    // Unlike other hive-family scanners there is no need to fill not-existed or partition
    // columns here: schema evolution is resolved inside paimon-cpp, and paimon partition
    // columns are materialized by the reader as regular data columns.
    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.evaluate_on_conjunct_ctxs_by_slot(chunk, &_conjunct_filter));
    _app_stats.rows_read += (*chunk)->num_rows();
    return Status::OK();
}

void PaimonScanner::do_close(RuntimeState*) noexcept {
    _arrow_record_batch.reset();
    if (_reader != nullptr) {
        _reader->Close();
        _reader.reset();
    }
    _read_chunk_template.reset();
    _convert_functions.clear();
    _cast_exprs.clear();
    _chunk_filter.clear();
    _conjunct_filter.clear();
    _paimon_file_system.reset();
    _memory_pool.reset();
    _pool.clear();
}

void PaimonScanner::do_update_counter(HdfsScannerProfile* profile) {
    RuntimeProfile* runtime_profile = profile->runtime_profile;
    auto metrics = _reader != nullptr ? _reader->GetReaderMetrics() : nullptr;
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
            COUNTER_SET(counter, counter_value);
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

Status PaimonScanner::_next_arrow_record_batch() {
    SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
    _arrow_record_batch_start_idx = 0;
    auto batch_result = _reader->NextBatch();
    if (!batch_result.ok()) {
        return Status::InternalError(
                fmt::format("Paimon reader failed to read next batch: {}", batch_result.status().ToString()));
    }
    auto& batch = batch_result.value();
    if (paimon::BatchReader::IsEofBatch(batch)) {
        _scanner_eof = true;
        return Status::EndOfFile("no data");
    }
    auto arrow_result = arrow::ImportRecordBatch(batch.first.get(), batch.second.get());
    if (!arrow_result.ok()) {
        return Status::InternalError(
                fmt::format("failed to import Paimon Arrow batch: {}", arrow_result.status().ToString()));
    }
    _arrow_record_batch = std::move(arrow_result).ValueOrDie();
    return Status::OK();
}

Status PaimonScanner::_append_arrow_record_batch_to_chunk(ChunkPtr& chunk) {
    DCHECK(_arrow_record_batch != nullptr);
    SCOPED_RAW_TIMER(&_app_stats.column_convert_ns);
    const size_t num_rows =
            std::min<int64_t>(_max_chunk_size, _arrow_record_batch->num_rows() - _arrow_record_batch_start_idx);
    _chunk_filter.assign(num_rows, 1);
    const auto& materialized_columns = _scanner_ctx->format_scan_context.materialized_columns;
    for (size_t i = 0; i < materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = materialized_columns[i].slot_desc;
        _convert_context.set_current_column(slot_desc->col_name(), slot_desc->type());
        Column* column = chunk->get_column_raw_ptr_by_slot_id(slot_desc->id());
        const auto arrow_column = _arrow_record_batch->GetColumnByName(std::string(materialized_columns[i].name()));
        RETURN_IF_ERROR(convert_arrow_array_to_column(_convert_functions[i].get(), num_rows, arrow_column.get(), column,
                                                      _arrow_record_batch_start_idx, /*chunk_start_idx=*/0,
                                                      &_chunk_filter, &_convert_context));
    }
    _arrow_record_batch_start_idx += num_rows;
    _app_stats.raw_rows_read += num_rows;
    return Status::OK();
}

Status PaimonScanner::_fill_dst_chunk(ChunkPtr& chunk) {
    const auto& materialized_columns = _scanner_ctx->format_scan_context.materialized_columns;
    const size_t row_count = chunk->filter(_chunk_filter);
    {
        SCOPED_RAW_TIMER(&_app_stats.column_convert_ns);
        for (size_t i = 0; i < materialized_columns.size(); ++i) {
            SlotDescriptor* slot_desc = materialized_columns[i].slot_desc;
            // Each cast only reads its own slot's raw column, so replacing the slot's
            // column right after evaluating it keeps the conversion in place.
            ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, chunk.get()));
            column = ColumnHelper::unfold_const_column(slot_desc->type(), row_count, column);
            chunk->get_column_by_slot_id(slot_desc->id()) = std::move(column);
        }
    }
    return Status::OK();
}

bool PaimonScanner::_arrow_record_batch_is_exhausted() const {
    return _arrow_record_batch == nullptr || _arrow_record_batch_start_idx >= _arrow_record_batch->num_rows();
}

} // namespace starrocks
