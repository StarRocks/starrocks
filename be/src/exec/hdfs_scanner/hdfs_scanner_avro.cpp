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

#include "exec/hdfs_scanner/hdfs_scanner_avro.h"

#include <fmt/format.h>

#include "base/time/timezone_utils.h"
#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config_scan_io_fwd.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "runtime/runtime_state.h"

namespace starrocks {

static const std::string kAvroProfileSectionPrefix = "Avro";

Status HdfsAvroScanner::do_init(RuntimeState* state, const HdfsScannerParams& /*params*/) {
    if (!TimezoneUtils::find_cctz_time_zone(state->timezone(), _timezone)) {
        return Status::InvalidArgument(fmt::format("Cannot find cctz time zone: {}", state->timezone()));
    }
    return Status::OK();
}

Status HdfsAvroScanner::do_open(RuntimeState* state) {
    // _scanner_ctx is populated by _build_scanner_context() before do_open() is called,
    // so materialized_columns is available here (not in do_init).
    //
    // NOTE: unlike ORC/Text scanners we do NOT call _scanner_ctx.update_materialized_columns()
    // here because the Avro writer schema is not yet available at this point — the file must
    // be opened first to read its embedded schema.  Consequences:
    //   1. Columns absent from the Avro file are handled by AvroReader (col_not_found_as_null=true),
    //      which fills them with NULL.  This is functionally correct for typical Hive Avro tables
    //      since Hive Avro schema evolution is uncommon and column defaults are rarely configured.
    //   2. materialize_slot_default_values is ignored — missing columns always get NULL rather than
    //      the configured default value.
    //   3. should_skip_by_evaluating_not_existed_slots() short-circuit is not applied.
    // TODO: open the file, extract the writer schema field-name set, then call
    // _scanner_ctx.update_materialized_columns() before building _column_readers to fully
    // support schema evolution defaults and not-existed-slot predicate short-circuiting.
    for (const auto& col : _scanner_ctx.materialized_columns) {
        _materialize_slot_descs.push_back(col.slot_desc);
        _column_readers.push_back(avrocpp::ColumnReader::get_nullable_column_reader(
                col.slot_desc->col_name(), col.slot_desc->type(), _timezone, /*null_as_error=*/false));
    }

    RETURN_IF_ERROR(open_random_access_file());
    auto input_stream = std::make_unique<AvroBufferInputStream>(_file.get(), config::avro_reader_buffer_size_bytes,
                                                                &_scanner_counter);

    // Extract split boundaries from the scan range so the reader honours split-level
    // parallelism.  FE marks AVRO as splittable; without these boundaries every split
    // scanner would read the whole file and produce duplicated rows.
    int64_t split_offset = 0;
    int64_t split_length = 0;
    if (_scanner_params.scan_range != nullptr) {
        split_offset = _scanner_params.scan_range->offset;
        split_length = _scanner_params.scan_range->length;
    }

    _avro_reader = std::make_unique<AvroReader>();
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    return _avro_reader->init(std::move(input_stream), _scanner_params.path, state, &_scanner_counter,
                              &_materialize_slot_descs, &_column_readers,
                              /*col_not_found_as_null=*/true, _file.get(), config::avro_reader_buffer_size_bytes,
                              split_offset, split_length, _scanner_params.avro_schema_json,
                              /*invalid_as_null=*/false, /*allow_direct_path=*/true);
}

Status HdfsAvroScanner::do_get_next(RuntimeState* state, ChunkPtr* chunk) {
    CHECK(chunk != nullptr);
    ChunkPtr& ck = *chunk;

    // Build a temporary chunk with only the materialized (Avro) columns.
    auto avro_chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); i++) {
        const auto& col_info = _scanner_ctx.materialized_columns[i];
        auto column = ColumnHelper::create_column(col_info.slot_desc->type(), true, false, 0, true);
        avro_chunk->append_column(std::move(column), col_info.slot_desc->id());
    }

    int64_t row_count = 0;
    Status st;
    {
        SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
        st = _avro_reader->read_chunk(avro_chunk, state->chunk_size(), &row_count);
    }
    if (!st.ok() && !st.is_end_of_file()) {
        return st;
    }
    if (row_count == 0) {
        return Status::EndOfFile("no more avro data");
    }

    _app_stats.raw_rows_read += row_count;

    // Merge materialized columns into the output chunk.
    if (!avro_chunk->is_empty()) {
        // Convert AdaptiveNullableColumn → NullableColumn.
        {
            SCOPED_RAW_TIMER(&_app_stats.column_convert_ns);
            _materialize_nullable_columns(avro_chunk);
            for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); i++) {
                const auto& col_info = _scanner_ctx.materialized_columns[i];
                ck->append_or_update_column(std::move(avro_chunk->get_column_by_slot_id(col_info.slot_desc->id())),
                                            col_info.slot_desc->id());
            }
        }
    }
    // Fill not-existed slots: fills the ___count___ column (count queries) and any
    // schema-evolution columns absent from this file. Also calls ck->set_num_rows().
    RETURN_IF_ERROR(_scanner_ctx.append_or_update_not_existed_columns_to_chunk(&ck, row_count));

    // Fill partition and extended constant columns.
    _scanner_ctx.append_or_update_partition_column_to_chunk(&ck, row_count);
    _scanner_ctx.append_or_update_extended_column_to_chunk(&ck, row_count);

    // Post-read row-level conjunct evaluation (Avro has no block statistics for pushdown).
    for (auto& it : _scanner_ctx.conjunct_ctxs_by_slot) {
        SCOPED_RAW_TIMER(&_app_stats.expr_filter_ns);
        RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(it.second, ck.get()));
        if (ck->num_rows() == 0) {
            break;
        }
    }

    // Note: _app_stats.rows_read is updated by the base class HdfsScanner::get_next
    // after do_get_next returns. Do NOT update it here to avoid double-counting.
    return Status::OK();
}

void HdfsAvroScanner::do_update_counter(HdfsScanProfile* profile) {
    RuntimeProfile* root = profile->runtime_profile;
    ADD_COUNTER(root, kAvroProfileSectionPrefix, TUnit::NONE);

    auto* direct_path_used = ADD_CHILD_COUNTER(root, "DirectPathUsed", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* rows_decoded = ADD_CHILD_COUNTER(root, "RowsDecoded", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_rows_decoded = ADD_CHILD_COUNTER(root, "DirectRowsDecoded", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* generic_rows_decoded = ADD_CHILD_COUNTER(root, "GenericRowsDecoded", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* block_count_rows = ADD_CHILD_COUNTER(root, "BlockCountRows", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_plan_entries = ADD_CHILD_COUNTER(root, "DirectPlanEntries", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_read_entries = ADD_CHILD_COUNTER(root, "DirectReadEntries", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_skip_entries = ADD_CHILD_COUNTER(root, "DirectSkipEntries", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_fast_skip_entries =
            ADD_CHILD_COUNTER(root, "DirectFastSkipEntries", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_fallback_skip_entries =
            ADD_CHILD_COUNTER(root, "DirectFallbackSkipEntries", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_read_field_calls =
            ADD_CHILD_COUNTER(root, "DirectReadFieldCalls", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_skip_field_calls =
            ADD_CHILD_COUNTER(root, "DirectSkipFieldCalls", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_fast_skip_calls =
            ADD_CHILD_COUNTER(root, "DirectFastSkipCalls", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* direct_fallback_skip_calls =
            ADD_CHILD_COUNTER(root, "DirectFallbackSkipCalls", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* input_stream_read_count =
            ADD_CHILD_COUNTER(root, "InputStreamReadCount", TUnit::UNIT, kAvroProfileSectionPrefix);
    auto* input_stream_read_timer = ADD_CHILD_TIMER(root, "InputStreamReadTime", kAvroProfileSectionPrefix);

    if (_avro_reader == nullptr) {
        COUNTER_UPDATE(input_stream_read_count, _scanner_counter.file_read_count);
        COUNTER_UPDATE(input_stream_read_timer, _scanner_counter.file_read_ns);
        return;
    }

    const auto& avro_stats = _avro_reader->stats();
    COUNTER_UPDATE(direct_path_used, avro_stats.direct_path_used);
    COUNTER_UPDATE(rows_decoded, avro_stats.rows_decoded);
    COUNTER_UPDATE(direct_rows_decoded, avro_stats.direct_rows_decoded);
    COUNTER_UPDATE(generic_rows_decoded, avro_stats.generic_rows_decoded);
    COUNTER_UPDATE(block_count_rows, avro_stats.block_count_rows);
    COUNTER_UPDATE(direct_plan_entries, avro_stats.direct_plan_entries);
    COUNTER_UPDATE(direct_read_entries, avro_stats.direct_read_entries);
    COUNTER_UPDATE(direct_skip_entries, avro_stats.direct_skip_entries);
    COUNTER_UPDATE(direct_fast_skip_entries, avro_stats.direct_fast_skip_entries);
    COUNTER_UPDATE(direct_fallback_skip_entries, avro_stats.direct_fallback_skip_entries);
    // Derived totals: total field-level calls = rows_decoded × per-plan entry count.
    COUNTER_UPDATE(direct_read_field_calls, avro_stats.direct_rows_decoded * avro_stats.direct_read_entries);
    COUNTER_UPDATE(direct_skip_field_calls, avro_stats.direct_rows_decoded * avro_stats.direct_skip_entries);
    COUNTER_UPDATE(direct_fast_skip_calls, avro_stats.direct_rows_decoded * avro_stats.direct_fast_skip_entries);
    COUNTER_UPDATE(direct_fallback_skip_calls,
                   avro_stats.direct_rows_decoded * avro_stats.direct_fallback_skip_entries);
    COUNTER_UPDATE(input_stream_read_count, _scanner_counter.file_read_count);
    COUNTER_UPDATE(input_stream_read_timer, _scanner_counter.file_read_ns);
}

void HdfsAvroScanner::do_close(RuntimeState* /*state*/) noexcept {
    _avro_reader.reset();
}

void HdfsAvroScanner::_materialize_nullable_columns(ChunkPtr& chunk) {
    chunk->materialized_nullable();
    for (int i = 0; i < chunk->num_columns(); i++) {
        auto* adaptive = down_cast<AdaptiveNullableColumn*>(chunk->get_column_raw_ptr_by_index(i));
        chunk->update_column_by_index(NullableColumn::create(adaptive->materialized_raw_data_column(),
                                                             adaptive->materialized_raw_null_column()),
                                      i);
    }
}

} // namespace starrocks
