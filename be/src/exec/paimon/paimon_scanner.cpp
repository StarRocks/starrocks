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

#include "exec/paimon/paimon_scanner.h"

#include <fs/paimon/paimon_file_system.h>

#include <memory>

#include "arrow/abi.h"
#include "arrow/c/bridge.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/csv_scanner.h"
#include "exec/orc_scanner.h"
#include "exec/parquet_scanner.h"
#include "fs/fs_broker.h"
#include "gutil/strings/substitute.h"
#include "io/compressed_input_stream.h"
#include "paimon/executor.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/table/source/data_split.h"
#include "paimon/table/source/table_read.h"
#include "paimon_evaluator.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/load_stream_mgr.h"

namespace starrocks {

Status PaimonScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return Status::OK();
}

void PaimonScanner::do_update_counter(HdfsScanProfile* profile) {}

Status PaimonScanner::do_open(RuntimeState* runtime_state) {
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    _max_batch_rows = 10000;
    _max_chunk_size = runtime_state->chunk_size() ? runtime_state->chunk_size() : 4096;

    const auto pool = GetDefaultPool();

    const auto deserialize_result = DataSplit::Deserialize(_scanner_params.paimon_split_info.data(),
                                                           _scanner_params.paimon_split_info.size(), pool);
    if (UNLIKELY(!deserialize_result.ok())) {
        return Status::InternalError(
                fmt::format("paimon::DataSplit::Deserialize failed : {}", deserialize_result.status().ToString()));
    }

    ReadContextBuilder read_context_builder(_scanner_params.paimon_table_path, _scanner_params.paimon_schema_id);
    std::vector<std::string> field_names;
    for (size_t i = 0; i < _scanner_ctx.materialized_columns.size(); i++) {
        auto materialized_column = _scanner_ctx.materialized_columns[i];
        field_names.emplace_back(materialized_column.name());
        _conv_funcs.emplace_back(std::make_unique<ConvertFuncTree>());
    }
    read_context_builder.SetReadSchema(field_names);
    read_context_builder.AddOption(PaimonOptions::ROOT_PATH, _scanner_params.paimon_table_path);
    read_context_builder.AddOption(Options::FILE_SYSTEM, PaimonFileSystemFactory::IDENTIFIER);
    read_context_builder.AddOption(Options::READ_BATCH_SIZE, std::to_string(_max_batch_rows));
    read_context_builder.EnablePrefetch(true);
    read_context_builder.EnableMultiThreadRowToBatch(true);
    read_context_builder.SetRowToBatchThreadNumber(2);

    std::vector<Expr*> conjuncts{};
    for (const auto& it : _scanner_params.conjunct_ctxs_by_slot) {
        for (const auto& it2 : it.second) {
            conjuncts.push_back(it2->root());
        }
    }
    // if no conjuncts, skip read index file
    if (!conjuncts.empty()) {
        PaimonEvaluator evaluator(_scanner_params.tuple_desc->slots());
        const auto predicate = evaluator.evaluate(&conjuncts);
        if (predicate != nullptr) {
            VLOG(10) << "Paimon scanner predicate : " << predicate->ToString();
            read_context_builder.SetPredicate(predicate);
        }
    }

    auto read_builder_result = read_context_builder.Finish();
    if (UNLIKELY(!read_builder_result.ok())) {
        return Status::InternalError(
                fmt::format("Paimon ReadContextBuilder error : {}", read_builder_result.status().ToString()));
    }
    auto create_result =
            paimon::TableRead::Create(std::move(read_builder_result).value(), pool, CreateDefaultExecutor());
    if (UNLIKELY(!create_result.ok())) {
        return Status::InternalError(
                fmt::format("Paimon SplitRead::Create error : {}", create_result.status().ToString()));
    }
    auto create_reader_result = create_result.value()->CreateReader(deserialize_result.value());
    if (UNLIKELY(!create_reader_result.ok())) {
        return Status::InternalError(
                fmt::format("Paimon CreateReader error : {}", create_reader_result.status().ToString()));
    }
    _reader = std::move(create_reader_result).value();

    for (auto i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
        _conv_funcs.emplace_back(std::make_unique<ConvertFuncTree>());
    }
    _cast_exprs.resize(_scanner_ctx.materialized_columns.size(), nullptr);
    _conv_ctx.state = runtime_state;
    _chunk_filter.reserve(0);
    _batch_start_idx = 0;
    _chunk_start_idx = 0;
    _scanner_eof = false;

    return Status::OK();
}

bool PaimonScanner::chunk_is_full() const {
    return _chunk_start_idx >= _max_chunk_size;
}
bool PaimonScanner::batch_is_exhausted() const {
    return _scanner_eof || _arrow_batch == nullptr || _batch_start_idx >= _arrow_batch->num_rows();
}

Status PaimonScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (batch_is_exhausted()) {
        while (true) {
            Status status = next_batch();
            if (_scanner_eof) {
                return status;
            }
            if (status.ok()) {
                break;
            }
            if (status.is_end_of_file()) {
                continue;
            }
            return status;
        }
    }
    RETURN_IF_ERROR(initialize_src_chunk(chunk));
    while (!_scanner_eof) {
        RETURN_IF_ERROR(append_batch_to_src_chunk(chunk));
        if (chunk_is_full()) {
            break;
        }
        auto status = next_batch();
        // obtain next batch if current batch is ok
        if (status.ok()) {
            continue;
        }
        // just return error except end of file.
        if (!status.is_end_of_file()) {
            return status;
        }

        // process end of file
        // if chunk is not empty, then just break the loop and finalize the chunk
        // so always empty chunks read data from files
        if (chunk->get()->num_rows() > 0) {
            break;
        }
        // the chunk is empty and the file reach its end, this situation happens when
        // size of file is nonzero but the file only contains no real data.
        // switch to next file and re-initialize src chunk.
        RETURN_IF_ERROR(next_batch());
        RETURN_IF_ERROR(initialize_src_chunk(chunk));
    }
    RETURN_IF_ERROR(finalize_src_chunk(chunk));
    RETURN_IF_ERROR(_scanner_ctx.append_or_update_not_existed_columns_to_chunk(chunk, _chunk_start_idx));
    RETURN_IF_ERROR(_scanner_ctx.evaluate_on_conjunct_ctxs_by_slot(chunk, &_chunk_filter));
    _scanner_ctx.append_or_update_partition_column_to_chunk(chunk, _chunk_start_idx);
    _chunk_start_idx = 0;
    return Status::OK();
}

void PaimonScanner::do_close(RuntimeState* runtime_state) noexcept {
    _pool.clear();
}

Status PaimonScanner::next_batch() {
    SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
    SCOPED_RAW_TIMER(&_app_stats.io_ns);
    _app_stats.io_count += 1;
    _batch_start_idx = 0;
    auto status = _reader.get()->NextBatch();
    if (status.ok()) {
        auto& c_array = status.value().first;
        auto& c_schema = status.value().second;
        if (!c_array) {
            _scanner_eof = true;
            return Status::EndOfFile("no data");
        }
        auto arrow_batch_result = arrow::ImportRecordBatch(c_array.get(), c_schema.get());
        if (UNLIKELY(!arrow_batch_result.ok())) {
            return Status::InternalError(
                    fmt::format("Arrow ImportRecordBatch : {}", arrow_batch_result.status().ToString()));
        }
        _arrow_batch = arrow_batch_result.ValueOrDie();
        return Status::OK();
    }
    if (UNLIKELY(!status.ok())) {
        return Status::InternalError(fmt::format("Paimon NextBatch error : {}", status.status().ToString()));
    }
    _scanner_eof = true;
    return Status::EndOfFile("no data");
}

Status PaimonScanner::initialize_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    _pool.clear();
    (*chunk) = std::make_shared<Chunk>();
    size_t column_pos = 0;
    _chunk_filter.clear();
    for (auto i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[i].slot_desc;
        if (slot_desc == nullptr) {
            continue;
        }
        auto* array = _arrow_batch->column(column_pos++).get();
        ColumnPtr column;
        RETURN_IF_ERROR(ParquetScanner::new_column(array->type().get(), slot_desc, &column, _conv_funcs[i].get(),
                                                   &_cast_exprs[i], _pool, true));
        column->reserve(_max_chunk_size);
        (*chunk)->append_column(column, slot_desc->id());
    }
    return Status::OK();
}

Status PaimonScanner ::append_batch_to_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_app_stats.column_convert_ns);
    size_t num_elements =
            std::min<size_t>((_max_chunk_size - _chunk_start_idx), (_arrow_batch->num_rows() - _batch_start_idx));
    size_t column_pos = 0;
    _chunk_filter.resize(_chunk_filter.size() + num_elements, 1);
    for (auto i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[i].slot_desc;
        if (slot_desc == nullptr) {
            continue;
        }
        _conv_ctx.current_slot = slot_desc;
        auto* array = _arrow_batch->column(column_pos++).get();
        auto& column = (*chunk)->get_column_by_slot_id(slot_desc->id());
        RETURN_IF_ERROR(ParquetScanner::convert_array_to_column(_conv_funcs[i].get(), num_elements, array, column,
                                                                _batch_start_idx, _chunk_start_idx, &_chunk_filter,
                                                                &_conv_ctx));
    }
    _chunk_start_idx += num_elements;
    _batch_start_idx += num_elements;
    _app_stats.raw_rows_read += num_elements;
    return Status::OK();
}

Status PaimonScanner::finalize_src_chunk(ChunkPtr* chunk) {
    auto num_rows = (*chunk)->filter(_chunk_filter);
    _app_stats.late_materialize_skip_rows += _chunk_start_idx - num_rows;
    ChunkPtr cast_chunk = std::make_shared<Chunk>();
    {
        SCOPED_RAW_TIMER(&_app_stats.cast_chunk_ns);
        for (auto i = 0; i < _scanner_ctx.materialized_columns.size(); ++i) {
            SlotDescriptor* slot_desc = _scanner_ctx.materialized_columns[i].slot_desc;
            if (slot_desc == nullptr) {
                continue;
            }
            ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, (*chunk).get()));
            column = ColumnHelper::unfold_const_column(slot_desc->type(), (*chunk)->num_rows(), column);
            cast_chunk->append_column(column, slot_desc->id());
        }
    }
    *chunk = cast_chunk;
    return Status::OK();
}

} // namespace starrocks
