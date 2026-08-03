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

#include <algorithm>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "connector/hive/paimon/paimon_evaluator.h"
#include "connector/hive/paimon/paimon_file_system.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "paimon/defs.h"
#include "paimon/read_context.h"
#include "paimon/table/source/split.h"
#include "paimon/table/source/table_read.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors_ext.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {

constexpr int64_t kPaimonReadBatchSize = 10000;

class TrackedPaimonMemoryPool final : public paimon::MemoryPool {
public:
    explicit TrackedPaimonMemoryPool(MemTracker* tracker) : _tracker(tracker), _delegate(paimon::GetDefaultPool()) {}

    void* Malloc(uint64_t size, uint64_t alignment = 0) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        return _delegate->Malloc(size, alignment);
    }

    void* Realloc(void* pointer, size_t old_size, size_t new_size, uint64_t alignment = 0) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        return _delegate->Realloc(pointer, old_size, new_size, alignment);
    }

    void Free(void* pointer, uint64_t size) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        _delegate->Free(pointer, size);
    }

    void Free(void* pointer, uint64_t size, uint64_t alignment) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        _delegate->Free(pointer, size, alignment);
    }

    uint64_t CurrentUsage() const override { return _delegate->CurrentUsage(); }
    uint64_t MaxMemoryUsage() const override { return _delegate->MaxMemoryUsage(); }

private:
    MemTracker* _tracker;
    std::shared_ptr<paimon::MemoryPool> _delegate;
};

Status create_arrow_column(const arrow::DataType* arrow_type, SlotDescriptor* slot_desc, MutableColumnPtr* column,
                           ConvertFuncTree* convert_function, Expr** cast_expr, ObjectPool* pool) {
    TypeDescriptor raw_type;
    bool need_cast = false;
    RETURN_IF_ERROR(build_arrow_column_convert_plan(arrow_type, &slot_desc->type(), slot_desc->is_nullable(), &raw_type,
                                                    convert_function, need_cast, true));
    *column = create_arrow_column_convert_dest(slot_desc->type(), raw_type, need_cast, slot_desc->is_nullable());
    if (!need_cast) {
        *cast_expr = pool->add(new ColumnRef(slot_desc));
        return Status::OK();
    }

    auto* column_ref = pool->add(new ColumnRef(slot_desc));
    *cast_expr = VectorizedCastExprFactory::from_type(raw_type, slot_desc->type(), column_ref, pool);
    if (*cast_expr == nullptr) {
        return illegal_converting_error(arrow_type->name(), slot_desc->type().debug_string());
    }
    return Status::OK();
}

StatusOr<std::shared_ptr<arrow::DataType>> create_missing_arrow_type(const TypeDescriptor& slot_type) {
    if (slot_type.type == TYPE_ARRAY && !slot_type.children.empty()) {
        ASSIGN_OR_RETURN(auto child_type, create_missing_arrow_type(slot_type.children[0]));
        return arrow::list(std::move(child_type));
    }
    if (slot_type.type == TYPE_MAP && slot_type.children.size() == 2) {
        ASSIGN_OR_RETURN(auto key_type, create_missing_arrow_type(slot_type.children[0]));
        ASSIGN_OR_RETURN(auto item_type, create_missing_arrow_type(slot_type.children[1]));
        return arrow::map(std::move(key_type), std::move(item_type));
    }
    if (slot_type.type == TYPE_STRUCT) {
        if (slot_type.field_names.size() != slot_type.children.size()) {
            return Status::InternalError("Paimon STRUCT field names do not match child types");
        }
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(slot_type.field_names.size());
        for (size_t i = 0; i < slot_type.field_names.size(); ++i) {
            ASSIGN_OR_RETURN(auto child_type, create_missing_arrow_type(slot_type.children[i]));
            fields.emplace_back(arrow::field(slot_type.field_names[i], std::move(child_type), true));
        }
        return arrow::struct_(std::move(fields));
    }
    return arrow::null();
}

StatusOr<std::shared_ptr<arrow::DataType>> project_arrow_type_for_slot(
        const std::shared_ptr<arrow::DataType>& arrow_type, const TypeDescriptor& slot_type) {
    if (slot_type.type == TYPE_STRUCT && arrow_type->id() == arrow::Type::STRUCT) {
        if (slot_type.field_names.size() != slot_type.children.size()) {
            return Status::InternalError("Paimon STRUCT field names do not match child types");
        }
        const auto* struct_type = down_cast<const arrow::StructType*>(arrow_type.get());
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(slot_type.field_names.size());
        for (size_t i = 0; i < slot_type.field_names.size(); ++i) {
            const auto& field_name = slot_type.field_names[i];
            auto source_field = struct_type->GetFieldByName(field_name);
            if (source_field == nullptr) {
                ASSIGN_OR_RETURN(auto missing_type, create_missing_arrow_type(slot_type.children[i]));
                fields.emplace_back(arrow::field(field_name, std::move(missing_type), true));
                continue;
            }
            ASSIGN_OR_RETURN(auto projected_type,
                             project_arrow_type_for_slot(source_field->type(), slot_type.children[i]));
            fields.emplace_back(source_field->WithType(std::move(projected_type)));
        }
        return arrow::struct_(std::move(fields));
    }

    if (slot_type.type == TYPE_ARRAY && !slot_type.children.empty() &&
        (arrow_type->id() == arrow::Type::LIST || arrow_type->id() == arrow::Type::LARGE_LIST)) {
        const auto* list_type = down_cast<const arrow::BaseListType*>(arrow_type.get());
        ASSIGN_OR_RETURN(auto projected_type,
                         project_arrow_type_for_slot(list_type->value_type(), slot_type.children[0]));
        auto value_field = list_type->value_field()->WithType(std::move(projected_type));
        return arrow_type->id() == arrow::Type::LIST ? arrow::list(std::move(value_field))
                                                     : arrow::large_list(std::move(value_field));
    }

    if (slot_type.type == TYPE_MAP && slot_type.children.size() == 2 && arrow_type->id() == arrow::Type::MAP) {
        const auto* map_type = down_cast<const arrow::MapType*>(arrow_type.get());
        ASSIGN_OR_RETURN(auto key_type, project_arrow_type_for_slot(map_type->key_type(), slot_type.children[0]));
        ASSIGN_OR_RETURN(auto item_type, project_arrow_type_for_slot(map_type->item_type(), slot_type.children[1]));
        return arrow::map(std::move(key_type), map_type->item_field()->WithType(std::move(item_type)),
                          map_type->keys_sorted());
    }

    return arrow_type;
}

} // namespace

PaimonScanner::~PaimonScanner() = default;

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

    _memory_pool = std::make_shared<TrackedPaimonMemoryPool>(runtime_state->query_mem_tracker_ptr().get());
    _paimon_file_system = std::make_shared<PaimonFileSystem>(_scanner_ctx->fs, &_fs_stats, &_app_stats);

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
        // Top-level name projection is type-safe for every Paimon logical type.
        // Nested pruning remains local in this first version: the converter
        // plan below is recursively aligned by field name against the actual
        // Arrow batch so projected STRUCT children cannot drift by position.
        context_builder.SetReadFieldNames(field_names);
    }
    if (table_descriptor != nullptr) {
        if (!table_descriptor->get_paimon_table_schema_json().empty()) {
            context_builder.SetTableSchema(std::string(table_descriptor->get_paimon_table_schema_json()));
        }
        if (!table_descriptor->get_paimon_branch().empty()) {
            context_builder.WithBranch(std::string(table_descriptor->get_paimon_branch()));
        }
        if (table_descriptor->is_paimon_data_evolution_enabled()) {
            // Files written with an older physical schema cannot evaluate a
            // predicate against the current logical schema. Let paimon-cpp
            // apply it after completing schema evolution instead.
            context_builder.EnablePredicateFilter(true);
        }
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
    context_builder.EnablePrefetch(false);
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

void PaimonScanner::do_close(RuntimeState* runtime_state) noexcept {
    // Imported Arrow batches can retain release callbacks backed by the reader's
    // arena, so release them before closing and destroying the BatchReader.
    _arrow_batch.reset();
    if (_reader != nullptr) {
        _reader->Close();
        _reader.reset();
    }
    _read_chunk.reset();
    _paimon_file_system.reset();
    _memory_pool.reset();
    _pool.clear();
}

Status PaimonScanner::_next_batch() {
    SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
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
            ASSIGN_OR_RETURN(auto projected_arrow_type,
                             project_arrow_type_for_slot(arrow_column->type(), slot_desc->type()));
            RETURN_IF_ERROR(create_arrow_column(projected_arrow_type.get(), slot_desc, &column,
                                                _convert_functions[i].get(), &_cast_exprs[i], &_pool));
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
    const size_t rows_before_filter = materialized_columns.empty() ? _chunk_start_idx : _read_chunk->num_rows();
    size_t row_count = rows_before_filter;
    if (_read_chunk->num_columns() > 0) {
        _read_chunk->filter(_chunk_filter);
        row_count = _read_chunk->num_rows();
    }
    _app_stats.late_materialize_skip_rows += rows_before_filter - row_count;

    ChunkPtr output = std::make_shared<Chunk>();
    for (size_t i = 0; i < materialized_columns.size(); ++i) {
        SlotDescriptor* slot_desc = materialized_columns[i].slot_desc;
        auto output_column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        output_column->reserve(row_count);
        output->append_column(std::move(output_column), slot_desc->id());

        ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, _read_chunk.get()));
        column = ColumnHelper::unfold_const_column(slot_desc->type(), row_count, column);
        // A passthrough ColumnRef aliases the reusable _read_chunk column. Move
        // its data into a fresh output column so the next do_get_next() reset
        // cannot clear a chunk that is still owned by the pipeline.
        output->get_column_by_slot_id(slot_desc->id())->swap_column(*column);
    }
    output->set_num_rows(row_count);

    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.append_side_columns_to_chunk(&output, row_count));
    RETURN_IF_ERROR(_scanner_ctx->format_scan_context.evaluate_all_predicates(&output));
    *chunk = std::move(output);
    return Status::OK();
}

bool PaimonScanner::_batch_is_exhausted() const {
    return _scanner_eof || _arrow_batch == nullptr || _batch_start_idx >= _arrow_batch->num_rows();
}

} // namespace starrocks
