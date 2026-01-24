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

#include "exec/benchmark_scanner.h"

#include <algorithm>

#include "benchgen/benchmark_suite.h"
#include "benchgen/record_batch_iterator_factory.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/file_scanner/parquet_scanner.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "runtime/runtime_state.h"
#include "util/arrow/utils.h"

namespace starrocks {

BenchmarkScanner::BenchmarkScanner(BenchmarkScannerParam param, const TupleDescriptor* tuple_desc)
        : _param(std::move(param)), _tuple_desc(tuple_desc), _slot_descs(tuple_desc->slots()) {}

Status BenchmarkScanner::open(RuntimeState* state) {
    _conv_ctx.state = state;
    _conv_ctx.current_file = _param.table_name;
    if (_param.options.chunk_size <= 0) {
        _param.options.chunk_size = state->chunk_size();
    }

    benchgen::SuiteId suite = benchgen::SuiteIdFromString(_param.db_name);
    if (suite == benchgen::SuiteId::kUnknown) {
        return Status::InvalidArgument("Unknown benchmark database: " + _param.db_name);
    }
    RETURN_STATUS_IF_ERROR(benchgen::MakeRecordBatchIterator(suite, _param.table_name, _param.options, &_iter));
    _schema = _iter->schema();
    if (_schema == nullptr) {
        return Status::InternalError("Benchmark iterator returned a null schema");
    }

    _max_chunk_size = state->chunk_size();
    if (_max_chunk_size == 0) {
        _max_chunk_size = static_cast<size_t>(_param.options.chunk_size);
    }
    if (_max_chunk_size == 0) {
        _max_chunk_size = 4096;
    }

    return _init_converters(_schema);
}

Status BenchmarkScanner::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    (void)state;
    *eos = false;
    if (_scanner_eof) {
        *eos = true;
        return Status::OK();
    }

    if (_batch == nullptr || _batch_start_idx >= _batch->num_rows()) {
        RETURN_IF_ERROR(_next_batch(eos));
        if (*eos) {
            return Status::OK();
        }
    }

    size_t batch_rows = static_cast<size_t>(_batch->num_rows());
    size_t num_elements = std::min(_max_chunk_size, batch_rows - _batch_start_idx);
    _chunk_filter.assign(num_elements, 1);

    ChunkPtr raw_chunk = std::make_shared<Chunk>();

    for (size_t i = 0; i < _slot_descs.size(); ++i) {
        SlotDescriptor* slot_desc = _slot_descs[i];
        MutableColumnPtr column = ColumnHelper::create_column(*_raw_type_descs[i], slot_desc->is_nullable());
        column->reserve(num_elements);
        raw_chunk->append_column(std::move(column), slot_desc->id());

        auto array = _batch->GetColumnByName(slot_desc->col_name());
        if (array == nullptr) {
            return Status::NotFound("Benchmark column " + slot_desc->col_name() + " not found in schema");
        }

        _conv_ctx.current_slot = slot_desc;
        RETURN_IF_ERROR(
                ParquetScanner::convert_array_to_column(_conv_funcs[i].get(), num_elements, array.get(),
                                                        raw_chunk->get_column_raw_ptr_by_slot_id(slot_desc->id()),
                                                        _batch_start_idx, 0, &_chunk_filter, &_conv_ctx));
    }

    raw_chunk->filter(_chunk_filter);

    ChunkPtr cast_chunk = std::make_shared<Chunk>();
    cast_chunk->reserve(raw_chunk->num_rows());
    for (size_t i = 0; i < _slot_descs.size(); ++i) {
        SlotDescriptor* slot_desc = _slot_descs[i];
        ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, raw_chunk.get()));
        column = ColumnHelper::unfold_const_column(slot_desc->type(), raw_chunk->num_rows(), std::move(column));
        cast_chunk->append_column(column, slot_desc->id());
    }

    *chunk = std::move(cast_chunk);
    _batch_start_idx += num_elements;
    return Status::OK();
}

void BenchmarkScanner::close(RuntimeState* state) {
    (void)state;
    _iter.reset();
    _batch.reset();
    _schema.reset();
}

Status BenchmarkScanner::_init_converters(const std::shared_ptr<arrow::Schema>& schema) {
    size_t slot_count = _slot_descs.size();
    _raw_type_descs.clear();
    _conv_funcs.clear();
    _cast_exprs.assign(slot_count, nullptr);
    _raw_type_descs.reserve(slot_count);
    _conv_funcs.reserve(slot_count);

    for (size_t i = 0; i < slot_count; ++i) {
        SlotDescriptor* slot_desc = _slot_descs[i];
        auto field = schema->GetFieldByName(slot_desc->col_name());
        if (field == nullptr) {
            return Status::NotFound("Benchmark column " + slot_desc->col_name() + " not found in schema");
        }

        auto raw_type_desc = std::make_unique<TypeDescriptor>();
        auto conv_func = std::make_unique<ConvertFuncTree>();
        bool need_cast = false;
        RETURN_IF_ERROR(ParquetScanner::build_dest(field->type().get(), &slot_desc->type(), slot_desc->is_nullable(),
                                                   raw_type_desc.get(), conv_func.get(), need_cast, false));

        Expr* expr = nullptr;
        if (!need_cast) {
            expr = _pool.add(new ColumnRef(slot_desc));
        } else {
            auto* slot = _pool.add(new ColumnRef(slot_desc));
            expr = VectorizedCastExprFactory::from_type(*raw_type_desc, slot_desc->type(), slot, &_pool);
            if (expr == nullptr) {
                return illegal_converting_error(field->type()->name(), slot_desc->type().debug_string());
            }
        }

        _raw_type_descs.emplace_back(std::move(raw_type_desc));
        _conv_funcs.emplace_back(std::move(conv_func));
        _cast_exprs[i] = expr;
    }

    return Status::OK();
}

Status BenchmarkScanner::_next_batch(bool* eos) {
    std::shared_ptr<arrow::RecordBatch> batch;
    RETURN_STATUS_IF_ERROR(_iter->Next(&batch));
    if (batch == nullptr) {
        _scanner_eof = true;
        *eos = true;
        return Status::OK();
    }

    _batch = std::move(batch);
    _batch_start_idx = 0;
    *eos = false;
    return Status::OK();
}

} // namespace starrocks
