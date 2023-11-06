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

#include "exec/parquet_scanner.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "fs/fs_broker.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "simd/simd.h"

namespace starrocks {

ParquetScanner::ParquetScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                               ScannerCounter* counter, bool schema_only)
        : FileScanner(state, profile, scan_range.params, counter, schema_only),
          _scan_range(scan_range),
          _next_file(0),
          _curr_file_reader(nullptr),
          _scanner_eof(false),
          _max_chunk_size(state->chunk_size() ? state->chunk_size() : 4096),
          _batch_start_idx(0),
          _chunk_start_idx(0) {
    _chunk_filter.reserve(_max_chunk_size);
    _conv_ctx.state = state;
}

ParquetScanner::~ParquetScanner() = default;

Status ParquetScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }
    auto range = _scan_range.ranges[0];
    _num_of_columns_from_file = range.__isset.num_of_columns_from_file
                                        ? implicit_cast<int>(range.num_of_columns_from_file)
                                        : implicit_cast<int>(_src_slot_descriptors.size());

    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        _conv_funcs.emplace_back(std::make_unique<ConvertFuncTree>());
    }
    _cast_exprs.resize(_num_of_columns_from_file, nullptr);

    // column from path
    if (range.__isset.num_of_columns_from_file) {
        int nums = range.columns_from_path.size();
        for (const auto& rng : _scan_range.ranges) {
            if (nums != rng.columns_from_path.size()) {
                return Status::InternalError("Different range different columns.");
            }
        }
    }
    return Status::OK();
}

Status ParquetScanner::initialize_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    _pool.clear();
    (*chunk) = std::make_shared<Chunk>();
    size_t column_pos = 0;
    _chunk_filter.clear();
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto* array = _batch->column(column_pos++).get();
        ColumnPtr column;
        RETURN_IF_ERROR(new_column(array->type().get(), slot_desc, &column, _conv_funcs[i].get(), &_cast_exprs[i],
                                   _pool, _strict_mode));
        column->reserve(_max_chunk_size);
        (*chunk)->append_column(column, slot_desc->id());
    }
    return Status::OK();
}

Status ParquetScanner::append_batch_to_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->fill_ns);
    size_t num_elements =
            std::min<size_t>((_max_chunk_size - _chunk_start_idx), (_batch->num_rows() - _batch_start_idx));
    size_t column_pos = 0;
    _chunk_filter.resize(_chunk_filter.size() + num_elements, 1);
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        _conv_ctx.current_slot = slot_desc;
        auto* array = _batch->column(column_pos++).get();
        auto& column = (*chunk)->get_column_by_slot_id(slot_desc->id());
        // for timestamp type, _state->timezone which is specified by user. convert function
        // obtains timezone from array. thus timezone in array should be rectified to
        // _state->timezone.
        if (array->type_id() == ArrowTypeId::TIMESTAMP) {
            auto* timestamp_type = down_cast<arrow::TimestampType*>(array->type().get());
            auto& mutable_timezone = (std::string&)timestamp_type->timezone();
            mutable_timezone = _state->timezone();
        }
        RETURN_IF_ERROR(convert_array_to_column(_conv_funcs[i].get(), num_elements, array, column, _batch_start_idx,
                                                _chunk_start_idx, &_chunk_filter, &_conv_ctx));
    }

    _chunk_start_idx += num_elements;
    _batch_start_idx += num_elements;
    return Status::OK();
}

Status ParquetScanner::finalize_src_chunk(ChunkPtr* chunk) {
    auto num_rows = (*chunk)->filter(_chunk_filter);
    _counter->num_rows_filtered += _chunk_start_idx - num_rows;
    ChunkPtr cast_chunk = std::make_shared<Chunk>();
    {
        if (VLOG_ROW_IS_ON) {
            VLOG_ROW << "before finalize chunk: " << (*chunk)->debug_columns();
        }
        SCOPED_RAW_TIMER(&_counter->cast_chunk_ns);
        for (auto i = 0; i < _num_of_columns_from_file; ++i) {
            SlotDescriptor* slot_desc = _src_slot_descriptors[i];
            if (slot_desc == nullptr) {
                continue;
            }

            ASSIGN_OR_RETURN(auto column, _cast_exprs[i]->evaluate_checked(nullptr, (*chunk).get()));
            column = ColumnHelper::unfold_const_column(slot_desc->type(), (*chunk)->num_rows(), column);
            cast_chunk->append_column(column, slot_desc->id());
        }
        auto range = _scan_range.ranges.at(_next_file - 1);
        if (range.__isset.num_of_columns_from_file) {
            fill_columns_from_path(cast_chunk, range.num_of_columns_from_file, range.columns_from_path,
                                   cast_chunk->num_rows());
        }
        if (VLOG_ROW_IS_ON) {
            VLOG_ROW << "after finalize chunk: " << cast_chunk->debug_columns();
        }
    }
    ASSIGN_OR_RETURN(auto dest_chunk, materialize(*chunk, cast_chunk));
    *chunk = dest_chunk;
    _chunk_start_idx = 0;
    return Status::OK();
}

// when first batch is accumulated into a column whose propre type must be
// selected. Two concepts used to depict this selection is explained at first:
// 1. arrow-column convertible:  an arrow type can convert to a column directly, e.g. HalfFloatArray -> FloatColumn
// 2. inter-column convertible:  a column can convert to another column, e.g. BinaryColumn -> FloatColumn
// An arrow type AT loading into column type LT is undergoes this steps:
// AT ===[arrow-column convert]===> LT0 ===[inter-column convert]===> LT
// case#0: convert recursively for nested types: TYPE_ARRAY, TYPE_MAP, TYPE_STRUCT
// case#1: if an optimized conv_func provided for AT converting to LT, then convert AT to LT directly
//  AT ===[conv_func] ===> LT ===[ColumnRef expr]===> LT
// case#2: if no optimized conv_func is provided, then convert AT to a strict LT, then use CastExpr for
//  inter-column converting.
//  AT ===[conv_func]===> strict LT ===[VectorizedCastExpr]===> LT
// case#3: otherwise, AT to LT is inconvertible and InterError is reported.

// build convert function tree, raw type desc to get cast function and create dest column,
Status ParquetScanner::build_dest(const arrow::DataType* arrow_type, const TypeDescriptor* type_desc, bool is_nullable,
                                  TypeDescriptor* raw_type_desc, ConvertFuncTree* conv_func, bool& need_cast,
                                  bool strict_mode) {
    auto at = arrow_type->id();
    auto lt = type_desc->type;
    conv_func->func = get_arrow_converter(at, lt, is_nullable, strict_mode);
    conv_func->children.clear();

    switch (lt) {
    case TYPE_ARRAY: {
        if (at != ArrowTypeId::LIST && at != ArrowTypeId::LARGE_LIST && at != ArrowTypeId::FIXED_SIZE_LIST) {
            return Status::InternalError(
                    fmt::format("Apache Arrow type (nested) {} does not match the type {} in StarRocks",
                                arrow_type->name(), type_to_string(lt)));
        }
        raw_type_desc->type = TYPE_ARRAY;
        TypeDescriptor type;
        auto cf = std::make_unique<ConvertFuncTree>();
        auto sub_at = arrow_type->field(0)->type();
        RETURN_IF_ERROR(
                build_dest(sub_at.get(), &type_desc->children[0], true, &type, cf.get(), need_cast, strict_mode));
        raw_type_desc->children.emplace_back(std::move(type));
        conv_func->children.emplace_back(std::move(cf));
        break;
    }
    case TYPE_MAP: {
        if (at != ArrowTypeId::MAP) {
            return Status::InternalError(
                    fmt::format("Apache Arrow type (nested) {} does not match the type {} in StarRocks",
                                arrow_type->name(), type_to_string(lt)));
        }

        raw_type_desc->type = TYPE_MAP;
        for (auto i = 0; i < 2; i++) {
            TypeDescriptor type;
            auto cf = std::make_unique<ConvertFuncTree>();
            auto sub_at = i == 0 ? down_cast<const arrow::MapType*>(arrow_type)->key_type()
                                 : down_cast<const arrow::MapType*>(arrow_type)->item_type();
            RETURN_IF_ERROR(
                    build_dest(sub_at.get(), &type_desc->children[i], true, &type, cf.get(), need_cast, strict_mode));
            raw_type_desc->children.emplace_back(std::move(type));
            conv_func->children.emplace_back(std::move(cf));
        }
        break;
    }
    case TYPE_STRUCT: {
        if (at != ArrowTypeId::STRUCT) {
            return Status::InternalError(
                    fmt::format("Apache Arrow type (nested) {} does not match the type {} in StarRocks",
                                arrow_type->name(), type_to_string(lt)));
        }
        auto field_size = type_desc->children.size();
        auto arrow_field_size = arrow_type->num_fields();

        raw_type_desc->type = TYPE_STRUCT;
        raw_type_desc->field_names = type_desc->field_names;
        conv_func->field_names = type_desc->field_names;
        for (auto i = 0; i < field_size; i++) {
            TypeDescriptor type;
            auto cf = std::make_unique<ConvertFuncTree>();
            auto sub_at = i >= arrow_field_size ? arrow::null() : arrow_type->field(i)->type();
            RETURN_IF_ERROR(
                    build_dest(sub_at.get(), &type_desc->children[i], true, &type, cf.get(), need_cast, strict_mode));
            raw_type_desc->children.emplace_back(std::move(type));
            conv_func->children.emplace_back(std::move(cf));
        }
        break;
    }
    default: {
        if (conv_func->func == nullptr) {
            need_cast = true;
            Status error = illegal_converting_error(arrow_type->name(), type_desc->debug_string());
            auto strict_pt = get_strict_type(at);
            if (strict_pt == TYPE_UNKNOWN) {
                return error;
            }
            auto strict_conv_func = get_arrow_converter(at, strict_pt, is_nullable, strict_mode);
            if (strict_conv_func == nullptr) {
                return error;
            }
            conv_func->func = strict_conv_func;
            raw_type_desc->type = strict_pt;
            switch (strict_pt) {
            case TYPE_DECIMAL128: {
                const auto* discrete_type = down_cast<const arrow::Decimal128Type*>(arrow_type);
                auto precision = discrete_type->precision();
                auto scale = discrete_type->scale();
                if (precision < 1 || precision > decimal_precision_limit<int128_t> || scale < 0 || scale > precision) {
                    return Status::InternalError(
                            strings::Substitute("Decimal($0, $1) is out of range.", precision, scale));
                }
                raw_type_desc->precision = precision;
                raw_type_desc->scale = scale;
                break;
            }
            case TYPE_VARCHAR: {
                raw_type_desc->len = TypeDescriptor::MAX_VARCHAR_LENGTH;
                break;
            }
            case TYPE_CHAR: {
                raw_type_desc->len = TypeDescriptor::MAX_CHAR_LENGTH;
                break;
            }
            case TYPE_DECIMALV2:
            case TYPE_DECIMAL32:
            case TYPE_DECIMAL64: {
                return Status::InternalError(
                        strings::Substitute("Apache Arrow type($0) does not match the type($1) in StarRocks",
                                            arrow_type->name(), type_to_string(strict_pt)));
            }
            default:
                break;
            }
        } else {
            *raw_type_desc = *type_desc;
        }
    }
    }
    return Status::OK();
}

Status ParquetScanner::new_column(const arrow::DataType* arrow_type, const SlotDescriptor* slot_desc, ColumnPtr* column,
                                  ConvertFuncTree* conv_func, Expr** expr, ObjectPool& pool, bool strict_mode) {
    auto& type_desc = slot_desc->type();
    auto* raw_type_desc = pool.add(new TypeDescriptor());
    bool need_cast = false;
    build_dest(arrow_type, &type_desc, slot_desc->is_nullable(), raw_type_desc, conv_func, need_cast, strict_mode);
    if (!need_cast) {
        *expr = pool.add(new ColumnRef(slot_desc));
        (*column) = ColumnHelper::create_column(type_desc, slot_desc->is_nullable());
    } else {
        auto slot = pool.add(new ColumnRef(slot_desc));
        *expr = VectorizedCastExprFactory::from_type(*raw_type_desc, slot_desc->type(), slot, &pool);
        if ((*expr) == nullptr) {
            return illegal_converting_error(arrow_type->name(), type_desc.debug_string());
        }
        *column = ColumnHelper::create_column(*raw_type_desc, slot_desc->is_nullable());
    }

    return Status::OK();
}

Status ParquetScanner::convert_array_to_column(ConvertFuncTree* conv_func, size_t num_elements,
                                               const arrow::Array* array, const ColumnPtr& column,
                                               size_t batch_start_idx, size_t chunk_start_idx, Filter* chunk_filter,
                                               ArrowConvertContext* conv_ctx) {
    uint8_t* null_data;
    Column* data_column;
    if (column->is_nullable()) {
        auto nullable_column = down_cast<NullableColumn*>(column.get());
        auto null_column = nullable_column->mutable_null_column();
        size_t null_count = fill_null_column(array, batch_start_idx, num_elements, null_column, chunk_start_idx);
        nullable_column->set_has_null(null_count != 0);
        null_data = &null_column->get_data().front() + chunk_start_idx;
        data_column = nullable_column->data_column().get();
    } else {
        null_data = nullptr;
        // Fill nullable array into not-nullable column, positions of NULLs is marked as 1
        fill_filter(array, batch_start_idx, num_elements, chunk_filter, chunk_start_idx, conv_ctx);
        data_column = column.get();
    }

    auto st = conv_func->func(array, batch_start_idx, num_elements, data_column, chunk_start_idx, null_data,
                              chunk_filter, conv_ctx, conv_func);
    if (st.ok() && column->is_nullable()) {
        // in some scene such as string length exceeds limit, the column will be set NULL, so we need reset has_null
        down_cast<NullableColumn*>(column.get())->update_has_null();
    }
    return st;
}

bool ParquetScanner::chunk_is_full() {
    return _chunk_start_idx >= _max_chunk_size;
}
bool ParquetScanner::batch_is_exhausted() {
    return _scanner_eof || _batch == nullptr || _batch_start_idx >= _batch->num_rows();
}

StatusOr<ChunkPtr> ParquetScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);
    ChunkPtr chunk;
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
                _curr_file_reader.reset();
                continue;
            }
            return status;
        }
    }
    RETURN_IF_ERROR(initialize_src_chunk(&chunk));
    while (!_scanner_eof) {
        RETURN_IF_ERROR(append_batch_to_src_chunk(&chunk));
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
        _curr_file_reader.reset();
        if (chunk->num_rows() > 0) {
            break;
        }
        // the chunk is empty and the file reach its end, this situation happens when
        // size of file is nonzero but the file only contains no real data.
        // switch to next file and re-initialize src chunk.
        RETURN_IF_ERROR(next_batch());
        RETURN_IF_ERROR(initialize_src_chunk(&chunk));
    }
    RETURN_IF_ERROR(finalize_src_chunk(&chunk));
    return std::move(chunk);
}

Status ParquetScanner::next_batch() {
    SCOPED_RAW_TIMER(&_counter->read_batch_ns);
    _batch_start_idx = 0;
    if (_curr_file_reader == nullptr) {
        if (_last_range_size - _last_file_scan_bytes != 0) {
            _state->update_num_bytes_scan_from_source(_last_range_size - _last_file_scan_bytes);
            _last_file_scan_bytes = 0;
            _last_file_scan_rows = 0;
            _next_batch_counter = 0;
            _last_range_size = 0;
        }
        RETURN_IF_ERROR(open_next_reader());
    }
    while (!_scanner_eof) {
        auto status = _curr_file_reader->next_batch(&_batch);
        if (status.ok() && _batch->num_rows() == 0) {
            continue;
        } else {
            if (status.ok()) {
                _next_batch_counter++;
                _last_file_scan_rows += _batch->num_rows();
                if (_next_batch_counter % 32 == 0) {
                    auto incr_bytes = (int64_t)((double)_last_file_scan_rows / _curr_file_reader->total_num_rows() *
                                                        _last_file_size -
                                                _last_file_scan_bytes);
                    _last_file_scan_bytes += incr_bytes;
                    _state->update_num_bytes_scan_from_source(incr_bytes);
                }
            }
            return status;
        }
    }
    return Status::EndOfFile("eof");
}

Status ParquetScanner::open_next_reader() {
    while (true) {
        if (_next_file >= _scan_range.ranges.size()) {
            _scanner_eof = true;
            return Status::OK();
        }
        std::shared_ptr<RandomAccessFile> file;
        const TBrokerRangeDesc& range_desc = _scan_range.ranges[_next_file];
        Status st = create_random_access_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params,
                                              CompressionTypePB::NO_COMPRESSION, &file);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to create random-access files. status: " << st.to_string()
                         << " file: " << range_desc.path;
            return st;
        }
        _conv_ctx.current_file = file->filename();
        auto parquet_file = std::make_shared<ParquetChunkFile>(file, 0);
        auto parquet_reader = std::make_shared<ParquetReaderWrap>(std::move(parquet_file), _num_of_columns_from_file,
                                                                  range_desc.start_offset, range_desc.size);
        _next_file++;
        int64_t file_size;
        RETURN_IF_ERROR(parquet_reader->size(&file_size));
        _last_file_size = file_size;
        _last_range_size = range_desc.size;
        // switch to next file if the current file is empty
        if (file_size == 0) {
            parquet_reader->close();
            continue;
        }
        _curr_file_reader = std::make_shared<ParquetChunkReader>(std::move(parquet_reader), _src_slot_descriptors,
                                                                 _state->timezone());
        return Status::OK();
    }
}

Status ParquetScanner::get_schema(std::vector<SlotDescriptor>* schema) {
    std::shared_ptr<RandomAccessFile> file;
    // TODO(fw): Infer schema from more files.
    const TBrokerRangeDesc& range_desc = _scan_range.ranges[0];
    Status st = create_random_access_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params,
                                          CompressionTypePB::NO_COMPRESSION, &file);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to create random-access files. status: " << st.to_string()
                     << " file: " << range_desc.path;
        return st;
    }
    _conv_ctx.current_file = file->filename();
    auto parquet_file = std::make_shared<ParquetChunkFile>(file, 0);
    auto parquet_reader = std::make_shared<ParquetReaderWrap>(std::move(parquet_file), _num_of_columns_from_file,
                                                              range_desc.start_offset, range_desc.size);
    return parquet_reader->get_schema(schema);
}

void ParquetScanner::close() {
    FileScanner::close();
    _curr_file_reader.reset();
    _pool.clear();
}

} // namespace starrocks
