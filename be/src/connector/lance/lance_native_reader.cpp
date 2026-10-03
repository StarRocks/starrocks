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

#include "connector/lance/lance_native_reader.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/util/byte_size.h>

#include <algorithm>

#include "column/arrow/arrow_to_starrocks_converter.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exprs/expr.h"
#include "formats/arrow/arrow_column_converter.h"
#include "gen_cpp/CloudConfiguration_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/value_generator.h"

namespace starrocks {
namespace {
SrLanceString to_lance_string(std::string_view value) {
    return {value.data(), value.size()};
}
bool is_cancelled(void* context) {
    return static_cast<RuntimeState*>(context)->is_cancelled();
}
Status lance_error_status(char* error) {
    std::string message = error == nullptr ? "Unknown Lance reader error" : error;
    sr_lance_free_error(error);
    return Status::IOError(message);
}
// The generic load converter permits narrowing integer casts. A stale external
// schema must fail a query instead of silently truncating dataset values.
Status validate_integer_width(const arrow::DataType* source, const TypeDescriptor& target) {
    if (target.type == TYPE_ARRAY && target.children.size() == 1 && source->num_fields() == 1) {
        return validate_integer_width(source->field(0)->type().get(), target.children[0]);
    }
    int target_bits;
    switch (target.type) {
    case TYPE_TINYINT:
        target_bits = 8;
        break;
    case TYPE_SMALLINT:
        target_bits = 16;
        break;
    case TYPE_INT:
        target_bits = 32;
        break;
    case TYPE_BIGINT:
        target_bits = 64;
        break;
    case TYPE_LARGEINT:
        target_bits = 128;
        break;
    default:
        return Status::OK();
    }
    int source_bits;
    switch (source->id()) {
    case arrow::Type::INT8:
        source_bits = 8;
        break;
    case arrow::Type::UINT8:
        source_bits = 9;
        break;
    case arrow::Type::INT16:
        source_bits = 16;
        break;
    case arrow::Type::UINT16:
        source_bits = 17;
        break;
    case arrow::Type::INT32:
        source_bits = 32;
        break;
    case arrow::Type::UINT32:
        source_bits = 33;
        break;
    case arrow::Type::INT64:
        source_bits = 64;
        break;
    case arrow::Type::UINT64:
        source_bits = 65;
        break;
    default:
        return Status::OK();
    }
    if (source_bits > target_bits) {
        return Status::DataQualityError("Lance integer type exceeds the projected column width");
    }
    return Status::OK();
}
// Preserve the Lance reader's UTC/floor semantics, including sub-second values
// before the epoch. Generic load converters use source timezones and truncation.
template <LogicalType LT>
Status convert_lance_temporal(const arrow::Array* array, size_t start, size_t count, Column* column, size_t destination,
                              uint8_t* nulls, Filter*, ArrowConvertContext*, ConvertFuncTree*) {
    int64_t units = 1000;
    if (array->type_id() == arrow::Type::TIMESTAMP) {
        switch (static_cast<const arrow::TimestampType*>(array->type().get())->unit()) {
        case arrow::TimeUnit::SECOND:
            units = 1;
            break;
        case arrow::TimeUnit::MILLI:
            units = 1000;
            break;
        case arrow::TimeUnit::MICRO:
            units = 1000000;
            break;
        case arrow::TimeUnit::NANO:
            units = 1000000000;
            break;
        }
    }
    auto& values = static_cast<RunTimeColumnType<LT>*>(column)->get_data();
    values.resize(destination + count);
    for (size_t i = 0; i < count; ++i) {
        if (nulls != nullptr && nulls[i] != 0) {
            values[destination + i] = DefaultValueGenerator<RunTimeCppType<LT>>::next_value();
            continue;
        }
        const int64_t value = array->type_id() == arrow::Type::DATE64
                                      ? static_cast<const arrow::Date64Array*>(array)->Value(start + i)
                                      : static_cast<const arrow::TimestampArray*>(array)->Value(start + i);
        int64_t seconds = value / units;
        int64_t fraction = value % units;
        if (fraction < 0) {
            --seconds;
            fraction += units;
        }
        // StarRocks DATE/DATETIME support years 0000 through 9999.
        if (seconds < -62167219200LL || seconds > 253402300799LL) {
            return Status::DataQualityError("Lance timestamp is outside the supported date range");
        }
        TimestampValue timestamp;
        timestamp.from_unix_second(seconds, fraction * 1000000 / units);
        if constexpr (LT == TYPE_DATE)
            values[destination + i] = static_cast<DateValue>(timestamp);
        else
            values[destination + i] = timestamp;
    }
    return Status::OK();
}

void configure_lance_temporal(const arrow::DataType* source, const TypeDescriptor& target, ConvertFuncTree* plan) {
    if (source->id() == arrow::Type::DATE64 || source->id() == arrow::Type::TIMESTAMP) {
        if (target.type == TYPE_DATE)
            plan->func = convert_lance_temporal<TYPE_DATE>;
        else if (target.type == TYPE_DATETIME)
            plan->func = convert_lance_temporal<TYPE_DATETIME>;
    } else if (target.type == TYPE_ARRAY && plan->children.size() == 1) {
        configure_lance_temporal(source->field(0)->type().get(), target.children[0], plan->children[0].get());
    }
}

// ImportRecordBatch consumes the C data on success. Also release anything it leaves
// behind on import failure; release callbacks are nulled when ownership transfers.
struct ArrowOutput {
    ArrowArray array{};
    ArrowSchema schema{};
    ~ArrowOutput() {
        if (array.release != nullptr) array.release(&array);
        if (schema.release != nullptr) schema.release(&schema);
    }
};
} // namespace

LanceNativeReader::LanceNativeReader() = default;
LanceNativeReader::~LanceNativeReader() {
    close();
}

Status LanceNativeReader::open(RuntimeState* state, const TupleDescriptor* tuple, const std::string& uri,
                               const TCloudConfiguration& cloud) {
    close();
    RETURN_IF_CANCELLED(state);
    _tuple = tuple;
    _mem_tracker = state->instance_mem_tracker_ptr();
    _state = state;
    _max_chunk_size = state->chunk_size();
    _init_read_fields();
    if (_field_names.empty()) return Status::NotSupported("Lance scan requires a materialized slot");
    return _open_reader(uri, cloud);
}

void LanceNativeReader::_init_read_fields() {
    _field_names.clear();
    _field_names.reserve(_tuple->slots().size());
    for (const auto* slot : _tuple->slots()) _field_names.emplace_back(slot->col_name());
}

Status LanceNativeReader::_open_reader(const std::string& dataset_uri, const TCloudConfiguration& cloud) {
    std::vector<SrLanceString> fields;
    fields.reserve(_field_names.size());
    for (const auto& name : _field_names) fields.emplace_back(to_lance_string(name));
    // StarRocks cloud properties are normalized into SDK storage options in Rust.
    std::vector<SrLanceStringPair> properties;
    for (const auto& [key, value] : cloud.cloud_properties)
        properties.push_back({to_lance_string(key), to_lance_string(value)});
    char* error = nullptr;
    int result = sr_lance_reader_open(to_lance_string(dataset_uri), 0, fields.data(), fields.size(), _max_chunk_size,
                                      cloud.cloud_type, properties.data(), properties.size(), {is_cancelled, _state},
                                      &_reader, &error);
    if (_state->is_cancelled()) {
        sr_lance_free_error(error);
        return Status::Cancelled("Lance scan cancelled");
    }
    if (result != SR_LANCE_NEXT_BATCH) return lance_error_status(error);
    return Status::OK();
}

Status LanceNativeReader::get_next(RuntimeState* state, ChunkPtr* chunk) {
    if (_reader == nullptr) return Status::InternalError("Lance reader is not open");
    RETURN_IF_CANCELLED(state);
    while (_batch_is_exhausted()) {
        RETURN_IF_ERROR(_next_batch());
    }
    RETURN_IF_CANCELLED(state);
    return _append_batch_to_read_chunk(chunk);
}

bool LanceNativeReader::_batch_is_exhausted() const {
    return _arrow_batch == nullptr || _batch_start_idx >= _arrow_batch->num_rows();
}

Status LanceNativeReader::_next_batch() {
    _release_batch();
    _batch_start_idx = 0;
    while (true) {
        RETURN_IF_CANCELLED(_state);
        ArrowOutput output;
        char* error = nullptr;
        int result;
        {
            SCOPED_RAW_TIMER(&_io_time_ns);
            result = sr_lance_reader_next(_reader, &output.array, &output.schema, &error);
        }
        if (result == SR_LANCE_NEXT_EOF) return Status::EndOfFile("Lance scan completed");
        if (result == SR_LANCE_NEXT_PENDING) continue;
        if (result != SR_LANCE_NEXT_BATCH) return lance_error_status(error);
        auto arrow_batch_result = arrow::ImportRecordBatch(&output.array, &output.schema);
        if (!arrow_batch_result.ok()) return Status::InternalError("Cannot import Lance Arrow batch");
        _arrow_batch = std::move(arrow_batch_result).ValueOrDie();
        const int64_t bytes = arrow::util::TotalBufferSize(*_arrow_batch);
        if (_mem_tracker != nullptr) {
            if (_mem_tracker->try_consume(bytes) != nullptr) {
                _arrow_batch.reset();
                return Status::MemoryLimitExceeded("Lance Arrow batch exceeds query memory limit");
            }
            _batch_bytes = bytes;
        }
        return Status::OK();
    }
}

Status LanceNativeReader::_append_batch_to_read_chunk(ChunkPtr* chunk) {
    const auto remaining_batch = _arrow_batch->num_rows() - _batch_start_idx;
    const auto num_elements = std::min<int64_t>(remaining_batch, _max_chunk_size);
    SCOPED_RAW_TIMER(&_convert_time_ns);
    RETURN_IF_ERROR(convert_batch(_state, _tuple, _arrow_batch->Slice(_batch_start_idx, num_elements), chunk));
    _batch_start_idx += num_elements;
    return Status::OK();
}

Status LanceNativeReader::convert_batch(RuntimeState* state, const TupleDescriptor* tuple,
                                        const std::shared_ptr<arrow::RecordBatch>& batch, ChunkPtr* chunk) {
    auto read_chunk = std::make_shared<Chunk>();
    auto dst = std::make_shared<Chunk>();
    ObjectPool pool;
    Filter chunk_filter(batch->num_rows(), 1);
    ArrowConvertContext conv_ctx;
    conv_ctx.timezone = state->timezone();
    bool conversion_error = false;
    conv_ctx.report_error_message = [&](const std::string&, const std::string&, int64_t) { conversion_error = true; };
    std::vector<Expr*> cast_exprs;
    for (const auto* slot_desc : tuple->slots()) {
        auto array = batch->GetColumnByName(std::string(slot_desc->col_name()));
        if (array == nullptr)
            return Status::InternalError("Missing projected Lance column: " + std::string(slot_desc->col_name()));
        if (!slot_desc->is_nullable() && array->null_count() != 0) {
            return Status::DataQualityError("Null in non-nullable Lance column: " + std::string(slot_desc->col_name()));
        }
        RETURN_IF_ERROR(validate_integer_width(array->type().get(), slot_desc->type()));
        ConvertFuncTree conv_func;
        Expr* cast_expr = nullptr;
        MutableColumnPtr column;
        RETURN_IF_ERROR(
                create_arrow_column(array->type().get(), slot_desc, &column, &conv_func, &cast_expr, pool, true));
        configure_lance_temporal(array->type().get(), slot_desc->type(), &conv_func);
        conv_ctx.set_current_column(slot_desc->col_name(), slot_desc->type());
        RETURN_IF_ERROR(convert_arrow_array_to_column(&conv_func, batch->num_rows(), array.get(), column.get(), 0, 0,
                                                      &chunk_filter, &conv_ctx));
        if (conversion_error || std::find(chunk_filter.begin(), chunk_filter.end(), 0) != chunk_filter.end()) {
            return Status::DataQualityError("Invalid value in Lance column: " + std::string(slot_desc->col_name()));
        }
        read_chunk->append_column(std::move(column), slot_desc->id());
        cast_exprs.push_back(cast_expr);
    }
    for (size_t i = 0; i < cast_exprs.size(); ++i) {
        const auto* slot_desc = tuple->slots()[i];
        ASSIGN_OR_RETURN(auto column, cast_exprs[i]->evaluate_checked(nullptr, read_chunk.get()));
        column = ColumnHelper::unfold_const_column(slot_desc->type(), batch->num_rows(), column);
        dst->append_column(std::move(column), slot_desc->id());
    }
    *chunk = std::move(dst);
    return Status::OK();
}

void LanceNativeReader::_release_batch() {
    _arrow_batch.reset();
    if (_batch_bytes != 0) {
        _mem_tracker->release(_batch_bytes);
        _batch_bytes = 0;
    }
}

void LanceNativeReader::close() {
    _release_batch();
    _batch_start_idx = 0;
    if (_reader != nullptr) {
        sr_lance_reader_close(_reader);
        _reader = nullptr;
    }
}
} // namespace starrocks
