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
LanceString view(std::string_view value) {
    return {value.data(), value.size()};
}
bool is_cancelled(void* context) {
    return static_cast<RuntimeState*>(context)->is_cancelled();
}
Status reader_error(char* error) {
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
    std::vector<LanceString> columns;
    for (const auto* slot : tuple->slots()) columns.push_back(view(slot->col_name()));
    if (columns.empty()) return Status::NotSupported("Lance scan requires a materialized slot");
    std::vector<LanceProperty> properties;
    for (const auto& [key, value] : cloud.cloud_properties) properties.push_back({view(key), view(value)});
    char* error = nullptr;
    int result = sr_lance_open(view(uri), 0, columns.data(), columns.size(), state->chunk_size(), cloud.cloud_type,
                               properties.data(), properties.size(), {is_cancelled, state}, &_reader, &error);
    if (state->is_cancelled()) {
        sr_lance_free_error(error);
        return Status::Cancelled("Lance scan cancelled");
    }
    if (result != 1) return reader_error(error);
    return Status::OK();
}

Status LanceNativeReader::get_next(RuntimeState* state, ChunkPtr* chunk) {
    if (_reader == nullptr) return Status::InternalError("Lance reader is not open");
    while (_batch == nullptr || _batch_offset == _batch->num_rows()) {
        RETURN_IF_CANCELLED(state);
        release_batch();
        _batch_offset = 0;
        ArrowOutput output;
        char* error = nullptr;
        int result;
        {
            SCOPED_RAW_TIMER(&_io_time_ns);
            result = sr_lance_next(_reader, &output.array, &output.schema, &error);
        }
        if (result == 0) return Status::EndOfFile("Lance scan completed");
        if (result == 2) continue;
        if (result != 1) return reader_error(error);
        auto imported = arrow::ImportRecordBatch(&output.array, &output.schema);
        if (!imported.ok()) return Status::InternalError("Cannot import Lance Arrow batch");
        _batch = std::move(imported).ValueOrDie();
        const int64_t bytes = arrow::util::TotalBufferSize(*_batch);
        if (_mem_tracker != nullptr) {
            if (_mem_tracker->try_consume(bytes) != nullptr) {
                _batch.reset();
                return Status::MemoryLimitExceeded("Lance Arrow batch exceeds query memory limit");
            }
            _batch_bytes = bytes;
        }
    }
    RETURN_IF_CANCELLED(state);
    auto count = std::min<int64_t>(_batch->num_rows() - _batch_offset, state->chunk_size());
    {
        SCOPED_RAW_TIMER(&_convert_time_ns);
        RETURN_IF_ERROR(convert_batch(state, _tuple, _batch->Slice(_batch_offset, count), chunk));
    }
    _batch_offset += count;
    return Status::OK();
}

Status LanceNativeReader::convert_batch(RuntimeState* state, const TupleDescriptor* tuple,
                                        const std::shared_ptr<arrow::RecordBatch>& batch, ChunkPtr* chunk) {
    auto raw = std::make_shared<Chunk>();
    auto result = std::make_shared<Chunk>();
    ObjectPool pool;
    Filter filter(batch->num_rows(), 1);
    ArrowConvertContext context;
    context.timezone = state->timezone();
    bool conversion_error = false;
    context.report_error_message = [&](const std::string&, const std::string&, int64_t) { conversion_error = true; };
    std::vector<Expr*> casts;
    for (const auto* slot : tuple->slots()) {
        auto array = batch->GetColumnByName(std::string(slot->col_name()));
        if (array == nullptr)
            return Status::InternalError("Missing projected Lance column: " + std::string(slot->col_name()));
        if (!slot->is_nullable() && array->null_count() != 0) {
            return Status::DataQualityError("Null in non-nullable Lance column: " + std::string(slot->col_name()));
        }
        RETURN_IF_ERROR(validate_integer_width(array->type().get(), slot->type()));
        ConvertFuncTree plan;
        Expr* cast = nullptr;
        MutableColumnPtr column;
        RETURN_IF_ERROR(create_arrow_column(array->type().get(), slot, &column, &plan, &cast, pool, true));
        configure_lance_temporal(array->type().get(), slot->type(), &plan);
        context.set_current_column(slot->col_name(), slot->type());
        RETURN_IF_ERROR(convert_arrow_array_to_column(&plan, batch->num_rows(), array.get(), column.get(), 0, 0,
                                                      &filter, &context));
        if (conversion_error || std::find(filter.begin(), filter.end(), 0) != filter.end()) {
            return Status::DataQualityError("Invalid value in Lance column: " + std::string(slot->col_name()));
        }
        raw->append_column(std::move(column), slot->id());
        casts.push_back(cast);
    }
    for (size_t i = 0; i < casts.size(); ++i) {
        const auto* slot = tuple->slots()[i];
        ASSIGN_OR_RETURN(auto column, casts[i]->evaluate_checked(nullptr, raw.get()));
        column = ColumnHelper::unfold_const_column(slot->type(), batch->num_rows(), column);
        result->append_column(std::move(column), slot->id());
    }
    *chunk = std::move(result);
    return Status::OK();
}

void LanceNativeReader::release_batch() {
    _batch.reset();
    if (_batch_bytes != 0) {
        _mem_tracker->release(_batch_bytes);
        _batch_bytes = 0;
    }
}

void LanceNativeReader::close() {
    release_batch();
    _batch_offset = 0;
    if (_reader != nullptr) {
        sr_lance_close(_reader);
        _reader = nullptr;
    }
}
} // namespace starrocks
