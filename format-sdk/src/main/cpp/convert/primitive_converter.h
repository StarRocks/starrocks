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
#pragma once

#include <arrow/array/builder_binary.h>
#include <arrow/builder.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <array>

// project dependencies
#include "column_converter.h"

// starrocks dependencies
#include "column/column.h"
#include "column/field.h"
#include "types/logical_type.h"

namespace starrocks::lake::format {

constexpr int64_t SECONDS_PER_DAY = 24 * 60 * 60;
constexpr int64_t MILLIS_PER_DAY = SECONDS_PER_DAY * 1000;
constexpr int64_t MILLIS_PER_SECOND = 1 * 1000;
constexpr int64_t MICROS_PER_MILLIS = 1 * 1000;
constexpr int64_t MICROS_PER_SECOND = 1 * 1000 * 1000;

template <arrow::Type::type ARROW_TYPE_ID, LogicalType SR_TYPE,
          typename = std::enable_if<arrow::is_boolean_type<arrow::TypeIdTraits<ARROW_TYPE_ID>>::value ||
                                    arrow::is_number_type<arrow::TypeIdTraits<ARROW_TYPE_ID>>::value ||
                                    arrow::is_decimal_type<arrow::TypeIdTraits<ARROW_TYPE_ID>>::value>>

class PrimitiveConverter : public ColumnConverter {
    using ArrowType = typename arrow::TypeIdTraits<ARROW_TYPE_ID>::Type;
    using ArrowArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
    using ArrowCType = typename arrow::TypeTraits<ArrowType>::CType;

    using SrColumnType = RunTimeColumnType<SR_TYPE>;
    using SrCppType = RunTimeCppType<SR_TYPE>;

private:
    cctz::time_zone _ctz;

public:
    PrimitiveConverter(const std::shared_ptr<arrow::DataType> arrow_type, const std::shared_ptr<Field> sr_field,
                       const arrow::MemoryPool* pool)
            : ColumnConverter(arrow_type, sr_field, pool) {
        if constexpr (ARROW_TYPE_ID == arrow::Type::TIMESTAMP) {
            const auto& real_type = arrow::internal::checked_pointer_cast<ArrowType>(_arrow_type);
            auto timezone = real_type->timezone();
            if (!TimezoneUtils::find_cctz_time_zone(timezone, _ctz)) {
                throw std::runtime_error("Unsupported timezone " + timezone);
            }
        } else {
            _ctz = cctz::local_time_zone();
        }
    };

    arrow::Status toSrColumn(const std::shared_ptr<arrow::Array> array, ColumnPtr& column) override {
        if (!column->is_nullable() && array->null_count() > 0) {
            return arrow::Status::Invalid("Column ", column->get_name(),
                                          " is non-nullable, but there are some null data in array.");
        }

        auto num_rows = array->length();
        column->resize(num_rows);
        // copy data column
        const auto& real_arrow_type = arrow::internal::checked_pointer_cast<ArrowType>(_arrow_type);
        const auto& real_array = arrow::internal::checked_pointer_cast<const ArrowArrayType>(array);
        const auto data_column = arrow::internal::checked_pointer_cast<SrColumnType>(get_data_column(column));
        if constexpr (SR_TYPE == TYPE_DATE || SR_TYPE == TYPE_DATETIME) {
            for (size_t i = 0; i < num_rows; ++i) {
                SrCppType value;
                if constexpr (std::is_base_of_v<arrow::Date32Type, ArrowType>) {
                    ArrowCType arrow_value = real_array->Value(i);
                    TimestampValue ts;
                    ts.from_unixtime(arrow_value * SECONDS_PER_DAY, _ctz);
                    value = static_cast<DateValue>(ts);
                } else if constexpr (std::is_base_of_v<arrow::Date64Type, ArrowType>) {
                    ArrowCType arrow_value = real_array->Value(i);
                    TimestampValue ts;
                    ts.from_unixtime(arrow_value / MILLIS_PER_SECOND, _ctz);
                    value = static_cast<DateValue>(ts);
                } else {
                    ArrowCType arrow_value = real_array->Value(i);
                    if (real_arrow_type->unit() == arrow::TimeUnit::MILLI) {
                        value.from_unixtime(arrow_value / MILLIS_PER_SECOND,
                                            arrow_value % MILLIS_PER_SECOND * MICROS_PER_MILLIS, _ctz);
                    } else if (real_arrow_type->unit() == arrow::TimeUnit::MICRO) {
                        value.from_unixtime(arrow_value / MICROS_PER_SECOND, arrow_value % MICROS_PER_SECOND, _ctz);
                    } else {
                        return arrow::Status::Invalid("Unsupported timeunit ", real_arrow_type->unit());
                    }
                }
                data_column->get_data()[i] = value;
            }
        } else if constexpr (SR_TYPE == TYPE_DECIMAL32 || SR_TYPE == TYPE_DECIMAL64 || SR_TYPE == TYPE_DECIMAL128) {
            for (size_t i = 0; i < num_rows; ++i) {
                SrCppType value = 0;
                arrow::Decimal128 arrow_value(real_array->GetValue(i));
                if constexpr (SR_TYPE == TYPE_DECIMAL32 || SR_TYPE == TYPE_DECIMAL64) {
                    ARROW_RETURN_NOT_OK(arrow_value.ToInteger(&value));
                } else {
                    value = arrow_value.high_bits();
                    value = value << 64 | arrow_value.low_bits();
                }
                data_column->get_data()[i] = value;
            }
        } else if constexpr (SR_TYPE == TYPE_LARGEINT) {
            for (size_t i = 0; i < num_rows; ++i) {
                SrCppType value = 0;
                arrow::Decimal256 arrow_value(real_array->GetValue(i));
                if (arrow::Decimal256::Abs(arrow_value) > arrow::Decimal256::GetMaxValue(40)) {
                    return arrow::Status::Invalid("The largeint value ", arrow_value, " is out of range");
                }
                const auto value_array = arrow_value.little_endian_array();
                value = value_array[1];
                value = value << 64 | value_array[0];
                data_column->get_data()[i] = value;
            }
        } else if constexpr (SR_TYPE == TYPE_BOOLEAN) {
            // arrow boolean use bitmap to storage the true/false,
            // so we can not copy memory directly.
            for (size_t i = 0; i < num_rows; ++i) {
                data_column->get_data()[i] = real_array->Value(i);
            }
        } else {
            const ArrowCType* array_data = real_array->raw_values();
            SrCppType* data = data_column->get_data().data();
            memcpy(data, array_data, num_rows * sizeof(SrCppType));
        }

        // copy null bitmap
        if (column->is_nullable()) {
            auto nullable = down_cast<NullableColumn*>(column.get());
            for (size_t i = 0; i < num_rows; ++i) {
                nullable->null_column_data()[i] = array->IsNull(i);
            }
            nullable->set_has_null(true);
        }

        return arrow::Status::OK();
    }
};

} // namespace starrocks::lake::format