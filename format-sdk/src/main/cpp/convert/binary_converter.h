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

// arrow dependencies
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/builder.h>
#include <arrow/status.h>
#include <arrow/type.h>

// project dependencies
#include "column_converter.h"

// starrocks dependencies
#include "column/column.h"
#include "column/column_helper.h"
#include "column/field.h"
#include "format/format_utils.h"
#include "types/logical_type.h"
#include "util/json.h"
#include "util/slice.h"

namespace starrocks::lake::format {

template <arrow::Type::type ARROW_TYPE_ID, LogicalType SR_TYPE,
          typename = arrow::enable_if_base_binary<typename arrow::TypeIdTraits<ARROW_TYPE_ID>::Type>>

class BinaryConverter : public ColumnConverter {
    using ArrowType = typename arrow::TypeIdTraits<ARROW_TYPE_ID>::Type;
    using ArrowArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;

    using SrColumnType = RunTimeColumnType<SR_TYPE>;
    using SrCppType = RunTimeCppType<SR_TYPE>;

public:
    BinaryConverter(const std::shared_ptr<arrow::DataType> arrow_type, const std::shared_ptr<Field> sr_field,
                    const arrow::MemoryPool* pool)
            : ColumnConverter(arrow_type, sr_field, pool){};

    arrow::Status toSrColumn(const std::shared_ptr<arrow::Array> array, ColumnPtr& column) override {
        if (!column->is_nullable() && array->null_count() > 0) {
            return arrow::Status::Invalid("Column ", column->get_name(),
                                          " is non-nullable, but there are some null data in array.");
        }

        auto num_rows = array->length();
        const auto& binary_array = arrow::internal::checked_pointer_cast<const ArrowArrayType>(array);
        for (size_t i = 0; i < num_rows; ++i) {
            if (array->IsNull(i)) {
                column->append_nulls(1);
                continue;
            }

            const auto value_view = binary_array->GetView(i);
            const Slice slice(value_view.data(), value_view.size());
            if constexpr (SR_TYPE == TYPE_CHAR || SR_TYPE == TYPE_VARCHAR || SR_TYPE == TYPE_VARBINARY) {
                // check value length when column is string type
                if constexpr (SR_TYPE == TYPE_CHAR || SR_TYPE == TYPE_VARCHAR) {
                    if (value_view.size() > _sr_field->length()) {
                        return arrow::Status::Invalid("The length of string '", value_view.data(), "' exceeds limit ",
                                                      _sr_field->length());
                    }
                }
                column->append_datum(Datum(slice));
            } else if constexpr (SR_TYPE == TYPE_LARGEINT) {
                SrCppType value = 0;
                if (DecimalV3Cast::from_string<int128_t>(&value, 40, 0, slice.data, slice.size)) {
                    return arrow::Status::Invalid("The largeint value ", slice, " is out of range");
                }
                column->append_datum(Datum(value));
            } else if constexpr (SR_TYPE == TYPE_JSON) {
                const auto result = JsonValue::parse(slice);
                if (!result.ok()) {
                    throw std::runtime_error(fmt::format("Failed to parse json, {}", result.status().message()));
                }
                JsonValue json = std::move(result).value();
                column->append_datum(Datum(&json));
            } else if constexpr (SR_TYPE == TYPE_OBJECT) { // bitmap column
                BitmapValue value(slice);
                column->append_datum(Datum(&value));
            } else if constexpr (SR_TYPE == TYPE_HLL) { // HLL column
                HyperLogLog value(slice);
                column->append_datum(Datum(&value));
            } else {
                return arrow::Status::TypeError("Can't convert starrocks type ", _sr_field->type()->type(),
                                                " from arrow binary.");
            }
        }
        return arrow::Status::OK();
    }

    arrow::Result<std::shared_ptr<arrow::Array>> toArrowArray(const std::shared_ptr<Column>& column) override {
        using ArrowBuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;

        std::unique_ptr<ArrowBuilderType> builder =
                std::make_unique<ArrowBuilderType>(_arrow_type, const_cast<arrow::MemoryPool*>(_pool));
        size_t num_rows = column->size();
        ARROW_RETURN_NOT_OK(builder->Reserve(num_rows));
        for (size_t i = 0; i < num_rows; ++i) {
            if (column->is_null(i)) {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
                continue;
            }

            auto* data_column = ColumnHelper::get_data_column(column.get());
            const SrCppType* column_data = down_cast<const SrColumnType*>(data_column)->get_data().data();
            if constexpr (SR_TYPE == TYPE_CHAR || SR_TYPE == TYPE_VARCHAR || SR_TYPE == TYPE_VARBINARY) {
                Slice slice = column_data[i];
                ARROW_RETURN_NOT_OK(builder->Append(slice.data, slice.size));
            } else if constexpr (SR_TYPE == TYPE_LARGEINT) {
                SrCppType sr_value = column_data[i];
                std::string value =
                        DecimalV3Cast::to_string<int128_t>(sr_value, starrocks::decimal_precision_limit<int128_t>, 0);
                ARROW_RETURN_NOT_OK(builder->Append(value));
            } else if constexpr (SR_TYPE == TYPE_JSON) {
                auto item = down_cast<const SrColumnType*>(data_column)->get_object(i);
                FORMAT_ASSIGN_OR_RAISE_ARROW_STATUS(auto json_value, item->to_string());
                ARROW_RETURN_NOT_OK(builder->Append(json_value));
            } else if constexpr (SR_TYPE == TYPE_OBJECT) { // bitmap column
                auto item = down_cast<const SrColumnType*>(data_column)->get_object(i);
                std::string buf;
                size_t serialize_size = item->get_size_in_bytes();
                buf.resize(serialize_size);
                item->write(buf.data());
                ARROW_RETURN_NOT_OK(builder->Append(buf.data(), serialize_size));
            } else if constexpr (SR_TYPE == TYPE_HLL) { // hll column
                auto item = down_cast<const SrColumnType*>(data_column)->get_object(i);
                std::string buf;
                size_t serialize_size = item->max_serialized_size();
                buf.resize(serialize_size);
                size_t size = item->serialize(reinterpret_cast<uint8_t*>(buf.data()));
                ARROW_RETURN_NOT_OK(builder->Append(buf.data(), size));
            } else {
                return arrow::Status::TypeError("Can't convert starrocks type ", _sr_field->type()->type(),
                                                " to arrow binary.");
            }
        }
        return builder->Finish();
    }
};

} // namespace starrocks::lake::format