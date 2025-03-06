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

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

// project dependencies
#include "binary_converter.h"
#include "column_converter.h"
#include "nested_converter.h"
#include "primitive_converter.h"

// starrocks dependencies
#include "column/const_column.h"
#include "column/field.h"
#include "types/map_type_info.h"

namespace starrocks::lake::format {

/*
 * ColumnConverter static functions
 */

arrow::Result<std::shared_ptr<ColumnConverter>> ColumnConverter::create(
        const std::shared_ptr<arrow::DataType>& arrow_type, const std::shared_ptr<Field>& sr_field,
        const arrow::MemoryPool* pool) {
    std::shared_ptr<ColumnConverter> result;

    int32_t converter_id = ARROW_CONVERTER_ID(arrow_type->id(), sr_field->type()->type());
    switch (converter_id) {
#define CONVERTER_CASE_IMPL(CONVERTER_ID, CONVERTER_CLASS_TYPE)             \
    case CONVERTER_ID:                                                      \
        result.reset(new CONVERTER_CLASS_TYPE(arrow_type, sr_field, pool)); \
        break;

#define CONVERTER_CASE(ARROW_TYPE_ID, SR_TYPE_ID, CONVERTER_CLASS_TYPE) \
    CONVERTER_CASE_IMPL(ARROW_CONVERTER_ID(ARROW_TYPE_ID, SR_TYPE_ID),  \
                        (CONVERTER_CLASS_TYPE<ARROW_TYPE_ID, SR_TYPE_ID>))

        CONVERTER_CASE(arrow::Type::BOOL, starrocks::LogicalType::TYPE_BOOLEAN, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::INT8, starrocks::LogicalType::TYPE_TINYINT, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::INT16, starrocks::LogicalType::TYPE_SMALLINT, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::INT32, starrocks::LogicalType::TYPE_INT, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::INT64, starrocks::LogicalType::TYPE_BIGINT, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::FLOAT, starrocks::LogicalType::TYPE_FLOAT, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::DOUBLE, starrocks::LogicalType::TYPE_DOUBLE, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::DECIMAL128, starrocks::LogicalType::TYPE_DECIMAL32, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::DECIMAL128, starrocks::LogicalType::TYPE_DECIMAL64, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::DECIMAL128, starrocks::LogicalType::TYPE_DECIMAL128, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::DATE32, starrocks::LogicalType::TYPE_DATE, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::DATE64, starrocks::LogicalType::TYPE_DATE, PrimitiveConverter)
        CONVERTER_CASE(arrow::Type::TIMESTAMP, starrocks::LogicalType::TYPE_DATETIME, PrimitiveConverter)

        // process binary, string, json type
        CONVERTER_CASE(arrow::Type::STRING, starrocks::LogicalType::TYPE_LARGEINT, BinaryConverter)
        CONVERTER_CASE(arrow::Type::BINARY, starrocks::LogicalType::TYPE_VARBINARY, BinaryConverter)
        CONVERTER_CASE(arrow::Type::STRING, starrocks::LogicalType::TYPE_CHAR, BinaryConverter)
        CONVERTER_CASE(arrow::Type::STRING, starrocks::LogicalType::TYPE_VARCHAR, BinaryConverter)
        CONVERTER_CASE(arrow::Type::STRING, starrocks::LogicalType::TYPE_JSON, BinaryConverter)
        CONVERTER_CASE(arrow::Type::BINARY, starrocks::LogicalType::TYPE_OBJECT, BinaryConverter)
        CONVERTER_CASE(arrow::Type::BINARY, starrocks::LogicalType::TYPE_HLL, BinaryConverter)

        // nested type: map, array, struct
        CONVERTER_CASE(arrow::Type::LIST, starrocks::LogicalType::TYPE_ARRAY, NestedConverter)
        CONVERTER_CASE(arrow::Type::MAP, starrocks::LogicalType::TYPE_MAP, NestedConverter)
        CONVERTER_CASE(arrow::Type::STRUCT, starrocks::LogicalType::TYPE_STRUCT, NestedConverter)

    default: {
        return arrow::Status::NotImplemented("Unsupported conversion between ", arrow_type->ToString(), " and ",
                                             sr_field->to_string());
    }
#undef CONVERTER_CASE
#undef CONVERTER_CASE_IMPL
    }

    if (arrow_type->num_fields() > 0) {
        ARROW_ASSIGN_OR_RAISE(auto children_converters,
                              ColumnConverter::create_children_converter(arrow_type, sr_field, pool));
        result->set_children(std::move(children_converters));
    }

    return result;
}

arrow::Result<ColumnConverterVector> ColumnConverter::create_children_converter(
        const std::shared_ptr<arrow::DataType>& arrow_type, const std::shared_ptr<Field>& sr_field,
        const arrow::MemoryPool* pool) {
    ColumnConverterVector result;

    if (arrow_type->id() == arrow::Type::LIST && sr_field->type()->type() == TYPE_ARRAY) {
        // list field's sub fields should have 1 sub field:element.
        if (!sr_field->has_sub_fields()) {
            return arrow::Status::Invalid("Has no sub fields for array field ", sr_field->name());
        }
        if (sr_field->sub_fields().size() != 1) {
            return arrow::Status::Invalid("Expected 1 sub fields for ", sr_field->name(), ", but got ",
                                          sr_field->sub_fields().size());
        }
        // arrow children datatype
        const auto& arrow_offset_datatype = arrow::int32();
        auto value_type = arrow::internal::checked_pointer_cast<arrow::ListType>(arrow_type)->value_type();

        // starrocks children fields
        auto sr_offset_field = std::make_shared<Field>(0, "offset", get_type_info(TYPE_INT), false);
        std::shared_ptr<Field> sr_element_field = std::make_shared<Field>(sr_field->sub_field(0));

        ARROW_ASSIGN_OR_RAISE(auto converter, ColumnConverter::create(arrow_offset_datatype, sr_offset_field, pool));
        result.emplace_back(converter);
        ARROW_ASSIGN_OR_RAISE(converter, ColumnConverter::create(value_type, sr_element_field, pool));
        result.emplace_back(converter);
    } else if (arrow_type->id() == arrow::Type::MAP && sr_field->type()->type() == TYPE_MAP) {
        // map field's sub fields should have 2 sub fields:key and value.
        if (!sr_field->has_sub_fields()) {
            return arrow::Status::Invalid("Has no sub fields for map field ", sr_field->name());
        }
        if (sr_field->sub_fields().size() != 2) {
            return arrow::Status::Invalid("Expected 2 sub fields for ", sr_field->name(), ", but got ",
                                          sr_field->sub_fields().size());
        }

        // arrow children datatype
        const auto& arrow_offset_datatype = arrow::int32();
        auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(arrow_type);
        auto key_type = map_type->key_type();
        auto value_type = map_type->item_type();

        // starrocks children fields
        auto sr_offset_field = std::make_shared<Field>(0, "offset", get_type_info(TYPE_INT), false);
        auto sr_key_field = std::make_shared<Field>(sr_field->sub_field(0));
        auto sr_value_field = std::make_shared<Field>(sr_field->sub_field(1));
        ARROW_ASSIGN_OR_RAISE(auto converter, ColumnConverter::create(arrow_offset_datatype, sr_offset_field, pool));
        result.emplace_back(converter);
        ARROW_ASSIGN_OR_RAISE(converter, ColumnConverter::create(key_type, sr_key_field, pool));
        result.emplace_back(converter);
        ARROW_ASSIGN_OR_RAISE(converter, ColumnConverter::create(value_type, sr_value_field, pool));
        result.emplace_back(converter);
    } else if (arrow_type->id() == arrow::Type::STRUCT && sr_field->type()->type() == TYPE_STRUCT) {
        if (!sr_field->has_sub_fields()) {
            return arrow::Status::Invalid("Has no sub fields for struct field ", sr_field->name());
        }
        if (arrow_type->num_fields() != sr_field->sub_fields().size()) {
            return arrow::Status::Invalid("Field number mismatch, arrow: ", arrow_type->num_fields(),
                                          ", sr: ", sr_field->sub_fields().size());
        }

        for (size_t idx = 0; idx < arrow_type->num_fields(); ++idx) {
            if (arrow_type->field(idx)->name() != sr_field->sub_field(idx).name()) {
                return arrow::Status::Invalid("Field name mismatch, arrow: ", arrow_type->field(idx)->name(),
                                              ", sr: ", sr_field->sub_field(idx).name());
            }
            std::shared_ptr<Field> sr_child_field = std::make_shared<Field>(sr_field->sub_field(idx));

            ARROW_ASSIGN_OR_RAISE(auto converter,
                                  ColumnConverter::create(arrow_type->field(idx)->type(), sr_child_field, pool));
            result.emplace_back(converter);
        }
    } else {
        return arrow::Status::NotImplemented("Unsupported conversion between ", arrow_type->ToString(), " and ",
                                             sr_field->to_string());
    }
    return result;
}

/*
 * ColumnConverter member functions
 */

/*
 * Convert starrocks null data to arrow null bitmap:
 * - arrow use 0 as null indicator and use bit
 * - while sr use 1 as null indicator and use byte
 */
arrow::Result<std::shared_ptr<arrow::Buffer>> ColumnConverter::convert_null_bitmap(const Buffer<uint8_t>& null_bytes) {
    std::shared_ptr<arrow::Buffer> null_bitmap;
    ARROW_ASSIGN_OR_RAISE(null_bitmap,
                          arrow::internal::BytesToBits(reinterpret_cast<const std::vector<uint8_t>&>(null_bytes),
                                                       const_cast<arrow::MemoryPool*>(_pool)));

    uint8_t* out_buf = null_bitmap->mutable_data();
    for (size_t i = 0; i < null_bitmap->capacity(); i++) {
        out_buf[i] = ~out_buf[i];
    }

    // set unused bit to zero
    size_t num_rows = null_bytes.size();
    arrow::bit_util::SetBitsTo(null_bitmap->mutable_data(), num_rows, null_bitmap->capacity() - num_rows, false);
    return null_bitmap;
}

ColumnPtr ColumnConverter::get_data_column(const ColumnPtr& column) {
    if (column->is_nullable()) {
        auto* nullable_column = down_cast<const NullableColumn*>(column.get());
        return nullable_column->data_column();
    }

    if (column->is_constant()) {
        auto* const_column = down_cast<const ConstColumn*>(column.get());
        return const_column->data_column();
    }

    return column;
}

} // namespace starrocks::lake::format