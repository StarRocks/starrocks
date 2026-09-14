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

#include "exprs/ai/ai_provider_options_builder.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cmath>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "base/string/utf8_check.h"
#include "base/types/decimal12.h"
#include "base/types/int128.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "types/decimalv2_value.h"
#include "types/decimalv3.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

using JsonBuffer = rapidjson::StringBuffer;
using JsonWriter = rapidjson::Writer<JsonBuffer, rapidjson::UTF8<>, rapidjson::UTF8<>, rapidjson::CrtAllocator,
                                     rapidjson::kWriteValidateEncodingFlag>;

Status invalid_options() {
    return Status::InvalidArgument("AI provider options are invalid");
}

Status require_json_write(bool success) {
    return success ? Status::OK() : invalid_options();
}

struct UnwrappedValue {
    const Column* column = nullptr;
    size_t row = 0;
    bool is_null = false;
};

StatusOr<UnwrappedValue> unwrap_value(const Column& input, size_t row) {
    const Column* column = &input;
    size_t current_row = row;
    if (current_row >= column->size()) {
        return invalid_options();
    }

    while (column->is_constant() || column->is_nullable()) {
        if (column->is_constant()) {
            const auto* constant = dynamic_cast<const ConstColumn*>(column);
            if (constant == nullptr || constant->data_column()->empty()) {
                return invalid_options();
            }
            column = constant->data_column_raw_ptr();
            current_row = 0;
            continue;
        }

        const auto* nullable = dynamic_cast<const NullableColumn*>(column);
        if (nullable == nullptr || current_row >= nullable->size()) {
            return invalid_options();
        }
        if (nullable->is_null(current_row)) {
            return UnwrappedValue{nullable->data_column_raw_ptr(), current_row, true};
        }
        column = nullable->data_column_raw_ptr();
    }

    if (current_row >= column->size()) {
        return invalid_options();
    }
    return UnwrappedValue{column, current_row, false};
}

template <typename ColumnType>
const ColumnType* checked_column(const UnwrappedValue& value) {
    return dynamic_cast<const ColumnType*>(value.column);
}

bool has_binary_storage(const Column& input) {
    const Column* column = &input;
    while (column->is_constant() || column->is_nullable()) {
        if (column->is_constant()) {
            const auto* constant = dynamic_cast<const ConstColumn*>(column);
            if (constant == nullptr) {
                return false;
            }
            column = constant->data_column_raw_ptr();
        } else {
            const auto* nullable = dynamic_cast<const NullableColumn*>(column);
            if (nullable == nullptr) {
                return false;
            }
            column = nullable->data_column_raw_ptr();
        }
    }
    return dynamic_cast<const BinaryColumn*>(column) != nullptr ||
           dynamic_cast<const LargeBinaryColumn*>(column) != nullptr;
}

bool is_untyped_map(const TypeDescriptor& type) {
    if (type.children.size() != 2) {
        return false;
    }
    const bool unresolved = type.children[0].type == TYPE_NULL && type.children[1].type == TYPE_NULL;
    // FE replaces unresolved NULL types with BOOLEAN before serializing an expression to Thrift.
    const bool wire_normalized = type.children[0].type == TYPE_BOOLEAN && type.children[1].type == TYPE_BOOLEAN;
    return unresolved || wire_normalized;
}

Status write_value(const Column& input, const TypeDescriptor& type, size_t row, JsonWriter* writer,
                   rapidjson::Type* root_type);

StatusOr<std::string> read_map_key(const Column& input, size_t row) {
    auto unwrapped = unwrap_value(input, row);
    if (!unwrapped.ok() || unwrapped->is_null) {
        return invalid_options();
    }
    if (const auto* binary = checked_column<BinaryColumn>(*unwrapped); binary != nullptr) {
        const Slice value = binary->get_slice(unwrapped->row);
        if (!validate_utf8(value.data, value.size)) {
            return invalid_options();
        }
        return std::string(value.data, value.size);
    }
    if (const auto* binary = checked_column<LargeBinaryColumn>(*unwrapped); binary != nullptr) {
        const Slice value = binary->get_slice(unwrapped->row);
        if (!validate_utf8(value.data, value.size)) {
            return invalid_options();
        }
        return std::string(value.data, value.size);
    }
    return invalid_options();
}

Status validate_and_write_map(const MapColumn& map, const TypeDescriptor& type, size_t row, bool top_level,
                              JsonWriter* writer) {
    const auto [offset, count] = map.get_map_offset_size(row);
    if (offset > map.keys().size() || count > map.keys().size() - offset || offset > map.values().size() ||
        count > map.values().size() - offset) {
        return invalid_options();
    }
    const bool untyped_empty = count == 0 && is_untyped_map(type);
    if (!untyped_empty &&
        (type.children.size() != 2 || type.children[0].type != TYPE_VARCHAR || !has_binary_storage(map.keys()))) {
        return invalid_options();
    }
    if (count == 0) {
        RETURN_IF_ERROR(require_json_write(writer->StartObject()));
        return require_json_write(writer->EndObject());
    }

    std::unordered_set<std::string> keys;
    RETURN_IF_ERROR(require_json_write(writer->StartObject()));
    for (size_t index = offset; index < offset + count; ++index) {
        auto key = read_map_key(map.keys(), index);
        if (!key.ok() || key->empty() || !keys.emplace(*key).second) {
            return invalid_options();
        }
        if (top_level && (*key == "model" || *key == "messages" || *key == "stream")) {
            return invalid_options();
        }
        RETURN_IF_ERROR(require_json_write(writer->Key(key->data(), key->size())));
        RETURN_IF_ERROR(write_value(map.values(), type.children[1], index, writer, nullptr));
    }
    return require_json_write(writer->EndObject());
}

template <typename ColumnType, typename Write>
Status write_fixed(const UnwrappedValue& value, Write&& write) {
    const auto* column = checked_column<ColumnType>(value);
    if (column == nullptr) {
        return invalid_options();
    }
    return require_json_write(write(column->immutable_data()[value.row]));
}

template <typename ColumnType, typename ValueType>
Status write_decimal(const UnwrappedValue& value, const TypeDescriptor& type, JsonWriter* writer) {
    const auto* column = checked_column<ColumnType>(value);
    if (column == nullptr) {
        return invalid_options();
    }
    const std::string number =
            DecimalV3Cast::to_string<ValueType>(column->immutable_data()[value.row], type.precision, type.scale);
    return require_json_write(writer->RawValue(number.data(), number.size(), rapidjson::kNumberType));
}

Status write_json(const UnwrappedValue& value, JsonWriter* writer, rapidjson::Type* root_type) {
    const auto* column = checked_column<JsonColumn>(value);
    if (column == nullptr || column->is_flat_json()) {
        return invalid_options();
    }
    auto json = column->get_object(value.row)->to_string();
    if (!json.ok() || json->find('\0') != std::string::npos) {
        return invalid_options();
    }

    rapidjson::Document document;
    document.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(json->data(),
                                                                                               json->size());
    if (document.HasParseError()) {
        return invalid_options();
    }
    if (root_type != nullptr) {
        *root_type = document.GetType();
    }
    return require_json_write(document.Accept(*writer));
}

Status write_value(const Column& input, const TypeDescriptor& type, size_t row, JsonWriter* writer,
                   rapidjson::Type* root_type) {
    auto unwrapped = unwrap_value(input, row);
    if (!unwrapped.ok()) {
        return unwrapped.status();
    }
    if (unwrapped->is_null || type.type == TYPE_NULL) {
        if (root_type != nullptr) {
            *root_type = rapidjson::kNullType;
        }
        return require_json_write(writer->Null());
    }

    switch (type.type) {
    case TYPE_BOOLEAN: {
        const auto* column = checked_column<BooleanColumn>(*unwrapped);
        if (column == nullptr) {
            return invalid_options();
        }
        const bool value = column->immutable_data()[unwrapped->row] != 0;
        if (root_type != nullptr) {
            *root_type = value ? rapidjson::kTrueType : rapidjson::kFalseType;
        }
        return require_json_write(writer->Bool(value));
    }
    case TYPE_TINYINT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<Int8Column>(*unwrapped, [writer](int8_t value) { return writer->Int(value); });
    case TYPE_UNSIGNED_TINYINT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<UInt8Column>(*unwrapped, [writer](uint8_t value) { return writer->Uint(value); });
    case TYPE_SMALLINT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<Int16Column>(*unwrapped, [writer](int16_t value) { return writer->Int(value); });
    case TYPE_UNSIGNED_SMALLINT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<UInt16Column>(*unwrapped, [writer](uint16_t value) { return writer->Uint(value); });
    case TYPE_INT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<Int32Column>(*unwrapped, [writer](int32_t value) { return writer->Int(value); });
    case TYPE_UNSIGNED_INT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<UInt32Column>(*unwrapped, [writer](uint32_t value) { return writer->Uint(value); });
    case TYPE_BIGINT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<Int64Column>(*unwrapped, [writer](int64_t value) { return writer->Int64(value); });
    case TYPE_UNSIGNED_BIGINT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<UInt64Column>(*unwrapped, [writer](uint64_t value) { return writer->Uint64(value); });
    case TYPE_LARGEINT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<Int128Column>(*unwrapped, [writer](int128_t value) {
            const std::string number = int128_to_string(value);
            return writer->RawValue(number.data(), number.size(), rapidjson::kNumberType);
        });
    case TYPE_INT256:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<Int256Column>(*unwrapped, [writer](const int256_t& value) {
            const std::string number = value.to_string();
            return writer->RawValue(number.data(), number.size(), rapidjson::kNumberType);
        });
    case TYPE_FLOAT:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        if (const auto* column = checked_column<FloatColumn>(*unwrapped); column != nullptr) {
            const float value = column->immutable_data()[unwrapped->row];
            if (!std::isfinite(value)) return invalid_options();
            return require_json_write(writer->Double(value));
        }
        return invalid_options();
    case TYPE_DOUBLE:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        if (const auto* column = checked_column<DoubleColumn>(*unwrapped); column != nullptr) {
            const double value = column->immutable_data()[unwrapped->row];
            if (!std::isfinite(value)) return invalid_options();
            return require_json_write(writer->Double(value));
        }
        return invalid_options();
    case TYPE_DECIMAL: {
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        using Decimal12Column = FixedLengthColumn<decimal12_t>;
        return write_fixed<Decimal12Column>(*unwrapped, [writer](const decimal12_t& value) {
            const std::string number = value.to_string();
            return writer->RawValue(number.data(), number.size(), rapidjson::kNumberType);
        });
    }
    case TYPE_DECIMALV2:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_fixed<DecimalColumn>(*unwrapped, [writer](const DecimalV2Value& value) {
            const std::string number = value.to_string();
            return writer->RawValue(number.data(), number.size(), rapidjson::kNumberType);
        });
    case TYPE_DECIMAL32:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_decimal<Decimal32Column, int32_t>(*unwrapped, type, writer);
    case TYPE_DECIMAL64:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_decimal<Decimal64Column, int64_t>(*unwrapped, type, writer);
    case TYPE_DECIMAL128:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_decimal<Decimal128Column, int128_t>(*unwrapped, type, writer);
    case TYPE_DECIMAL256:
        if (root_type != nullptr) *root_type = rapidjson::kNumberType;
        return write_decimal<Decimal256Column, int256_t>(*unwrapped, type, writer);
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        if (root_type != nullptr) {
            *root_type = rapidjson::kStringType;
        }
        if (const auto* column = checked_column<BinaryColumn>(*unwrapped); column != nullptr) {
            const Slice value = column->get_slice(unwrapped->row);
            return require_json_write(writer->String(value.data, value.size));
        }
        if (const auto* column = checked_column<LargeBinaryColumn>(*unwrapped); column != nullptr) {
            const Slice value = column->get_slice(unwrapped->row);
            return require_json_write(writer->String(value.data, value.size));
        }
        return invalid_options();
    }
    case TYPE_JSON:
        return write_json(*unwrapped, writer, root_type);
    case TYPE_ARRAY: {
        if (type.children.size() != 1) {
            return invalid_options();
        }
        const auto* array = checked_column<ArrayColumn>(*unwrapped);
        if (array == nullptr) {
            return invalid_options();
        }
        if (root_type != nullptr) *root_type = rapidjson::kArrayType;
        const auto [offset, count] = array->get_element_offset_size(unwrapped->row);
        if (offset > array->elements().size() || count > array->elements().size() - offset) {
            return invalid_options();
        }
        RETURN_IF_ERROR(require_json_write(writer->StartArray()));
        for (size_t index = offset; index < offset + count; ++index) {
            RETURN_IF_ERROR(write_value(array->elements(), type.children[0], index, writer, nullptr));
        }
        return require_json_write(writer->EndArray());
    }
    case TYPE_MAP: {
        const auto* map = checked_column<MapColumn>(*unwrapped);
        if (map == nullptr) {
            return invalid_options();
        }
        if (root_type != nullptr) *root_type = rapidjson::kObjectType;
        return validate_and_write_map(*map, type, unwrapped->row, false, writer);
    }
    case TYPE_STRUCT: {
        const auto* structure = checked_column<StructColumn>(*unwrapped);
        if (structure == nullptr || structure->fields_size() != type.children.size() ||
            type.field_names.size() != type.children.size()) {
            return invalid_options();
        }
        if (root_type != nullptr) *root_type = rapidjson::kObjectType;
        RETURN_IF_ERROR(require_json_write(writer->StartObject()));
        for (size_t index = 0; index < type.children.size(); ++index) {
            RETURN_IF_ERROR(
                    require_json_write(writer->Key(type.field_names[index].data(), type.field_names[index].size())));
            RETURN_IF_ERROR(write_value(*structure->get_column_by_idx(index), type.children[index], unwrapped->row,
                                        writer, nullptr));
        }
        return require_json_write(writer->EndObject());
    }
    default:
        return invalid_options();
    }
}

AIProviderOptionKind provider_option_kind(rapidjson::Type type) {
    switch (type) {
    case rapidjson::kNullType:
        return AIProviderOptionKind::NULL_VALUE;
    case rapidjson::kFalseType:
        return AIProviderOptionKind::FALSE_VALUE;
    case rapidjson::kTrueType:
        return AIProviderOptionKind::TRUE_VALUE;
    case rapidjson::kObjectType:
        return AIProviderOptionKind::OBJECT;
    case rapidjson::kArrayType:
        return AIProviderOptionKind::ARRAY;
    case rapidjson::kStringType:
        return AIProviderOptionKind::STRING;
    case rapidjson::kNumberType:
        return AIProviderOptionKind::NUMBER;
    }
    return AIProviderOptionKind::NULL_VALUE;
}

StatusOr<std::pair<std::string, AIProviderOptionKind>> serialize_value(const Column& column, const TypeDescriptor& type,
                                                                       size_t row) {
    JsonBuffer buffer;
    JsonWriter writer(buffer);
    rapidjson::Type value_type = rapidjson::kNullType;
    RETURN_IF_ERROR(write_value(column, type, row, &writer, &value_type));
    if (!writer.IsComplete()) {
        return invalid_options();
    }
    return std::make_pair(std::string(buffer.GetString(), buffer.GetSize()), provider_option_kind(value_type));
}

} // namespace

StatusOr<AIProviderOptions> build_ai_provider_options(const Column& column, const TypeDescriptor& type, size_t row) {
    if (type.type != TYPE_MAP) {
        return invalid_options();
    }
    auto unwrapped = unwrap_value(column, row);
    if (!unwrapped.ok()) {
        return unwrapped.status();
    }

    AIProviderOptions::Members members;
    if (unwrapped->is_null) {
        return AIProviderOptions::create(std::move(members));
    }

    const auto* map = checked_column<MapColumn>(*unwrapped);
    if (map == nullptr) {
        return invalid_options();
    }
    const auto [offset, count] = map->get_map_offset_size(unwrapped->row);
    if (offset > map->keys().size() || count > map->keys().size() - offset || offset > map->values().size() ||
        count > map->values().size() - offset) {
        return invalid_options();
    }
    const bool untyped_empty = count == 0 && is_untyped_map(type);
    if (!untyped_empty &&
        (type.children.size() != 2 || type.children[0].type != TYPE_VARCHAR || !has_binary_storage(map->keys()))) {
        return invalid_options();
    }
    if (count == 0) {
        return AIProviderOptions::create(std::move(members));
    }

    std::unordered_set<std::string> keys;
    members.reserve(count);
    for (size_t index = offset; index < offset + count; ++index) {
        auto key = read_map_key(map->keys(), index);
        if (!key.ok() || key->empty() || !keys.emplace(*key).second || *key == "model" || *key == "messages" ||
            *key == "stream") {
            return invalid_options();
        }
        auto value = serialize_value(map->values(), type.children[1], index);
        if (!value.ok()) {
            return value.status();
        }
        members.emplace_back(AIProviderOption{
                .key = std::move(*key), .serialized_json = std::move(value->first), .kind = value->second});
    }

    return AIProviderOptions::create(std::move(members));
}

} // namespace starrocks
