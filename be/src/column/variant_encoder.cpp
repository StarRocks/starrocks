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

#include "column/variant_encoder.h"

#include <arrow/util/endian.h>
#include <velocypack/vpack.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "base/decimal_types.h"
#include "base/status.h"
#include "base/string/slice.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "types/time_types.h"
#include "types/type_descriptor.h"
#include "types/type_info.h"
#include "types/variant.h"

namespace starrocks {

constexpr uint8_t kVariantMetadataVersion = 1;
constexpr uint8_t kVariantMetadataSortedMask = 0b10000;
constexpr uint8_t kVariantMetadataOffsetSizeShift = 6;

// Appends `value` to `out` in little-endian byte order.
// arrow::bit_util::ToLittleEndian is available via the transitive include chain:
//   types/variant_value.h → types/variant.h → <arrow/util/endian.h>
template <typename T>
inline void append_little_endian(std::string* out, T value) {
    T le_value = arrow::bit_util::ToLittleEndian(value);
    out->append(reinterpret_cast<const char*>(&le_value), sizeof(T));
}

inline std::string encode_null_value() {
    return std::string(VariantValue::kEmptyValue);
}

void VariantEncoder::append_array_container(std::string* out, const std::vector<uint32_t>& end_offsets,
                                            std::string_view payload) {
    const uint32_t num_elements = static_cast<uint32_t>(end_offsets.size());
    const uint8_t is_large = num_elements > 255 ? 1 : 0;
    const uint8_t num_elements_size = is_large ? 4 : 1;
    const uint8_t offset_size = VariantEncoder::minimal_uint_size(static_cast<uint32_t>(payload.size()));
    const uint8_t vheader = static_cast<uint8_t>((offset_size - 1) | (is_large << 2));
    const char header = static_cast<char>((vheader << VariantValue::kValueHeaderBitShift) |
                                          static_cast<uint8_t>(VariantValue::BasicType::ARRAY));

    out->reserve(out->size() + 1 + num_elements_size + (num_elements + 1) * offset_size + payload.size());
    out->push_back(header);
    VariantEncoder::append_uint_le(out, num_elements, num_elements_size);
    VariantEncoder::append_uint_le(out, 0, offset_size);
    for (uint32_t end_offset : end_offsets) {
        VariantEncoder::append_uint_le(out, end_offset, offset_size);
    }
    out->append(payload.data(), payload.size());
}

void VariantEncoder::append_object_container(std::string* out, const std::vector<uint32_t>& field_ids,
                                             const std::vector<uint32_t>& end_offsets, std::string_view payload) {
    DCHECK_EQ(field_ids.size(), end_offsets.size());
    const uint32_t num_elements = static_cast<uint32_t>(field_ids.size());
    const uint8_t is_large = num_elements > 255 ? 1 : 0;
    const uint8_t num_elements_size = is_large ? 4 : 1;

    uint32_t max_field_id = 0;
    for (uint32_t field_id : field_ids) {
        max_field_id = std::max(max_field_id, field_id);
    }
    const uint8_t field_id_size = VariantEncoder::minimal_uint_size(max_field_id);
    const uint8_t field_offset_size = VariantEncoder::minimal_uint_size(static_cast<uint32_t>(payload.size()));
    const uint8_t vheader =
            static_cast<uint8_t>((field_offset_size - 1) | ((field_id_size - 1) << 2) | (is_large << 4));
    const char header = static_cast<char>((vheader << VariantValue::kValueHeaderBitShift) |
                                          static_cast<uint8_t>(VariantValue::BasicType::OBJECT));

    out->reserve(out->size() + 1 + num_elements_size + num_elements * field_id_size +
                 (num_elements + 1) * field_offset_size + payload.size());
    out->push_back(header);
    VariantEncoder::append_uint_le(out, num_elements, num_elements_size);
    for (uint32_t field_id : field_ids) {
        VariantEncoder::append_uint_le(out, field_id, field_id_size);
    }
    VariantEncoder::append_uint_le(out, 0, field_offset_size);
    for (uint32_t end_offset : end_offsets) {
        VariantEncoder::append_uint_le(out, end_offset, field_offset_size);
    }
    out->append(payload.data(), payload.size());
}

static std::string key_datum_to_string(TypeInfo* type_info, const Datum& datum) {
    if (datum.is_null()) {
        return "null";
    }

    switch (type_info->type()) {
    case TYPE_BOOLEAN: {
        auto v = datum.get_int8();
        return type_info->to_string(&v);
    }
    case TYPE_TINYINT: {
        auto v = datum.get_int8();
        return type_info->to_string(&v);
    }
    case TYPE_SMALLINT: {
        auto v = datum.get_int16();
        return type_info->to_string(&v);
    }
    case TYPE_INT: {
        auto v = datum.get_int32();
        return type_info->to_string(&v);
    }
    case TYPE_BIGINT: {
        auto v = datum.get_int64();
        return type_info->to_string(&v);
    }
    case TYPE_LARGEINT: {
        auto v = datum.get_int128();
        return type_info->to_string(&v);
    }
    case TYPE_FLOAT: {
        auto v = datum.get_float();
        return type_info->to_string(&v);
    }
    case TYPE_DOUBLE: {
        auto v = datum.get_double();
        return type_info->to_string(&v);
    }
    case TYPE_DATE: {
        auto v = datum.get_date();
        return type_info->to_string(&v);
    }
    case TYPE_DATETIME: {
        auto v = datum.get_timestamp();
        return type_info->to_string(&v);
    }
    case TYPE_DECIMALV2: {
        auto v = datum.get_decimal();
        return type_info->to_string(&v);
    }
    case TYPE_DECIMAL32: {
        auto v = datum.get_int32();
        return type_info->to_string(&v);
    }
    case TYPE_DECIMAL64: {
        auto v = datum.get_int64();
        return type_info->to_string(&v);
    }
    case TYPE_DECIMAL128: {
        auto v = datum.get_int128();
        return type_info->to_string(&v);
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY: {
        auto v = datum.get_slice();
        return type_info->to_string(&v);
    }
    default:
        return "";
    }
}

static std::string encode_boolean(bool value) {
    const auto type = value ? VariantType::BOOLEAN_TRUE : VariantType::BOOLEAN_FALSE;
    char header = static_cast<char>((static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::PRIMITIVE));
    return std::string(1, header);
}

static void append_int128_le(std::string& out, int128_t value) {
    int64_t low = static_cast<int64_t>(value);
    int64_t high = static_cast<int64_t>(value >> 64);
    append_little_endian(&out, low);
    append_little_endian(&out, high);
}

static std::string encode_string_value(const Slice& value) {
    std::string out;
    const size_t len = value.size;
    if (len <= 63) {
        uint8_t header = static_cast<uint8_t>((len << VariantValue::kValueHeaderBitShift) |
                                              static_cast<uint8_t>(VariantValue::BasicType::SHORT_STRING));
        out.push_back(static_cast<char>(header));
        out.append(value.data, len);
    } else {
        uint8_t header = static_cast<uint8_t>((static_cast<uint8_t>(VariantType::STRING) << 2) |
                                              static_cast<uint8_t>(VariantValue::BasicType::PRIMITIVE));
        out.push_back(static_cast<char>(header));
        VariantEncoder::append_uint_le(&out, static_cast<uint32_t>(len), sizeof(uint32_t));
        out.append(value.data, len);
    }
    return out;
}

template <LogicalType LT, typename EncodeValueFn>
Status encode_column_with_viewer(const ColumnPtr& column, ColumnBuilder<TYPE_VARIANT>* builder,
                                 bool allow_throw_exception, EncodeValueFn&& encode_value) {
    const size_t num_rows = column->size();
    ColumnPtr data_column = column;
    bool is_const = false;

    if (data_column->is_constant()) {
        const auto* const_col = down_cast<const ConstColumn*>(data_column.get());
        if (const_col->only_null()) {
            for (size_t i = 0; i < num_rows; ++i) {
                builder->append_null();
            }
            return Status::OK();
        }
        data_column = const_col->data_column();
        is_const = true;
    }

    const NullableColumn* nullable = nullptr;
    if (data_column->is_nullable()) {
        nullable = down_cast<const NullableColumn*>(data_column.get());
        data_column = nullable->data_column();
    }

    auto typed_column = ColumnHelper::cast_to<LT>(data_column);
    const auto& data = typed_column->immutable_data();
    for (size_t row = 0; row < num_rows; ++row) {
        const size_t data_row = is_const ? 0 : row;
        if (nullable && nullable->is_null(data_row)) {
            builder->append_null();
            continue;
        }

        bool is_null = false;
        StatusOr<VariantRowValue> encoded = encode_value(data, data_row, &is_null);
        if (!encoded.ok()) {
            if (allow_throw_exception) {
                return encoded.status();
            }
            builder->append_null();
            continue;
        }
        if (is_null) {
            builder->append_null();
            continue;
        }
        builder->append(std::move(encoded.value()));
    }
    return Status::OK();
}

template <typename T>
std::string encode_primitive_number(VariantType type, T value) {
    std::string out;
    out.reserve(VariantValue::kHeaderSizeBytes + sizeof(T));
    char header = static_cast<char>((static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::PRIMITIVE));
    out.push_back(header);
    append_little_endian(&out, value);
    return out;
}

std::string encode_string_or_binary(VariantType type, std::string_view value) {
    std::string out;
    out.reserve(VariantValue::kHeaderSizeBytes + sizeof(uint32_t) + value.size());
    char header = static_cast<char>((static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::PRIMITIVE));
    out.push_back(header);
    append_little_endian<uint32_t>(&out, static_cast<uint32_t>(value.size()));
    out.append(value.data(), value.size());
    return out;
}

std::string VariantEncoder::encode_object_from_fields(const std::map<uint32_t, std::string>& fields) {
    std::string out;
    std::string payload;
    std::vector<uint32_t> field_ids;
    std::vector<uint32_t> end_offsets;
    field_ids.reserve(fields.size());
    end_offsets.reserve(fields.size());
    for (const auto& [field_id, value] : fields) {
        payload.append(value.data(), value.size());
        field_ids.emplace_back(field_id);
        end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
    }
    VariantEncoder::append_object_container(&out, field_ids, end_offsets, payload);
    return out;
}

std::string VariantEncoder::encode_array_from_elements(const std::vector<std::string>& elements) {
    std::string out;
    std::string payload;
    std::vector<uint32_t> end_offsets;
    end_offsets.reserve(elements.size());
    for (const auto& element : elements) {
        payload.append(element.data(), element.size());
        end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
    }
    VariantEncoder::append_array_container(&out, end_offsets, payload);
    return out;
}

StatusOr<std::string> VariantEncoder::build_variant_metadata(const std::unordered_set<std::string>& keys,
                                                             std::unordered_map<std::string, uint32_t>* key_to_id) {
    if (keys.empty()) {
        return std::string(VariantMetadata::kEmptyMetadata.data(), VariantMetadata::kEmptyMetadata.size());
    }

    std::vector<std::string> sorted_keys;
    sorted_keys.reserve(keys.size());
    for (const auto& key : keys) {
        sorted_keys.push_back(key);
    }
    std::sort(sorted_keys.begin(), sorted_keys.end());

    uint32_t total_string_size = 0;
    for (const auto& key : sorted_keys) {
        if (key.size() > std::numeric_limits<uint32_t>::max() - total_string_size) {
            return Status::VariantError("Variant metadata string size overflow");
        }
        total_string_size += static_cast<uint32_t>(key.size());
    }

    const uint32_t dict_size = static_cast<uint32_t>(sorted_keys.size());
    const uint32_t max_value = std::max(dict_size, total_string_size);
    const uint8_t offset_size = VariantEncoder::minimal_uint_size(max_value);

    uint8_t header = kVariantMetadataVersion;
    header |= kVariantMetadataSortedMask;
    header |= static_cast<uint8_t>((offset_size - 1) << kVariantMetadataOffsetSizeShift);

    std::string metadata;
    metadata.reserve(1 + offset_size * (dict_size + 2) + total_string_size);
    metadata.push_back(static_cast<char>(header));
    VariantEncoder::append_uint_le(&metadata, dict_size, offset_size);

    uint32_t offset = 0;
    VariantEncoder::append_uint_le(&metadata, offset, offset_size);
    for (const auto& key : sorted_keys) {
        offset += static_cast<uint32_t>(key.size());
        VariantEncoder::append_uint_le(&metadata, offset, offset_size);
    }
    for (const auto& key : sorted_keys) {
        metadata.append(key.data(), key.size());
    }

    if (key_to_id != nullptr) {
        key_to_id->reserve(sorted_keys.size());
        for (uint32_t i = 0; i < sorted_keys.size(); ++i) {
            key_to_id->emplace(sorted_keys[i], i);
        }
    }
    return std::move(metadata);
}

Status collect_object_keys(const vpack::Slice& slice, std::unordered_set<std::string>* keys) {
    try {
        if (slice.isObject()) {
            vpack::ObjectIterator it(slice);
            for (; it.valid(); it.next()) {
                auto current = *it;
                auto key = current.key.stringView();
                keys->emplace(key.data(), key.size());
                RETURN_IF_ERROR(collect_object_keys(current.value, keys));
            }
        } else if (slice.isArray()) {
            vpack::ArrayIterator it(slice);
            for (; it.valid(); it.next()) {
                RETURN_IF_ERROR(collect_object_keys(it.value(), keys));
            }
        }
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }

    return Status::OK();
}

template <typename T>
inline void append_primitive_number(std::string* out, VariantType type, T value) {
    char header = static_cast<char>((static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::PRIMITIVE));
    out->push_back(header);
    append_little_endian(out, value);
}

inline void append_boolean_value(std::string* out, bool value) {
    const auto type = value ? VariantType::BOOLEAN_TRUE : VariantType::BOOLEAN_FALSE;
    char header = static_cast<char>((static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::PRIMITIVE));
    out->push_back(header);
}

inline void append_string_or_binary_value(std::string* out, VariantType type, std::string_view value) {
    char header = static_cast<char>((static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::PRIMITIVE));
    out->push_back(header);
    append_little_endian<uint32_t>(out, static_cast<uint32_t>(value.size()));
    out->append(value.data(), value.size());
}

inline void append_string_value(std::string* out, const Slice& value) {
    const size_t len = value.size;
    if (len <= 63) {
        uint8_t header = static_cast<uint8_t>((len << VariantValue::kValueHeaderBitShift) |
                                              static_cast<uint8_t>(VariantValue::BasicType::SHORT_STRING));
        out->push_back(static_cast<char>(header));
        out->append(value.data, len);
    } else {
        uint8_t header = static_cast<uint8_t>((static_cast<uint8_t>(VariantType::STRING) << 2) |
                                              static_cast<uint8_t>(VariantValue::BasicType::PRIMITIVE));
        out->push_back(static_cast<char>(header));
        VariantEncoder::append_uint_le(out, static_cast<uint32_t>(len), sizeof(uint32_t));
        out->append(value.data, len);
    }
}

Status encode_json_to_variant_value(const vpack::Slice& slice,
                                    const std::unordered_map<std::string, uint32_t>& key_to_id, std::string* out) {
    try {
        if (slice.isNone() || slice.isNull()) {
            VariantEncoder::append_null_value(out);
            return Status::OK();
        }
        if (slice.isBool()) {
            append_boolean_value(out, slice.getBool());
            return Status::OK();
        }
        if (slice.isInt() || slice.isSmallInt()) {
            int64_t v = slice.getIntUnchecked();
            if (v >= std::numeric_limits<int8_t>::min() && v <= std::numeric_limits<int8_t>::max())
                append_primitive_number<int8_t>(out, VariantType::INT8, static_cast<int8_t>(v));
            else if (v >= std::numeric_limits<int16_t>::min() && v <= std::numeric_limits<int16_t>::max())
                append_primitive_number<int16_t>(out, VariantType::INT16, static_cast<int16_t>(v));
            else if (v >= std::numeric_limits<int32_t>::min() && v <= std::numeric_limits<int32_t>::max())
                append_primitive_number<int32_t>(out, VariantType::INT32, static_cast<int32_t>(v));
            else
                append_primitive_number<int64_t>(out, VariantType::INT64, v);
            return Status::OK();
        }
        if (slice.isUInt()) {
            uint64_t value = slice.getUIntUnchecked();
            if (value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                append_primitive_number<int64_t>(out, VariantType::INT64, static_cast<int64_t>(value));
                return Status::OK();
            }
            append_primitive_number<double>(out, VariantType::DOUBLE, static_cast<double>(value));
            return Status::OK();
        }
        if (slice.isDouble()) {
            append_primitive_number<double>(out, VariantType::DOUBLE, slice.getDouble());
            return Status::OK();
        }
        if (slice.isString() || slice.isBinary()) {
            vpack::ValueLength len;
            const char* str = slice.getStringUnchecked(len);
            append_string_or_binary_value(out, VariantType::STRING, std::string_view(str, len));
            return Status::OK();
        }
        if (slice.isArray()) {
            vpack::ArrayIterator it(slice);
            std::string payload;
            std::vector<uint32_t> end_offsets;
            end_offsets.reserve(slice.length());
            for (; it.valid(); it.next()) {
                RETURN_IF_ERROR(encode_json_to_variant_value(it.value(), key_to_id, &payload));
                end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
            }
            VariantEncoder::append_array_container(out, end_offsets, payload);
            return Status::OK();
        }
        if (slice.isObject()) {
            vpack::ObjectIterator it(slice);
            std::map<uint32_t, vpack::Slice> fields;
            for (; it.valid(); it.next()) {
                auto current = *it;
                auto key = current.key.stringView();
                auto it_key = key_to_id.find(std::string(key.data(), key.size()));
                if (it_key == key_to_id.end()) {
                    return Status::VariantError("Variant metadata missing field: " +
                                                std::string(key.data(), key.size()));
                }
                fields[it_key->second] = current.value;
            }
            std::string payload;
            payload.reserve(slice.byteSize());
            std::vector<uint32_t> field_ids;
            std::vector<uint32_t> end_offsets;
            field_ids.reserve(fields.size());
            end_offsets.reserve(fields.size());
            for (const auto& [field_id, field_value] : fields) {
                RETURN_IF_ERROR(encode_json_to_variant_value(field_value, key_to_id, &payload));
                field_ids.emplace_back(field_id);
                end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
            }
            VariantEncoder::append_object_container(out, field_ids, end_offsets, payload);
            return Status::OK();
        }
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }

    return Status::NotSupported(
            fmt::format("Unsupported JSON type {} for variant encoding type", valueTypeName(slice.type())));
}

StatusOr<std::string> encode_json_to_variant_value(const vpack::Slice& slice,
                                                   const std::unordered_map<std::string, uint32_t>& key_to_id) {
    std::string out;
    RETURN_IF_ERROR(encode_json_to_variant_value(slice, key_to_id, &out));
    return out;
}

Status collect_object_keys_from_datum(const Datum& datum, const TypeDescriptor& type,
                                      std::unordered_set<std::string>* keys) {
    if (datum.is_null()) {
        return Status::OK();
    }

    switch (type.type) {
    case TYPE_ARRAY: {
        const auto& elements = datum.get_array();
        const auto& element_type = type.children[0];
        for (const auto& element : elements) {
            RETURN_IF_ERROR(collect_object_keys_from_datum(element, element_type, keys));
        }
        return Status::OK();
    }
    case TYPE_MAP: {
        const auto& map = datum.get_map();
        const auto& key_type = type.children[0];
        const auto& value_type = type.children[1];
        const TypeInfoPtr key_type_info = get_type_info(key_type);
        for (const auto& [key, value] : map) {
            const Datum key_datum(key);
            if (key_datum.is_null()) {
                return Status::NotSupported("Map key is null");
            }
            keys->insert(key_datum_to_string(key_type_info.get(), key_datum));
            RETURN_IF_ERROR(collect_object_keys_from_datum(value, value_type, keys));
        }
        return Status::OK();
    }
    case TYPE_STRUCT: {
        const auto& fields = datum.get_struct();
        DCHECK_EQ(type.field_names.size(), type.children.size());
        if (fields.size() != type.children.size()) {
            return Status::InvalidArgument(fmt::format("Struct field size {} does not match type children size {}",
                                                       fields.size(), type.children.size()));
        }
        if (type.field_names.size() != fields.size()) {
            return Status::InvalidArgument(fmt::format("Struct field names size {} does not match field count {}",
                                                       type.field_names.size(), fields.size()));
        }
        for (size_t i = 0; i < fields.size(); ++i) {
            const std::string& field_name = type.field_names[i];
            keys->insert(field_name);
            RETURN_IF_ERROR(collect_object_keys_from_datum(fields[i], type.children[i], keys));
        }
        return Status::OK();
    }
    case TYPE_JSON: {
        const JsonValue* json = datum.get_json();
        if (json != nullptr && !json->is_null_or_none()) {
            return collect_object_keys(json->to_vslice(), keys);
        }
        return Status::OK();
    }
    default:
        return Status::OK();
    }
}

Status encode_datum_to_variant_value(const Datum& datum, const TypeDescriptor& type,
                                     const std::unordered_map<std::string, uint32_t>& key_to_id, std::string* out) {
    if (datum.is_null()) {
        VariantEncoder::append_null_value(out);
        return Status::OK();
    }

    switch (type.type) {
    case TYPE_NULL:
        VariantEncoder::append_null_value(out);
        return Status::OK();
    case TYPE_BOOLEAN:
        append_boolean_value(out, datum.get<int8_t>() != 0);
        return Status::OK();
    case TYPE_TINYINT:
        append_primitive_number<int8_t>(out, VariantType::INT8, datum.get_int8());
        return Status::OK();
    case TYPE_SMALLINT:
        append_primitive_number<int16_t>(out, VariantType::INT16, datum.get_int16());
        return Status::OK();
    case TYPE_INT:
        append_primitive_number<int32_t>(out, VariantType::INT32, datum.get_int32());
        return Status::OK();
    case TYPE_BIGINT:
        append_primitive_number<int64_t>(out, VariantType::INT64, datum.get_int64());
        return Status::OK();
    case TYPE_LARGEINT: {
        uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL16) << VariantValue::kValueHeaderBitShift;
        out->push_back(static_cast<char>(header));
        out->push_back(0); // scale
        append_int128_le(*out, datum.get_int128());
        return Status::OK();
    }
    case TYPE_FLOAT:
        append_primitive_number<float>(out, VariantType::FLOAT, datum.get_float());
        return Status::OK();
    case TYPE_DOUBLE:
        append_primitive_number<double>(out, VariantType::DOUBLE, datum.get_double());
        return Status::OK();
    case TYPE_DECIMALV2: {
        uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL16) << VariantValue::kValueHeaderBitShift;
        out->push_back(static_cast<char>(header));
        out->push_back(static_cast<char>(DecimalV2Value::SCALE));
        append_int128_le(*out, datum.get_decimal().value());
        return Status::OK();
    }
    case TYPE_DECIMAL32: {
        if (type.scale < 0 || type.scale > decimal_precision_limit<int32_t>) {
            return Status::InvalidArgument(fmt::format("Invalid decimal32 scale for variant encoding: {}", type.scale));
        }
        uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL4) << VariantValue::kValueHeaderBitShift;
        out->push_back(static_cast<char>(header));
        out->push_back(static_cast<char>(type.scale));
        append_little_endian(out, datum.get_int32());
        return Status::OK();
    }
    case TYPE_DECIMAL64: {
        if (type.scale < 0 || type.scale > decimal_precision_limit<int64_t>) {
            return Status::InvalidArgument(fmt::format("Invalid decimal64 scale for variant encoding: {}", type.scale));
        }
        uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL8) << VariantValue::kValueHeaderBitShift;
        out->push_back(static_cast<char>(header));
        out->push_back(static_cast<char>(type.scale));
        append_little_endian(out, datum.get_int64());
        return Status::OK();
    }
    case TYPE_DECIMAL128: {
        if (type.scale < 0 || type.scale > decimal_precision_limit<int128_t>) {
            return Status::InvalidArgument(
                    fmt::format("Invalid decimal128 scale for variant encoding: {}", type.scale));
        }
        uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL16) << VariantValue::kValueHeaderBitShift;
        out->push_back(static_cast<char>(header));
        out->push_back(static_cast<char>(type.scale));
        append_int128_le(*out, datum.get_int128());
        return Status::OK();
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        append_string_value(out, datum.get_slice());
        return Status::OK();
    case TYPE_JSON: {
        const JsonValue* json = datum.get_json();
        if (json == nullptr || json->is_null_or_none()) {
            VariantEncoder::append_null_value(out);
            return Status::OK();
        }
        return encode_json_to_variant_value(json->to_vslice(), key_to_id, out);
    }
    case TYPE_DATE: {
        int32_t days = datum.get_date().to_days_since_unix_epoch();
        uint8_t header = static_cast<uint8_t>(VariantType::DATE) << VariantValue::kValueHeaderBitShift;
        out->push_back(static_cast<char>(header));
        append_little_endian(out, days);
        return Status::OK();
    }
    case TYPE_DATETIME: {
        int64_t micros = datum.get_timestamp().to_unix_microsecond();
        uint8_t header = static_cast<uint8_t>(VariantType::TIMESTAMP_NTZ) << VariantValue::kValueHeaderBitShift;
        out->push_back(static_cast<char>(header));
        append_little_endian(out, micros);
        return Status::OK();
    }
    case TYPE_TIME: {
        double seconds = datum.get_double();
        int64_t micros = static_cast<int64_t>(seconds * USECS_PER_SEC);
        uint8_t header = static_cast<uint8_t>(VariantType::TIME_NTZ) << VariantValue::kValueHeaderBitShift;
        out->push_back(static_cast<char>(header));
        append_little_endian(out, micros);
        return Status::OK();
    }
    case TYPE_ARRAY: {
        const auto& elements = datum.get_array();
        const auto& element_type = type.children[0];
        std::string payload;
        std::vector<uint32_t> end_offsets;
        end_offsets.reserve(elements.size());
        for (const auto& element : elements) {
            RETURN_IF_ERROR(encode_datum_to_variant_value(element, element_type, key_to_id, &payload));
            end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
        }
        VariantEncoder::append_array_container(out, end_offsets, payload);
        return Status::OK();
    }
    case TYPE_MAP: {
        const auto& map = datum.get_map();
        const auto& key_type = type.children[0];
        const auto& value_type = type.children[1];
        const TypeInfoPtr key_type_info = get_type_info(key_type);
        std::map<uint32_t, const Datum*> fields;
        for (const auto& [key, value] : map) {
            const Datum key_datum(key);
            if (key_datum.is_null()) {
                return Status::NotSupported("Map key is null");
            }
            std::string key_str = key_datum_to_string(key_type_info.get(), key_datum);
            auto it = key_to_id.find(key_str);
            if (it == key_to_id.end()) {
                return Status::VariantError("Variant metadata missing field: " + key_str);
            }
            fields[it->second] = &value;
        }
        std::string payload;
        std::vector<uint32_t> field_ids;
        std::vector<uint32_t> end_offsets;
        field_ids.reserve(fields.size());
        end_offsets.reserve(fields.size());
        for (const auto& [field_id, field_value] : fields) {
            RETURN_IF_ERROR(encode_datum_to_variant_value(*field_value, value_type, key_to_id, &payload));
            field_ids.emplace_back(field_id);
            end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
        }
        VariantEncoder::append_object_container(out, field_ids, end_offsets, payload);
        return Status::OK();
    }
    case TYPE_STRUCT: {
        const auto& fields = datum.get_struct();
        DCHECK_EQ(type.field_names.size(), type.children.size());
        if (fields.size() != type.children.size()) {
            return Status::InvalidArgument(fmt::format("Struct field size {} does not match type children size {}",
                                                       fields.size(), type.children.size()));
        }
        if (type.field_names.size() != fields.size()) {
            return Status::InvalidArgument(fmt::format("Struct field names size {} does not match field count {}",
                                                       type.field_names.size(), fields.size()));
        }
        std::vector<std::pair<uint32_t, size_t>> ordered_fields;
        ordered_fields.reserve(fields.size());
        for (size_t i = 0; i < fields.size(); ++i) {
            const std::string& field_name = type.field_names[i];
            auto it = key_to_id.find(field_name);
            if (it == key_to_id.end()) {
                return Status::VariantError("Variant metadata missing field: " + field_name);
            }
            ordered_fields.emplace_back(it->second, i);
        }
        std::sort(ordered_fields.begin(), ordered_fields.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
        std::string payload;
        std::vector<uint32_t> field_ids;
        std::vector<uint32_t> end_offsets;
        field_ids.reserve(ordered_fields.size());
        end_offsets.reserve(ordered_fields.size());
        for (const auto& [field_id, field_index] : ordered_fields) {
            RETURN_IF_ERROR(encode_datum_to_variant_value(fields[field_index], type.children[field_index], key_to_id,
                                                          &payload));
            field_ids.emplace_back(field_id);
            end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
        }
        VariantEncoder::append_object_container(out, field_ids, end_offsets, payload);
        return Status::OK();
    }
    default:
        return Status::NotSupported(fmt::format("Unsupported type {} for variant encoding", type.debug_string()));
    }
}

StatusOr<std::string> encode_datum_to_variant_value(const Datum& datum, const TypeDescriptor& type,
                                                    const std::unordered_map<std::string, uint32_t>& key_to_id) {
    std::string out;
    RETURN_IF_ERROR(encode_datum_to_variant_value(datum, type, key_to_id, &out));
    return out;
}

StatusOr<VariantRowValue> VariantEncoder::encode_datum(const Datum& datum, const TypeDescriptor& type) {
    if (datum.is_null()) {
        return VariantRowValue::from_null();
    }
    std::unordered_set<std::string> keys;
    RETURN_IF_ERROR(collect_object_keys_from_datum(datum, type, &keys));
    std::unordered_map<std::string, uint32_t> key_to_id;
    ASSIGN_OR_RETURN(auto metadata, build_variant_metadata(keys, &key_to_id));
    ASSIGN_OR_RETURN(std::string value, encode_datum_to_variant_value(datum, type, key_to_id));
    return VariantRowValue::create(metadata, std::string_view(value));
}

StatusOr<VariantRowValue> VariantEncoder::encode_json_to_variant(const JsonValue& json) {
    return encode_vslice_to_variant(json.to_vslice());
}

StatusOr<VariantRowValue> VariantEncoder::encode_vslice_to_variant(const vpack::Slice& slice) {
    std::unordered_set<std::string> keys;
    RETURN_IF_ERROR(collect_object_keys(slice, &keys));
    std::unordered_map<std::string, uint32_t> key_to_id;
    ASSIGN_OR_RETURN(auto metadata, build_variant_metadata(keys, &key_to_id));
    ASSIGN_OR_RETURN(std::string value, encode_json_to_variant_value(slice, key_to_id));
    return VariantRowValue::create(metadata, value);
}

StatusOr<VariantRowValue> VariantEncoder::encode_json_text_to_variant(std::string_view json_text) {
    if (json_text.empty()) {
        return encode_json_to_variant(JsonValue::from_string(Slice("", 0)));
    }
    try {
        auto builder = vpack::Parser::fromJson(json_text.data(), json_text.size());
        JsonValue parsed;
        parsed.assign(*builder);
        return encode_json_to_variant(parsed);
    } catch (const vpack::Exception&) {
        // Keep backward compatibility for plain scalar text input (for example "abc").
        return encode_json_to_variant(JsonValue::from_string(Slice(json_text.data(), json_text.size())));
    }
}

Status VariantEncoder::encode_column(const ColumnPtr& column, const TypeDescriptor& type,
                                     ColumnBuilder<TYPE_VARIANT>* builder, bool allow_throw_exception) {
    const size_t num_rows = column->size();
    if (num_rows == 0) {
        return Status::OK();
    }

    if (type.type == TYPE_ARRAY || type.type == TYPE_MAP || type.type == TYPE_STRUCT) {
        ColumnPtr data_column = column;
        bool is_const = false;

        if (data_column->is_constant()) {
            const auto* const_col = down_cast<const ConstColumn*>(data_column.get());
            if (const_col->only_null()) {
                builder->append_nulls(static_cast<int>(num_rows));
                return Status::OK();
            }
            data_column = const_col->data_column();
            is_const = true;
        }

        const NullableColumn* nullable = nullptr;
        if (data_column->is_nullable()) {
            nullable = down_cast<const NullableColumn*>(data_column.get());
            data_column = nullable->data_column();
        }

        for (size_t row = 0; row < num_rows; ++row) {
            const size_t data_row = is_const ? 0 : row;
            if (nullable && nullable->is_null(data_row)) {
                builder->append_null();
                continue;
            }

            const Datum datum = data_column->get(data_row);
            std::unordered_set<std::string> keys;
            Status collect_status = collect_object_keys_from_datum(datum, type, &keys);
            if (!collect_status.ok()) {
                if (allow_throw_exception) {
                    return collect_status;
                }
                builder->append_null();
                continue;
            }

            std::unordered_map<std::string, uint32_t> key_to_id;
            auto metadata_result = build_variant_metadata(keys, &key_to_id);
            if (!metadata_result.ok()) {
                if (allow_throw_exception) {
                    return metadata_result.status();
                }
                builder->append_null();
                continue;
            }

            auto value_result = encode_datum_to_variant_value(datum, type, key_to_id);
            if (!value_result.ok()) {
                if (allow_throw_exception) {
                    return value_result.status();
                }
                builder->append_null();
                continue;
            }

            auto variant_result = VariantRowValue::create(metadata_result.value(), value_result.value());
            if (!variant_result.ok()) {
                if (allow_throw_exception) {
                    return variant_result.status();
                }
                builder->append_null();
                continue;
            }
            builder->append(std::move(variant_result.value()));
        }

        return Status::OK();
    }

    switch (type.type) {
    case TYPE_NULL: {
        builder->append_nulls(static_cast<int>(num_rows));
        return Status::OK();
    }
    case TYPE_BOOLEAN: {
        return encode_column_with_viewer<TYPE_BOOLEAN>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, encode_boolean(data[data_row] != 0));
                });
    }
    case TYPE_TINYINT: {
        return encode_column_with_viewer<TYPE_TINYINT>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::INT8) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_SMALLINT: {
        return encode_column_with_viewer<TYPE_SMALLINT>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::INT16) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_INT: {
        return encode_column_with_viewer<TYPE_INT>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::INT32) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_BIGINT: {
        return encode_column_with_viewer<TYPE_BIGINT>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::INT64) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_LARGEINT: {
        return encode_column_with_viewer<TYPE_LARGEINT>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL16) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(0); // scale
                    append_int128_le(out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_FLOAT: {
        return encode_column_with_viewer<TYPE_FLOAT>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::FLOAT) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_DOUBLE: {
        return encode_column_with_viewer<TYPE_DOUBLE>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::DOUBLE) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_DECIMALV2: {
        return encode_column_with_viewer<TYPE_DECIMALV2>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL16) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(static_cast<char>(DecimalV2Value::SCALE));
                    append_int128_le(out, data[data_row].value());
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_DECIMAL32: {
        return encode_column_with_viewer<TYPE_DECIMAL32>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    if (type.scale < 0 || type.scale > decimal_precision_limit<int32_t>) {
                        return Status::InvalidArgument(
                                fmt::format("Invalid decimal32 scale for variant encoding: {}", type.scale));
                    }
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL4) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(static_cast<char>(type.scale));
                    append_little_endian(&out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_DECIMAL64: {
        return encode_column_with_viewer<TYPE_DECIMAL64>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    if (type.scale < 0 || type.scale > decimal_precision_limit<int64_t>) {
                        return Status::InvalidArgument(
                                fmt::format("Invalid decimal64 scale for variant encoding: {}", type.scale));
                    }
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL8) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(static_cast<char>(type.scale));
                    append_little_endian(&out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_DECIMAL128: {
        return encode_column_with_viewer<TYPE_DECIMAL128>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    if (type.scale < 0 || type.scale > decimal_precision_limit<int128_t>) {
                        return Status::InvalidArgument(
                                fmt::format("Invalid decimal128 scale for variant encoding: {}", type.scale));
                    }
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL16) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(static_cast<char>(type.scale));
                    append_int128_le(out, data[data_row]);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_DECIMAL256: {
        if (allow_throw_exception) {
            return Status::NotSupported("DECIMAL256 is not supported in VARIANT encoding");
        }
        builder->append_nulls(static_cast<int>(num_rows));
        return Status::OK();
    }
    case TYPE_CHAR: {
        return encode_column_with_viewer<TYPE_CHAR>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, encode_string_value(data[data_row]));
                });
    }
    case TYPE_VARCHAR: {
        return encode_column_with_viewer<TYPE_VARCHAR>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, encode_string_value(data[data_row]));
                });
    }
    case TYPE_JSON: {
        return encode_column_with_viewer<TYPE_JSON>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    JsonValue* json = data[data_row];
                    if (json == nullptr || json->is_null_or_none()) {
                        *value_is_null = true;
                        return VariantRowValue::from_null();
                    }
                    return VariantEncoder::encode_json_to_variant(*json);
                });
    }
    case TYPE_DATE: {
        return encode_column_with_viewer<TYPE_DATE>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    int32_t days = data[data_row].to_days_since_unix_epoch();
                    uint8_t header = static_cast<uint8_t>(VariantType::DATE) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, days);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_DATETIME: {
        return encode_column_with_viewer<TYPE_DATETIME>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    int64_t micros = data[data_row].to_unix_microsecond();
                    uint8_t header = static_cast<uint8_t>(VariantType::TIMESTAMP_NTZ) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, micros);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    case TYPE_TIME: {
        return encode_column_with_viewer<TYPE_TIME>(
                column, builder, allow_throw_exception,
                [&](const auto& data, size_t data_row, bool* value_is_null) -> StatusOr<VariantRowValue> {
                    double seconds = data[data_row];
                    int64_t micros = static_cast<int64_t>(seconds * USECS_PER_SEC);
                    uint8_t header = static_cast<uint8_t>(VariantType::TIME_NTZ) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, micros);
                    return VariantRowValue(VariantMetadata::kEmptyMetadata, out);
                });
    }
    default: {
        if (allow_throw_exception) {
            return Status::NotSupported("Variant encoding does not support cast from type: " + type.debug_string());
        }
        builder->append_nulls(static_cast<int>(num_rows));
        return Status::OK();
    }
    }
}

} // namespace starrocks
