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

#include "util/variant_encoder.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/util/endian.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "common/status.h"
#include "runtime/time_types.h"
#include "runtime/types.h"
#include "util/decimal_types.h"
#include "util/slice.h"
#include "util/variant.h"
#include "velocypack/vpack.h"

namespace starrocks {

template <typename T>
void append_little_endian(std::string* out, T value) {
    T le_value = arrow::bit_util::ToLittleEndian(value);
    out->append(reinterpret_cast<const char*>(&le_value), sizeof(T));
}

void append_uint_le(std::string* out, uint32_t value, uint8_t size) {
    uint32_t le_value = arrow::bit_util::ToLittleEndian(value);
    out->append(reinterpret_cast<const char*>(&le_value), size);
}

uint8_t minimal_uint_size(uint32_t value) {
    if (value <= 0xFF) {
        return 1;
    }
    if (value <= 0xFFFF) {
        return 2;
    }
    if (value <= 0xFFFFFF) {
        return 3;
    }
    return 4;
}

std::string encode_null_value() {
    return std::string(VariantValue::kEmptyValue);
}

std::string encode_boolean(bool value) {
    const auto type = value ? VariantType::BOOLEAN_TRUE : VariantType::BOOLEAN_FALSE;
    char header = static_cast<char>((static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::PRIMITIVE));
    return std::string(1, header);
}

constexpr uint8_t kCastVariantMetadataVersion = 1;

void append_int128_le(std::string& out, int128_t value) {
    int64_t low = static_cast<int64_t>(value);
    int64_t high = static_cast<int64_t>(value >> 64);
    append_little_endian(&out, low);
    append_little_endian(&out, high);
}

std::string encode_string_value(const Slice& value) {
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
        append_uint_le(&out, static_cast<uint32_t>(len), sizeof(uint32_t));
        out.append(value.data, len);
    }
    return out;
}

StatusOr<std::string> build_array_value(const std::vector<std::string>& elements) {
    const uint32_t num_elements = elements.size();
    uint64_t data_size = 0;
    for (const auto& elem : elements) {
        data_size += elem.size();
    }
    if (data_size > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("Variant array data size exceeds 4GB");
    }

    const uint8_t offset_size = minimal_uint_size(static_cast<uint32_t>(data_size));
    const bool is_large = num_elements > 0xFF;
    const uint8_t num_elements_size = is_large ? 4 : 1;

    uint8_t header = static_cast<uint8_t>((offset_size - 1) & 0b11);
    header |= static_cast<uint8_t>((is_large ? 1 : 0) << 2);

    std::string out;
    out.reserve(1 + num_elements_size + (num_elements + 1) * offset_size + data_size);
    out.push_back(static_cast<char>(header));
    append_uint_le(&out, num_elements, num_elements_size);

    uint32_t offset = 0;
    append_uint_le(&out, offset, offset_size);
    for (const auto& elem : elements) {
        offset += elem.size();
        append_uint_le(&out, offset, offset_size);
    }
    for (const auto& elem : elements) {
        out.append(elem);
    }
    return out;
}

StatusOr<std::string> build_object_value(const std::vector<uint32_t>& field_ids,
                                         const std::vector<std::string>& field_values, uint32_t dict_size) {
    const uint32_t num_fields = field_ids.size();
    if (num_fields != field_values.size()) {
        return Status::InvalidArgument("Variant object fields size mismatch");
    }

    uint64_t data_size = 0;
    for (const auto& value : field_values) {
        data_size += value.size();
    }
    if (data_size > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("Variant object data size exceeds 4GB");
    }

    const uint8_t id_size = minimal_uint_size(dict_size == 0 ? 0 : dict_size - 1);
    const uint8_t offset_size = minimal_uint_size(static_cast<uint32_t>(data_size));
    const bool is_large = num_fields > 0xFF;
    const uint8_t num_elements_size = is_large ? 4 : 1;

    uint8_t header = static_cast<uint8_t>((offset_size - 1) & 0b11);
    header |= static_cast<uint8_t>(((id_size - 1) & 0b11) << 2);
    header |= static_cast<uint8_t>((is_large ? 1 : 0) << 4);

    std::string out;
    out.reserve(1 + num_elements_size + num_fields * id_size + (num_fields + 1) * offset_size + data_size);
    out.push_back(static_cast<char>(header));
    append_uint_le(&out, num_fields, num_elements_size);
    for (uint32_t id : field_ids) {
        append_uint_le(&out, id, id_size);
    }

    uint32_t offset = 0;
    append_uint_le(&out, offset, offset_size);
    for (const auto& value : field_values) {
        offset += value.size();
        append_uint_le(&out, offset, offset_size);
    }
    for (const auto& value : field_values) {
        out.append(value);
    }
    return out;
}

class VariantRowValueEncoder {
public:
    StatusOr<VariantRowValue> make_variant(const std::string& value) {
        std::string metadata = _build_metadata();
        if (metadata.size() + value.size() > VariantRowValue::kMaxVariantSize) {
            return Status::CapacityLimitExceed("Variant value size exceeds 16MB limit");
        }
        return VariantRowValue(metadata, value);
    }

    StatusOr<std::string> encode_json_value(const vpack::Slice& slice) {
        if (slice.isNull()) {
            return encode_null_value();
        }
        if (slice.isBool()) {
            bool value = slice.getBool();
            uint8_t header = static_cast<uint8_t>((value ? VariantType::BOOLEAN_TRUE : VariantType::BOOLEAN_FALSE))
                             << 2;
            return std::string(1, static_cast<char>(header));
        }
        if (slice.isInt() || slice.isSmallInt()) {
            int64_t value = slice.getInt();
            if (value >= std::numeric_limits<int8_t>::min() && value <= std::numeric_limits<int8_t>::max()) {
                uint8_t header = static_cast<uint8_t>(VariantType::INT8) << 2;
                std::string out(1, static_cast<char>(header));
                append_little_endian(&out, static_cast<int8_t>(value));
                return out;
            }
            if (value >= std::numeric_limits<int16_t>::min() && value <= std::numeric_limits<int16_t>::max()) {
                uint8_t header = static_cast<uint8_t>(VariantType::INT16) << 2;
                std::string out(1, static_cast<char>(header));
                append_little_endian(&out, static_cast<int16_t>(value));
                return out;
            }
            if (value >= std::numeric_limits<int32_t>::min() && value <= std::numeric_limits<int32_t>::max()) {
                uint8_t header = static_cast<uint8_t>(VariantType::INT32) << 2;
                std::string out(1, static_cast<char>(header));
                append_little_endian(&out, static_cast<int32_t>(value));
                return out;
            }
            uint8_t header = static_cast<uint8_t>(VariantType::INT64) << 2;
            std::string out(1, static_cast<char>(header));
            append_little_endian(&out, value);
            return out;
        }
        if (slice.isUInt()) {
            uint64_t value = slice.getUInt();
            if (value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                uint8_t header = static_cast<uint8_t>(VariantType::INT64) << 2;
                std::string out(1, static_cast<char>(header));
                append_little_endian(&out, static_cast<int64_t>(value));
                return out;
            }
            uint8_t header = static_cast<uint8_t>(VariantType::DOUBLE) << 2;
            std::string out(1, static_cast<char>(header));
            append_little_endian(&out, static_cast<double>(value));
            return out;
        }
        if (slice.isDouble()) {
            uint8_t header = static_cast<uint8_t>(VariantType::DOUBLE) << 2;
            std::string out(1, static_cast<char>(header));
            append_little_endian(&out, slice.getDouble());
            return out;
        }
        if (slice.isString()) {
            auto sv = slice.stringView();
            return encode_string_value(Slice(sv.data(), sv.size()));
        }
        if (slice.isArray()) {
            std::vector<std::string> elements;
            for (auto it : vpack::ArrayIterator(slice)) {
                ASSIGN_OR_RETURN(auto value, encode_json_value(it));
                elements.emplace_back(std::move(value));
            }
            return build_array_value(elements);
        }
        if (slice.isObject()) {
            std::vector<uint32_t> field_ids;
            std::vector<std::string> field_values;
            for (auto it : vpack::ObjectIterator(slice)) {
                std::string_view key = it.key.stringView();
                if (key.empty()) {
                    continue;
                }
                uint32_t field_id = _get_or_add_key(key);
                ASSIGN_OR_RETURN(auto value, encode_json_value(it.value));
                field_ids.emplace_back(field_id);
                field_values.emplace_back(std::move(value));
            }
            return build_object_value(field_ids, field_values, _dict.size());
        }

        return Status::NotSupported("Unsupported JSON type for VARIANT encoding");
    }

private:
    uint32_t _get_or_add_key(std::string_view key) {
        auto it = _dict_index.find(std::string(key));
        if (it != _dict_index.end()) {
            return it->second;
        }
        uint32_t index = _dict.size();
        _dict.emplace_back(key);
        _dict_index.emplace(_dict.back(), index);
        return index;
    }

    std::string _build_metadata() const {
        if (_dict.empty()) {
            return std::string(VariantMetadata::kEmptyMetadata);
        }

        std::vector<uint32_t> offsets;
        offsets.reserve(_dict.size() + 1);
        uint32_t offset = 0;
        offsets.emplace_back(offset);
        for (const auto& key : _dict) {
            offset += key.size();
            offsets.emplace_back(offset);
        }

        const uint32_t dict_size = _dict.size();
        const uint32_t max_value = std::max(dict_size, offset);
        const uint8_t offset_size = minimal_uint_size(max_value);

        uint8_t header = kCastVariantMetadataVersion & 0b1111;
        header |= static_cast<uint8_t>((offset_size - 1) << 6);

        std::string out;
        out.reserve(1 + offset_size * (_dict.size() + 1) + offset);
        out.push_back(static_cast<char>(header));
        for (uint32_t value : offsets) {
            append_uint_le(&out, value, offset_size);
        }
        for (const auto& key : _dict) {
            out.append(key);
        }
        return out;
    }

    std::vector<std::string> _dict;
    std::unordered_map<std::string, uint32_t> _dict_index;
};

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

    ColumnViewer<LT> viewer(data_column);
    for (size_t row = 0; row < num_rows; ++row) {
        const size_t data_row = is_const ? 0 : row;
        if (nullable && nullable->is_null(data_row)) {
            builder->append_null();
            continue;
        }

        VariantRowValueEncoder encoder;
        bool is_null = false;
        StatusOr<VariantRowValue> encoded = encode_value(viewer, data_row, encoder, &is_null);
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

std::string encode_object_from_fields(const std::map<uint32_t, std::string>& fields) {
    const uint32_t num_elements = static_cast<uint32_t>(fields.size());
    uint8_t is_large = num_elements > 255 ? 1 : 0;
    const uint8_t num_elements_size = is_large ? 4 : 1;

    uint32_t max_field_id = 0;
    uint32_t total_data_size = 0;
    for (const auto& [field_id, value] : fields) {
        max_field_id = std::max(max_field_id, field_id);
        total_data_size += static_cast<uint32_t>(value.size());
    }

    const uint8_t field_id_size = minimal_uint_size(max_field_id);
    const uint8_t field_offset_size = minimal_uint_size(total_data_size);

    uint8_t vheader = static_cast<uint8_t>((field_offset_size - 1) | ((field_id_size - 1) << 2) | (is_large << 4));
    char header = static_cast<char>((vheader << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::OBJECT));

    std::string out;
    out.reserve(1 + num_elements_size + num_elements * field_id_size + (num_elements + 1) * field_offset_size +
                total_data_size);
    out.push_back(header);
    append_uint_le(&out, num_elements, num_elements_size);

    for (const auto& [field_id, value] : fields) {
        append_uint_le(&out, field_id, field_id_size);
    }

    uint32_t offset = 0;
    append_uint_le(&out, offset, field_offset_size);
    for (const auto& [field_id, value] : fields) {
        offset += static_cast<uint32_t>(value.size());
        append_uint_le(&out, offset, field_offset_size);
    }

    for (const auto& [field_id, value] : fields) {
        out.append(value.data(), value.size());
    }

    return out;
}

std::string encode_array_from_elements(const std::vector<std::string>& elements) {
    const uint32_t num_elements = static_cast<uint32_t>(elements.size());
    uint8_t is_large = num_elements > 255 ? 1 : 0;
    const uint8_t num_elements_size = is_large ? 4 : 1;

    uint32_t total_data_size = 0;
    for (const auto& element : elements) {
        total_data_size += static_cast<uint32_t>(element.size());
    }

    const uint8_t offset_size = minimal_uint_size(total_data_size);
    uint8_t vheader = static_cast<uint8_t>((offset_size - 1) | (is_large << 2));
    char header = static_cast<char>((vheader << VariantValue::kValueHeaderBitShift) |
                                    static_cast<uint8_t>(VariantValue::BasicType::ARRAY));

    std::string out;
    out.reserve(1 + num_elements_size + (num_elements + 1) * offset_size + total_data_size);
    out.push_back(header);
    append_uint_le(&out, num_elements, num_elements_size);

    uint32_t offset = 0;
    append_uint_le(&out, offset, offset_size);
    for (const auto& element : elements) {
        offset += static_cast<uint32_t>(element.size());
        append_uint_le(&out, offset, offset_size);
    }

    for (const auto& element : elements) {
        out.append(element.data(), element.size());
    }

    return out;
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

constexpr uint8_t kVariantMetadataVersion = 1;
constexpr uint8_t kVariantMetadataSortedMask = 0b10000;
constexpr uint8_t kVariantMetadataOffsetSizeShift = 6;

struct VariantMetadataBuildResult {
    std::string metadata;
    std::unordered_map<std::string, uint32_t> key_to_id;
};

StatusOr<VariantMetadataBuildResult> build_variant_metadata(const std::unordered_set<std::string>& keys) {
    if (keys.empty()) {
        VariantMetadataBuildResult result;
        result.metadata.assign(VariantMetadata::kEmptyMetadata.data(), VariantMetadata::kEmptyMetadata.size());
        return result;
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
    const uint8_t offset_size = minimal_uint_size(max_value);

    uint8_t header = kVariantMetadataVersion;
    header |= kVariantMetadataSortedMask;
    header |= static_cast<uint8_t>((offset_size - 1) << kVariantMetadataOffsetSizeShift);

    std::string metadata;
    metadata.reserve(1 + offset_size * (dict_size + 2) + total_string_size);
    metadata.push_back(static_cast<char>(header));

    append_uint_le(&metadata, dict_size, offset_size);

    uint32_t offset = 0;
    append_uint_le(&metadata, offset, offset_size);
    for (const auto& key : sorted_keys) {
        offset += static_cast<uint32_t>(key.size());
        append_uint_le(&metadata, offset, offset_size);
    }

    for (const auto& key : sorted_keys) {
        metadata.append(key.data(), key.size());
    }

    VariantMetadataBuildResult result;
    result.metadata = std::move(metadata);
    result.key_to_id.reserve(sorted_keys.size());
    for (uint32_t i = 0; i < sorted_keys.size(); ++i) {
        result.key_to_id.emplace(sorted_keys[i], i);
    }

    return result;
}

StatusOr<std::string> encode_json_to_variant_value(const vpack::Slice& slice,
                                                   const std::unordered_map<std::string, uint32_t>& key_to_id) {
    try {
        if (slice.isNone() || slice.isNull()) {
            return encode_null_value();
        }
        if (slice.isBool()) {
            return encode_boolean(slice.getBool());
        }
        if (slice.isInt() || slice.isSmallInt()) {
            return encode_primitive_number<int64_t>(VariantType::INT64, slice.getIntUnchecked());
        }
        if (slice.isUInt()) {
            uint64_t value = slice.getUIntUnchecked();
            if (value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                return encode_primitive_number<int64_t>(VariantType::INT64, static_cast<int64_t>(value));
            }
            return encode_primitive_number<double>(VariantType::DOUBLE, static_cast<double>(value));
        }
        if (slice.isDouble()) {
            return encode_primitive_number<double>(VariantType::DOUBLE, slice.getDouble());
        }
        if (slice.isString() || slice.isBinary()) {
            vpack::ValueLength len;
            const char* str = slice.getStringUnchecked(len);
            return encode_string_or_binary(VariantType::STRING, std::string_view(str, len));
        }
        if (slice.isArray()) {
            vpack::ArrayIterator it(slice);
            std::vector<std::string> elements;
            elements.reserve(slice.length());
            for (; it.valid(); it.next()) {
                ASSIGN_OR_RETURN(std::string element, encode_json_to_variant_value(it.value(), key_to_id));
                elements.emplace_back(std::move(element));
            }
            return encode_array_from_elements(elements);
        }
        if (slice.isObject()) {
            vpack::ObjectIterator it(slice);
            std::map<uint32_t, std::string> fields;
            for (; it.valid(); it.next()) {
                auto current = *it;
                auto key = current.key.stringView();
                auto it_key = key_to_id.find(std::string(key.data(), key.size()));
                if (it_key == key_to_id.end()) {
                    return Status::VariantError("Variant metadata missing field: " +
                                                std::string(key.data(), key.size()));
                }
                ASSIGN_OR_RETURN(std::string field_value, encode_json_to_variant_value(current.value, key_to_id));
                fields[it_key->second] = std::move(field_value);
            }
            return encode_object_from_fields(fields);
        }
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }

    return Status::NotSupported(
            fmt::format("Unsupported JSON type {} for variant encoding type", valueTypeName(slice.type())));
}

StatusOr<VariantRowValue> VariantEncoder::encode_json_to_variant(const JsonValue& json) {
    vpack::Slice slice = json.to_vslice();
    std::unordered_set<std::string> keys;
    RETURN_IF_ERROR(collect_object_keys(slice, &keys));
    ASSIGN_OR_RETURN(auto metadata_result, build_variant_metadata(keys));
    ASSIGN_OR_RETURN(std::string value, encode_json_to_variant_value(slice, metadata_result.key_to_id));
    return VariantRowValue::create(metadata_result.metadata, value);
}

Status VariantEncoder::encode_column(const ColumnPtr& column, const TypeDescriptor& type,
                                     ColumnBuilder<TYPE_VARIANT>* builder, bool allow_throw_exception) {
    const size_t num_rows = column->size();
    if (num_rows == 0) {
        return Status::OK();
    }

    if (type.type == TYPE_ARRAY || type.type == TYPE_MAP || type.type == TYPE_STRUCT) {
        if (allow_throw_exception) {
            return Status::NotSupported("CAST to VARIANT from complex type is not supported yet: " +
                                        type.debug_string());
        }
        builder->append_nulls(static_cast<int>(num_rows));
        return Status::OK();
    }

    switch (type.type) {
    case TYPE_NULL: {
        builder->append_nulls(static_cast<int>(num_rows));
        return Status::OK();
    }
    // TODO(mofei): Add ARRAY/MAP/STRUCT support here using encode_column_with_viewer once encoding is ready.
    case TYPE_BOOLEAN: {
        return encode_column_with_viewer<TYPE_BOOLEAN>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    bool value = viewer.value(data_row);
                    uint8_t header =
                            static_cast<uint8_t>((value ? VariantType::BOOLEAN_TRUE : VariantType::BOOLEAN_FALSE)) << 2;
                    return encoder.make_variant(std::string(1, static_cast<char>(header)));
                });
    }
    case TYPE_TINYINT: {
        return encode_column_with_viewer<TYPE_TINYINT>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::INT8) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, viewer.value(data_row));
                    return encoder.make_variant(out);
                });
    }
    case TYPE_SMALLINT: {
        return encode_column_with_viewer<TYPE_SMALLINT>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::INT16) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, viewer.value(data_row));
                    return encoder.make_variant(out);
                });
    }
    case TYPE_INT: {
        return encode_column_with_viewer<TYPE_INT>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::INT32) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, viewer.value(data_row));
                    return encoder.make_variant(out);
                });
    }
    case TYPE_BIGINT: {
        return encode_column_with_viewer<TYPE_BIGINT>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::INT64) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, viewer.value(data_row));
                    return encoder.make_variant(out);
                });
    }
    case TYPE_LARGEINT: {
        return encode_column_with_viewer<TYPE_LARGEINT>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL16) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(0); // scale
                    append_int128_le(out, viewer.value(data_row));
                    return encoder.make_variant(out);
                });
    }
    case TYPE_FLOAT: {
        return encode_column_with_viewer<TYPE_FLOAT>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::FLOAT) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, viewer.value(data_row));
                    return encoder.make_variant(out);
                });
    }
    case TYPE_DOUBLE: {
        return encode_column_with_viewer<TYPE_DOUBLE>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::DOUBLE) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, viewer.value(data_row));
                    return encoder.make_variant(out);
                });
    }
    case TYPE_DECIMALV2: {
        return encode_column_with_viewer<TYPE_DECIMALV2>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL16) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(static_cast<char>(DecimalV2Value::SCALE));
                    append_int128_le(out, viewer.value(data_row).value());
                    return encoder.make_variant(out);
                });
    }
    case TYPE_DECIMAL32: {
        return encode_column_with_viewer<TYPE_DECIMAL32>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    if (type.scale < 0 || type.scale > decimal_precision_limit<int32_t>) {
                        return Status::InvalidArgument(
                                fmt::format("Invalid decimal32 scale for variant encoding: {}", type.scale));
                    }
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL4) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(static_cast<char>(type.scale));
                    append_little_endian(&out, viewer.value(data_row));
                    return encoder.make_variant(out);
                });
    }
    case TYPE_DECIMAL64: {
        return encode_column_with_viewer<TYPE_DECIMAL64>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    if (type.scale < 0 || type.scale > decimal_precision_limit<int64_t>) {
                        return Status::InvalidArgument(
                                fmt::format("Invalid decimal64 scale for variant encoding: {}", type.scale));
                    }
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL8) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(static_cast<char>(type.scale));
                    append_little_endian(&out, viewer.value(data_row));
                    return encoder.make_variant(out);
                });
    }
    case TYPE_DECIMAL128: {
        return encode_column_with_viewer<TYPE_DECIMAL128>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    if (type.scale < 0 || type.scale > decimal_precision_limit<int128_t>) {
                        return Status::InvalidArgument(
                                fmt::format("Invalid decimal128 scale for variant encoding: {}", type.scale));
                    }
                    uint8_t header = static_cast<uint8_t>(VariantType::DECIMAL16) << 2;
                    std::string out(1, static_cast<char>(header));
                    out.push_back(static_cast<char>(type.scale));
                    append_int128_le(out, viewer.value(data_row));
                    return encoder.make_variant(out);
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
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    return encoder.make_variant(encode_string_value(viewer.value(data_row)));
                });
    }
    case TYPE_VARCHAR: {
        return encode_column_with_viewer<TYPE_VARCHAR>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    return encoder.make_variant(encode_string_value(viewer.value(data_row)));
                });
    }
    case TYPE_JSON: {
        return encode_column_with_viewer<TYPE_JSON>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    JsonValue* json = viewer.value(data_row);
                    if (json == nullptr || json->is_null_or_none()) {
                        *value_is_null = true;
                        return VariantRowValue::from_null();
                    }
                    ASSIGN_OR_RETURN(auto encoded, encoder.encode_json_value(json->to_vslice()));
                    return encoder.make_variant(std::move(encoded));
                });
    }
    case TYPE_DATE: {
        return encode_column_with_viewer<TYPE_DATE>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    int32_t days = viewer.value(data_row).to_days_since_unix_epoch();
                    uint8_t header = static_cast<uint8_t>(VariantType::DATE) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, days);
                    return encoder.make_variant(out);
                });
    }
    case TYPE_DATETIME: {
        return encode_column_with_viewer<TYPE_DATETIME>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    int64_t micros = viewer.value(data_row).to_unix_microsecond();
                    uint8_t header = static_cast<uint8_t>(VariantType::TIMESTAMP_NTZ) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, micros);
                    return encoder.make_variant(out);
                });
    }
    case TYPE_TIME: {
        return encode_column_with_viewer<TYPE_TIME>(
                column, builder, allow_throw_exception,
                [&](const auto& viewer, size_t data_row, VariantRowValueEncoder& encoder,
                    bool* value_is_null) -> StatusOr<VariantRowValue> {
                    double seconds = viewer.value(data_row);
                    int64_t micros = static_cast<int64_t>(seconds * USECS_PER_SEC);
                    uint8_t header = static_cast<uint8_t>(VariantType::TIME_NTZ) << 2;
                    std::string out(1, static_cast<char>(header));
                    append_little_endian(&out, micros);
                    return encoder.make_variant(out);
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
