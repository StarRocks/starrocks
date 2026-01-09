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
#include "common/status.h"
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

StatusOr<VariantRowValue> encode_json_to_variant(const JsonValue& json) {
    vpack::Slice slice = json.to_vslice();
    std::unordered_set<std::string> keys;
    RETURN_IF_ERROR(collect_object_keys(slice, &keys));
    ASSIGN_OR_RETURN(auto metadata_result, build_variant_metadata(keys));
    ASSIGN_OR_RETURN(std::string value, encode_json_to_variant_value(slice, metadata_result.key_to_id));
    return VariantRowValue::create(metadata_result.metadata, value);
}

} // namespace starrocks
