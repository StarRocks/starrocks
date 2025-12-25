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

#include <arrow/util/endian.h>
#include <glog/logging.h>
#include <util/variant.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <charconv>
#include <iomanip>
#include <string_view>

#include "common/statusor.h"
#include "runtime/decimalv3.h"
#include "types/timestamp_value.h"
#include "util/url_coding.h"

namespace starrocks {

static std::string basic_type_to_string(VariantValue::BasicType type) {
    switch (type) {
    case VariantValue::BasicType::PRIMITIVE:
        return "Primitive";
    case VariantValue::BasicType::SHORT_STRING:
        return "ShortString";
    case VariantValue::BasicType::OBJECT:
        return "Object";
    case VariantValue::BasicType::ARRAY:
        return "Array";
    default:
        return "Unknown";
    }
}

std::string VariantUtil::variant_type_to_string(VariantType type) {
    switch (type) {
    case VariantType::NULL_TYPE:
        return "Null";
    case VariantType::BOOLEAN_TRUE:
        return "Boolean(true)";
    case VariantType::BOOLEAN_FALSE:
        return "Boolean(false)";
    case VariantType::INT8:
        return "Int8";
    case VariantType::INT16:
        return "Int16";
    case VariantType::INT32:
        return "Int32";
    case VariantType::INT64:
        return "Int64";
    case VariantType::FLOAT:
        return "Float";
    case VariantType::DOUBLE:
        return "Double";
    case VariantType::DECIMAL4:
        return "Decimal4";
    case VariantType::DECIMAL8:
        return "Decimal8";
    case VariantType::DECIMAL16:
        return "Decimal16";

    case VariantType::DATE:
        return "Date";
    case VariantType::TIMESTAMP_TZ:
        return "TimestampTz";
    case VariantType::TIMESTAMP_NTZ:
        return "TimestampNtz";
    case VariantType::TIME_NTZ:
        return "TimeNtz";
    case VariantType::TIMESTAMP_TZ_NANOS:
        return "TimestampTzNanos";
    case VariantType::TIMESTAMP_NTZ_NANOS:
        return "TimestampNtzNanos";
    case VariantType::BINARY:
        return "Binary";
    case VariantType::STRING:
        return "String";
    case VariantType::UUID:
        return "Uuid";
    case VariantType::OBJECT:
        return "Object";
    case VariantType::ARRAY:
        return "Array";
    }
    return "Unknown";
}

VariantMetadata::VariantMetadata(std::string_view metadata) : _metadata(metadata) {
    // Empty metadata is at least 3 bytes: version, dictionarySize and
    // at least one offset.
    DCHECK(!metadata.empty()) << "Variant metadata cannot be empty";
    DCHECK(metadata.size() >= 3) << "Variant metadata size is too short: " << std::to_string(metadata.size());

    const uint8_t version = header() & kVersionMask;
    DCHECK(version == kSupportedVersion) << "Unsupported variant version: " << std::to_string(version);

    const uint8_t offset_sz = offset_size();
    _dict_size = VariantUtil::read_little_endian_unsigned32(metadata.data() + kHeaderSizeBytes, offset_sz);
}

uint8_t VariantMetadata::header() const {
    return static_cast<uint8_t>(_metadata[0]);
}

bool VariantMetadata::is_sorted_and_unique() const {
    return (header() & kSortedStringMask) != 0;
}

uint8_t VariantMetadata::offset_size() const {
    // variant header stores offsetSize - 1
    return ((header() & kOffsetMask) >> kOffsetSizeBitShift) + 1;
}

uint32_t VariantMetadata::dict_size() const {
    return _dict_size;
}

StatusOr<std::string> VariantMetadata::get_key(uint32_t index) const {
    uint8_t offset_sz = offset_size();
    uint32_t dict_sz = dict_size();
    if (index >= dict_sz) {
        return Status::VariantError("Variant index out of range: " + std::to_string(index) +
                                    " >= " + std::to_string(dict_sz));
    }

    size_t offset_start_pos = kHeaderSizeBytes + offset_sz + (index * offset_sz);
    uint32_t value_offset = VariantUtil::read_little_endian_unsigned32(_metadata.data() + offset_start_pos, offset_sz);
    uint32_t value_next_offset =
            VariantUtil::read_little_endian_unsigned32(_metadata.data() + offset_start_pos + offset_sz, offset_sz);
    uint32_t key_size = value_next_offset - value_offset;
    size_t string_start = kHeaderSizeBytes + offset_sz * (dict_sz + 2) + value_offset;
    if (string_start + key_size > _metadata.size()) {
        return Status::VariantError("Variant string out of range");
    }

    std::string field_key(_metadata.data() + string_start, key_size);
    return field_key;
}

static constexpr uint8_t kBinarySearchThreshold = 32;

std::vector<uint32_t> VariantMetadata::get_index(std::string_view key) const {
    uint32_t dict_sz = dict_size();
    bool is_sorted_unique = is_sorted_and_unique();
    std::vector<uint32_t> indexes;

    if (is_sorted_unique && dict_sz > kBinarySearchThreshold) {
        // binary search
        uint32_t left = 0;
        uint32_t right = dict_sz - 1;
        while (left <= right) {
            uint32_t mid = left + (right - left) / 2;
            auto status = get_key(mid);
            if (!status.ok()) {
                return indexes;
            }
            std::string_view field_key = status.value();
            int cmp = field_key.compare(key);
            if (cmp == 0) {
                indexes.push_back(mid);
                break;
            }

            if (cmp < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
    } else {
        uint8_t offset_sz = this->offset_size();
        uint32_t dict_key_offset = 0;
        uint32_t dict_next_key_offset = 0;
        const uint32_t key_start_offset = kHeaderSizeBytes + offset_sz * (dict_sz + 1 + 1);
        for (uint32_t i = 0; i < dict_sz; i++) {
            size_t offset_start_pos = kHeaderSizeBytes + (i + 1) * offset_sz;
            dict_key_offset = dict_next_key_offset;
            dict_next_key_offset = VariantUtil::read_little_endian_unsigned32(
                    _metadata.data() + offset_start_pos + offset_sz, offset_sz);
            uint32_t dict_key_size = dict_next_key_offset - dict_key_offset;
            size_t dict_key_start = key_start_offset + dict_key_offset;
            if (dict_key_start + dict_key_size > _metadata.size()) {
                throw Status::VariantError("Invalid Variant metadata: string data out of range");
            }

            std::string_view field_key{_metadata.data() + dict_key_start, dict_key_size};
            if (field_key == key) {
                indexes.push_back(i);
            }
        }
    }

    return indexes;
}

// Variant value class
VariantValue::VariantValue(std::string_view value) : _value(value) {
    DCHECK(!value.empty()) << "Variant value cannot be empty";
}

VariantValue::BasicType VariantValue::basic_type() const {
    return static_cast<VariantValue::BasicType>(_value[0] & kBasicTypeMask);
}

VariantType VariantValue::type() const {
    switch (basic_type()) {
    case VariantValue::BasicType::PRIMITIVE:
        return static_cast<VariantType>(value_header());
    case VariantValue::BasicType::SHORT_STRING:
        return VariantType::STRING; // Short string is treated as a string type.
    case VariantValue::BasicType::OBJECT:
        return VariantType::OBJECT;
    case VariantValue::BasicType::ARRAY:
        return VariantType::ARRAY;
    default:
        return VariantType::NULL_TYPE; // Should not happen, but return NULL_TYPE as a fallback.
    }
}

StatusOr<VariantObjectInfo> VariantValue::get_object_info() const {
    const std::string_view& value = _value;
    VariantValue::BasicType basic_type =
            static_cast<VariantValue::BasicType>(static_cast<uint8_t>(value[0]) & VariantValue::kBasicTypeMask);
    if (basic_type != VariantValue::BasicType::OBJECT) {
        return Status::VariantError("Cannot parse object info: basic_type is not OBJECT, basic_type=" +
                                    basic_type_to_string(basic_type));
    }

    uint8_t value_header = (static_cast<uint8_t>(value[0]) >> VariantValue::kValueHeaderBitShift) & 0x3F;
    uint8_t field_offset_size = (value_header & 0b11) + 1;
    uint8_t field_id_size = ((value_header >> 2) & 0b11) + 1;
    // Indicates how many bytes are used to encode the number of elements
    bool is_large = ((value_header >> 4) & 0b1);
    // If is_large is 0, 1 byte is used, and if is_large is 1, 4 bytes are used.
    uint8_t num_elements_size = is_large ? 4 : 1;
    if (value.size() < static_cast<size_t>(1 + num_elements_size)) {
        return Status::VariantError("Too short object value: " + std::to_string(value.size()) + " for at least " +
                                    std::to_string(1 + num_elements_size));
    }

    uint32_t num_elements = VariantUtil::read_little_endian_unsigned32(value.data() + VariantValue::kHeaderSizeBytes,
                                                                       num_elements_size);

    VariantObjectInfo object_info{};
    object_info.num_elements = num_elements;
    object_info.id_size = field_id_size;
    object_info.offset_size = field_offset_size;
    object_info.id_start_offset = 1 + num_elements_size;

    // Check for potential overflow in offset calculation
    if (num_elements > 0 && field_id_size > 0) {
        uint64_t id_list_size = static_cast<uint64_t>(num_elements) * static_cast<uint64_t>(field_id_size);
        if (id_list_size > UINT32_MAX || object_info.id_start_offset > UINT32_MAX - id_list_size) {
            return Status::VariantError("Object metadata overflow: num_elements=" + std::to_string(num_elements) +
                                        ", field_id_size=" + std::to_string(field_id_size));
        }
        object_info.offset_start_offset = object_info.id_start_offset + static_cast<uint32_t>(id_list_size);
    } else {
        object_info.offset_start_offset = object_info.id_start_offset;
    }

    // Check for overflow in data offset calculation
    uint64_t offset_list_size = static_cast<uint64_t>(num_elements + 1) * static_cast<uint64_t>(field_offset_size);
    if (offset_list_size > UINT32_MAX || object_info.offset_start_offset > UINT32_MAX - offset_list_size) {
        return Status::VariantError("Object offset list overflow: num_elements=" + std::to_string(num_elements) +
                                    ", field_offset_size=" + std::to_string(field_offset_size));
    }
    object_info.data_start_offset = object_info.offset_start_offset + static_cast<uint32_t>(offset_list_size);

    // Check the boundary with the final offset
    if (object_info.data_start_offset > value.size()) {
        return Status::VariantError(
                "Invalid object value: data_start_offset=" + std::to_string(object_info.data_start_offset) +
                ", value_size=" + std::to_string(value.size()));
    }

    {
        const uint32_t final_offset = VariantUtil::read_little_endian_unsigned32(
                value.data() + object_info.offset_start_offset + num_elements * field_offset_size, field_offset_size);
        // It could be less than value size since it could be a sub-object.
        if (final_offset > UINT32_MAX - object_info.data_start_offset ||
            final_offset + object_info.data_start_offset > value.size()) {
            return Status::VariantError("Invalid object value: final_offset=" + std::to_string(final_offset) +
                                        ", data_start_offset=" + std::to_string(object_info.data_start_offset) +
                                        ", value_size=" + std::to_string(value.size()));
        }
    }

    return StatusOr<VariantObjectInfo>(object_info);
}

StatusOr<VariantArrayInfo> VariantValue::get_array_info() const {
    const std::string_view& value = _value;
    VariantValue::BasicType basic_type =
            static_cast<VariantValue::BasicType>(static_cast<uint8_t>(value[0]) & VariantValue::kBasicTypeMask);
    if (basic_type != VariantValue::BasicType::ARRAY) {
        return Status::VariantError("Cannot parse array info: basic_type is not ARRAY, basic_type=" +
                                    basic_type_to_string(basic_type));
    }

    uint8_t value_header = (static_cast<uint8_t>(value[0]) >> VariantValue::kValueHeaderBitShift) & 0x3F;
    // represents the number of bytes used to encode the field offset.
    uint8_t field_offset_size = (value_header & 0b11) + 1;
    // is_large is a 1-bit value that indicates how many bytes are used to encode the number of elements.
    bool is_large = ((value_header >> 2) & 0b1);
    uint8_t num_elements_size = is_large ? 4 : 1;
    if (value.size() < static_cast<size_t>(1 + num_elements_size)) {
        return Status::VariantError("Too short array value: " + std::to_string(value.size()) + " for at least " +
                                    std::to_string(1 + num_elements_size));
    }

    uint32_t num_elements = VariantUtil::read_little_endian_unsigned32(value.data() + VariantValue::kHeaderSizeBytes,
                                                                       num_elements_size);

    VariantArrayInfo array_info{};
    array_info.num_elements = num_elements;
    array_info.offset_size = field_offset_size;
    array_info.offset_start_offset = VariantValue::kHeaderSizeBytes + num_elements_size;

    // Check for potential overflow in offset calculation
    uint64_t offset_list_size = static_cast<uint64_t>(num_elements + 1) * static_cast<uint64_t>(field_offset_size);
    if (offset_list_size > UINT32_MAX || array_info.offset_start_offset > UINT32_MAX - offset_list_size) {
        return Status::VariantError("Array offset list overflow: num_elements=" + std::to_string(num_elements) +
                                    ", field_offset_size=" + std::to_string(field_offset_size));
    }
    array_info.data_start_offset = array_info.offset_start_offset + static_cast<uint32_t>(offset_list_size);

    if (array_info.data_start_offset > value.size()) {
        return Status::VariantError(
                "Invalid array value: data_start_offset=" + std::to_string(array_info.data_start_offset) +
                ", value_size=" + std::to_string(value.size()));
    }

    return StatusOr<VariantArrayInfo>(array_info);
}

uint8_t VariantValue::value_header() const {
    return static_cast<uint8_t>(_value[0]) >> kValueHeaderBitShift;
}

Status VariantValue::validate_basic_type(VariantValue::BasicType type) const {
    if (basic_type() != type) {
        return Status::VariantError("Expected basic type: " + basic_type_to_string(type) +
                                    ", but got: " + basic_type_to_string(basic_type()));
    }

    return Status::OK();
}

Status VariantValue::validate_primitive_type(VariantType type, size_t size_required) const {
    RETURN_IF_ERROR(validate_basic_type(VariantValue::BasicType::PRIMITIVE));

    auto primitive_type = static_cast<VariantType>(value_header());
    if (primitive_type != type) {
        return Status::VariantError("Expected primitive type: " + VariantUtil::variant_type_to_string(type) +
                                    ", but got: " + VariantUtil::variant_type_to_string(primitive_type));
    }

    if (_value.size() < size_required) {
        return Status::VariantError("Value is too short, expected at least " + std::to_string(size_required) +
                                    " bytes for type " + VariantUtil::variant_type_to_string(type) +
                                    ", but got: " + std::to_string(_value.size()) + " bytes");
    }

    return Status::OK();
}

template <typename PrimitiveType>
StatusOr<PrimitiveType> VariantValue::get_primitive(VariantType type) const {
    RETURN_IF_ERROR(validate_primitive_type(type, sizeof(PrimitiveType) + kHeaderSizeBytes));

    PrimitiveType primitive_value{};
    memcpy(&primitive_value, _value.data() + kHeaderSizeBytes, sizeof(PrimitiveType));
    primitive_value = arrow::bit_util::FromLittleEndian(primitive_value);

    return primitive_value;
}

StatusOr<bool> VariantValue::get_bool() const {
    RETURN_IF_ERROR(validate_basic_type(basic_type()));

    // extract the primitive type from the header
    VariantType primitive_type = static_cast<VariantType>(value_header());
    if (primitive_type == VariantType::BOOLEAN_TRUE) {
        return true;
    }
    if (primitive_type == VariantType::BOOLEAN_FALSE) {
        return false;
    }

    return Status::VariantError("Not a variant primitive boolean type with primitive type: " +
                                VariantUtil::variant_type_to_string(primitive_type));
}

StatusOr<int8_t> VariantValue::get_int8() const {
    return get_primitive<int8_t>(VariantType::INT8);
}

StatusOr<int16_t> VariantValue::get_int16() const {
    return get_primitive<int16_t>(VariantType::INT16);
}

StatusOr<int32_t> VariantValue::get_int32() const {
    return get_primitive<int32_t>(VariantType::INT32);
}

StatusOr<int64_t> VariantValue::get_int64() const {
    return get_primitive<int64_t>(VariantType::INT64);
}

StatusOr<float> VariantValue::get_float() const {
    return get_primitive<float>(VariantType::FLOAT);
}

StatusOr<double> VariantValue::get_double() const {
    return get_primitive<double>(VariantType::DOUBLE);
}

template <typename DecimalType>
StatusOr<VariantDecimalValue<DecimalType>> VariantValue::get_primitive_decimal(VariantType type) const {
    RETURN_IF_ERROR(validate_primitive_type(type, sizeof(DecimalType) + kHeaderSizeBytes + kDecimalScaleSizeBytes));

    uint8_t scale = _value[kHeaderSizeBytes];
    DecimalType decimal_value = 0;

    if constexpr (std::is_same_v<DecimalType, int128_t>) {
        // Handle int128_t using an array of two int64_t values
        std::array<int64_t, 2> low_high_bits;
        memcpy(&low_high_bits[0], _value.data() + 2, sizeof(int64_t));
        memcpy(&low_high_bits[1], _value.data() + 10, sizeof(int64_t));
        arrow::bit_util::little_endian::ToNative(low_high_bits);

        // Combine into int128_t
        memcpy(&decimal_value, low_high_bits.data(), sizeof(int128_t));
    } else {
        // For smaller types, use direct conversion
        memcpy(&decimal_value, _value.data() + kHeaderSizeBytes + kDecimalScaleSizeBytes, sizeof(DecimalType));
        decimal_value = arrow::bit_util::FromLittleEndian(decimal_value);
    }

    return VariantDecimalValue<DecimalType>{scale, decimal_value};
}

StatusOr<VariantDecimalValue<int32_t>> VariantValue::get_decimal4() const {
    return get_primitive_decimal<int32_t>(VariantType::DECIMAL4);
}

StatusOr<VariantDecimalValue<int64_t>> VariantValue::get_decimal8() const {
    return get_primitive_decimal<int64_t>(VariantType::DECIMAL8);
}

StatusOr<VariantDecimalValue<int128_t>> VariantValue::get_decimal16() const {
    return get_primitive_decimal<int128_t>(VariantType::DECIMAL16);
}

template <>
std::string VariantDecimalValue<int32_t>::to_string() const {
    return DecimalV3Cast::to_string<int32_t>(value, decimal_precision_limit<int32_t>, scale);
}
template <>
std::string VariantDecimalValue<int64_t>::to_string() const {
    return DecimalV3Cast::to_string<int64_t>(value, decimal_precision_limit<int64_t>, scale);
}
template <>
std::string VariantDecimalValue<int128_t>::to_string() const {
    return DecimalV3Cast::to_string<int128_t>(value, decimal_precision_limit<int128_t>, scale);
}

StatusOr<std::string_view> VariantValue::get_primitive_string_or_binary(VariantType type) const {
    // BINARY and STRING are both 4 byte little-endian size
    RETURN_IF_ERROR(validate_primitive_type(type, kHeaderSizeBytes + 4));

    uint32_t length = VariantUtil::read_little_endian_unsigned32(_value.data() + kHeaderSizeBytes, sizeof(uint32_t));
    if (_value.size() < length + kHeaderSizeBytes + 4) {
        return Status::VariantError("Invalid string value: too short for specified length");
    }

    return std::string_view(_value.data() + kHeaderSizeBytes + 4, length);
}

StatusOr<std::string_view> VariantValue::get_string() const {
    VariantValue::BasicType btype = basic_type();
    if (btype == VariantValue::BasicType::SHORT_STRING) {
        // The short string header value is the length of the string.
        uint8_t short_string_length = value_header();
        if (_value.size() < static_cast<size_t>(short_string_length + kHeaderSizeBytes)) {
            return Status::VariantError("Invalid short string: too short: " + std::to_string(_value.size()) +
                                        " for at least " + std::to_string(short_string_length + kHeaderSizeBytes));
        }

        return std::string_view(_value.data() + kHeaderSizeBytes, short_string_length);
    }

    if (btype == VariantValue::BasicType::PRIMITIVE) {
        return get_primitive_string_or_binary(VariantType::STRING);
    }

    return Status::VariantError("Required a string or a short string, but got: " + basic_type_to_string(btype));
}

StatusOr<std::string_view> VariantValue::get_binary() const {
    RETURN_IF_ERROR(validate_basic_type(VariantValue::BasicType::PRIMITIVE));

    return get_primitive_string_or_binary(VariantType::BINARY);
}

StatusOr<int32_t> VariantValue::get_date() const {
    return get_primitive<int32_t>(VariantType::DATE);
}

StatusOr<int64_t> VariantValue::get_time_micros_ntz() const {
    return get_primitive<int64_t>(VariantType::TIME_NTZ);
}

StatusOr<int64_t> VariantValue::get_timestamp_micros() const {
    return get_primitive<int64_t>(VariantType::TIMESTAMP_TZ);
}

StatusOr<int64_t> VariantValue::get_timestamp_micros_ntz() const {
    return get_primitive<int64_t>(VariantType::TIMESTAMP_NTZ);
}

StatusOr<int64_t> VariantValue::get_timestamp_nanos_tz() const {
    return get_primitive<int64_t>(VariantType::TIMESTAMP_TZ_NANOS);
}

StatusOr<int64_t> VariantValue::get_timestamp_nanos_ntz() const {
    return get_primitive<int64_t>(VariantType::TIMESTAMP_NTZ_NANOS);
}

StatusOr<std::array<uint8_t, 16>> VariantValue::get_uuid() const {
    RETURN_IF_ERROR(validate_basic_type(VariantValue::BasicType::PRIMITIVE));

    RETURN_IF_ERROR(validate_primitive_type(VariantType::UUID, 16 + kHeaderSizeBytes));

    std::array<uint8_t, 16> uuid_value;
    memcpy(uuid_value.data(), _value.data() + kHeaderSizeBytes, sizeof(uuid_value));
    return uuid_value;
}

StatusOr<uint32_t> VariantValue::num_elements() const {
    switch (VariantValue::BasicType btype = basic_type()) {
    case VariantValue::BasicType::OBJECT: {
        auto status = get_object_info();
        if (!status.ok()) {
            return status.status();
        }
        return status.value().num_elements;
    }
    case VariantValue::BasicType::ARRAY: {
        auto status = get_array_info();
        if (!status.ok()) {
            return status.status();
        }
        return status.value().num_elements;
    }
    default:
        return Status::VariantError("Cannot get number of elements for basic type: " + basic_type_to_string(btype));
    }
}

StatusOr<VariantValue> VariantValue::get_object_by_key(const VariantMetadata& metadata, std::string_view key) const {
    RETURN_IF_ERROR(validate_basic_type(VariantValue::BasicType::OBJECT));

    auto obj_status = get_object_info();
    if (!obj_status.ok()) {
        return obj_status.status();
    }

    const auto [num_elements, id_start_offset, id_size, offset_start_offset, offset_size, data_start_offset] =
            obj_status.value();
    const std::vector<uint32_t> dict_indexes = metadata.get_index(key);
    if (dict_indexes.empty()) {
        return Status::NotFound("Field key not exists: " + std::string(key));
    }

    for (uint32_t dict_index : dict_indexes) {
        std::optional<uint32_t> field_index_opt;
        for (uint32_t i = 0; i < num_elements; i++) {
            uint32_t field_id =
                    VariantUtil::read_little_endian_unsigned32(_value.data() + id_start_offset + i * id_size, id_size);
            if (field_id == dict_index) {
                field_index_opt = i;
                break;
            }
        }

        if (!field_index_opt.has_value()) {
            continue;
        }

        const uint32_t field_index = field_index_opt.value();
        const uint32_t offset = VariantUtil::read_little_endian_unsigned32(
                _value.data() + offset_start_offset + field_index * offset_size, offset_size);
        if (data_start_offset + offset >= _value.size()) {
            return Status::VariantError("Offset is out of bounds: " + std::to_string(offset) +
                                        ", data_start_offset: " + std::to_string(data_start_offset) +
                                        ", value_size: " + std::to_string(_value.size()));
        }

        return VariantValue(_value.substr(data_start_offset + offset));
    }

    return Status::NotFound("Field key not found: " + std::string(key));
}

StatusOr<VariantValue> VariantValue::get_element_at_index(const VariantMetadata& metadata, uint32_t index) const {
    RETURN_IF_ERROR(validate_basic_type(VariantValue::BasicType::ARRAY));

    auto array_info_status = get_array_info();
    if (!array_info_status.ok()) {
        return array_info_status.status();
    }

    const auto& info = array_info_status.value();
    if (index >= info.num_elements) {
        return Status::VariantError("Array index out of range: " + std::to_string(index) +
                                    " >= " + std::to_string(info.num_elements));
    }

    uint32_t offset = VariantUtil::read_little_endian_unsigned32(
            _value.data() + info.offset_start_offset + index * info.offset_size, info.offset_size);
    if (info.data_start_offset + offset >= _value.size()) {
        return Status::VariantError("Offset is out of bounds: " + std::to_string(offset) +
                                    ", data_start_offset: " + std::to_string(info.data_start_offset) +
                                    ", value_size: " + std::to_string(_value.size()));
    }

    const std::string_view element_value = _value.substr(info.data_start_offset + offset);
    return VariantValue{element_value};
}

static std::string epoch_day_to_date(int32_t epoch_days) {
    std::time_t raw_time = epoch_days * 86400; // to seconds
    std::tm* ptm = std::gmtime(&raw_time);     // to UTC
    char buffer[11];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", ptm);
    return buffer;
}

// Escape a string according to JSON specification (RFC 8259)
static std::string escape_json_string(std::string_view str) {
    std::stringstream ss;
    for (unsigned char c : str) {
        switch (c) {
        case '"':
            ss << "\\\"";
            break;
        case '\\':
            ss << "\\\\";
            break;
        case '\b':
            ss << "\\b";
            break;
        case '\f':
            ss << "\\f";
            break;
        case '\n':
            ss << "\\n";
            break;
        case '\r':
            ss << "\\r";
            break;
        case '\t':
            ss << "\\t";
            break;
        default:
            // Control characters (U+0000 through U+001F) must be escaped
            if (c < 0x20) {
                ss << "\\u" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(c);
            } else {
                ss << c;
            }
            break;
        }
    }
    return ss.str();
}

void append_quoted_string(std::stringstream& ss, const std::string& str) {
    ss << '"' << escape_json_string(str) << '"';
}

std::string remove_trailing_zeros(const std::string& str) {
    const size_t dot_pos = str.find('.');
    if (dot_pos == std::string::npos) {
        return str;
    }

    const size_t last_nonzero = str.find_last_not_of('0');
    if (last_nonzero == dot_pos) {
        return str.substr(0, dot_pos + 2); // Keep ".0"
    }
    if (last_nonzero != std::string::npos && last_nonzero > dot_pos) {
        return str.substr(0, last_nonzero + 1);
    }

    return str;
}

template <typename FloatType>
static std::string float_to_json_string_impl(FloatType value) {
    if (!std::isfinite(value)) {
        return "null";
    }

    char buffer[32];
    int precision = std::is_same_v<FloatType, float> ? std::numeric_limits<float>::max_digits10
                                                     : std::numeric_limits<double>::max_digits10;

    auto [ptr, ec] = std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::general, precision);
    if (ec != std::errc()) {
        return "null";
    }

    std::string result(buffer, ptr - buffer);
    if (result.find('.') == std::string::npos && result.find('e') == std::string::npos &&
        result.find('E') == std::string::npos) {
        result += ".0";
    }

    return result;
}

Status VariantUtil::variant_to_json(const VariantMetadata& metadata, const VariantValue& variant,
                                    std::stringstream& json_str, cctz::time_zone timezone) {
    switch (variant.type()) {
    case VariantType::NULL_TYPE:
        json_str << "null";
        break;
    case VariantType::BOOLEAN_TRUE:
    case VariantType::BOOLEAN_FALSE: {
        bool res = *variant.get_bool();
        json_str << (res ? "true" : "false");
        break;
    }
    case VariantType::INT8:
        json_str << std::to_string(*variant.get_int8());
        break;
    case VariantType::INT16:
        json_str << std::to_string(*variant.get_int16());
        break;
    case VariantType::INT32:
        json_str << std::to_string(*variant.get_int32());
        break;
    case VariantType::INT64:
        json_str << std::to_string(*variant.get_int64());
        break;
    case VariantType::FLOAT: {
        const float f = *variant.get_float();
        json_str << float_to_json_string_impl(f);
        break;
    }
    case VariantType::DOUBLE: {
        const double d = *variant.get_double();
        json_str << float_to_json_string_impl(d);
        break;
    }
    case VariantType::DECIMAL4: {
        VariantDecimalValue<int32_t> decimal = *variant.get_decimal4();
        json_str << remove_trailing_zeros(decimal.to_string());
        break;
    }
    case VariantType::DECIMAL8: {
        VariantDecimalValue<int64_t> decimal = *variant.get_decimal8();
        json_str << remove_trailing_zeros(decimal.to_string());
        break;
    }
    case VariantType::DECIMAL16: {
        VariantDecimalValue<int128_t> decimal = *variant.get_decimal16();
        json_str << remove_trailing_zeros(decimal.to_string());
        break;
    }
    case VariantType::STRING: {
        const std::string_view str_view = *variant.get_string();
        const std::string str(str_view.data(), str_view.size());
        append_quoted_string(json_str, str);
        break;
    }
    case VariantType::BINARY: {
        const std::string_view binary = *variant.get_binary();
        const std::string binary_str(binary.data(), binary.size());
        std::string encoded;
        base64_encode(binary_str, &encoded);
        append_quoted_string(json_str, encoded);
        break;
    }
    case VariantType::UUID: {
        const auto uuid_arr = *variant.get_uuid();
        boost::uuids::uuid uuid{};
        for (size_t i = 0; i < uuid.size(); ++i) {
            uuid.data[i] = uuid_arr[i];
        }
        append_quoted_string(json_str, boost::uuids::to_string(uuid));
        break;
    }
    case VariantType::DATE: {
        int32_t date = *variant.get_date();
        std::string date_str = epoch_day_to_date(date);
        append_quoted_string(json_str, date_str);
        break;
    }
    case VariantType::TIMESTAMP_TZ: {
        const int64_t timestamp_micros = *variant.get_timestamp_micros();
        TimestampValue tsv{};
        tsv.from_unix_second(timestamp_micros / 1000000, timestamp_micros % 1000000);
        std::string timestamp_str = timestamp::to_string_with_timezone<false, false>(tsv.timestamp(), timezone);
        append_quoted_string(json_str, timestamp_str);
        break;
    }
    case VariantType::TIMESTAMP_NTZ: {
        const int64_t timestamp_micros = *variant.get_timestamp_micros_ntz();
        TimestampValue tsv{};
        tsv.from_unix_second(timestamp_micros / 1000000, timestamp_micros % 1000000);
        std::string timestamp_str = tsv.to_string(false);
        append_quoted_string(json_str, timestamp_str);
        break;
    }
    case VariantType::OBJECT: {
        auto info = variant.get_object_info();
        if (!info.ok()) {
            return info.status();
        }
        const std::string_view& value = variant.raw();
        const auto& [num_elements, id_start_offset, id_size, offset_start_offset, offset_size, data_start_offset] =
                info.value();
        json_str << "{";
        for (size_t i = 0; i < num_elements; ++i) {
            if (i > 0) {
                json_str << ",";
            }

            uint32_t id =
                    VariantUtil::read_little_endian_unsigned32(value.data() + id_start_offset + i * id_size, id_size);
            uint32_t offset = VariantUtil::read_little_endian_unsigned32(
                    value.data() + offset_start_offset + i * offset_size, offset_size);
            auto key = metadata.get_key(id);
            if (!key.ok()) {
                return key.status();
            }

            json_str << "\"" << escape_json_string(*key) << "\":";

            if (uint32_t next_pos = data_start_offset + offset; next_pos < value.size()) {
                std::string_view next_value = value.substr(next_pos, value.size() - next_pos);
                // Recursively convert the next value to JSON
                auto status = variant_to_json(metadata, VariantValue{next_value}, json_str, timezone);
                if (!status.ok()) {
                    return status;
                }
            } else {
                return Status::InternalError("Invalid offset in object: " + std::to_string(offset));
            }
        }
        json_str << "}";
        break;
    }
    case VariantType::ARRAY: {
        auto info = variant.get_array_info();
        if (!info.ok()) {
            return info.status();
        }
        const std::string_view& value = variant.raw();
        const auto& [num_elements, offset_size, offset_start_offset, data_start_offset] = info.value();
        json_str << "[";
        for (size_t i = 0; i < num_elements; ++i) {
            if (i > 0) {
                json_str << ",";
            }

            uint32_t offset = VariantUtil::read_little_endian_unsigned32(
                    value.data() + offset_start_offset + i * offset_size, offset_size);
            if (uint32_t next_pos = data_start_offset + offset; next_pos < value.size()) {
                std::string_view next_value = value.substr(next_pos, value.size() - next_pos);
                // Recursively convert the next value to JSON
                auto status = variant_to_json(metadata, VariantValue{next_value}, json_str, timezone);
                if (!status.ok()) {
                    return status;
                }
            } else {
                return Status::InternalError("Invalid offset in array: " + std::to_string(offset));
            }
        }
        json_str << "]";
        break;
    }
    default:
        return Status::NotSupported("Unsupported variant type: " + VariantUtil::variant_type_to_string(variant.type()));
    }

    return Status::OK();
}

} // namespace starrocks
