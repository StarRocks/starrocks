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
#include <formats/parquet/variant.h>
#include <glog/logging.h>

#include "common/statusor.h"
#include "runtime/decimalv3.h"

namespace starrocks {

inline uint32_t read_little_endian_unsigned32(const void* from, uint8_t size) {
    DCHECK_LE(size, 4);
    DCHECK_GE(size, 1);

    uint32_t result = 0;
    memcpy(&result, from, size);
    return arrow::bit_util::FromLittleEndian(result);
}

VariantMetadata::VariantMetadata(std::string_view metadata) : _metadata(metadata) {
    // Empty metadata is at least 3 bytes: version, dictionarySize and
    // at least one offset.
    DCHECK(!metadata.empty()) << "Variant metadata cannot be empty";
    DCHECK(metadata.size() >= 3) << "Variant metadata size is too short: " << std::to_string(metadata.size());

    const uint8_t version = header() & kVersionMask;
    DCHECK(version == kSupportedVersion) << "Unsupported variant version: " << std::to_string(version);

    const uint8_t offset_sz = offset_size();
    _dict_size = read_little_endian_unsigned32(metadata.data() + kHeaderSizeBytes, offset_sz);
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
    uint32_t value_offset = read_little_endian_unsigned32(_metadata.data() + offset_start_pos, offset_sz);
    uint32_t value_next_offset =
            read_little_endian_unsigned32(_metadata.data() + offset_start_pos + offset_sz, offset_sz);
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
            dict_next_key_offset =
                    read_little_endian_unsigned32(_metadata.data() + offset_start_pos + offset_sz, offset_sz);
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
Variant::Variant(const VariantMetadata& metadata, std::string_view value) : _metadata(metadata), _value(value) {
    DCHECK(!value.empty()) << "Variant value cannot be empty";
}

BasicType Variant::basic_type() const {
    return static_cast<BasicType>(_value[0] & kBasicTypeMask);
}

VariantType Variant::type() const {
    switch (basic_type()) {
    case BasicType::PRIMITIVE: {
        auto primitive_type = static_cast<VariantPrimitiveType>(value_header());
        switch (primitive_type) {
        case VariantPrimitiveType::NULL_TYPE:
            return VariantType::NULL_TYPE;
        case VariantPrimitiveType::BOOLEAN_TRUE:
        case VariantPrimitiveType::BOOLEAN_FALSE:
            return VariantType::BOOLEAN;
        case VariantPrimitiveType::INT8:
            return VariantType::INT8;
        case VariantPrimitiveType::INT16:
            return VariantType::INT16;
        case VariantPrimitiveType::INT32:
            return VariantType::INT32;
        case VariantPrimitiveType::INT64:
            return VariantType::INT64;
        case VariantPrimitiveType::FLOAT:
            return VariantType::FLOAT;
        case VariantPrimitiveType::DOUBLE:
            return VariantType::DOUBLE;
        case VariantPrimitiveType::DECIMAL4:
            return VariantType::DECIMAL4;
        case VariantPrimitiveType::DECIMAL8:
            return VariantType::DECIMAL8;
        case VariantPrimitiveType::DECIMAL16:
            return VariantType::DECIMAL16;
        case VariantPrimitiveType::DATE:
            return VariantType::DATE;
        case VariantPrimitiveType::TIMESTAMP_TZ:
            return VariantType::TIMESTAMP_TZ;
        case VariantPrimitiveType::TIMESTAMP_NTZ:
            return VariantType::TIMESTAMP_NTZ;
        case VariantPrimitiveType::TIME_NTZ:
            return VariantType::TIME_NTZ;
        case VariantPrimitiveType::TIMESTAMP_TZ_NANOS:
            return VariantType::TIMESTAMP_TZ_NANOS;
        case VariantPrimitiveType::TIMESTAMP_NTZ_NANOS:
            return VariantType::TIMESTAMP_NTZ_NANOS;
        case VariantPrimitiveType::BINARY:
            return VariantType::BINARY;
        case VariantPrimitiveType::STRING:
            return VariantType::STRING;
        case VariantPrimitiveType::UUID:
            return VariantType::UUID;
        }
    }
    case BasicType::SHORT_STRING:
        return VariantType::STRING; // Short string is treated as a string type.
    case BasicType::OBJECT:
        return VariantType::OBJECT;
    case BasicType::ARRAY:
        return VariantType::ARRAY;
    default:
        return VariantType::NULL_TYPE; // Should not happen, but return NULL_TYPE as a fallback.
    }
}

const VariantMetadata& Variant::metadata() const {
    return _metadata;
}

std::string_view Variant::value() const {
    return _value;
}

StatusOr<ObjectInfo> get_object_info(std::string_view value) {
    BasicType basic_type = static_cast<BasicType>(static_cast<uint8_t>(value[0]) & Variant::kBasicTypeMask);
    if (basic_type != BasicType::OBJECT) {
        return Status::VariantError("Cannot read object value as " + basic_type_to_string(basic_type));
    }

    uint8_t value_header = (static_cast<uint8_t>(value[0]) >> Variant::kValueHeaderBitShift) & 0x3F;
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

    uint32_t num_elements = read_little_endian_unsigned32(value.data() + Variant::kHeaderSizeBytes, num_elements_size);

    ObjectInfo object_info{};
    object_info.num_elements = num_elements;
    object_info.id_size = field_id_size;
    object_info.offset_size = field_offset_size;
    object_info.id_start_offset = 1 + num_elements_size;
    object_info.offset_start_offset = object_info.id_start_offset + num_elements * field_id_size;
    object_info.data_start_offset = object_info.offset_start_offset + (num_elements + 1) * field_offset_size;
    // Check the boundary with the final offset
    if (object_info.data_start_offset > value.size()) {
        return Status::VariantError(
                "Invalid object value: data_start_offset=" + std::to_string(object_info.data_start_offset) +
                ", value_size=" + std::to_string(value.size()));
    }

    {
        const uint32_t final_offset = read_little_endian_unsigned32(
                value.data() + object_info.offset_start_offset + num_elements * field_offset_size, field_offset_size);
        // It could be less than value size since it could be a sub-object.
        if (final_offset + object_info.data_start_offset > value.size()) {
            return Status::VariantError("Invalid object value: final_offset=" + std::to_string(final_offset) +
                                        ", data_start_offset=" + std::to_string(object_info.data_start_offset) +
                                        ", value_size=" + std::to_string(value.size()));
        }
    }

    return StatusOr<ObjectInfo>(object_info);
}

StatusOr<ArrayInfo> get_array_info(std::string_view value) {
    BasicType basic_type = static_cast<BasicType>(static_cast<uint8_t>(value[0]) & Variant::kBasicTypeMask);
    if (basic_type != BasicType::ARRAY) {
        return Status::VariantError("Cannot read array value as " + basic_type_to_string(basic_type));
    }

    uint8_t value_header = (static_cast<uint8_t>(value[0]) >> Variant::kValueHeaderBitShift) & 0x3F;
    // represents the number of bytes used to encode the field offset.
    uint8_t field_offset_size = (value_header & 0b11) + 1;
    // is_large is a 1-bit value that indicates how many bytes are used to encode the number of elements.
    bool is_large = ((value_header >> 2) & 0b1);
    uint8_t num_elements_size = is_large ? 4 : 1;
    if (value.size() < static_cast<size_t>(1 + num_elements_size)) {
        return Status::VariantError("Too short array value: " + std::to_string(value.size()) + " for at least " +
                                    std::to_string(1 + num_elements_size));
    }

    uint32_t num_elements = read_little_endian_unsigned32(value.data() + Variant::kHeaderSizeBytes, num_elements_size);

    ArrayInfo array_info{};
    array_info.num_elements = num_elements;
    array_info.offset_size = field_offset_size;
    array_info.offset_start_offset = Variant::kHeaderSizeBytes + num_elements_size;
    array_info.data_start_offset = array_info.offset_start_offset + field_offset_size * (num_elements + 1);

    if (array_info.data_start_offset > value.size()) {
        return Status::VariantError(
                "Invalid array value: data_start_offset=" + std::to_string(array_info.data_start_offset) +
                ", value_size=" + std::to_string(value.size()));
    }

    return StatusOr<ArrayInfo>(array_info);
}

std::string basic_type_to_string(BasicType type) {
    switch (type) {
    case BasicType::PRIMITIVE:
        return "Primitive";
    case BasicType::SHORT_STRING:
        return "ShortString";
    case BasicType::OBJECT:
        return "Object";
    case BasicType::ARRAY:
        return "Array";
    default:
        return "Unknown";
    }
}

std::string primitive_type_to_string(VariantPrimitiveType type) {
    switch (type) {
    case VariantPrimitiveType::NULL_TYPE:
        return "Null";
    case VariantPrimitiveType::BOOLEAN_TRUE:
        return "BooleanTrue";
    case VariantPrimitiveType::BOOLEAN_FALSE:
        return "BooleanFalse";
    case VariantPrimitiveType::INT8:
        return "Int8";
    case VariantPrimitiveType::INT16:
        return "Int16";
    case VariantPrimitiveType::INT32:
        return "Int32";
    case VariantPrimitiveType::INT64:
        return "Int64";
    case VariantPrimitiveType::FLOAT:
        return "Float";
    case VariantPrimitiveType::DOUBLE:
        return "Double";
    case VariantPrimitiveType::DECIMAL4:
        return "Decimal4";
    case VariantPrimitiveType::DECIMAL8:
        return "Decimal8";
    case VariantPrimitiveType::DECIMAL16:
        return "Decimal16";
    case VariantPrimitiveType::DATE:
        return "Date";
    case VariantPrimitiveType::TIMESTAMP_TZ:
        return "TimestampTz";
    case VariantPrimitiveType::TIMESTAMP_NTZ:
        return "TimestampNtz";
    case VariantPrimitiveType::TIME_NTZ:
        return "Time";
    case VariantPrimitiveType::TIMESTAMP_TZ_NANOS:
        return "TimestampTzNanos";
    case VariantPrimitiveType::TIMESTAMP_NTZ_NANOS:
        return "TimestampNtzNanos";
    case VariantPrimitiveType::BINARY:
        return "Binary";
    case VariantPrimitiveType::STRING:
        return "String";
    case VariantPrimitiveType::UUID:
        return "Uuid";
    }
    return "Unknown";
}

std::string variant_type_to_string(VariantType type) {
    switch (type) {
    case VariantType::NULL_TYPE:
        return "Null";
    case VariantType::BOOLEAN:
        return "Boolean";
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
    case VariantType::OBJECT:
        return "Object";
    case VariantType::ARRAY:
        return "Array";
    case VariantType::UUID:
        return "Uuid";
    }
    return "Unknown";
}

uint8_t Variant::value_header() const {
    return static_cast<uint8_t>(_value[0]) >> kValueHeaderBitShift;
}

Status Variant::validate_basic_type(BasicType type) const {
    if (basic_type() != type) {
        return Status::VariantError("Expected basic type: " + basic_type_to_string(type) +
                                    ", but got: " + basic_type_to_string(basic_type()));
    }

    return Status::OK();
}

Status Variant::validate_primitive_type(VariantPrimitiveType type, size_t size_required) const {
    RETURN_IF_ERROR(validate_basic_type(BasicType::PRIMITIVE));

    auto primitive_type = static_cast<VariantPrimitiveType>(value_header());
    if (primitive_type != type) {
        return Status::VariantError("Expected primitive type: " + primitive_type_to_string(type) +
                                    ", but got: " + primitive_type_to_string(primitive_type));
    }

    if (_value.size() < size_required) {
        return Status::VariantError("Value is too short, expected at least " + std::to_string(size_required) +
                                    " bytes for type " + primitive_type_to_string(type) +
                                    ", but got: " + std::to_string(_value.size()) + " bytes");
    }

    return Status::OK();
}

template <typename PrimitiveType>
StatusOr<PrimitiveType> Variant::get_primitive(VariantPrimitiveType type) const {
    RETURN_IF_ERROR(validate_primitive_type(type, sizeof(PrimitiveType) + kHeaderSizeBytes));

    PrimitiveType primitive_value{};
    memcpy(&primitive_value, _value.data() + kHeaderSizeBytes, sizeof(PrimitiveType));
    primitive_value = arrow::bit_util::FromLittleEndian(primitive_value);

    return primitive_value;
}

StatusOr<bool> Variant::get_bool() const {
    RETURN_IF_ERROR(validate_basic_type(basic_type()));

    // extract the primitive type from the header
    VariantPrimitiveType primitive_type = static_cast<VariantPrimitiveType>(value_header());
    if (primitive_type == VariantPrimitiveType::BOOLEAN_TRUE) {
        return true;
    }
    if (primitive_type == VariantPrimitiveType::BOOLEAN_FALSE) {
        return false;
    }

    return Status::VariantError("Not a variant primitive boolean type with primitive type: " +
                                primitive_type_to_string(primitive_type));
}

StatusOr<int8_t> Variant::get_int8() const {
    return get_primitive<int8_t>(VariantPrimitiveType::INT8);
}

StatusOr<int16_t> Variant::get_int16() const {
    return get_primitive<int16_t>(VariantPrimitiveType::INT16);
}

StatusOr<int32_t> Variant::get_int32() const {
    return get_primitive<int32_t>(VariantPrimitiveType::INT32);
}

StatusOr<int64_t> Variant::get_int64() const {
    return get_primitive<int64_t>(VariantPrimitiveType::INT64);
}

StatusOr<float> Variant::get_float() const {
    return get_primitive<float>(VariantPrimitiveType::FLOAT);
}

StatusOr<double> Variant::get_double() const {
    return get_primitive<double>(VariantPrimitiveType::DOUBLE);
}

template <typename DecimalType>
StatusOr<DecimalValue<DecimalType>> Variant::get_primitive_decimal(VariantPrimitiveType type) const {
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

    return DecimalValue<DecimalType>{scale, decimal_value};
}

StatusOr<DecimalValue<int32_t>> Variant::get_decimal4() const {
    return get_primitive_decimal<int32_t>(VariantPrimitiveType::DECIMAL4);
}

StatusOr<DecimalValue<int64_t>> Variant::get_decimal8() const {
    return get_primitive_decimal<int64_t>(VariantPrimitiveType::DECIMAL8);
}

StatusOr<DecimalValue<int128_t>> Variant::get_decimal16() const {
    return get_primitive_decimal<int128_t>(VariantPrimitiveType::DECIMAL16);
}

StatusOr<std::string_view> Variant::get_primitive_string_or_binary(VariantPrimitiveType type) const {
    // BINARY and STRING are both 4 byte little-endian size
    RETURN_IF_ERROR(validate_primitive_type(type, kHeaderSizeBytes + 4));

    uint32_t length = read_little_endian_unsigned32(_value.data() + kHeaderSizeBytes, sizeof(uint32_t));
    if (_value.size() < length + kHeaderSizeBytes + 4) {
        return Status::VariantError("Invalid string value: too short for specified length");
    }

    return std::string_view(_value.data() + kHeaderSizeBytes + 4, length);
}

StatusOr<std::string_view> Variant::get_string() const {
    BasicType btype = basic_type();
    if (btype == BasicType::SHORT_STRING) {
        // The short string header value is the length of the string.
        uint8_t short_string_length = value_header();
        if (_value.size() < static_cast<size_t>(short_string_length + kHeaderSizeBytes)) {
            return Status::VariantError("Invalid short string: too short: " + std::to_string(_value.size()) +
                                        " for at least " + std::to_string(short_string_length + kHeaderSizeBytes));
        }

        return std::string_view(_value.data() + kHeaderSizeBytes, short_string_length);
    }

    if (btype == BasicType::PRIMITIVE) {
        return get_primitive_string_or_binary(VariantPrimitiveType::STRING);
    }

    return Status::VariantError("Required a string or a short string, but got: " + basic_type_to_string(btype));
}

StatusOr<std::string_view> Variant::get_binary() const {
    RETURN_IF_ERROR(validate_basic_type(BasicType::PRIMITIVE));

    return get_primitive_string_or_binary(VariantPrimitiveType::BINARY);
}

StatusOr<int32_t> Variant::get_date() const {
    return get_primitive<int32_t>(VariantPrimitiveType::DATE);
}

StatusOr<int64_t> Variant::get_time_micros_ntz() const {
    return get_primitive<int64_t>(VariantPrimitiveType::TIME_NTZ);
}

StatusOr<int64_t> Variant::get_timestamp_micros() const {
    return get_primitive<int64_t>(VariantPrimitiveType::TIMESTAMP_TZ);
}

StatusOr<int64_t> Variant::get_timestamp_micros_ntz() const {
    return get_primitive<int64_t>(VariantPrimitiveType::TIMESTAMP_NTZ);
}

StatusOr<int64_t> Variant::get_timestamp_nanos_tz() const {
    return get_primitive<int64_t>(VariantPrimitiveType::TIMESTAMP_TZ_NANOS);
}

StatusOr<int64_t> Variant::get_timestamp_nanos_ntz() const {
    return get_primitive<int64_t>(VariantPrimitiveType::TIMESTAMP_NTZ_NANOS);
}

StatusOr<std::array<uint8_t, 16>> Variant::get_uuid() const {
    RETURN_IF_ERROR(validate_basic_type(BasicType::PRIMITIVE));

    RETURN_IF_ERROR(validate_primitive_type(VariantPrimitiveType::UUID, 16 + kHeaderSizeBytes));

    std::array<uint8_t, 16> uuid_value;
    memcpy(uuid_value.data(), _value.data() + kHeaderSizeBytes, sizeof(uuid_value));
    return uuid_value;
}

StatusOr<uint32_t> Variant::num_elements() const {
    switch (BasicType btype = basic_type()) {
    case BasicType::OBJECT: {
        auto status = get_object_info(_value);
        if (!status.ok()) {
            return status.status();
        }
        return status.value().num_elements;
    }
    case BasicType::ARRAY: {
        auto status = get_array_info(_value);
        if (!status.ok()) {
            return status.status();
        }
        return status.value().num_elements;
    }
    default:
        return Status::VariantError("Cannot get number of elements for basic type: " + basic_type_to_string(btype));
    }
}

StatusOr<Variant> Variant::get_object_by_key(std::string_view key) const {
    RETURN_IF_ERROR(validate_basic_type(BasicType::OBJECT));

    auto obj_status = get_object_info(_value);
    if (!obj_status.ok()) {
        return obj_status.status();
    }

    const auto [num_elements, id_start_offset, id_size, offset_start_offset, offset_size, data_start_offset] =
            obj_status.value();
    const std::vector<uint32_t> dict_indexes = _metadata.get_index(key);
    if (dict_indexes.empty()) {
        return Status::NotFound("Field key not exists: " + std::string(key));
    }

    for (uint32_t dict_index : dict_indexes) {
        std::optional<uint32_t> field_index_opt;
        for (uint32_t i = 0; i < num_elements; i++) {
            uint32_t field_id = read_little_endian_unsigned32(_value.data() + id_start_offset + i * id_size, id_size);
            if (field_id == dict_index) {
                field_index_opt = i;
                break;
            }
        }

        if (!field_index_opt.has_value()) {
            continue;
        }

        const uint32_t field_index = field_index_opt.value();
        const uint32_t offset = read_little_endian_unsigned32(
                _value.data() + offset_start_offset + field_index * offset_size, offset_size);
        if (data_start_offset + offset >= _value.size()) {
            return Status::VariantError("Offset is out of bounds: " + std::to_string(offset) +
                                        ", data_start_offset: " + std::to_string(data_start_offset) +
                                        ", value_size: " + std::to_string(_value.size()));
        }

        return Variant(_metadata, _value.substr(data_start_offset + offset));
    }

    return Status::NotFound("Field key not found: " + std::string(key));
}

StatusOr<Variant> Variant::get_element_at_index(uint32_t index) const {
    RETURN_IF_ERROR(validate_basic_type(BasicType::ARRAY));

    auto array_info_status = get_array_info(_value);
    if (!array_info_status.ok()) {
        return array_info_status.status();
    }

    const ArrayInfo& info = array_info_status.value();
    if (index >= info.num_elements) {
        return Status::VariantError("Array index out of range: " + std::to_string(index) +
                                    " >= " + std::to_string(info.num_elements));
    }

    uint32_t offset = read_little_endian_unsigned32(_value.data() + info.offset_start_offset + index * info.offset_size,
                                                    info.offset_size);
    if (info.data_start_offset + offset >= _value.size()) {
        return Status::VariantError("Offset is out of bounds: " + std::to_string(offset) +
                                    ", data_start_offset: " + std::to_string(info.data_start_offset) +
                                    ", value_size: " + std::to_string(_value.size()));
    }

    const std::string_view element_value = _value.substr(info.data_start_offset + offset);
    return Variant{_metadata, element_value};
}

} // namespace starrocks
