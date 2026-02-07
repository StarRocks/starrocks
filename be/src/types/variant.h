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

#include <arrow/util/endian.h>
#include <cctz/time_zone.h>

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/decimal_types.h"
#include "common/status.h"

namespace starrocks {

struct VariantEncodingContext;

enum class VariantType : uint8_t {
    // Primitive types
    NULL_TYPE = 0,
    BOOLEAN_TRUE = 1,
    BOOLEAN_FALSE = 2,
    INT8 = 3,
    INT16 = 4,
    INT32 = 5,
    INT64 = 6,
    DOUBLE = 7,
    DECIMAL4 = 8,
    DECIMAL8 = 9,
    DECIMAL16 = 10,
    DATE = 11,
    TIMESTAMP_TZ = 12,
    TIMESTAMP_NTZ = 13,
    FLOAT = 14,
    BINARY = 15,
    STRING = 16,
    TIME_NTZ = 17, // Time without timezone, stored as microseconds
    TIMESTAMP_TZ_NANOS = 18,
    TIMESTAMP_NTZ_NANOS = 19,
    UUID = 20,
    // Compex types
    OBJECT = 21,
    ARRAY = 22,
};

template <typename D>
struct VariantDecimalValue {
    uint8_t scale;
    DecimalType<D> value;

    template <typename T>
    T to_float() const {
        return static_cast<T>(static_cast<double>(value) / std::pow(10, scale));
    }

    explicit operator float() const { return to_float<float>(); }
    explicit operator double() const { return to_float<double>(); }

    template <typename T>
    T to_int() const {
        if (scale == 0) {
            return static_cast<T>(value);
        }
        D value_copy = value;
        for (int i = 0; i < scale; i++) {
            value_copy /= 10;
        }
        return static_cast<T>(value_copy);
    }

    explicit operator int8_t() const { return to_int<int8_t>(); }

    explicit operator int16_t() const { return to_int<int16_t>(); }

    explicit operator int32_t() const { return to_int<int32_t>(); }

    explicit operator int64_t() const { return to_int<int64_t>(); }

    explicit operator int128_t() const { return to_int<int128_t>(); }

    std::string to_string() const;
};

class VariantValue;
class VariantMetadata;

struct VariantUtil {
    static Status variant_to_json(const VariantMetadata& metadata, const VariantValue& value,
                                  std::stringstream& json_str, cctz::time_zone timezone = cctz::local_time_zone());

    static inline uint32_t read_little_endian_unsigned32(const void* from, uint8_t size) {
        DCHECK_LE(size, 4);
        DCHECK_GE(size, 1);

        if (size == 1) {
            return static_cast<uint32_t>(*(static_cast<const uint8_t*>(from)));
        }

        uint32_t result = 0;
        __builtin_memcpy(&result, from, size);
        return arrow::bit_util::FromLittleEndian(result);
    }

    static std::string variant_type_to_string(VariantType type);
};

class VariantMetadata {
public:
    // We probably will optimize internal state like to build indexes for dictionary lookups.
    // so the cost of creating VariantMetadata is not trivial
    explicit VariantMetadata(std::string_view metadata) : _metadata(metadata) {
        if (_metadata.data() == kEmptyMetadata.data()) {
            return;
        }

        // Empty metadata is at least 3 bytes: version, dictionarySize and
        // at least one offset.
        DCHECK(!metadata.empty()) << "Variant metadata cannot be empty";
        DCHECK(metadata.size() >= 3) << "Variant metadata size is too short: " << std::to_string(metadata.size());

        const uint8_t version = header() & kVersionMask;
        DCHECK(version == kSupportedVersion) << "Unsupported variant version: " << std::to_string(version);

        // unintialize dict size, will be lazily computed
        _dict_size = -1;
    }
    VariantMetadata() : _metadata(kEmptyMetadata) {}

    // return the field name for the index
    StatusOr<std::string_view> get_key(uint32_t index) const;

    // return the metadata raw string view
    std::string_view raw() const { return _metadata; }

    static constexpr char kEmptyMetadataChars[] = {0x1, 0x0, 0x0};
    static constexpr std::string_view kEmptyMetadata{kEmptyMetadataChars, sizeof(kEmptyMetadataChars)};

    bool operator==(const VariantMetadata& other) const { return _metadata == other._metadata; }

private:
    friend struct VariantEncodingContext;
    static constexpr uint8_t kVersionMask = 0b1111;
    static constexpr uint8_t kSupportedVersion = 1;
    static constexpr size_t kHeaderSizeBytes = 1;
    static constexpr uint8_t kSortedStringMask = 0b10000;
    static constexpr uint8_t kOffsetMask = 0b11000000;
    static constexpr uint8_t kOffsetSizeBitShift = 6;
    static constexpr uint8_t kOffsetSizeMask = 0b11;

    uint8_t header() const { return static_cast<uint8_t>(_metadata[0]); }
    bool is_sorted_and_unique() const { return (header() & kSortedStringMask) != 0; }
    uint8_t offset_size() const { return ((header() & kOffsetMask) >> kOffsetSizeBitShift) + 1; }
    // indicating the number of strings in the dictionary
    uint32_t dict_size() const {
        if (_dict_size != -1) {
            return _dict_size;
        }
        const uint8_t offset_sz = offset_size();
        _dict_size = VariantUtil::read_little_endian_unsigned32(_metadata.data() + kHeaderSizeBytes, offset_sz);
        return _dict_size;
    }

    std::string_view _metadata;
    mutable int32_t _dict_size{0};

    friend class VariantValue;
    // return the index for the key in the dictionary
    // we use void* to avoid expose specific type in the header
    Status _get_index(std::string_view key, void* indexes, int hint) const;

    Status _build_lookup_index() const;

    // fields for optimization. they are computed lazily
    struct LookupIndex {
        bool has_built = false;
        std::vector<std::string_view> dict_strings;
    };

    mutable LookupIndex _lookup_index;
};

struct VariantEncodingContext {
    std::unordered_set<std::string> keys;
    std::unordered_map<std::string_view, uint32_t> key_to_id;
    std::string_view metadata_raw;
    bool metadata_built = false;

    void reset();
    Status use_metadata(const VariantMetadata& meta);
};

// Representing the details of a Variant of Object

/**
 *                5   4  3     2 1     0
 *              +---+---+-------+-------+
 * value_header |   |   |       |       |
 *              +---+---+-------+-------+
 *                    ^     ^       ^
 *                    |     |       +-- field_offset_size_minus_one
 *                    |     +-- field_id_size_minus_one
 *                    +-- is_large
 */

struct VariantObjectInfo {
    // Number of elements in the array or object
    uint32_t num_elements;
    // The byte offset of the field id
    uint32_t id_start_offset;
    // The number of bytes used to encode the field ids
    uint8_t id_size;
    // The number of bytes used to encode the field offsets
    uint32_t offset_start_offset;
    // The size of the field offset list
    uint8_t offset_size;
    // The byte offset of the field data
    uint32_t data_start_offset;
};

// Representing the details of a Variant of Array

/**
 *                5         3  2  1     0
 *               +-----------+---+-------+
 * value_header  |           |   |       |
 *               +-----------+---+-------+
 *                             ^     ^
 *                             |     +-- field_offset_size_minus_one
 *                             +-- is_large
 */
struct VariantArrayInfo {
    // Number of elements in the array
    uint32_t num_elements;
    // The size of the field offset list
    uint8_t offset_size;
    // The byte offset of the field offset list
    uint32_t offset_start_offset;
    // The byte offset of the field data
    uint32_t data_start_offset;
};

class VariantValue {
public:
    enum class BasicType : uint8_t { PRIMITIVE = 0, SHORT_STRING = 1, OBJECT = 2, ARRAY = 3 };

    /**
     * kEmptyVariant represents a NULL variant value.
     * The byte value is: (VariantType::NULL_TYPE << 2) | BasicType::PRIMITIVE (0)
     * This is used as the default value for empty VariantRowValue objects.
     */
    static constexpr char null_chars[1] = {static_cast<uint8_t>(VariantType::NULL_TYPE) << 2};
    static constexpr std::string_view kEmptyValue{null_chars, sizeof(null_chars)};

public:
    explicit VariantValue(std::string_view value) : _value(value) {
        DCHECK(!value.empty()) << "Variant value cannot be empty";
    }
    VariantValue() : _value(kEmptyValue) {}

    static constexpr uint8_t kHeaderSizeBytes = 1;
    static constexpr size_t kDecimalScaleSizeBytes = 1;
    static constexpr uint8_t kBasicTypeMask = 0b00000011;
    static constexpr uint8_t kValueHeaderBitShift = 2;

    BasicType basic_type() const { return static_cast<VariantValue::BasicType>(_value[0] & kBasicTypeMask); }
    std::string_view raw() const { return _value; }
    VariantType type() const {
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
    bool is_null() const { return _value[0] == 0; }

    // Get the primitive boolean value.
    StatusOr<bool> get_bool() const;
    // Get the primitive int8 value.
    StatusOr<int8_t> get_int8() const;
    // Get the primitive int16 value.
    StatusOr<int16_t> get_int16() const;
    // Get the primitive int32 value.
    StatusOr<int32_t> get_int32() const;
    // Get the primitive int64 value.
    StatusOr<int64_t> get_int64() const;
    // Get the string value, including both short string optimization and primitive string type.
    StatusOr<std::string_view> get_string() const;
    // Get the binary value.
    StatusOr<std::string_view> get_binary() const;
    // Get the primitive float value.
    StatusOr<float> get_float() const;
    // Get the primitive double value.
    StatusOr<double> get_double() const;
    // Get the decimal value
    StatusOr<VariantDecimalValue<int32_t>> get_decimal4() const;
    StatusOr<VariantDecimalValue<int64_t>> get_decimal8() const;
    StatusOr<VariantDecimalValue<int128_t>> get_decimal16() const;
    // Get the date value as days since Unix epoch.
    StatusOr<int32_t> get_date() const;
    // Get the time value without timezone as microseconds since midnight.
    StatusOr<int64_t> get_time_micros_ntz() const;
    // Get the timestamp value with UTC timezone as microseconds since Unix epoch.
    StatusOr<int64_t> get_timestamp_micros() const;
    // Get the timestamp value without timezone as microseconds since Unix epoch.
    StatusOr<int64_t> get_timestamp_micros_ntz() const;
    // Get the timestamp value with UTC timezone as nanoseconds since Unix epoch.
    StatusOr<int64_t> get_timestamp_nanos_tz() const;
    // Get the timestamp value without timezone as nanoseconds since Unix epoch.
    StatusOr<int64_t> get_timestamp_nanos_ntz() const;
    // Get the UUID value as a 16-byte array.
    StatusOr<std::array<uint8_t, 16>> get_uuid() const;

    // Get the number of elements.
    // For array, it returns the number of elements in the array.
    // For object, it returns the number of fields in the object.
    StatusOr<uint32_t> num_elements() const;

    // Get the value of the object field by key.
    // returns the value of the field with the given key
    StatusOr<VariantValue> get_object_by_key(const VariantMetadata& metadata, std::string_view key) const;

    // Get the variant value of the object field
    // returns the value of the field with the given field id
    StatusOr<VariantValue> get_element_at_index(const VariantMetadata& metadata, uint32_t index) const;

    StatusOr<VariantObjectInfo> get_object_info() const;

    StatusOr<VariantArrayInfo> get_array_info() const;

    bool operator==(const VariantValue& other) const { return _value == other._value; }

private:
    uint8_t value_header() const { return static_cast<uint8_t>(_value[0]) >> kValueHeaderBitShift; }
    Status validate_primitive_type(VariantType type, size_t size_required) const;

    template <typename PrimitiveType>
    StatusOr<PrimitiveType> get_primitive(VariantType type) const;

    StatusOr<std::string_view> get_primitive_string_or_binary(VariantType type) const;

    template <typename DecimalType>
    StatusOr<VariantDecimalValue<DecimalType>> get_primitive_decimal(VariantType type) const;

    /**
     * Value layout:
     *  7                                  2 1          0
     * +------------------------------------+------------+
     * |            value_header            | basic_type |
     * +------------------------------------+------------+
     * |                                                 |
     * :                   value_data                    :  <-- 0 or more bytes
     * |                                                 |
     * +-------------------------------------------------+
     */
    std::string_view _value;
};

} // namespace starrocks
