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

#include <cstdint>

#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "util/decimal_types.h"

namespace starrocks {

enum class BasicType { PRIMITIVE = 0, SHORT_STRING = 1, OBJECT = 2, ARRAY = 3 };

std::string basic_type_to_string(BasicType type);

enum class VariantPrimitiveType : uint8_t {
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
};

std::string primitive_type_to_string(VariantPrimitiveType type);

enum class VariantType {
    OBJECT,
    ARRAY,
    NULL_TYPE,
    BOOLEAN,
    INT8,
    INT16,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    DECIMAL4,
    DECIMAL8,
    DECIMAL16,
    DATE,
    TIMESTAMP_TZ,
    TIMESTAMP_NTZ,
    TIME_NTZ,
    TIMESTAMP_TZ_NANOS,
    TIMESTAMP_NTZ_NANOS,
    BINARY,
    STRING,
    UUID
};

std::string variant_type_to_string(VariantType type);

template <typename D>
struct DecimalValue {
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
};

class VariantMetadata {
public:
    explicit VariantMetadata(std::string_view metadata);

    uint8_t header() const;
    bool is_sorted_and_unique() const;
    uint8_t offset_size() const;
    // indicating the number of strings in the dictionary
    uint32_t dict_size() const;
    // return the index for the key in the dictionary
    std::vector<uint32_t> get_index(std::string_view key) const;
    // return the field name for the index
    StatusOr<std::string> get_key(uint32_t index) const;

    // return the metadata raw string view
    std::string_view get_raw() const { return _metadata; }

    static constexpr char kEmptyMetadataChars[] = {0x1, 0x0, 0x0};
    static constexpr std::string_view kEmptyMetadata{kEmptyMetadataChars, sizeof(kEmptyMetadataChars)};

private:
    static constexpr uint8_t kVersionMask = 0b1111;
    static constexpr uint8_t kSupportedVersion = 1;
    static constexpr size_t kHeaderSizeBytes = 1;
    static constexpr uint8_t kSortedStringMask = 0b10000;
    static constexpr uint8_t kOffsetMask = 0b11000000;
    static constexpr uint8_t kOffsetSizeBitShift = 6;
    static constexpr uint8_t kOffsetSizeMask = 0b11;

    std::string_view _metadata;
    uint32_t _dict_size{0};
};

class Variant {
public:
    explicit Variant(const VariantMetadata& metadata, std::string_view value);
    Variant(const std::string_view metadata, std::string_view value) : Variant(VariantMetadata(metadata), value) {}

    static constexpr uint8_t kHeaderSizeBytes = 1;
    static constexpr size_t kDecimalScaleSizeBytes = 1;
    static constexpr uint8_t kBasicTypeMask = 0b00000011;
    static constexpr uint8_t kValueHeaderBitShift = 2;

    BasicType basic_type() const;
    const VariantMetadata& metadata() const;
    std::string_view value() const;
    VariantType type() const;

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
    StatusOr<DecimalValue<int32_t>> get_decimal4() const;
    StatusOr<DecimalValue<int64_t>> get_decimal8() const;
    StatusOr<DecimalValue<int128_t>> get_decimal16() const;
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
    StatusOr<Variant> get_object_by_key(std::string_view key) const;

    // Get the variant value of the object field
    // returns the value of the field with the given field id
    StatusOr<Variant> get_element_at_index(uint32_t index) const;

private:
    uint8_t value_header() const;
    Status validate_basic_type(BasicType type) const;

    Status validate_primitive_type(VariantPrimitiveType type, size_t size_required) const;

    template <typename PrimitiveType>
    StatusOr<PrimitiveType> get_primitive(VariantPrimitiveType type) const;

    StatusOr<std::string_view> get_primitive_string_or_binary(VariantPrimitiveType type) const;

    template <typename DecimalType>
    StatusOr<DecimalValue<DecimalType>> get_primitive_decimal(VariantPrimitiveType type) const;

    VariantMetadata _metadata;
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

// Representing the details of a Variant {@link BasicType::OBJECT}.
struct ObjectInfo {
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

// Representing the details of a Variant {@link BasicType::ARRAY}.
struct ArrayInfo {
    // Number of elements in the array
    uint32_t num_elements;
    // The size of the field offset list
    uint8_t offset_size;
    // The byte offset of the field offset list
    uint32_t offset_start_offset;
    // The byte offset of the field data
    uint32_t data_start_offset;
};

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
StatusOr<ObjectInfo> get_object_info(std::string_view value);

/**
 *                5         3  2  1     0
 *               +-----------+---+-------+
 * value_header  |           |   |       |
 *               +-----------+---+-------+
 *                             ^     ^
 *                             |     +-- field_offset_size_minus_one
 *                             +-- is_large
 */
StatusOr<ArrayInfo> get_array_info(std::string_view value);
} // namespace starrocks
