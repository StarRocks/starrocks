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

#include "formats/avro/cpp/numeric_column.h"

#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "formats/avro/cpp/utils.h"
#include "runtime/decimalv3.h"
#include "util/numeric_types.h"
#include "util/string_parser.hpp"

namespace starrocks {

// ------ bool, integer, float column ------

template <typename FromType, typename ToType>
static inline bool checked_cast(const FromType& from, ToType* to) {
    *to = static_cast<ToType>(from);

    // NOTE: use lowest() because float and double needed.
    DIAGNOSTIC_PUSH
#if defined(__clang__)
    DIAGNOSTIC_IGNORE("-Wimplicit-int-float-conversion")
#endif
    return check_signed_number_overflow<FromType, ToType>(from);
    DIAGNOSTIC_POP
}

template <typename FromType, typename ToType>
static inline Status check_append_numeric_column(const FromType& from, const std::string& col_name,
                                                 FixedLengthColumn<ToType>* column) {
    ToType to{};
    if (checked_cast(from, &to)) {
        return Status::DataQualityError(fmt::format("Value is overflow. value: {}, column: {}", from, col_name));
    }

    column->append_numbers(&to, sizeof(to));
    return Status::OK();
}

template <typename T>
static Status add_numeric_column_with_numeric_value(const avro::GenericDatum& datum, const std::string& col_name,
                                                    FixedLengthColumn<T>* column) {
    switch (datum.type()) {
    case avro::AVRO_INT: {
        const auto& from = datum.value<int32_t>();
        return check_append_numeric_column<int32_t, T>(from, col_name, column);
    }

    case avro::AVRO_LONG: {
        const auto& from = datum.value<int64_t>();
        return check_append_numeric_column<int64_t, T>(from, col_name, column);
    }

    case avro::AVRO_FLOAT: {
        const auto& from = datum.value<float>();
        return check_append_numeric_column<float, T>(from, col_name, column);
    }

    case avro::AVRO_DOUBLE: {
        const auto& from = datum.value<double>();
        return check_append_numeric_column<double, T>(from, col_name, column);
    }

    case avro::AVRO_BOOL: {
        const auto& in = datum.value<bool>();
        int from = in ? 1 : 0;
        return check_append_numeric_column<int, T>(from, col_name, column);
    }

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to number. column: {}", avro::toString(datum.type()), col_name));
    }
}

template <typename T>
static inline Status get_numeric_value_from_string(const std::string& col_name, const std::string& from, T *to) {
    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
    if constexpr (std::is_floating_point<T>::value) {
        *to = StringParser::string_to_float<T>(from.data(), from.size(), &parse_result);
    } else {
        *to = StringParser::string_to_int<T>(from.data(), from.size(), &parse_result);
    }

    if (parse_result == StringParser::PARSE_SUCCESS) {
        return Status::OK();
    }

    // Attemp to parse the string as float.
    auto d = StringParser::string_to_float<double>(from.data(), from.size(), &parse_result);
    if (parse_result == StringParser::PARSE_SUCCESS) {
        if (checked_cast(d, to)) {
            return Status::DataQualityError(fmt::format("Value is overflow. value: {}, column: {}", d, col_name));
        }
        return Status::OK();
    }

    return Status::DataQualityError(
            fmt::format("Unable to cast string value to number. value: {}, column: {}", from, col_name));
}

template <typename T>
static Status add_numeric_column_with_string_value(const avro::GenericDatum& datum, const std::string& col_name,
                                                   FixedLengthColumn<T>* column) {
    const auto& from = datum.value<std::string>();
    T to{};
    RETURN_IF_ERROR(get_numeric_value_from_string<T>(col_name, from, &to));
    column->append_numbers(&to, sizeof(to));
    return Status::OK();
}

template <typename T>
Status add_numeric_column(const avro::GenericDatum& datum, const std::string& col_name, Column* column) {
    auto numeric_column = down_cast<FixedLengthColumn<T>*>(column);
    switch (datum.type()) {
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
        return add_numeric_column_with_numeric_value<T>(datum, col_name, numeric_column);

    case avro::AVRO_STRING:
        return add_numeric_column_with_string_value<T>(datum, col_name, numeric_column);

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to number. column: {}", avro::toString(datum.type()), col_name));
    }
}

template Status add_numeric_column<int8_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                           Column* column);
template Status add_numeric_column<uint8_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                            Column* column);
template Status add_numeric_column<int16_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                            Column* column);
template Status add_numeric_column<int32_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                            Column* column);
template Status add_numeric_column<int64_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                            Column* column);
template Status add_numeric_column<int128_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                             Column* column);
template Status add_numeric_column<float>(const avro::GenericDatum& datum, const std::string& col_name,
                                          Column* column);
template Status add_numeric_column<double>(const avro::GenericDatum& datum, const std::string& col_name,
                                           Column* column);

// ------ decimal column ------

template <typename FromType, typename ToType>
static inline Status check_append_numeric_to_decimal_column(const FromType& from, const std::string& col_name,
                                                            const TypeDescriptor& type_desc,
                                                            DecimalV3Column<ToType>* column) {
    auto precision = type_desc.precision;
    auto scale = type_desc.scale;

    ToType to{};
    bool fail = false;
    if constexpr (std::is_floating_point<FromType>::value) {
        fail = DecimalV3Cast::from_float<FromType, ToType>(from, scale, &to);
    } else {
        fail = DecimalV3Cast::from_integer<FromType, ToType, true>(from, scale, &to);
    }
    if (fail) {
        return Status::DataQualityError(fmt::format("Value is overflow. value: {}, column: {}, type: decimal({},{})",
                                                    from, col_name, precision, scale));
    }

    column->append(to);
    return Status::OK();
}

template <typename T>
static Status add_decimal_column_with_numeric_value(const avro::GenericDatum& datum, const std::string& col_name,
                                                    const TypeDescriptor& type_desc, DecimalV3Column<T>* column) {
    switch (datum.type()) {
    case avro::AVRO_INT: {
        const auto& from = datum.value<int32_t>();
        return check_append_numeric_to_decimal_column<int32_t, T>(from, col_name, type_desc, column);
    }

    case avro::AVRO_LONG: {
        const auto& from = datum.value<int64_t>();
        return check_append_numeric_to_decimal_column<int64_t, T>(from, col_name, type_desc, column);
    }

    case avro::AVRO_FLOAT: {
        const auto& from = datum.value<float>();
        return check_append_numeric_to_decimal_column<float, T>(from, col_name, type_desc, column);
    }

    case avro::AVRO_DOUBLE: {
        const auto& from = datum.value<double>();
        return check_append_numeric_to_decimal_column<double, T>(from, col_name, type_desc, column);
    }

    case avro::AVRO_BOOL: {
        const auto& in = datum.value<bool>();
        int from = in ? 1 : 0;
        return check_append_numeric_to_decimal_column<int, T>(from, col_name, type_desc, column);
    }

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to decimal. column: {}", avro::toString(datum.type()), col_name));
    }
}

template <typename T>
static Status add_decimal_column_with_string_value(const avro::GenericDatum& datum, const std::string& col_name,
                                                   const TypeDescriptor& type_desc, DecimalV3Column<T>* column) {
    const auto& from = datum.value<std::string>();
    T to{};
    RETURN_IF_ERROR(get_numeric_value_from_string<T>(col_name, from, &to));
    return check_append_numeric_to_decimal_column<T, T>(to, col_name, type_desc, column);
}

template <typename T>
static inline Status check_append_bytes_to_decimal_column(const std::vector<uint8_t>& from, const std::string& col_name,
                                                          int32_t avro_precision, int32_t avro_scale,
                                                          const TypeDescriptor& type_desc, DecimalV3Column<T>* column) {
    // get avro decimal value, maybe negative
    int64_t long_v = 0;
    for (size_t i = 0; i < from.size(); ++i) {
        long_v = (long_v << 8) |from[i];
    }
    if ((from[0] & 0x80) != 0) {
        long_v -= (1LL << (8 * from.size()));
    }

    // check whether precision and scale are same
    int new_precision = type_desc.precision;
    int new_scale = type_desc.scale;
    if (new_precision == avro_precision && new_scale == avro_scale) {
        column->append(static_cast<T>(long_v));
        return Status::OK();
    }

    // to new decimal value if are different
    int64_t x = std::abs(long_v) / avro_scale;
    int64_t tmp_x = x;
    for (int i = 0; i < new_precision - new_scale; ++i) {
        tmp_x /= 10;
    }
    if (tmp_x != 0) {
        return Status::DataQualityError(
                fmt::format("Value is overflow. value: {}, column: {}, type: decimal({},{})", long_v, col_name,
                            new_precision, new_scale));
    }

    int64_t y = std::abs(long_v) % avro_scale;
    if (new_scale > avro_scale) {
        for (int i = 0; i < new_scale - avro_scale; ++i) {
            y *= 10;
        }
    } else if (new_scale < avro_scale) {
        int64_t multiple = 1;
        for (int i = 0; i < avro_scale - new_scale; ++i) {
            multiple *= 10;
        }

        if (y % multiple != 0) {
            return Status::DataQualityError(
                    fmt::format("Value is overflow. value: {}, column: {}, type: decimal({},{})", long_v, col_name,
                                new_precision, new_scale));
        }

        y /= multiple;
    }

    int64_t new_value = x * new_scale + y;
    if (long_v < 0) {
        new_value *= -1;
    }

    column->append(static_cast<T>(new_value));
    return Status::OK();
}

template <typename T>
static Status add_decimal_column_with_bytes_value(const avro::GenericDatum& datum, const std::string& col_name,
                                                  const TypeDescriptor& type_desc, DecimalV3Column<T>* column) {
    auto logical_type = datum.logicalType();
    if (logical_type.type() != avro::LogicalType::DECIMAL) {
        return Status::NotSupported(fmt::format("Logical type of {} is {}, column: {}", avro::toString(datum.type()),
                                                AvroUtils::logical_type_to_string(datum.logicalType()), col_name));
    }

    auto precision = logical_type.precision();
    auto scale = logical_type.scale();

    switch (datum.type()) {
    case avro::AVRO_BYTES: {
        const auto& from = datum.value<std::vector<uint8_t>>();
        return check_append_bytes_to_decimal_column<T>(from, col_name, precision, scale, type_desc, column);
    }

    case avro::AVRO_FIXED: {
        const auto& fixed = datum.value<avro::GenericFixed>();
        const auto& from = fixed.value();
        return check_append_bytes_to_decimal_column<T>(from, col_name, precision, scale, type_desc, column);
    }

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to decimal. column: {}", avro::toString(datum.type()), col_name));
    }
}

template <typename T>
Status add_decimal_column(const avro::GenericDatum& datum, const std::string& col_name, const TypeDescriptor& type_desc,
                          Column* column) {
    auto decimal_column = down_cast<DecimalV3Column<T>*>(column);
    switch (datum.type()) {
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
        return add_decimal_column_with_numeric_value<T>(datum, col_name, type_desc, decimal_column);

    case avro::AVRO_STRING:
        return add_decimal_column_with_string_value<T>(datum, col_name, type_desc, decimal_column);

    case avro::AVRO_BYTES:
    case avro::AVRO_FIXED:
        return add_decimal_column_with_bytes_value<T>(datum, col_name, type_desc, decimal_column);

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to decimal. column: {}", avro::toString(datum.type()), col_name));
    }
}

template Status add_decimal_column<int32_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                            const TypeDescriptor& type_desc, Column* column);
template Status add_decimal_column<int64_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                            const TypeDescriptor& type_desc, Column* column);
template Status add_decimal_column<int128_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                             const TypeDescriptor& type_desc, Column* column);

} // namespace starrocks
