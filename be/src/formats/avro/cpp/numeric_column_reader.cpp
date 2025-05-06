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

#include "formats/avro/cpp/numeric_column_reader.h"

#include "formats/avro/cpp/utils.h"
#include "runtime/decimalv3.h"
#include "util/numeric_types.h"
#include "util/string_parser.hpp"

namespace starrocks::avrocpp {

// ------ integer, float ------

template <typename T>
Status NumericColumnReader<T>::read_datum(const avro::GenericDatum& datum, Column* column) {
    auto numeric_column = down_cast<FixedLengthColumn<T>*>(column);
    switch (datum.type()) {
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
        return read_numeric_value(datum, numeric_column);

    case avro::AVRO_STRING:
        return read_string_value(datum, numeric_column);

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to number. column: {}", avro::toString(datum.type()), _col_name));
    }
}

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
Status NumericColumnReader<T>::read_numeric_value(const avro::GenericDatum& datum, FixedLengthColumn<T>* column) {
    switch (datum.type()) {
    case avro::AVRO_INT: {
        const auto& from = datum.value<int32_t>();
        return check_append_numeric_column<int32_t, T>(from, _col_name, column);
    }

    case avro::AVRO_LONG: {
        const auto& from = datum.value<int64_t>();
        return check_append_numeric_column<int64_t, T>(from, _col_name, column);
    }

    case avro::AVRO_FLOAT: {
        const auto& from = datum.value<float>();
        return check_append_numeric_column<float, T>(from, _col_name, column);
    }

    case avro::AVRO_DOUBLE: {
        const auto& from = datum.value<double>();
        return check_append_numeric_column<double, T>(from, _col_name, column);
    }

    case avro::AVRO_BOOL: {
        const auto& in = datum.value<bool>();
        int from = in ? 1 : 0;
        return check_append_numeric_column<int, T>(from, _col_name, column);
    }

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to number. column: {}", avro::toString(datum.type()), _col_name));
    }
}

template <typename T>
static inline Status get_numeric_value_from_string(const std::string& col_name, const std::string& from, T* to) {
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
Status NumericColumnReader<T>::read_string_value(const avro::GenericDatum& datum, FixedLengthColumn<T>* column) {
    const auto& from = datum.value<std::string>();
    T to{};
    RETURN_IF_ERROR(get_numeric_value_from_string<T>(_col_name, from, &to));
    column->append_numbers(&to, sizeof(to));
    return Status::OK();
}

template class NumericColumnReader<int8_t>;
template class NumericColumnReader<int16_t>;
template class NumericColumnReader<int32_t>;
template class NumericColumnReader<int64_t>;
template class NumericColumnReader<int128_t>;
template class NumericColumnReader<float>;
template class NumericColumnReader<double>;

// ------ boolean ------

Status BooleanColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    auto numeric_column = down_cast<FixedLengthColumn<uint8_t>*>(column);
    switch (datum.type()) {
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
        return read_numeric_value(datum, numeric_column);

    case avro::AVRO_STRING:
        return read_string_value(datum, numeric_column);

    default:
        return Status::NotSupported(fmt::format("Unsupported avro type {} to boolean. column: {}",
                                                avro::toString(datum.type()), _col_name));
    }
}

Status BooleanColumnReader::read_numeric_value(const avro::GenericDatum& datum, FixedLengthColumn<uint8_t>* column) {
    switch (datum.type()) {
    case avro::AVRO_INT: {
        bool from = datum.value<int32_t>() != 0 ? true : false;
        column->append(from);
        return Status::OK();
    }

    case avro::AVRO_LONG: {
        bool from = datum.value<int64_t>() != 0 ? true : false;
        column->append(from);
        return Status::OK();
    }

    case avro::AVRO_FLOAT: {
        const auto& from = datum.value<float>();
        column->append(implicit_cast<bool>(from));
        return Status::OK();
    }

    case avro::AVRO_DOUBLE: {
        const auto& from = datum.value<double>();
        column->append(implicit_cast<bool>(from));
        return Status::OK();
    }

    case avro::AVRO_BOOL: {
        column->append(datum.value<bool>());
        return Status::OK();
    }

    default:
        return Status::NotSupported(fmt::format("Unsupported avro type {} to boolean. column: {}",
                                                avro::toString(datum.type()), _col_name));
    }
}

Status BooleanColumnReader::read_string_value(const avro::GenericDatum& datum, FixedLengthColumn<uint8_t>* column) {
    const auto& from = datum.value<std::string>();

    StringParser::ParseResult r;
    bool v = StringParser::string_to_bool(from.data(), from.size(), &r);
    if (r == StringParser::PARSE_SUCCESS) {
        column->append(v);
        return Status::OK();
    }
    v = implicit_cast<bool>(StringParser::string_to_float<double>(from.data(), from.size(), &r));
    if (r == StringParser::PARSE_SUCCESS) {
        column->append(v);
        return Status::OK();
    }

    return Status::DataQualityError(
            fmt::format("Unable to cast string value to boolean. value: {}, column: {}", from, _col_name));
}

// ------ decimal ------

template <typename T>
Status DecimalColumnReader<T>::read_datum(const avro::GenericDatum& datum, Column* column) {
    auto decimal_column = down_cast<DecimalV3Column<T>*>(column);
    switch (datum.type()) {
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
        return read_numeric_value(datum, decimal_column);

    case avro::AVRO_STRING:
        return read_string_value(datum, decimal_column);

    case avro::AVRO_BYTES:
    case avro::AVRO_FIXED:
        return read_bytes_value(datum, decimal_column);

    default:
        return Status::NotSupported(fmt::format("Unsupported avro type {} to decimal. column: {}",
                                                avro::toString(datum.type()), _col_name));
    }
}

template <typename FromType, typename ToType>
static inline Status check_append_numeric_to_decimal_column(const FromType& from, const std::string& col_name,
                                                            int precision, int scale, DecimalV3Column<ToType>* column) {
    ToType to{};
    bool fail = false;
    if constexpr (std::is_floating_point<FromType>::value) {
        fail = DecimalV3Cast::from_float<FromType, ToType>(from, get_scale_factor<ToType>(scale), &to);
    } else {
        fail = DecimalV3Cast::from_integer<FromType, ToType, true>(from, get_scale_factor<ToType>(scale), &to);
    }
    if (fail) {
        return Status::DataQualityError(fmt::format("Value is overflow. value: {}, column: {}, type: decimal({},{})",
                                                    from, col_name, precision, scale));
    }

    column->append(to);
    return Status::OK();
}

template <typename T>
Status DecimalColumnReader<T>::read_numeric_value(const avro::GenericDatum& datum, DecimalV3Column<T>* column) {
    switch (datum.type()) {
    case avro::AVRO_INT: {
        const auto& from = datum.value<int32_t>();
        return check_append_numeric_to_decimal_column<int32_t, T>(from, _col_name, _precision, _scale, column);
    }

    case avro::AVRO_LONG: {
        const auto& from = datum.value<int64_t>();
        return check_append_numeric_to_decimal_column<int64_t, T>(from, _col_name, _precision, _scale, column);
    }

    case avro::AVRO_FLOAT: {
        const auto& from = datum.value<float>();
        return check_append_numeric_to_decimal_column<float, T>(from, _col_name, _precision, _scale, column);
    }

    case avro::AVRO_DOUBLE: {
        const auto& from = datum.value<double>();
        return check_append_numeric_to_decimal_column<double, T>(from, _col_name, _precision, _scale, column);
    }

    case avro::AVRO_BOOL: {
        const auto& in = datum.value<bool>();
        int from = in ? 1 : 0;
        return check_append_numeric_to_decimal_column<int, T>(from, _col_name, _precision, _scale, column);
    }

    default:
        return Status::NotSupported(fmt::format("Unsupported avro type {} to decimal. column: {}",
                                                avro::toString(datum.type()), _col_name));
    }
}

template <typename T>
Status DecimalColumnReader<T>::read_string_value(const avro::GenericDatum& datum, DecimalV3Column<T>* column) {
    const auto& from = datum.value<std::string>();

    T to{};
    bool fail = DecimalV3Cast::from_string<T>(&to, _precision, _scale, from.data(), from.size());
    if (fail) {
        return Status::DataQualityError(
                fmt::format("Avro string to decimal failed. value: {}, column: {}, type: decimal({},{})", from,
                            _col_name, _precision, _scale));
    }

    column->append(to);
    return Status::OK();
}

template <typename T>
static inline Status check_append_bytes_to_decimal_column(const std::vector<uint8_t>& from, const std::string& col_name,
                                                          int32_t avro_precision, int32_t avro_scale, int new_precision,
                                                          int new_scale, DecimalV3Column<T>* column) {
    // get avro decimal value
    auto integer_v = AvroUtils::bytes_to_decimal_integer(from);

    // check whether precision and scale are same
    if (new_precision == avro_precision && new_scale == avro_scale) {
        column->append(static_cast<T>(integer_v));
        return Status::OK();
    }

    // to new decimal value if different
    int128_t x = std::abs(integer_v) / get_scale_factor<T>(avro_scale);
    if (x / get_scale_factor<T>(new_precision - new_scale) != 0) {
        return Status::DataQualityError(
                fmt::format("Value is overflow. value: {}, column: {}, "
                            "original type: decimal({},{}), new type: decimal({},{})",
                            integer_v, col_name, avro_precision, avro_scale, new_precision, new_scale));
    }

    int128_t y = std::abs(integer_v) % get_scale_factor<T>(avro_scale);
    if (new_scale > avro_scale) {
        y *= get_scale_factor<T>(new_scale - avro_scale);
    } else if (new_scale < avro_scale) {
        if (y % get_scale_factor<T>(avro_scale - new_scale) != 0) {
            return Status::DataQualityError(
                    fmt::format("Value is overflow. value: {}, column: {}, "
                                "original type: decimal({},{}), new type: decimal({},{})",
                                integer_v, col_name, avro_precision, avro_scale, new_precision, new_scale));
        }

        y /= get_scale_factor<T>(avro_scale - new_scale);
    }

    int128_t new_value = x * get_scale_factor<T>(new_scale) + y;
    if (integer_v < 0) {
        new_value *= -1;
    }

    column->append(static_cast<T>(new_value));
    return Status::OK();
}

template <typename T>
Status DecimalColumnReader<T>::read_bytes_value(const avro::GenericDatum& datum, DecimalV3Column<T>* column) {
    auto logical_type = datum.logicalType();
    if (logical_type.type() != avro::LogicalType::DECIMAL) {
        return Status::NotSupported(fmt::format("Logical type of {} is {}, column: {}", avro::toString(datum.type()),
                                                AvroUtils::logical_type_to_string(datum.logicalType()), _col_name));
    }

    auto avro_precision = logical_type.precision();
    auto avro_scale = logical_type.scale();

    switch (datum.type()) {
    case avro::AVRO_BYTES: {
        const auto& from = datum.value<std::vector<uint8_t>>();
        return check_append_bytes_to_decimal_column<T>(from, _col_name, avro_precision, avro_scale, _precision, _scale,
                                                       column);
    }

    case avro::AVRO_FIXED: {
        const auto& fixed = datum.value<avro::GenericFixed>();
        const auto& from = fixed.value();
        return check_append_bytes_to_decimal_column<T>(from, _col_name, avro_precision, avro_scale, _precision, _scale,
                                                       column);
    }

    default:
        return Status::NotSupported(fmt::format("Unsupported avro type {} to decimal. column: {}",
                                                avro::toString(datum.type()), _col_name));
    }
}

template class DecimalColumnReader<int32_t>;
template class DecimalColumnReader<int64_t>;
template class DecimalColumnReader<int128_t>;

} // namespace starrocks::avrocpp
