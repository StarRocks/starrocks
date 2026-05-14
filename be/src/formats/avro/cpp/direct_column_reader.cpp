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

#include "formats/avro/cpp/direct_column_reader.h"

#include <fmt/format.h>

#include <avrocpp/Decoder.hh>
#include <avrocpp/Exception.hh>
#include <avrocpp/LogicalType.hh>
#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "base/types/numeric_types.h"
#include "column/adaptive_nullable_column.h"
#include "column/binary_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "formats/avro/cpp/utils.h"
#include "types/decimalv3.h"
#include "types/logical_type.h"

namespace starrocks::avrocpp {

namespace {

template <typename FromType, typename ToType>
static inline bool checked_cast(const FromType& from, ToType* to) {
    if constexpr (std::is_floating_point_v<FromType> && std::is_integral_v<ToType>) {
        if (std::isnan(from) || std::isinf(from)) return true;
    }

    DIAGNOSTIC_PUSH
#if defined(__clang__)
    DIAGNOSTIC_IGNORE("-Wimplicit-int-float-conversion")
#endif
    if (check_signed_number_overflow<FromType, ToType>(from)) return true;
    DIAGNOSTIC_POP

    *to = static_cast<ToType>(from);
    return false;
}

template <typename FromType, typename ToType>
static inline Status check_append_numeric_column(const FromType& from, std::string_view col_name,
                                                 FixedLengthColumn<ToType>* column) {
    ToType to{};
    if (checked_cast(from, &to)) {
        return Status::DataQualityError(fmt::format("Value is overflow. value: {}, column: {}", from, col_name));
    }

    column->append_numbers(&to, sizeof(to));
    return Status::OK();
}

static inline Status check_append_binary_column(std::string_view from, std::string_view col_name,
                                                const TypeDescriptor& type_desc, BinaryColumn* column) {
    if (UNLIKELY(from.size() > type_desc.len)) {
        return Status::DataQualityError(
                fmt::format("Value length is beyond the capacity. column: {}, capacity: {}", col_name, type_desc.len));
    }

    column->append(Slice(from));
    return Status::OK();
}

template <typename T>
static inline Status check_append_bytes_to_decimal_column(const std::vector<uint8_t>& from, std::string_view col_name,
                                                          int32_t avro_precision, int32_t avro_scale, int new_precision,
                                                          int new_scale, DecimalV3Column<T>* column) {
    auto integer_v = AvroUtils::bytes_to_decimal_integer(from);

    if (new_precision == avro_precision && new_scale == avro_scale) {
        column->append(static_cast<T>(integer_v));
        return Status::OK();
    }

    int128_t x = starrocks::abs(integer_v) / get_scale_factor<T>(avro_scale);
    if (x / get_scale_factor<T>(new_precision - new_scale) != 0) {
        return Status::DataQualityError(
                fmt::format("Value is overflow. value: {}, column: {}, "
                            "original type: decimal({},{}), new type: decimal({},{})",
                            integer_v, col_name, avro_precision, avro_scale, new_precision, new_scale));
    }

    int128_t y = starrocks::abs(integer_v) % get_scale_factor<T>(avro_scale);
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

static bool unwrap_nullable_union(const avro::NodePtr& node, avro::NodePtr* value_node, size_t* null_branch,
                                  size_t* value_branch) {
    if (node->type() != avro::AVRO_UNION) {
        *value_node = node;
        *null_branch = static_cast<size_t>(-1);
        *value_branch = 0;
        return true;
    }

    *value_node = nullptr;
    *null_branch = static_cast<size_t>(-1);
    *value_branch = 0;

    bool found_value = false;
    bool found_null = false;

    for (size_t i = 0; i < node->leaves(); ++i) {
        const auto& branch = node->leafAt(i);
        if (branch->type() == avro::AVRO_NULL) {
            if (found_null) {
                return false;
            }
            found_null = true;
            *null_branch = i;
        } else {
            if (found_value) {
                return false;
            }
            found_value = true;
            *value_branch = i;
            *value_node = branch;
        }
    }
    return found_value;
}

static bool is_supported_direct_type(const TypeDescriptor& type_desc, const avro::NodePtr& node) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        return node->type() == avro::AVRO_BOOL || node->type() == avro::AVRO_INT || node->type() == avro::AVRO_LONG ||
               node->type() == avro::AVRO_FLOAT || node->type() == avro::AVRO_DOUBLE;
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
        return (node->type() == avro::AVRO_BYTES || node->type() == avro::AVRO_FIXED) &&
               node->logicalType().type() == avro::LogicalType::DECIMAL;
    case TYPE_DATE:
        return node->type() == avro::AVRO_INT && node->logicalType().type() == avro::LogicalType::DATE;
    case TYPE_DATETIME:
        return node->type() == avro::AVRO_LONG && (node->logicalType().type() == avro::LogicalType::TIMESTAMP_MILLIS ||
                                                   node->logicalType().type() == avro::LogicalType::TIMESTAMP_MICROS);
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY:
        return node->type() == avro::AVRO_STRING || node->type() == avro::AVRO_BYTES ||
               node->type() == avro::AVRO_FIXED || node->type() == avro::AVRO_ENUM;
    default:
        return false;
    }
}

class ScalarDirectColumnReader final : public DirectColumnReader {
public:
    ScalarDirectColumnReader(std::string_view col_name, const TypeDescriptor& type_desc, avro::NodePtr node,
                             avro::NodePtr value_node, size_t null_branch, size_t value_branch,
                             const cctz::time_zone& timezone)
            : _col_name(col_name),
              _type_desc(type_desc),
              _node(std::move(node)),
              _value_node(std::move(value_node)),
              _null_branch(null_branch),
              _value_branch(value_branch),
              _timezone(timezone) {}

    Status read_field(avro::Decoder& decoder, AdaptiveNullableColumn* column) override {
        if (_node->type() == avro::AVRO_UNION) {
            size_t branch = decoder.decodeUnionIndex();
            if (branch == _null_branch) {
                column->append_nulls(1);
                return Status::OK();
            }
            if (branch != _value_branch) {
                return Status::NotSupported(
                        fmt::format("Unsupported avro union branch {}. column: {}", branch, _col_name));
            }
        }

        auto* data_column = column->begin_append_not_default_value();
        RETURN_IF_ERROR(read_non_null(decoder, data_column));
        column->finish_append_one_not_default_value();
        return Status::OK();
    }

private:
    Status read_non_null(avro::Decoder& decoder, Column* column) {
        switch (_type_desc.type) {
        case TYPE_BOOLEAN:
            return read_bool(decoder, down_cast<FixedLengthColumn<uint8_t>*>(column));
        case TYPE_TINYINT:
            return read_number<int8_t>(decoder, down_cast<FixedLengthColumn<int8_t>*>(column));
        case TYPE_SMALLINT:
            return read_number<int16_t>(decoder, down_cast<FixedLengthColumn<int16_t>*>(column));
        case TYPE_INT:
            return read_number<int32_t>(decoder, down_cast<FixedLengthColumn<int32_t>*>(column));
        case TYPE_BIGINT:
            return read_number<int64_t>(decoder, down_cast<FixedLengthColumn<int64_t>*>(column));
        case TYPE_LARGEINT:
            return read_number<int128_t>(decoder, down_cast<FixedLengthColumn<int128_t>*>(column));
        case TYPE_FLOAT:
            return read_number<float>(decoder, down_cast<FixedLengthColumn<float>*>(column));
        case TYPE_DOUBLE:
            return read_number<double>(decoder, down_cast<FixedLengthColumn<double>*>(column));
        case TYPE_DECIMAL32:
            return read_decimal<int32_t>(decoder, down_cast<DecimalV3Column<int32_t>*>(column));
        case TYPE_DECIMAL64:
            return read_decimal<int64_t>(decoder, down_cast<DecimalV3Column<int64_t>*>(column));
        case TYPE_DECIMAL128:
            return read_decimal<int128_t>(decoder, down_cast<DecimalV3Column<int128_t>*>(column));
        case TYPE_DATE:
            return read_date(decoder, down_cast<FixedLengthColumn<DateValue>*>(column));
        case TYPE_DATETIME:
            return read_datetime(decoder, down_cast<FixedLengthColumn<TimestampValue>*>(column));
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_VARBINARY:
            return read_binary(decoder, down_cast<BinaryColumn*>(column));
        default:
            return Status::NotSupported(fmt::format("Unsupported direct reader type. column: {}", _col_name));
        }
    }

    Status read_bool(avro::Decoder& decoder, FixedLengthColumn<uint8_t>* column) {
        switch (_value_node->type()) {
        case avro::AVRO_BOOL:
            column->append(decoder.decodeBool());
            return Status::OK();
        case avro::AVRO_INT:
            column->append(decoder.decodeInt() != 0);
            return Status::OK();
        case avro::AVRO_LONG:
            column->append(decoder.decodeLong() != 0);
            return Status::OK();
        case avro::AVRO_FLOAT:
            column->append(implicit_cast<bool>(decoder.decodeFloat()));
            return Status::OK();
        case avro::AVRO_DOUBLE:
            column->append(implicit_cast<bool>(decoder.decodeDouble()));
            return Status::OK();
        default:
            return Status::NotSupported(fmt::format("Unsupported avro type {} to boolean. column: {}",
                                                    avro::toString(_value_node->type()), _col_name));
        }
    }

    template <typename T>
    Status read_number(avro::Decoder& decoder, FixedLengthColumn<T>* column) {
        switch (_value_node->type()) {
        case avro::AVRO_BOOL:
            return check_append_numeric_column<int, T>(decoder.decodeBool() ? 1 : 0, _col_name, column);
        case avro::AVRO_INT:
            return check_append_numeric_column<int32_t, T>(decoder.decodeInt(), _col_name, column);
        case avro::AVRO_LONG:
            return check_append_numeric_column<int64_t, T>(decoder.decodeLong(), _col_name, column);
        case avro::AVRO_FLOAT:
            return check_append_numeric_column<float, T>(decoder.decodeFloat(), _col_name, column);
        case avro::AVRO_DOUBLE:
            return check_append_numeric_column<double, T>(decoder.decodeDouble(), _col_name, column);
        default:
            return Status::NotSupported(fmt::format("Unsupported avro type {} to number. column: {}",
                                                    avro::toString(_value_node->type()), _col_name));
        }
    }

    template <typename T>
    Status read_decimal(avro::Decoder& decoder, DecimalV3Column<T>* column) {
        auto logical_type = _value_node->logicalType();
        if (logical_type.type() != avro::LogicalType::DECIMAL) {
            return Status::NotSupported(fmt::format("Logical type of {} is {}, column: {}",
                                                    avro::toString(_value_node->type()),
                                                    AvroUtils::logical_type_to_string(logical_type), _col_name));
        }

        if (_value_node->type() == avro::AVRO_BYTES) {
            decoder.decodeBytes(_bytes_buf);
        } else {
            decoder.decodeFixed(_value_node->fixedSize(), _bytes_buf);
        }

        return check_append_bytes_to_decimal_column<T>(_bytes_buf, _col_name, logical_type.precision(),
                                                       logical_type.scale(), _type_desc.precision, _type_desc.scale,
                                                       column);
    }

    Status read_date(avro::Decoder& decoder, FixedLengthColumn<DateValue>* column) {
        column->append(AvroUtils::int_to_date_value(decoder.decodeInt()));
        return Status::OK();
    }

    Status read_datetime(avro::Decoder& decoder, FixedLengthColumn<TimestampValue>* column) {
        int64_t from = decoder.decodeLong();
        TimestampValue to{};
        switch (_value_node->logicalType().type()) {
        case avro::LogicalType::TIMESTAMP_MILLIS:
            to.from_unixtime(from / 1000, (from % 1000) * 1000, _timezone);
            column->append(to);
            return Status::OK();
        case avro::LogicalType::TIMESTAMP_MICROS:
            to.from_unixtime(from / 1000000, from % 1000000, _timezone);
            column->append(to);
            return Status::OK();
        default:
            return Status::NotSupported(
                    fmt::format("Logical type of {} is {}. column: {}", avro::toString(_value_node->type()),
                                AvroUtils::logical_type_to_string(_value_node->logicalType()), _col_name));
        }
    }

    Status read_binary(avro::Decoder& decoder, BinaryColumn* column) {
        switch (_value_node->type()) {
        case avro::AVRO_STRING:
        case avro::AVRO_BYTES:
            // STRING and BYTES share the same wire format (varint length + raw bytes).
            // Decode into _bytes_buf to avoid a std::string intermediate.
            decoder.decodeBytes(_bytes_buf);
            return check_append_binary_column(
                    std::string_view(reinterpret_cast<const char*>(_bytes_buf.data()), _bytes_buf.size()), _col_name,
                    _type_desc, column);
        case avro::AVRO_FIXED:
            decoder.decodeFixed(_value_node->fixedSize(), _bytes_buf);
            return check_append_binary_column(
                    std::string_view(reinterpret_cast<const char*>(_bytes_buf.data()), _bytes_buf.size()), _col_name,
                    _type_desc, column);
        case avro::AVRO_ENUM: {
            size_t index = decoder.decodeEnum();
            if (index >= _value_node->names()) {
                return Status::DataQualityError(fmt::format("Invalid enum index {}. column: {}", index, _col_name));
            }
            return check_append_binary_column(_value_node->nameAt(index), _col_name, _type_desc, column);
        }
        default:
            return Status::NotSupported(fmt::format("Unsupported avro type {} to string. column: {}",
                                                    avro::toString(_value_node->type()), _col_name));
        }
    }

    std::string _col_name;
    TypeDescriptor _type_desc;
    avro::NodePtr _node;
    avro::NodePtr _value_node;
    size_t _null_branch;
    size_t _value_branch;
    cctz::time_zone _timezone;
    std::vector<uint8_t> _bytes_buf;
};

} // namespace

DirectColumnReaderUniquePtr DirectColumnReader::make(std::string_view col_name, const TypeDescriptor& type_desc,
                                                     const avro::NodePtr& node, const cctz::time_zone& timezone) {
    avro::NodePtr value_node;
    size_t null_branch = static_cast<size_t>(-1);
    size_t value_branch = 0;
    if (!unwrap_nullable_union(node, &value_node, &null_branch, &value_branch)) {
        return nullptr;
    }
    if (!is_supported_direct_type(type_desc, value_node)) {
        return nullptr;
    }
    return std::make_unique<ScalarDirectColumnReader>(col_name, type_desc, node, value_node, null_branch, value_branch,
                                                      timezone);
}

void DirectColumnReader::skip_node(avro::Decoder& decoder, const avro::NodePtr& node) {
    switch (node->type()) {
    case avro::AVRO_NULL:
        decoder.decodeNull();
        return;
    case avro::AVRO_BOOL:
        (void)decoder.decodeBool();
        return;
    case avro::AVRO_INT:
        (void)decoder.decodeInt();
        return;
    case avro::AVRO_LONG:
        (void)decoder.decodeLong();
        return;
    case avro::AVRO_FLOAT:
        (void)decoder.decodeFloat();
        return;
    case avro::AVRO_DOUBLE:
        (void)decoder.decodeDouble();
        return;
    case avro::AVRO_STRING:
        decoder.skipString();
        return;
    case avro::AVRO_BYTES:
        decoder.skipBytes();
        return;
    case avro::AVRO_FIXED:
        decoder.skipFixed(node->fixedSize());
        return;
    case avro::AVRO_ENUM:
        (void)decoder.decodeEnum();
        return;
    case avro::AVRO_UNION: {
        size_t branch = decoder.decodeUnionIndex();
        skip_node(decoder, node->leafAt(branch));
        return;
    }
    case avro::AVRO_RECORD:
        for (size_t i = 0; i < node->leaves(); ++i) {
            skip_node(decoder, node->leafAt(i));
        }
        return;
    case avro::AVRO_ARRAY:
        for (size_t n = decoder.skipArray(); n != 0; n = decoder.arrayNext()) {
            for (size_t i = 0; i < n; ++i) {
                skip_node(decoder, node->leafAt(0));
            }
        }
        return;
    case avro::AVRO_MAP:
        for (size_t n = decoder.skipMap(); n != 0; n = decoder.mapNext()) {
            for (size_t i = 0; i < n; ++i) {
                decoder.skipString();
                skip_node(decoder, node->leafAt(1));
            }
        }
        return;
    default:
        throw avro::Exception(fmt::format("Unsupported avro type to skip: {}", avro::toString(node->type())));
    }
}

} // namespace starrocks::avrocpp
