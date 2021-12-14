// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "numeric_column.h"

#include "column/fixed_length_column.h"
#include "gutil/strings/substitute.h"
#include "util/string_parser.hpp"

namespace starrocks::vectorized {

template <typename FromType, typename ToType>
static inline bool checked_cast(const FromType& from, ToType* to) {
    *to = static_cast<ToType>(from);
    if constexpr (std::numeric_limits<ToType>::is_integer) {
        return (from > std::numeric_limits<ToType>::max() || from < std::numeric_limits<ToType>::min());
    }

    return false;
}

// The value must be in type simdjson::ondemand::json_type::number;
template <typename T>
static Status add_column_with_numeric_value(FixedLengthColumn<T>* column, const TypeDescriptor& type_desc,
                                            const std::string& name, simdjson::ondemand::value& value) {
    simdjson::ondemand::number_type tp = value.get_number_type();

    switch (tp) {
    case simdjson::ondemand::number_type::signed_integer: {
        int64_t in = value.get_int64();
        T out{};

        if (!checked_cast(in, &out)) {
            column->append_numbers(&out, sizeof(out));
        } else {
            auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, in);
            return Status::InvalidArgument(err_msg);
        }
        return Status::OK();
    }

    case simdjson::ondemand::number_type::unsigned_integer: {
        uint64_t in = value.get_uint64();
        T out{};

        if (!checked_cast(in, &out)) {
            column->append_numbers(&out, sizeof(out));
        } else {
            auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, in);
            return Status::InvalidArgument(err_msg);
        }
        return Status::OK();
    }

    case simdjson::ondemand::number_type::floating_point_number: {
        double in = value.get_double();
        T out{};

        if (!checked_cast(in, &out)) {
            column->append_numbers(&out, sizeof(out));
        } else {
            auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, in);
            return Status::InvalidArgument(err_msg);
        }
        return Status::OK();
    }
    }
    return Status::OK();
}

// The value must be in type simdjson::ondemand::json_type::string;
template <typename T>
static Status add_column_with_string_value(FixedLengthColumn<T>* column, const TypeDescriptor& type_desc,
                                           const std::string& name, simdjson::ondemand::value& value) {
    std::string_view sv = value.get_string();

    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;

    T v{};
    if constexpr (std::is_floating_point<T>::value) {
        v = StringParser::string_to_float<T>(sv.data(), sv.length(), &parse_result);
    } else {
        v = StringParser::string_to_int<T>(sv.data(), sv.length(), &parse_result);
    }

    if (parse_result == StringParser::PARSE_SUCCESS) {
        column->append_numbers(&v, sizeof(v));
        return Status::OK();
    } else {
        // Attemp to parse the string as float.
        auto d = StringParser::string_to_float<double>(sv.data(), sv.length(), &parse_result);
        if (parse_result == StringParser::PARSE_SUCCESS) {
            if (!checked_cast(d, &v)) {
                column->append_numbers(&v, sizeof(v));
                return Status::OK();
            } else {
                auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, d);
                return Status::InvalidArgument(err_msg);
            }
        }

        std::string err_msg = strings::Substitute("Unable to cast string value to BIGINT. value=$0, column=$1",
                                                  std::string(sv.data(), sv.size()), name);
        return Status::InvalidArgument(err_msg);
    }
}

template <typename T>
Status add_numeric_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                          simdjson::ondemand::value& value) {
    auto numeric_column = down_cast<FixedLengthColumn<T>*>(column);

    try {
        simdjson::ondemand::json_type tp = value.type();
        switch (tp) {
        case simdjson::ondemand::json_type::null: {
            numeric_column->append_nulls(1);
            return Status::OK();
        }

        case simdjson::ondemand::json_type::number: {
            return add_column_with_numeric_value(numeric_column, type_desc, name, value);
        }

        case simdjson::ondemand::json_type::string: {
            return add_column_with_string_value(numeric_column, type_desc, name, value);
        }

        default: {
            auto err_msg = strings::Substitute("Unsupported value type. Numeric type is required. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        }
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as number, column=$0", name);
        return Status::DataQualityError(err_msg);
    }
}

template Status add_numeric_column<int128_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                             simdjson::ondemand::value& value);
template Status add_numeric_column<int64_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            simdjson::ondemand::value& value);
template Status add_numeric_column<int32_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            simdjson::ondemand::value& value);
template Status add_numeric_column<int16_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            simdjson::ondemand::value& value);
template Status add_numeric_column<int8_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            simdjson::ondemand::value& value);
template Status add_numeric_column<double>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            simdjson::ondemand::value& value);
template Status add_numeric_column<float>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            simdjson::ondemand::value& value);

} // namespace starrocks::vectorized
