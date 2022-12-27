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

#include "numeric_column.h"

#include "column/fixed_length_column.h"
#include "gutil/strings/substitute.h"
#include "util/string_parser.hpp"

namespace starrocks {

template <typename FromType, typename ToType>
static inline bool checked_cast(const FromType& from, ToType* to) {
    *to = static_cast<ToType>(from);

    // NOTE: use lowest() because float and double needed.
    // Needs to covnert long and int128_t to double to compare, so disable compiler's complain
    DIAGNOSTIC_PUSH
#if defined(__clang__)
    DIAGNOSTIC_IGNORE("-Wimplicit-const-int-float-conversion")
#endif
    return (from < std::numeric_limits<ToType>::lowest() || from > std::numeric_limits<ToType>::max());
    DIAGNOSTIC_POP
}

// The value must be in type simdjson::ondemand::json_type::number;
template <typename T>
static Status add_column_with_numeric_value(FixedLengthColumn<T>* column, const TypeDescriptor& type_desc,
                                            const std::string& name, simdjson::ondemand::value* value) {
    simdjson::ondemand::number_type tp = value->get_number_type();

    switch (tp) {
    case simdjson::ondemand::number_type::signed_integer: {
        int64_t in = value->get_int64();
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
        T out{};
        // When get_uint64 throws an exception here, we need to recheck it with get_int64.
        // This is because currently there exists a bug in simdjson, for number <= -9223372036854775808,
        // simdjson will recognized it as unsigned_integer, and use get_uint64() to parse it, which will
        // raise an exception here. So here we need to recheck by get_int64().
        // This bug in simdjson is expected to be solved by the following commit, later we can remove
        // the redundant error handling logic here.
        // https://github.com/simdjson/simdjson/commit/b169dc2ea71bc8e9296de9749c42d78f80c8a633
        bool checked_cast_flag = true;
        uint64_t in = 0;
        try {
            in = value->get_uint64();
            checked_cast_flag = checked_cast(in, &out);
        } catch (simdjson::simdjson_error& e) {
            // only needs to handle int64_t::min here, throw exception for any other number
            // get_int64() itself may also throw exception
            if (value->get_int64() == std::numeric_limits<int64_t>::min()) {
                checked_cast_flag = checked_cast(std::numeric_limits<int64_t>::min(), &out);
            } else {
                throw simdjson::simdjson_error(simdjson::error_code::NUMBER_OUT_OF_RANGE);
            }
        }

        if (!checked_cast_flag) {
            column->append_numbers(&out, sizeof(out));
        } else {
            auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, in);
            return Status::InvalidArgument(err_msg);
        }
        return Status::OK();
    }

    case simdjson::ondemand::number_type::floating_point_number: {
        double in = value->get_double();
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
                                           const std::string& name, simdjson::ondemand::value* value) {
    std::string_view sv = value->get_string();

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
                          simdjson::ondemand::value* value) {
    auto numeric_column = down_cast<FixedLengthColumn<T>*>(column);

    try {
        simdjson::ondemand::json_type tp = value->type();
        switch (tp) {
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
        auto err_msg = strings::Substitute("Failed to parse value as number, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

template Status add_numeric_column<int128_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                             simdjson::ondemand::value* value);
template Status add_numeric_column<int64_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            simdjson::ondemand::value* value);
template Status add_numeric_column<int32_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            simdjson::ondemand::value* value);
template Status add_numeric_column<int16_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            simdjson::ondemand::value* value);
template Status add_numeric_column<int8_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                           simdjson::ondemand::value* value);
template Status add_numeric_column<double>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                           simdjson::ondemand::value* value);
template Status add_numeric_column<float>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                          simdjson::ondemand::value* value);

} // namespace starrocks
