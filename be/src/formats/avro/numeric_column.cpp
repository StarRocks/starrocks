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
    DIAGNOSTIC_PUSH
#if defined(__clang__)
    DIAGNOSTIC_IGNORE("-Wimplicit-int-float-conversion")
#endif
    return (from < std::numeric_limits<ToType>::lowest() || from > std::numeric_limits<ToType>::max());
    DIAGNOSTIC_POP
}

template <typename T>
static Status add_column_with_numeric_value(FixedLengthColumn<T>* column, const TypeDescriptor& type_desc,
                                            const std::string& name, const avro_value_t& value) {
    switch (avro_value_get_type(&value)) {
    case AVRO_INT32: {
        int in;
        if (avro_value_get_int(&value, &in) != 0) {
            auto err_msg = strings::Substitute("Get int value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        T out{};

        if (!checked_cast(in, &out)) {
            column->append_numbers(&out, sizeof(out));
        } else {
            auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, in);
            return Status::InvalidArgument(err_msg);
        }
        return Status::OK();
    }
    case AVRO_INT64: {
        int64_t in;
        if (avro_value_get_long(&value, &in) != 0) {
            auto err_msg = strings::Substitute("Get int64 value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        T out{};

        if (!checked_cast(in, &out)) {
            column->append_numbers(&out, sizeof(out));
        } else {
            auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, in);
            return Status::InvalidArgument(err_msg);
        }
        return Status::OK();
    }
    case AVRO_BOOLEAN: {
        int in;
        if (avro_value_get_boolean(&value, &in) != 0) {
            auto err_msg = strings::Substitute("Get boolean value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
        T out{};

        if (!checked_cast(in, &out)) {
            column->append_numbers(&out, sizeof(out));
        } else {
            auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, in);
            return Status::InvalidArgument(err_msg);
        }
        return Status::OK();
    }

    case AVRO_FLOAT: {
        float in;
        if (avro_value_get_float(&value, &in) != 0) {
            auto err_msg = strings::Substitute("Get float value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }

        T out{};

        if (!checked_cast(in, &out)) {
            column->append_numbers(&out, sizeof(out));
        } else {
            auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, in);
            return Status::InvalidArgument(err_msg);
        }
        return Status::OK();
    }

    case AVRO_DOUBLE: {
        double in;
        if (avro_value_get_double(&value, &in) != 0) {
            auto err_msg = strings::Substitute("Get double value error. column=$0", name);
            return Status::InvalidArgument(err_msg);
        }

        T out{};

        if (!checked_cast(in, &out)) {
            column->append_numbers(&out, sizeof(out));
        } else {
            auto err_msg = strings::Substitute("Value is overflow. column=$0, value=$1", name, in);
            return Status::InvalidArgument(err_msg);
        }
        return Status::OK();
    }

    default: {
        auto err_msg = strings::Substitute("Unsupported value type. column=$0", name);
        return Status::DataQualityError(err_msg);
    }
    }
    return Status::OK();
}

template <typename T>
static Status add_column_with_string_value_numeric(FixedLengthColumn<T>* column, const TypeDescriptor& type_desc,
                                                   const std::string& name, const avro_value_t& value) {
    const char* in;
    size_t size;
    if (avro_value_get_string(&value, &in, &size) != 0) {
        auto err_msg = strings::Substitute("Get string value error. column=$0", name);
        return Status::InvalidArgument(err_msg);
    }

    // The size returned for a string object will include the NUL terminator,
    // it will be one more than you’d get from calling strlen on the content.
    // Please refer to this link: https://avro.apache.org/docs/1.11.1/api/c/
    --size;

    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;

    T v{};
    if constexpr (std::is_floating_point<T>::value) {
        v = StringParser::string_to_float<T>(in, size, &parse_result);
    } else {
        v = StringParser::string_to_int<T>(in, size, &parse_result);
    }

    if (parse_result == StringParser::PARSE_SUCCESS) {
        column->append_numbers(&v, sizeof(v));
        return Status::OK();
    } else {
        // Attemp to parse the string as float.
        auto d = StringParser::string_to_float<double>(in, size, &parse_result);
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
                                                  std::string(in, size), name);
        return Status::InvalidArgument(err_msg);
    }
}

template <typename T>
Status add_numeric_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                          const avro_value_t& value) {
    auto numeric_column = down_cast<FixedLengthColumn<T>*>(column);
    avro_type_t type = avro_value_get_type(&value);
    switch (type) {
    case AVRO_INT32:
    case AVRO_INT64:
    case AVRO_FLOAT:
    case AVRO_DOUBLE:
    case AVRO_BOOLEAN: {
        return add_column_with_numeric_value(numeric_column, type_desc, name, value);
    }

    case AVRO_STRING: {
        return add_column_with_string_value_numeric(numeric_column, type_desc, name, value);
    }

    default: {
        auto err_msg = strings::Substitute("Unsupported value type. Numeric type is required. column=$0", name);
        return Status::InvalidArgument(err_msg);
    }
    }
    return Status::OK();
}

template Status add_numeric_column<int64_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            const avro_value_t& value);
template Status add_numeric_column<int32_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            const avro_value_t& value);
template Status add_numeric_column<int16_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            const avro_value_t& value);
template Status add_numeric_column<int8_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                           const avro_value_t& value);
template Status add_numeric_column<uint8_t>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                            const avro_value_t& value);
template Status add_numeric_column<double>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                           const avro_value_t& value);
template Status add_numeric_column<float>(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                          const avro_value_t& value);

} // namespace starrocks
