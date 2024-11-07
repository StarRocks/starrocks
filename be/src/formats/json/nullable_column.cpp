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

#include "nullable_column.h"

#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/nullable_column.h"
#include "formats/json/binary_column.h"
#include "formats/json/map_column.h"
#include "formats/json/struct_column.h"
#include "gutil/strings/substitute.h"
#include "types/logical_type.h"
#include "util/string_parser.hpp"

namespace starrocks {

static Status add_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                  simdjson::ondemand::value* value);

template <typename T>
static Status add_adaptive_nullable_numeric_column(Column* column, const TypeDescriptor& type_desc,
                                                   const std::string& name, simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);

    try {
        auto& data_column = nullable_column->begin_append_not_default_value();
        RETURN_IF_ERROR(add_numeric_column<T>(data_column.get(), type_desc, name, value));
        nullable_column->finish_append_one_not_default_value();
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as number, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

template Status add_adaptive_nullable_numeric_column<int128_t>(Column* column, const TypeDescriptor& type_desc,
                                                               const std::string& name,
                                                               simdjson::ondemand::value* value);
template Status add_adaptive_nullable_numeric_column<int64_t>(Column* column, const TypeDescriptor& type_desc,
                                                              const std::string& name,
                                                              simdjson::ondemand::value* value);
template Status add_adaptive_nullable_numeric_column<int32_t>(Column* column, const TypeDescriptor& type_desc,
                                                              const std::string& name,
                                                              simdjson::ondemand::value* value);
template Status add_adaptive_nullable_numeric_column<int16_t>(Column* column, const TypeDescriptor& type_desc,
                                                              const std::string& name,
                                                              simdjson::ondemand::value* value);
template Status add_adaptive_nullable_numeric_column<int8_t>(Column* column, const TypeDescriptor& type_desc,
                                                             const std::string& name, simdjson::ondemand::value* value);
template Status add_adaptive_nullable_numeric_column<double>(Column* column, const TypeDescriptor& type_desc,
                                                             const std::string& name, simdjson::ondemand::value* value);
template Status add_adaptive_nullable_numeric_column<float>(Column* column, const TypeDescriptor& type_desc,
                                                            const std::string& name, simdjson::ondemand::value* value);

template <typename T>
static Status add_nullable_numeric_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                          simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();

    try {
        RETURN_IF_ERROR(add_numeric_column<T>(data_column.get(), type_desc, name, value));
        null_column->append(0);
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as number, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

template Status add_nullable_numeric_column<int128_t>(Column* column, const TypeDescriptor& type_desc,
                                                      const std::string& name, simdjson::ondemand::value* value);
template Status add_nullable_numeric_column<int64_t>(Column* column, const TypeDescriptor& type_desc,
                                                     const std::string& name, simdjson::ondemand::value* value);
template Status add_nullable_numeric_column<int32_t>(Column* column, const TypeDescriptor& type_desc,
                                                     const std::string& name, simdjson::ondemand::value* value);
template Status add_nullable_numeric_column<int16_t>(Column* column, const TypeDescriptor& type_desc,
                                                     const std::string& name, simdjson::ondemand::value* value);
template Status add_nullable_numeric_column<int8_t>(Column* column, const TypeDescriptor& type_desc,
                                                    const std::string& name, simdjson::ondemand::value* value);
template Status add_nullable_numeric_column<double>(Column* column, const TypeDescriptor& type_desc,
                                                    const std::string& name, simdjson::ondemand::value* value);
template Status add_nullable_numeric_column<float>(Column* column, const TypeDescriptor& type_desc,
                                                   const std::string& name, simdjson::ondemand::value* value);

static Status add_boolean_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                 simdjson::ondemand::value* value) {
    auto bool_column = down_cast<FixedLengthColumn<uint8_t>*>(column);

    try {
        simdjson::ondemand::json_type tp = value->type();
        switch (tp) {
        case simdjson::ondemand::json_type::boolean: {
            bool v = value->get_bool();
            bool_column->append(v);
            return Status::OK();
        }

        case simdjson::ondemand::json_type::number: {
            simdjson::ondemand::number_type nt = value->get_number_type();
            switch (nt) {
            case simdjson::ondemand::number_type::signed_integer: {
                int64_t v = value->get_int64();
                bool_column->append(v != 0);
                return Status::OK();
            }
            case simdjson::ondemand::number_type::unsigned_integer: {
                uint64_t v = value->get_uint64();
                bool_column->append(v != 0);
                return Status::OK();
            }
            case simdjson::ondemand::number_type::big_integer: {
                auto s = value->raw_json_token();
                StringParser::ParseResult r;
                auto v = StringParser::string_to_int<int128_t>(s.data(), s.size(), &r);
                if (r != StringParser::PARSE_SUCCESS) {
                    auto err_msg = strings::Substitute("Fail to convert big integer. column=$0, value=$1", name, s);
                    return Status::InvalidArgument(err_msg);
                }
                bool_column->append(v != 0);
                return Status::OK();
            }
            case simdjson::ondemand::number_type::floating_point_number: {
                double v = value->get_double();
                bool_column->append(v != 0);
                return Status::OK();
            }
            default: {
                std::stringstream ss;
                ss << "Unsupported number type. column=" << name << ", type=" << nt;
                return Status::InvalidArgument(ss.str());
            }
            }
        }

        case simdjson::ondemand::json_type::string: {
            std::string_view s = value->get_string();
            StringParser::ParseResult r;
            auto v = StringParser::string_to_int<int32_t>(s.data(), s.size(), &r);
            if (r != StringParser::PARSE_SUCCESS || std::isnan(v) || std::isinf(v)) {
                bool b = StringParser::string_to_bool(s.data(), s.size(), &r);
                if (r != StringParser::PARSE_SUCCESS) {
                    auto err_msg = strings::Substitute("Failed to parse string value as boolean. column=$0, value=$1",
                                                       name, s);
                    return Status::InvalidArgument(err_msg);
                }
                bool_column->append(b);
            } else {
                bool_column->append(v != 0);
            }
            return Status::OK();
        }

        default: {
            std::stringstream ss;
            ss << "Unsupported value type to boolean. column=" << name << ", type=" << tp;
            return Status::InvalidArgument(ss.str());
        }
        }
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as boolean, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

static Status add_adaptive_nullable_boolean_column(Column* column, const TypeDescriptor& type_desc,
                                                   const std::string& name, simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);

    try {
        auto& data_column = nullable_column->begin_append_not_default_value();
        RETURN_IF_ERROR(add_boolean_column(data_column.get(), type_desc, name, value));
        nullable_column->finish_append_one_not_default_value();
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as boolean type, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

static Status add_nullable_boolean_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                          simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();

    try {
        RETURN_IF_ERROR(add_boolean_column(data_column.get(), type_desc, name, value));
        null_column->append(0);
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as boolean type, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

static Status add_adaptive_nullable_binary_column(Column* column, const TypeDescriptor& type_desc,
                                                  const std::string& name, simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);

    try {
        auto& data_column = nullable_column->begin_append_not_default_value();
        RETURN_IF_ERROR(add_binary_column(data_column.get(), type_desc, name, value));
        nullable_column->finish_append_one_not_default_value();
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as binary type, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

static Status add_nullable_binary_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                         simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();

    try {
        RETURN_IF_ERROR(add_binary_column(data_column.get(), type_desc, name, value));
        null_column->append(0);
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as binary type, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

static Status add_adaptive_nullable_native_json_column(Column* column, const TypeDescriptor& type_desc,
                                                       const std::string& name, simdjson::ondemand::value* value) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);

    try {
        auto& data_column = nullable_column->begin_append_not_default_value();
        RETURN_IF_ERROR(add_native_json_column(data_column.get(), type_desc, name, value));
        nullable_column->finish_append_one_not_default_value();
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as json type, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

static Status add_nullable_native_json_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                              simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();

    try {
        RETURN_IF_ERROR(add_native_json_column(data_column.get(), type_desc, name, value));
        null_column->append(0);
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as json type, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

static Status add_adaptive_nullable_array_column(Column* column, const TypeDescriptor& type_desc,
                                                 const std::string& name, simdjson::ondemand::value* value) {
    try {
        if (value->type() == simdjson::ondemand::json_type::array) {
            auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
            auto array_column = down_cast<ArrayColumn*>(nullable_column->mutable_begin_append_not_default_value());
            auto& elems_column = array_column->elements_column();

            simdjson::ondemand::array arr = value->get_array();
            uint32_t n = 0;
            for (auto a : arr) {
                simdjson::ondemand::value item = a.value();
                RETURN_IF_ERROR(add_nullable_column(elems_column.get(), type_desc.children[0], name, &item, true));
                n++;
            }

            auto offsets = array_column->offsets_column();
            uint32_t sz = offsets->get_data().back() + n;
            offsets->append_numbers(&sz, sizeof(sz));
            nullable_column->finish_append_one_not_default_value();

            return Status::OK();
        } else {
            auto err_msg = strings::Substitute("Failed to parse value as array, column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as array, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

static Status add_nullable_array_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                        simdjson::ondemand::value* value) {
    try {
        if (value->type() == simdjson::ondemand::json_type::array) {
            auto nullable_column = down_cast<NullableColumn*>(column);
            auto array_column = down_cast<ArrayColumn*>(nullable_column->mutable_data_column());
            auto null_column = nullable_column->null_column();
            auto& elems_column = array_column->elements_column();

            simdjson::ondemand::array arr = value->get_array();
            uint32_t n = 0;
            for (auto a : arr) {
                simdjson::ondemand::value item = a.value();
                RETURN_IF_ERROR(add_nullable_column(elems_column.get(), type_desc.children[0], name, &item, true));
                n++;
            }

            auto offsets = array_column->offsets_column();
            uint32_t sz = offsets->get_data().back() + n;
            offsets->append_numbers(&sz, sizeof(sz));
            null_column->append(0);

            return Status::OK();
        } else {
            auto err_msg = strings::Substitute("Failed to parse value as array, column=$0", name);
            return Status::InvalidArgument(err_msg);
        }
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as array, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

static Status add_adaptive_nullable_struct_column(Column* column, const TypeDescriptor& type_desc,
                                                  const std::string& name, simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_struct_column(data_column.get(), type_desc, name, value));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

static Status add_nullable_struct_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                         simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();
    RETURN_IF_ERROR(add_struct_column(data_column.get(), type_desc, name, value));
    null_column->append(0);
    return Status::OK();
}

static Status add_adaptive_nullable_map_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                               simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_map_column(data_column.get(), type_desc, name, value));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

static Status add_nullable_map_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                      simdjson::ondemand::value* value) {
    DCHECK(!value->is_null());
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();
    RETURN_IF_ERROR(add_map_column(data_column.get(), type_desc, name, value));
    null_column->append(0);
    return Status::OK();
}

static Status add_adaptive_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                           simdjson::ondemand::value* value) {
    // The type mappint should be in accord with JsonScanner::_construct_json_types();
    // the json lib don't support get_int128_t(), so we load with BinaryColumn and then convert to LargeIntColumn
    switch (type_desc.type) {
    case TYPE_BIGINT:
        return add_adaptive_nullable_numeric_column<int64_t>(column, type_desc, name, value);
    case TYPE_INT:
        return add_adaptive_nullable_numeric_column<int32_t>(column, type_desc, name, value);
    case TYPE_SMALLINT:
        return add_adaptive_nullable_numeric_column<int16_t>(column, type_desc, name, value);
    case TYPE_TINYINT:
        return add_adaptive_nullable_numeric_column<int8_t>(column, type_desc, name, value);
    case TYPE_DOUBLE:
        return add_adaptive_nullable_numeric_column<double>(column, type_desc, name, value);
    case TYPE_FLOAT:
        return add_adaptive_nullable_numeric_column<float>(column, type_desc, name, value);
    case TYPE_BOOLEAN:
        return add_adaptive_nullable_boolean_column(column, type_desc, name, value);
    case TYPE_JSON:
        return add_adaptive_nullable_native_json_column(column, type_desc, name, value);
    case TYPE_ARRAY:
        return add_adaptive_nullable_array_column(column, type_desc, name, value);
    case TYPE_STRUCT:
        return add_adaptive_nullable_struct_column(column, type_desc, name, value);
    case TYPE_MAP:
        return add_adaptive_nullable_map_column(column, type_desc, name, value);
    default:
        return add_adaptive_nullable_binary_column(column, type_desc, name, value);
    }
}

static Status add_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                  simdjson::ondemand::value* value) {
    // The type mappint should be in accord with JsonScanner::_construct_json_types();
    // the json lib don't support get_int128_t(), so we load with BinaryColumn and then convert to LargeIntColumn
    switch (type_desc.type) {
    case TYPE_BIGINT:
        return add_nullable_numeric_column<int64_t>(column, type_desc, name, value);
    case TYPE_INT:
        return add_nullable_numeric_column<int32_t>(column, type_desc, name, value);
    case TYPE_SMALLINT:
        return add_nullable_numeric_column<int16_t>(column, type_desc, name, value);
    case TYPE_TINYINT:
        return add_nullable_numeric_column<int8_t>(column, type_desc, name, value);
    case TYPE_DOUBLE:
        return add_nullable_numeric_column<double>(column, type_desc, name, value);
    case TYPE_FLOAT:
        return add_nullable_numeric_column<float>(column, type_desc, name, value);
    case TYPE_BOOLEAN:
        return add_nullable_boolean_column(column, type_desc, name, value);
    case TYPE_JSON:
        return add_nullable_native_json_column(column, type_desc, name, value);
    case TYPE_ARRAY:
        return add_nullable_array_column(column, type_desc, name, value);
    case TYPE_STRUCT:
        return add_nullable_struct_column(column, type_desc, name, value);
    case TYPE_MAP:
        return add_nullable_map_column(column, type_desc, name, value);
    default:
        return add_nullable_binary_column(column, type_desc, name, value);
    }
}

Status add_adaptive_nullable_column_by_json_object(Column* column, const TypeDescriptor& type_desc,
                                                   const std::string& name, simdjson::ondemand::object* value,
                                                   bool invalid_as_null) {
    try {
        auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);

        auto& data_column = nullable_column->begin_append_not_default_value();

        switch (type_desc.type) {
        case TYPE_JSON: {
            RETURN_IF_ERROR(add_native_json_column(data_column.get(), type_desc, name, value));
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR: {
            RETURN_IF_ERROR(add_binary_column_from_json_object(data_column.get(), type_desc, name, value));
            break;
        }
        default:
            return Status::NotSupported("json object could only load into JSON/VARCHAR column");
        }

        nullable_column->finish_append_one_not_default_value();

        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status add_adaptive_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                    simdjson::ondemand::value* value, bool invalid_as_null) {
    try {
        if (value == nullptr || value->is_null()) {
            column->append_nulls(1);
            return Status::OK();
        }

        auto st = add_adaptive_nullable_column(column, type_desc, name, value);
        if (!st.ok() && invalid_as_null) {
            column->append_nulls(1);
            return Status::OK();
        }
        return st;
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

Status add_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                           simdjson::ondemand::value* value, bool invalid_as_null) {
    try {
        if (value == nullptr || value->is_null()) {
            column->append_nulls(1);
            return Status::OK();
        }

        auto st = add_nullable_column(column, type_desc, name, value);
        if (!st.ok() && invalid_as_null) {
            column->append_nulls(1);
            return Status::OK();
        }
        return st;
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}

} // namespace starrocks
