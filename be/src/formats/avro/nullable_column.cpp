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
#include "gutil/strings/substitute.h"
#include "types/logical_type.h"

namespace starrocks {

template <typename T>
static Status add_adaptive_nullable_numeric_column(Column* column, const TypeDescriptor& type_desc,
                                                   const std::string& name, const avro_value_t& value) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    if (avro_value_get_type(&value) == AVRO_NULL) {
        column->append_nulls(1);
        return Status::OK();
    }
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_numeric_column<T>(data_column.get(), type_desc, name, value));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

template Status add_adaptive_nullable_numeric_column<int64_t>(Column* column, const TypeDescriptor& type_desc,
                                                              const std::string& name, const avro_value_t& value);
template Status add_adaptive_nullable_numeric_column<int32_t>(Column* column, const TypeDescriptor& type_desc,
                                                              const std::string& name, const avro_value_t& value);
template Status add_adaptive_nullable_numeric_column<int16_t>(Column* column, const TypeDescriptor& type_desc,
                                                              const std::string& name, const avro_value_t& value);
template Status add_adaptive_nullable_numeric_column<int8_t>(Column* column, const TypeDescriptor& type_desc,
                                                             const std::string& name, const avro_value_t& value);
template Status add_adaptive_nullable_numeric_column<uint8_t>(Column* column, const TypeDescriptor& type_desc,
                                                              const std::string& name, const avro_value_t& value);
template Status add_adaptive_nullable_numeric_column<double>(Column* column, const TypeDescriptor& type_desc,
                                                             const std::string& name, const avro_value_t& value);
template Status add_adaptive_nullable_numeric_column<float>(Column* column, const TypeDescriptor& type_desc,
                                                            const std::string& name, const avro_value_t& value);

template <typename T>
static Status add_nullable_numeric_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                          const avro_value_t& value) {
    auto nullable_column = down_cast<NullableColumn*>(column);

    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();
    if (avro_value_get_type(&value) == AVRO_NULL) {
        data_column->append_default(1);
        null_column->append(1);
        return Status::OK();
    }

    RETURN_IF_ERROR(add_numeric_column<T>(data_column.get(), type_desc, name, value));

    null_column->append(0);
    return Status::OK();
}

static Status add_adpative_nullable_binary_column(Column* column, const TypeDescriptor& type_desc,
                                                  const std::string& name, const avro_value_t& value) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    if (avro_value_get_type(&value) == AVRO_NULL) {
        nullable_column->append_nulls(1);
        return Status::OK();
    }
    auto& data_column = nullable_column->begin_append_not_default_value();

    RETURN_IF_ERROR(add_binary_column(data_column.get(), type_desc, name, value));

    nullable_column->finish_append_one_not_default_value();

    return Status::OK();
}

static Status add_nullable_binary_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                         const avro_value_t& value) {
    auto nullable_column = down_cast<NullableColumn*>(column);

    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();
    if (avro_value_get_type(&value) == AVRO_NULL) {
        nullable_column->append_nulls(1);
        return Status::OK();
    }
    RETURN_IF_ERROR(add_binary_column(data_column.get(), type_desc, name, value));
    null_column->append(0);
    return Status::OK();
}

static Status add_adpative_nullable_native_json_column(Column* column, const TypeDescriptor& type_desc,
                                                       const std::string& name, const avro_value_t& value) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    if (avro_value_get_type(&value) == AVRO_NULL) {
        nullable_column->append_nulls(1);
        return Status::OK();
    }
    auto& data_column = nullable_column->begin_append_not_default_value();

    RETURN_IF_ERROR(add_native_json_column(data_column.get(), type_desc, name, value));

    nullable_column->finish_append_one_not_default_value();

    return Status::OK();
}

static Status add_nullable_native_json_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                              const avro_value_t& value) {
    auto nullable_column = down_cast<NullableColumn*>(column);

    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();
    if (avro_value_get_type(&value) == AVRO_NULL) {
        column->append_nulls(1);
        return Status::OK();
    }
    RETURN_IF_ERROR(add_native_json_column(data_column.get(), type_desc, name, value));

    null_column->append(0);
    return Status::OK();
}

static Status add_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                  const avro_value_t& value) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN:
        return add_nullable_numeric_column<int8_t>(column, type_desc, name, value);
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
    case TYPE_JSON:
        return add_nullable_native_json_column(column, type_desc, name, value);
    case TYPE_ARRAY: {
        if (avro_value_get_type(&value) == AVRO_ARRAY) {
            auto nullable_column = down_cast<NullableColumn*>(column);

            auto array_column = down_cast<ArrayColumn*>(nullable_column->mutable_data_column());
            auto null_column = nullable_column->null_column();

            auto& elems_column = array_column->elements_column();
            size_t n = 0;
            if (avro_value_get_size(&value, &n) != 0) {
                auto err_msg = strings::Substitute("Failed to get array size, column=$0", name);
                return Status::InvalidArgument(err_msg);
            }
            for (int i = 0; i < n; i++) {
                avro_value_t element;
                if (avro_value_get_by_index(&value, i, &element, nullptr) != 0) {
                    auto err_msg = strings::Substitute("Failed to get array element, column=$0", name);
                    return Status::InvalidArgument(err_msg);
                }
                RETURN_IF_ERROR(add_nullable_column(elems_column.get(), type_desc.children[0], name, element));
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
    }

    default:
        return add_nullable_binary_column(column, type_desc, name, value);
    }
}

static Status add_adpative_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                           const avro_value_t& value) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN:
        return add_adaptive_nullable_numeric_column<uint8_t>(column, type_desc, name, value);
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
    case TYPE_JSON:
        return add_adpative_nullable_native_json_column(column, type_desc, name, value);
    case TYPE_ARRAY: {
        if (avro_value_get_type(&value) == AVRO_ARRAY) {
            auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);

            auto array_column = down_cast<ArrayColumn*>(nullable_column->mutable_begin_append_not_default_value());

            auto& elems_column = array_column->elements_column();
            size_t n = 0;
            if (avro_value_get_size(&value, &n) != 0) {
                auto err_msg = strings::Substitute("Failed to get array size, column=$0", name);
                return Status::InvalidArgument(err_msg);
            }
            for (int i = 0; i < n; i++) {
                avro_value_t element;
                if (avro_value_get_by_index(&value, i, &element, nullptr) != 0) {
                    auto err_msg = strings::Substitute("Failed to get array element, column=$0", name);
                    return Status::InvalidArgument(err_msg);
                }
                RETURN_IF_ERROR(add_nullable_column(elems_column.get(), type_desc.children[0], name, element));
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
    }
    default:
        return add_adpative_nullable_binary_column(column, type_desc, name, value);
    }
}

Status add_adaptive_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                    const avro_value_t& value, bool invalid_as_null) {
    if (avro_value_get_type(&value) == AVRO_NULL) {
        column->append_nulls(1);
        return Status::OK();
    }

    auto st = add_adpative_nullable_column(column, type_desc, name, value);
    if (UNLIKELY(!st.ok() && invalid_as_null)) {
        column->append_nulls(1);
        return Status::OK();
    }
    return st;
}

Status add_nullable_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                           const avro_value_t& value, bool invalid_as_null) {
    if (avro_value_get_type(&value) == AVRO_NULL) {
        column->append_nulls(1);
        return Status::OK();
    }

    auto st = add_nullable_column(column, type_desc, name, value);
    if (!st.ok() && invalid_as_null) {
        column->append_nulls(1);
        return Status::OK();
    }
    return st;
}

} // namespace starrocks
