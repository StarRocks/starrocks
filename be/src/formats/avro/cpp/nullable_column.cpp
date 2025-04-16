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

#include "formats/avro/cpp/nullable_column.h"

#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/nullable_column.h"
#include "formats/avro/cpp/binary_column.h"
#include "formats/avro/cpp/complex_column.h"
#include "formats/avro/cpp/date_column.h"
#include "formats/avro/cpp/numeric_column.h"
#include "types/logical_type.h"

namespace starrocks {

// ------ adaptive nullable column ------

template <typename T>
static Status add_adaptive_nullable_numeric_column(const avro::GenericDatum& datum, const std::string& col_name,
                                                   Column* column) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_numeric_column<T>(datum, col_name, data_column.get()));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

template Status add_adaptive_nullable_numeric_column<int8_t>(const avro::GenericDatum& datum,
                                                             const std::string& col_name, Column* column);
template Status add_adaptive_nullable_numeric_column<uint8_t>(const avro::GenericDatum& datum,
                                                              const std::string& col_name, Column* column);
template Status add_adaptive_nullable_numeric_column<int16_t>(const avro::GenericDatum& datum,
                                                              const std::string& col_name, Column* column);
template Status add_adaptive_nullable_numeric_column<int32_t>(const avro::GenericDatum& datum,
                                                              const std::string& col_name, Column* column);
template Status add_adaptive_nullable_numeric_column<int64_t>(const avro::GenericDatum& datum,
                                                              const std::string& col_name, Column* column);
template Status add_adaptive_nullable_numeric_column<int128_t>(const avro::GenericDatum& datum,
                                                               const std::string& col_name, Column* column);
template Status add_adaptive_nullable_numeric_column<float>(const avro::GenericDatum& datum,
                                                            const std::string& col_name, Column* column);
template Status add_adaptive_nullable_numeric_column<double>(const avro::GenericDatum& datum,
                                                             const std::string& col_name, Column* column);

template <typename T>
static Status add_adaptive_nullable_decimal_column(const avro::GenericDatum& datum, const std::string& col_name,
                                                   const TypeDescriptor& type_desc, Column* column) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_decimal_column<T>(datum, col_name, type_desc, data_column.get()));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

template Status add_adaptive_nullable_decimal_column<int32_t>(const avro::GenericDatum& datum,
                                                              const std::string& col_name,
                                                              const TypeDescriptor& type_desc, Column* column);
template Status add_adaptive_nullable_decimal_column<int64_t>(const avro::GenericDatum& datum,
                                                              const std::string& col_name,
                                                              const TypeDescriptor& type_desc, Column* column);
template Status add_adaptive_nullable_decimal_column<int128_t>(const avro::GenericDatum& datum,
                                                               const std::string& col_name,
                                                               const TypeDescriptor& type_desc, Column* column);

static Status add_adaptive_nullable_date_column(const avro::GenericDatum& datum, const std::string& col_name,
                                                Column* column) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_date_column(datum, col_name, data_column.get()));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

static Status add_adaptive_nullable_datetime_column(const avro::GenericDatum& datum, const std::string& col_name,
                                                    const cctz::time_zone& timezone, Column* column) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_datetime_column(datum, col_name, timezone, data_column.get()));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

static Status add_adaptive_nullable_struct_column(const avro::GenericDatum& datum, const std::string& col_name,
                                                  const TypeDescriptor& type_desc, bool invalid_as_null,
                                                  const cctz::time_zone& timezone, Column* column) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_struct_column(datum, col_name, type_desc, invalid_as_null, timezone, data_column.get()));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

static Status add_adaptive_nullable_array_column(const avro::GenericDatum& datum, const std::string& col_name,
                                                 const TypeDescriptor& type_desc, bool invalid_as_null,
                                                 const cctz::time_zone& timezone, Column* column) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_array_column(datum, col_name, type_desc, invalid_as_null, timezone, data_column.get()));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

static Status add_adaptive_nullable_map_column(const avro::GenericDatum& datum, const std::string& col_name,
                                               const TypeDescriptor& type_desc, bool invalid_as_null,
                                               const cctz::time_zone& timezone, Column* column) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_map_column(datum, col_name, type_desc, invalid_as_null, timezone, data_column.get()));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

static Status add_adaptive_nullable_binary_column(const avro::GenericDatum& datum, const std::string& col_name,
                                                  const TypeDescriptor& type_desc, Column* column) {
    auto nullable_column = down_cast<AdaptiveNullableColumn*>(column);
    auto& data_column = nullable_column->begin_append_not_default_value();
    RETURN_IF_ERROR(add_binary_column(datum, col_name, type_desc, data_column.get()));
    nullable_column->finish_append_one_not_default_value();
    return Status::OK();
}

static Status add_adaptive_nullable_column_impl(const avro::GenericDatum& datum, const std::string& col_name,
                                                const TypeDescriptor& type_desc, bool invalid_as_null,
                                                const cctz::time_zone& timezone, Column* column) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN:
        return add_adaptive_nullable_numeric_column<uint8_t>(datum, col_name, column);
    case TYPE_TINYINT:
        return add_adaptive_nullable_numeric_column<int8_t>(datum, col_name, column);
    case TYPE_SMALLINT:
        return add_adaptive_nullable_numeric_column<int16_t>(datum, col_name, column);
    case TYPE_INT:
        return add_adaptive_nullable_numeric_column<int32_t>(datum, col_name, column);
    case TYPE_BIGINT:
        return add_adaptive_nullable_numeric_column<int64_t>(datum, col_name, column);
    case TYPE_LARGEINT:
        return add_adaptive_nullable_numeric_column<int128_t>(datum, col_name, column);
    case TYPE_FLOAT:
        return add_adaptive_nullable_numeric_column<float>(datum, col_name, column);
    case TYPE_DOUBLE:
        return add_adaptive_nullable_numeric_column<double>(datum, col_name, column);
    case TYPE_DECIMAL32:
        return add_adaptive_nullable_decimal_column<int32_t>(datum, col_name, type_desc, column);
    case TYPE_DECIMAL64:
        return add_adaptive_nullable_decimal_column<int64_t>(datum, col_name, type_desc, column);
    case TYPE_DECIMAL128:
        return add_adaptive_nullable_decimal_column<int128_t>(datum, col_name, type_desc, column);
    case TYPE_DATE:
        return add_adaptive_nullable_date_column(datum, col_name, column);
    case TYPE_DATETIME:
        return add_adaptive_nullable_datetime_column(datum, col_name, timezone, column);
    case TYPE_STRUCT:
        return add_adaptive_nullable_struct_column(datum, col_name, type_desc, invalid_as_null, timezone, column);
    case TYPE_ARRAY:
        return add_adaptive_nullable_array_column(datum, col_name, type_desc, invalid_as_null, timezone, column);
    case TYPE_MAP:
        return add_adaptive_nullable_map_column(datum, col_name, type_desc, invalid_as_null, timezone, column);
    default:
        return add_adaptive_nullable_binary_column(datum, col_name, type_desc, column);
    }
}

Status add_adaptive_nullable_column(const avro::GenericDatum& datum, const std::string& col_name,
                                    const TypeDescriptor& type_desc, bool invalid_as_null,
                                    const cctz::time_zone& timezone, Column* column) {
    auto type = datum.type();
    if (type == avro::AVRO_NULL) {
        column->append_nulls(1);
        return Status::OK();
    } else if (type == avro::AVRO_UNION) {
        const auto& union_value = datum.value<avro::GenericUnion>();
        return add_adaptive_nullable_column(union_value.datum(), col_name, type_desc, invalid_as_null, timezone,
                                            column);
    }

    auto st = add_adaptive_nullable_column_impl(datum, col_name, type_desc, invalid_as_null, timezone, column);
    if (st.is_data_quality_error() && invalid_as_null) {
        column->append_nulls(1);
        return Status::OK();
    }
    return st;
}

// ------ nullable column ------

template <typename T>
static Status add_nullable_numeric_column(const avro::GenericDatum& datum, const std::string& col_name,
                                          Column* column) {
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& data_column = nullable_column->data_column();
    auto& null_column = nullable_column->null_column();

    RETURN_IF_ERROR(add_numeric_column<T>(datum, col_name, data_column.get()));
    null_column->append(0);
    return Status::OK();
}

template Status add_nullable_numeric_column<int8_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                                    Column* column);
template Status add_nullable_numeric_column<uint8_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                                     Column* column);
template Status add_nullable_numeric_column<int16_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                                     Column* column);
template Status add_nullable_numeric_column<int32_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                                     Column* column);
template Status add_nullable_numeric_column<int64_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                                     Column* column);
template Status add_nullable_numeric_column<int128_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                                      Column* column);
template Status add_nullable_numeric_column<float>(const avro::GenericDatum& datum, const std::string& col_name,
                                                   Column* column);
template Status add_nullable_numeric_column<double>(const avro::GenericDatum& datum, const std::string& col_name,
                                                    Column* column);

template <typename T>
static Status add_nullable_decimal_column(const avro::GenericDatum& datum, const std::string& col_name,
                                          const TypeDescriptor& type_desc, Column* column) {
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& data_column = nullable_column->data_column();
    auto& null_column = nullable_column->null_column();

    RETURN_IF_ERROR(add_decimal_column<T>(datum, col_name, type_desc, data_column.get()));
    null_column->append(0);
    return Status::OK();
}

template Status add_nullable_decimal_column<int32_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                                     const TypeDescriptor& type_desc, Column* column);
template Status add_nullable_decimal_column<int64_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                                     const TypeDescriptor& type_desc, Column* column);
template Status add_nullable_decimal_column<int128_t>(const avro::GenericDatum& datum, const std::string& col_name,
                                                      const TypeDescriptor& type_desc, Column* column);

static Status add_nullable_date_column(const avro::GenericDatum& datum, const std::string& col_name,
                                       Column* column) {
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& data_column = nullable_column->data_column();
    auto& null_column = nullable_column->null_column();

    RETURN_IF_ERROR(add_date_column(datum, col_name, data_column.get()));
    null_column->append(0);
    return Status::OK();
}

static Status add_nullable_datetime_column(const avro::GenericDatum& datum, const std::string& col_name,
                                           const cctz::time_zone& timezone, Column* column) {
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& data_column = nullable_column->data_column();
    auto& null_column = nullable_column->null_column();

    RETURN_IF_ERROR(add_datetime_column(datum, col_name, timezone, data_column.get()));
    null_column->append(0);
    return Status::OK();
}

static Status add_nullable_struct_column(const avro::GenericDatum& datum, const std::string& col_name,
                                         const TypeDescriptor& type_desc, bool invalid_as_null,
                                         const cctz::time_zone& timezone, Column* column) {
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& data_column = nullable_column->data_column();
    auto& null_column = nullable_column->null_column();

    RETURN_IF_ERROR(add_struct_column(datum, col_name, type_desc, invalid_as_null, timezone, data_column.get()));
    null_column->append(0);
    return Status::OK();
}

static Status add_nullable_array_column(const avro::GenericDatum& datum, const std::string& col_name,
                                        const TypeDescriptor& type_desc, bool invalid_as_null,
                                        const cctz::time_zone& timezone, Column* column) {
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& data_column = nullable_column->data_column();
    auto& null_column = nullable_column->null_column();

    RETURN_IF_ERROR(add_array_column(datum, col_name, type_desc, invalid_as_null, timezone, data_column.get()));
    null_column->append(0);
    return Status::OK();
}

static Status add_nullable_map_column(const avro::GenericDatum& datum, const std::string& col_name,
                                      const TypeDescriptor& type_desc, bool invalid_as_null,
                                      const cctz::time_zone& timezone, Column* column) {
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& data_column = nullable_column->data_column();
    auto& null_column = nullable_column->null_column();

    RETURN_IF_ERROR(add_map_column(datum, col_name, type_desc, invalid_as_null, timezone, data_column.get()));
    null_column->append(0);
    return Status::OK();
}

static Status add_nullable_binary_column(const avro::GenericDatum& datum, const std::string& col_name,
                                         const TypeDescriptor& type_desc, Column* column) {
    auto nullable_column = down_cast<NullableColumn*>(column);
    auto& data_column = nullable_column->data_column();
    auto& null_column = nullable_column->null_column();

    RETURN_IF_ERROR(add_binary_column(datum, col_name, type_desc, data_column.get()));
    null_column->append(0);
    return Status::OK();
}

static Status add_nullable_column_impl(const avro::GenericDatum& datum, const std::string& col_name,
                                       const TypeDescriptor& type_desc, bool invalid_as_null,
                                       const cctz::time_zone& timezone, Column* column) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN:
        return add_nullable_numeric_column<uint8_t>(datum, col_name, column);
    case TYPE_TINYINT:
        return add_nullable_numeric_column<int8_t>(datum, col_name, column);
    case TYPE_SMALLINT:
        return add_nullable_numeric_column<int16_t>(datum, col_name, column);
    case TYPE_INT:
        return add_nullable_numeric_column<int32_t>(datum, col_name, column);
    case TYPE_BIGINT:
        return add_nullable_numeric_column<int64_t>(datum, col_name, column);
    case TYPE_LARGEINT:
        return add_nullable_numeric_column<int128_t>(datum, col_name, column);
    case TYPE_FLOAT:
        return add_nullable_numeric_column<float>(datum, col_name, column);
    case TYPE_DOUBLE:
        return add_nullable_numeric_column<double>(datum, col_name, column);
    case TYPE_DECIMAL32:
        return add_nullable_decimal_column<int32_t>(datum, col_name, type_desc, column);
    case TYPE_DECIMAL64:
        return add_nullable_decimal_column<int64_t>(datum, col_name, type_desc, column);
    case TYPE_DECIMAL128:
        return add_nullable_decimal_column<int128_t>(datum, col_name, type_desc, column);
    case TYPE_DATE:
        return add_nullable_date_column(datum, col_name, column);
    case TYPE_DATETIME:
        return add_nullable_datetime_column(datum, col_name, timezone, column);
    case TYPE_STRUCT:
        return add_nullable_struct_column(datum, col_name, type_desc, invalid_as_null, timezone, column);
    case TYPE_ARRAY:
        return add_nullable_array_column(datum, col_name, type_desc, invalid_as_null, timezone, column);
    case TYPE_MAP:
        return add_nullable_map_column(datum, col_name, type_desc, invalid_as_null, timezone, column);
    default:
        return add_nullable_binary_column(datum, col_name, type_desc, column);
    }
}

Status add_nullable_column(const avro::GenericDatum& datum, const std::string& col_name,
                           const TypeDescriptor& type_desc, bool invalid_as_null, const cctz::time_zone& timezone,
                           Column* column) {
    auto type = datum.type();
    if (type == avro::AVRO_NULL) {
        column->append_nulls(1);
        return Status::OK();
    } else if (type == avro::AVRO_UNION) {
        const auto& union_value = datum.value<avro::GenericUnion>();
        return add_nullable_column(union_value.datum(), col_name, type_desc, invalid_as_null, timezone, column);
    }

    auto st = add_nullable_column_impl(datum, col_name, type_desc, invalid_as_null, timezone, column);
    if (st.is_data_quality_error() && invalid_as_null) {
        column->append_nulls(1);
        return Status::OK();
    }
    return st;
}

} // namespace starrocks
