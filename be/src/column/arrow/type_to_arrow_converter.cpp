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

// This file contains code originally based on Apache Doris'
// be/src/util/arrow/row_batch.cpp under the Apache License 2.0.

#include "column/arrow/type_to_arrow_converter.h"

#include <arrow/type.h>
#include <fmt/format.h>

#include "gutil/strings/substitute.h"

namespace starrocks {

Status convert_to_arrow_type(const TypeDescriptor& type, std::shared_ptr<arrow::DataType>* result) {
    switch (type.type) {
    case TYPE_BOOLEAN:
        *result = arrow::boolean();
        break;
    case TYPE_TINYINT:
        *result = arrow::int8();
        break;
    case TYPE_SMALLINT:
        *result = arrow::int16();
        break;
    case TYPE_INT:
        *result = arrow::int32();
        break;
    case TYPE_BIGINT:
        *result = arrow::int64();
        break;
    case TYPE_FLOAT:
        *result = arrow::float32();
        break;
    case TYPE_DOUBLE:
        *result = arrow::float64();
        break;
    case TYPE_TIME:
        *result = arrow::float64();
        break;
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_HLL:
    case TYPE_DECIMAL:
    case TYPE_LARGEINT:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_JSON:
        *result = arrow::utf8();
        break;
    case TYPE_VARBINARY:
        *result = arrow::binary();
        break;
    case TYPE_DECIMALV2:
        *result = std::make_shared<arrow::Decimal128Type>(27, 9);
        break;
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
        *result = std::make_shared<arrow::Decimal128Type>(type.precision, type.scale);
        break;
    case TYPE_DECIMAL256:
        *result = std::make_shared<arrow::Decimal256Type>(type.precision, type.scale);
        break;
    case TYPE_ARRAY: {
        std::shared_ptr<arrow::DataType> type0;
        RETURN_IF_ERROR(convert_to_arrow_type(type.children[0], &type0));
        *result = arrow::list(type0);
        break;
    }
    case TYPE_MAP: {
        std::shared_ptr<arrow::DataType> type0;
        RETURN_IF_ERROR(convert_to_arrow_type(type.children[0], &type0));
        std::shared_ptr<arrow::DataType> type1;
        RETURN_IF_ERROR(convert_to_arrow_type(type.children[1], &type1));
        *result = arrow::map(type0, type1);
        break;
    }
    case TYPE_STRUCT: {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        if (type.field_names.size() != type.children.size()) {
            return Status::InternalError(
                    fmt::format("Struct filed names' size {} mismatch children size {} in convert_to_arrow_type()",
                                type.field_names.size(), type.children.size()));
        }
        for (auto i = 0; i < type.children.size(); ++i) {
            std::shared_ptr<arrow::DataType> type0;
            RETURN_IF_ERROR(convert_to_arrow_type(type.children[i], &type0));
            fields.emplace_back(arrow::field(type.field_names[i], type0));
        }
        *result = arrow::struct_(fields);
        break;
    }
    default:
        return Status::InvalidArgument(strings::Substitute("Unknown logical type($0)", type.type));
    }
    return Status::OK();
}

Status convert_to_arrow_type_for_flight_sql(const TypeDescriptor& type, std::shared_ptr<arrow::DataType>* result) {
    switch (type.type) {
    case TYPE_BOOLEAN:
        *result = arrow::boolean();
        break;
    case TYPE_TINYINT:
        *result = arrow::int8();
        break;
    case TYPE_SMALLINT:
        *result = arrow::int16();
        break;
    case TYPE_INT:
        *result = arrow::int32();
        break;
    case TYPE_BIGINT:
        *result = arrow::int64();
        break;
    case TYPE_LARGEINT:
        *result = std::make_shared<arrow::Decimal128Type>(38, 0);
        break;
    case TYPE_FLOAT:
        *result = arrow::float32();
        break;
    case TYPE_DOUBLE:
        *result = arrow::float64();
        break;
    case TYPE_TIME:
        *result = arrow::float64();
        break;
    case TYPE_DATE:
        *result = arrow::date32();
        break;
    case TYPE_DATETIME:
        *result = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
        break;
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_JSON:
        *result = arrow::utf8();
        break;
    case TYPE_VARBINARY:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_PERCENTILE:
        // HLL, BITMAP, and PERCENTILE are converted to binary with null values, matching MySQL output.
        *result = arrow::binary();
        break;
    case TYPE_DECIMALV2:
        *result = std::make_shared<arrow::Decimal128Type>(27, 9);
        break;
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
        *result = std::make_shared<arrow::Decimal128Type>(type.precision, type.scale);
        break;
    case TYPE_DECIMAL256:
        *result = std::make_shared<arrow::Decimal256Type>(type.precision, type.scale);
        break;
    case TYPE_ARRAY: {
        std::shared_ptr<arrow::DataType> type0;
        RETURN_IF_ERROR(convert_to_arrow_type_for_flight_sql(type.children[0], &type0));
        *result = arrow::list(type0);
        break;
    }
    case TYPE_MAP: {
        std::shared_ptr<arrow::DataType> type0;
        RETURN_IF_ERROR(convert_to_arrow_type_for_flight_sql(type.children[0], &type0));
        std::shared_ptr<arrow::DataType> type1;
        RETURN_IF_ERROR(convert_to_arrow_type_for_flight_sql(type.children[1], &type1));
        *result = arrow::map(type0, type1);
        break;
    }
    case TYPE_STRUCT: {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        if (type.field_names.size() != type.children.size()) {
            return Status::InternalError(
                    fmt::format("Struct filed names' size {} mismatch children size {} in convert_to_arrow_type()",
                                type.field_names.size(), type.children.size()));
        }
        for (auto i = 0; i < type.children.size(); ++i) {
            std::shared_ptr<arrow::DataType> type0;
            RETURN_IF_ERROR(convert_to_arrow_type_for_flight_sql(type.children[i], &type0));
            fields.emplace_back(arrow::field(type.field_names[i], type0));
        }
        *result = arrow::struct_(fields);
        break;
    }
    default:
        return Status::InvalidArgument(strings::Substitute("Unknown logical type($0)", type.type));
    }
    return Status::OK();
}

Status convert_to_arrow_field(const TypeDescriptor& desc, const std::string& col_name, bool is_nullable,
                              std::shared_ptr<arrow::Field>* field) {
    std::shared_ptr<arrow::DataType> type;
    RETURN_IF_ERROR(convert_to_arrow_type(desc, &type));
    // Keep the column name for compatibility. Arrow consumers must not adjust column order by name.
    *field = arrow::field(col_name, type, is_nullable);
    return Status::OK();
}

Status convert_to_arrow_field_for_flight_sql(const TypeDescriptor& desc, const std::string& col_name, bool is_nullable,
                                             std::shared_ptr<arrow::Field>* field, int32_t version) {
    std::shared_ptr<arrow::DataType> type;

    if (const auto lt = desc.type; lt == TYPE_HLL || lt == TYPE_OBJECT || lt == TYPE_PERCENTILE) {
        is_nullable = true;
    }
    RETURN_IF_ERROR(convert_to_arrow_type_for_flight_sql(desc, &type));

    // Keep the column name for compatibility. Arrow consumers must not adjust column order by name.
    *field = arrow::field(col_name, type, is_nullable);
    return Status::OK();
}

} // namespace starrocks
