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

#include "formats/parquet/metadata.h"

#include <sstream>

#include "formats/parquet/schema.h"

namespace starrocks::parquet {

Status FileMetaData::init(const tparquet::FileMetaData& t_metadata, bool case_sensitive) {
    // construct schema from thrift
    RETURN_IF_ERROR(_schema.from_thrift(t_metadata.schema, case_sensitive));
    _num_rows = t_metadata.num_rows;

    _t_metadata = t_metadata;
    return Status::OK();
}

std::string FileMetaData::debug_string() const {
    std::stringstream ss;
    ss << "schema=" << _schema.debug_string();
    return ss.str();
}

std::shared_ptr<const ::parquet::LogicalType> LogicalTypeFromThrift(const tparquet::SchemaElement& schema_element) {
    if (!schema_element.__isset.logicalType) {
        return nullptr;
    }
    auto& type = schema_element.logicalType;
    if (type.__isset.STRING) {
        return ::parquet::StringLogicalType::Make();
    } else if (type.__isset.MAP) {
        return ::parquet::MapLogicalType::Make();
    } else if (type.__isset.LIST) {
        return ::parquet::ListLogicalType::Make();
    } else if (type.__isset.ENUM) {
        return ::parquet::EnumLogicalType::Make();
    } else if (type.__isset.DECIMAL) {
        return ::parquet::DecimalLogicalType::Make(type.DECIMAL.precision, type.DECIMAL.scale);
    } else if (type.__isset.DATE) {
        return ::parquet::DateLogicalType::Make();
    } else if (type.__isset.TIME) {
        ::parquet::LogicalType::TimeUnit::unit unit;
        if (type.TIME.unit.__isset.MILLIS) {
            unit = ::parquet::LogicalType::TimeUnit::MILLIS;
        } else if (type.TIME.unit.__isset.MICROS) {
            unit = ::parquet::LogicalType::TimeUnit::MICROS;
        } else if (type.TIME.unit.__isset.NANOS) {
            unit = ::parquet::LogicalType::TimeUnit::NANOS;
        } else {
            unit = ::parquet::LogicalType::TimeUnit::UNKNOWN;
        }
        return ::parquet::TimeLogicalType::Make(type.TIME.isAdjustedToUTC, unit);
    } else if (type.__isset.TIMESTAMP) {
        ::parquet::LogicalType::TimeUnit::unit unit;
        if (type.TIMESTAMP.unit.__isset.MILLIS) {
            unit = ::parquet::LogicalType::TimeUnit::MILLIS;
        } else if (type.TIMESTAMP.unit.__isset.MICROS) {
            unit = ::parquet::LogicalType::TimeUnit::MICROS;
        } else if (type.TIMESTAMP.unit.__isset.NANOS) {
            unit = ::parquet::LogicalType::TimeUnit::NANOS;
        } else {
            unit = ::parquet::LogicalType::TimeUnit::UNKNOWN;
        }
        return ::parquet::TimestampLogicalType::Make(type.TIMESTAMP.isAdjustedToUTC, unit);
    } else if (type.__isset.INTEGER) {
        return ::parquet::IntLogicalType::Make(static_cast<int>(type.INTEGER.bitWidth), type.INTEGER.isSigned);
    } else if (type.__isset.UNKNOWN) {
        return ::parquet::NullLogicalType::Make();
    } else if (type.__isset.JSON) {
        return ::parquet::JSONLogicalType::Make();
    } else if (type.__isset.BSON) {
        return ::parquet::BSONLogicalType::Make();
    } else if (type.__isset.UUID) {
        return ::parquet::UUIDLogicalType::Make();
    }
    return nullptr;
}

::parquet::ConvertedType::type ConvertedTypeFromThrift(const tparquet::SchemaElement& schema_element) {
    if (!schema_element.__isset.converted_type) {
        return ::parquet::ConvertedType::type::UNDEFINED;
    }
    const auto& type = schema_element.converted_type;
    switch (type) {
    case tparquet::ConvertedType::UTF8:
        return ::parquet::ConvertedType::UTF8;
    case tparquet::ConvertedType::MAP:
        return ::parquet::ConvertedType::MAP;
    case tparquet::ConvertedType::MAP_KEY_VALUE:
        return ::parquet::ConvertedType::MAP_KEY_VALUE;
    case tparquet::ConvertedType::LIST:
        return ::parquet::ConvertedType::LIST;
    case tparquet::ConvertedType::ENUM:
        return ::parquet::ConvertedType::ENUM;
    case tparquet::ConvertedType::DECIMAL:
        return ::parquet::ConvertedType::DECIMAL;
    case tparquet::ConvertedType::DATE:
        return ::parquet::ConvertedType::DATE;
    case tparquet::ConvertedType::TIME_MILLIS:
        return ::parquet::ConvertedType::TIME_MILLIS;
    case tparquet::ConvertedType::TIME_MICROS:
        return ::parquet::ConvertedType::TIME_MICROS;
    case tparquet::ConvertedType::TIMESTAMP_MILLIS:
        return ::parquet::ConvertedType::TIMESTAMP_MILLIS;
    case tparquet::ConvertedType::TIMESTAMP_MICROS:
        return ::parquet::ConvertedType::TIMESTAMP_MICROS;
    case tparquet::ConvertedType::UINT_8:
        return ::parquet::ConvertedType::UINT_8;
    case tparquet::ConvertedType::UINT_16:
        return ::parquet::ConvertedType::UINT_16;
    case tparquet::ConvertedType::UINT_32:
        return ::parquet::ConvertedType::UINT_32;
    case tparquet::ConvertedType::UINT_64:
        return ::parquet::ConvertedType::UINT_64;
    case tparquet::ConvertedType::INT_8:
        return ::parquet::ConvertedType::INT_8;
    case tparquet::ConvertedType::INT_16:
        return ::parquet::ConvertedType::INT_16;
    case tparquet::ConvertedType::INT_32:
        return ::parquet::ConvertedType::INT_32;
    case tparquet::ConvertedType::INT_64:
        return ::parquet::ConvertedType::INT_64;
    case tparquet::ConvertedType::JSON:
        return ::parquet::ConvertedType::JSON;
    case tparquet::ConvertedType::BSON:
        return ::parquet::ConvertedType::BSON;
    case tparquet::ConvertedType::INTERVAL:
        return ::parquet::ConvertedType::INTERVAL;
    default:
        return ::parquet::ConvertedType::UNDEFINED;
    }
}

::parquet::Type::type PrimitiveTypeFromThrift(const tparquet::SchemaElement& schema_element) {
    if (!schema_element.__isset.type) {
        return ::parquet::Type::type::UNDEFINED;
    }
    const auto& type = schema_element.type;
    switch (type) {
    case ::tparquet::Type::BOOLEAN:
        return ::parquet::Type::BOOLEAN;
    case ::tparquet::Type::INT32:
        return ::parquet::Type::INT32;
    case ::tparquet::Type::INT64:
        return ::parquet::Type::INT64;
    case ::tparquet::Type::INT96:
        return ::parquet::Type::INT96;
    case ::tparquet::Type::DOUBLE:
        return ::parquet::Type::DOUBLE;
    case ::tparquet::Type::FLOAT:
        return ::parquet::Type::FLOAT;
    case ::tparquet::Type::BYTE_ARRAY:
        return ::parquet::Type::BYTE_ARRAY;
    case ::tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        return ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
    default:
        return ::parquet::Type::UNDEFINED;
    }
}

} // namespace starrocks::parquet
