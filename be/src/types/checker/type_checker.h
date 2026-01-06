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

/**
 * Type Checker Framework
 * 
 * This file defines the type checker interface and concrete implementations for validating
 * and converting Java types to StarRocks logical types. The framework supports both:
 * 
 * 1. XML Configuration-based Type Mapping (Recommended)
 *    - Type mappings can be defined in XML configuration files
 *    - Location specified via STARROCKS_TYPE_CHECKER_CONFIG environment variable
 *    - Default location: $STARROCKS_HOME/conf/type_checker_config.xml
 *    - Provides dynamic type registration without recompilation
 *    - See type_checker_xml_loader.h for XML format details
 * 
 * 2. Hardcoded Type Mapping (Backward Compatible)
 *    - Falls back to hardcoded type checkers if XML is unavailable
 *    - Ensures backward compatibility with existing deployments
 *    - No configuration required
 * 
 * Usage Example:
 *   TypeCheckerManager& manager = TypeCheckerManager::getInstance();
 *   StatusOr<LogicalType> result = manager.checkType("java.lang.Integer", slot_desc);
 * 
 * Type Checker Responsibilities:
 *   - Validate compatibility between Java class types and StarRocks slot types
 *   - Return the appropriate StarRocks LogicalType for the conversion
 *   - Provide clear error messages when type mismatches occur
 * 
 * Supported Java Types:
 *   - Primitive types: Byte, Short, Integer, Long, Boolean, Float, Double
 *   - String types: String
 *   - Temporal types: Date, Time, Timestamp, LocalDate, LocalDateTime
 *   - Numeric types: BigInteger, BigDecimal
 *   - Binary types: byte[], ByteArray
 *   - Database-specific types: Oracle, ClickHouse, SQL Server extensions
 */

#pragma once
#include <string>

#include "common/status.h"
#include "common/statusor.h"
#include "runtime/descriptors.h"
#include "types/logical_type.h"

namespace starrocks {
class TypeChecker {
public:
    virtual ~TypeChecker() = default;
    virtual StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const = 0;
};

#define DEFINE_TYPE_CHECKER(class_name)                                                                             \
    class class_name : public TypeChecker {                                                                         \
    public:                                                                                                         \
        StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override; \
    };

// Define type checker for java.lang.Byte
DEFINE_TYPE_CHECKER(ByteTypeChecker)

// Define type checker for com.clickhouse.data.value.UnsignedByte
DEFINE_TYPE_CHECKER(ClickHouseUnsignedByteTypeChecker)

// Define type checker for java.lang.Short
DEFINE_TYPE_CHECKER(ShortTypeChecker)

// Define type checker for com.clickhouse.data.value.UnsignedShort
DEFINE_TYPE_CHECKER(ClickHouseUnsignedShortTypeChecker)

// Define type checker for java.lang.Integer
DEFINE_TYPE_CHECKER(IntegerTypeChecker)

// Define type checker for java.lang.String
DEFINE_TYPE_CHECKER(StringTypeChecker)

// Define type checker for com.clickhouse.data.value.UnsignedInteger
DEFINE_TYPE_CHECKER(ClickHouseUnsignedIntegerTypeChecker)

// Define type checker for java.lang.Long
DEFINE_TYPE_CHECKER(LongTypeChecker)

// Define type checker for java.math.BigInteger
DEFINE_TYPE_CHECKER(BigIntegerTypeChecker)

// Define type checker for com.clickhouse.data.value.UnsignedLong
DEFINE_TYPE_CHECKER(ClickHouseUnsignedLongTypeChecker)

// Define type checker for java.lang.Boolean
DEFINE_TYPE_CHECKER(BooleanTypeChecker)

// Define type checker for java.lang.Float
DEFINE_TYPE_CHECKER(FloatTypeChecker)

// Define type checker for java.lang.Double
DEFINE_TYPE_CHECKER(DoubleTypeChecker)

// Define type checker for java.sql.Timestamp
DEFINE_TYPE_CHECKER(TimestampTypeChecker)

// Define type checker for java.sql.Date
DEFINE_TYPE_CHECKER(DateTypeChecker)

// Define type checker for java.sql.Time
DEFINE_TYPE_CHECKER(TimeTypeChecker)

// Define type checker for java.time.LocalDateTime
DEFINE_TYPE_CHECKER(LocalDateTimeTypeChecker)

// Define type checker for java.time.LocalDate
DEFINE_TYPE_CHECKER(LocalDateTypeChecker)

// Define type checker for java.math.BigDecimal
DEFINE_TYPE_CHECKER(BigDecimalTypeChecker)

// Define type checker for oracle.sql.TIMESTAMP, oracle.sql.TIMESTAMPLTZ, or oracle.sql.TIMESTAMPTZ
DEFINE_TYPE_CHECKER(OracleTimestampClassTypeChecker)

// Define type checker for microsoft.sql.DateTimeOffset
DEFINE_TYPE_CHECKER(SqlServerDateTimeOffsetTypeChecker)

// Define type checker for byte array, oracle.jdbc.OracleBlob, UUID, or [B
DEFINE_TYPE_CHECKER(ByteArrayTypeChecker)

// Define default type checker for unspecified types
DEFINE_TYPE_CHECKER(DefaultTypeChecker)

} // namespace starrocks