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

// Define type checker for java.math.BigDecimal
DEFINE_TYPE_CHECKER(BigDecimalTypeChecker)

// Define type checker for oracle.sql.TIMESTAMP, oracle.sql.TIMESTAMPLTZ, or oracle.sql.TIMESTAMPTZ
DEFINE_TYPE_CHECKER(OracleTimestampClassTypeChecker)

// Define type checker for microsoft.sql.DateTimeOffset
DEFINE_TYPE_CHECKER(SqlServerDateTimeOffsetTypeChecker)

// Define type checker for byte array, oracle.jdbc.OracleBlob, or [B
DEFINE_TYPE_CHECKER(ByteArrayTypeChecker)

// Define default type checker for unspecified types
DEFINE_TYPE_CHECKER(DefaultTypeChecker)

} // namespace starrocks