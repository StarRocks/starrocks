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

#include <gtest/gtest.h>

#include "types/type_checker_manager.h"

namespace starrocks {

class TypeCheckerTest : public ::testing::Test {
protected:
    TypeCheckerManager& type_checker_manager_ = TypeCheckerManager::getInstance();
};

// Define unit test for java.lang.Byte
TEST_F(TypeCheckerTest, SupportByteType) {
    SlotDescriptor boolean_type_slot(0, "boolean_type_slot", TypeDescriptor(TYPE_BOOLEAN));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Byte", &boolean_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_BOOLEAN);

    SlotDescriptor tinyint_type_slot(0, "tinyint_type_slot", TypeDescriptor(TYPE_TINYINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Byte", &tinyint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_TINYINT);

    SlotDescriptor smallint_type_slot(0, "smallint_type_slot", TypeDescriptor(TYPE_SMALLINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Byte", &smallint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_TINYINT);
}

TEST_F(TypeCheckerTest, NotSupportByteType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_CHAR));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Byte", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for com.clickhouse.data.value.UnsignedByte
TEST_F(TypeCheckerTest, SupportClickHouseUnsignedByteType) {
    SlotDescriptor smallint_type_slot(0, "smallint_type_slot", TypeDescriptor(TYPE_SMALLINT));
    auto status_or_type =
            type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedByte", &smallint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_SMALLINT);

    SlotDescriptor int_type_slot(0, "int_type_slot", TypeDescriptor(TYPE_INT));
    status_or_type = type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedByte", &int_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_SMALLINT);

    SlotDescriptor bigint_type_slot(0, "bigint_type_slot", TypeDescriptor(TYPE_BIGINT));
    status_or_type = type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedByte", &bigint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_SMALLINT);
}

TEST_F(TypeCheckerTest, NotSupportClickHouseUnsignedByteType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_CHAR));
    auto status_or_type = type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedByte", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.lang.Short
TEST_F(TypeCheckerTest, SupportShortType) {
    SlotDescriptor tinyint_type_slot(0, "tinyint_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Short", &tinyint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_SMALLINT);

    SlotDescriptor smallint_type_slot(0, "smallint_type_slot", TypeDescriptor(TYPE_SMALLINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Short", &smallint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_SMALLINT);

    SlotDescriptor int_type_slot(0, "int_type_slot", TypeDescriptor(TYPE_INT));
    status_or_type = type_checker_manager_.checkType("java.lang.Short", &int_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_SMALLINT);

    SlotDescriptor bigint_type_slot(0, "bigint_type_slot", TypeDescriptor(TYPE_BIGINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Short", &bigint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_SMALLINT);
}

TEST_F(TypeCheckerTest, NotSupportShortType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_CHAR));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Short", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for com.clickhouse.data.value.UnsignedShort
TEST_F(TypeCheckerTest, SupportClickHouseUnsignedShortType) {
    SlotDescriptor int_type_slot(0, "int_type_slot", TypeDescriptor(TYPE_INT));
    auto status_or_type = type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedShort", &int_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_INT);

    SlotDescriptor bigint_type_slot(0, "bigint_type_slot", TypeDescriptor(TYPE_BIGINT));
    status_or_type = type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedShort", &bigint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_INT);
}

TEST_F(TypeCheckerTest, NotSupportClickHouseUnsignedShortType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_CHAR));
    auto status_or_type =
            type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedShort", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.lang.Integer
TEST_F(TypeCheckerTest, SupportIntegerType) {
    SlotDescriptor tinyint_type_slot(0, "tinyint_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Integer", &tinyint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_INT);

    SlotDescriptor smallint_type_slot(0, "smallint_type_slot", TypeDescriptor(TYPE_SMALLINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Integer", &smallint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_INT);

    SlotDescriptor int_type_slot(0, "int_type_slot", TypeDescriptor(TYPE_INT));
    status_or_type = type_checker_manager_.checkType("java.lang.Integer", &int_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_INT);

    SlotDescriptor bigint_type_slot(0, "bigint_type_slot", TypeDescriptor(TYPE_BIGINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Integer", &bigint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_INT);
}

TEST_F(TypeCheckerTest, NotSupportIntegerType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_CHAR));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Integer", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.lang.String
TEST_F(TypeCheckerTest, SupportStringType) {
    SlotDescriptor char_type_slot(0, "char_type_slot", TypeDescriptor(TYPE_CHAR));
    auto status_or_type = type_checker_manager_.checkType("java.lang.String", &char_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);

    SlotDescriptor varchar_type_slot(0, "varchar_type_slot", TypeDescriptor(TYPE_VARCHAR));
    status_or_type = type_checker_manager_.checkType("java.lang.String", &varchar_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportStringType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.lang.String", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for com.clickhouse.data.value.UnsignedInteger
TEST_F(TypeCheckerTest, SupportClickHouseUnsignedIntegerType) {
    SlotDescriptor bigint_type_slot(0, "bigint_type_slot", TypeDescriptor(TYPE_BIGINT));
    auto status_or_type =
            type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedInteger", &bigint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_BIGINT);
}

TEST_F(TypeCheckerTest, NotSupportClickHouseUnsignedIntegerType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type =
            type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedInteger", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.lang.Long
TEST_F(TypeCheckerTest, SupportLongType) {
    SlotDescriptor bigint_type_slot(0, "bigint_type_slot", TypeDescriptor(TYPE_BIGINT));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Long", &bigint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_BIGINT);
}

TEST_F(TypeCheckerTest, NotSupportLongType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Long", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.math.BigInteger
TEST_F(TypeCheckerTest, SupportBigIntegerType) {
    SlotDescriptor largeint_type_slot(0, "largeint_type_slot", TypeDescriptor(TYPE_LARGEINT));
    auto status_or_type = type_checker_manager_.checkType("java.math.BigInteger", &largeint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
    SlotDescriptor varchar_type_slot(0, "varchar_type_slot", TypeDescriptor(TYPE_VARCHAR));
    status_or_type = type_checker_manager_.checkType("java.math.BigInteger", &varchar_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportBigIntegerType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.math.BigInteger", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for com.clickhouse.data.value.UnsignedLong
TEST_F(TypeCheckerTest, SupportClickHouseUnsignedLongType) {
    SlotDescriptor largeint_type_slot(0, "largeint_type_slot", TypeDescriptor(TYPE_LARGEINT));
    auto status_or_type =
            type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedLong", &largeint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportClickHouseUnsignedLongType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("com.clickhouse.data.value.UnsignedLong", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.lang.Boolean
TEST_F(TypeCheckerTest, SupportBooleanType) {
    SlotDescriptor boolean_type_slot(0, "boolean_type_slot", TypeDescriptor(TYPE_BOOLEAN));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Boolean", &boolean_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_BOOLEAN);

    SlotDescriptor smallint_type_slot(0, "smallint_type_slot", TypeDescriptor(TYPE_SMALLINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Boolean", &smallint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_BOOLEAN);

    SlotDescriptor int_type_slot(0, "int_type_slot", TypeDescriptor(TYPE_INT));
    status_or_type = type_checker_manager_.checkType("java.lang.Boolean", &int_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_BOOLEAN);

    SlotDescriptor bigint_type_slot(0, "bigint_type_slot", TypeDescriptor(TYPE_BIGINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Boolean", &bigint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_BOOLEAN);
}

TEST_F(TypeCheckerTest, NotSupportBooleanType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Boolean", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.lang.Float
TEST_F(TypeCheckerTest, SupportFloatType) {
    SlotDescriptor float_type_slot(0, "float_type_slot", TypeDescriptor(TYPE_FLOAT));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Float", &float_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_FLOAT);
}

TEST_F(TypeCheckerTest, NotSupportFloatType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Float", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.lang.Double
TEST_F(TypeCheckerTest, SupportDoubleType) {
    SlotDescriptor double_type_slot(0, "double_type_slot", TypeDescriptor(TYPE_DOUBLE));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Double", &double_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_DOUBLE);

    SlotDescriptor float_type_slot(0, "float_type_slot", TypeDescriptor(TYPE_FLOAT));
    status_or_type = type_checker_manager_.checkType("java.lang.Double", &float_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_DOUBLE);
}

TEST_F(TypeCheckerTest, NotSupportDoubleType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Double", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.sql.Timestamp
TEST_F(TypeCheckerTest, SupportSqlTimestampType) {
    SlotDescriptor datetime_type_slot(0, "datetime_type_slot", TypeDescriptor(TYPE_DATETIME));
    auto status_or_type = type_checker_manager_.checkType("java.sql.Timestamp", &datetime_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);

    SlotDescriptor varchar_type_slot(0, "float_type_slot", TypeDescriptor(TYPE_VARCHAR));
    status_or_type = type_checker_manager_.checkType("java.sql.Timestamp", &varchar_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportSqlTimestampType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.sql.Timestamp", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.sql.Date
TEST_F(TypeCheckerTest, SupportSqlDateType) {
    SlotDescriptor date_type_slot(0, "date_type_slot", TypeDescriptor(TYPE_DATE));
    auto status_or_type = type_checker_manager_.checkType("java.sql.Date", &date_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportSqlDateType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.sql.Date", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.sql.Time
TEST_F(TypeCheckerTest, SupportSqlTimeType) {
    SlotDescriptor time_type_slot(0, "time_type_slot", TypeDescriptor(TYPE_TIME));
    auto status_or_type = type_checker_manager_.checkType("java.sql.Time", &time_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_TIME);
}

TEST_F(TypeCheckerTest, NotSupportSqlTimeType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.sql.Time", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.time.LocalDateTime
TEST_F(TypeCheckerTest, SupportLocalDateTimeType) {
    SlotDescriptor localdatetime_type_slot(0, "localdatetime_type_slot", TypeDescriptor(TYPE_DATETIME));
    auto status_or_type = type_checker_manager_.checkType("java.time.LocalDateTime", &localdatetime_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportLocalDateTimeType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.time.LocalDateTime", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for java.math.BigDecimal
TEST_F(TypeCheckerTest, SupporBigDecimalType) {
    SlotDescriptor decimal32_type_slot(0, "decimal32_type_slot", TypeDescriptor(TYPE_DECIMAL32));
    auto status_or_type = type_checker_manager_.checkType("java.math.BigDecimal", &decimal32_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);

    SlotDescriptor decimal64_type_slot(0, "decimal64_type_slot", TypeDescriptor(TYPE_DECIMAL64));
    status_or_type = type_checker_manager_.checkType("java.math.BigDecimal", &decimal64_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);

    SlotDescriptor decimal128_type_slot(0, "decimal128_type_slot", TypeDescriptor(TYPE_DECIMAL128));
    status_or_type = type_checker_manager_.checkType("java.math.BigDecimal", &decimal128_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);

    SlotDescriptor varchar_type_slot(0, "varchar_type_slot", TypeDescriptor(TYPE_VARCHAR));
    status_or_type = type_checker_manager_.checkType("java.math.BigDecimal", &varchar_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);

    SlotDescriptor double_type_slot(0, "double_type_slot", TypeDescriptor(TYPE_DOUBLE));
    status_or_type = type_checker_manager_.checkType("java.math.BigDecimal", &double_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportBigDecimalType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.math.BigDecimal", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for oracle.sql.TIMESTAMP, oracle.sql.TIMESTAMPLTZ, or oracle.sql.TIMESTAMPTZ
TEST_F(TypeCheckerTest, SupporOracleTimestampType) {
    SlotDescriptor varchar_type_slot(0, "varchar_type_slot", TypeDescriptor(TYPE_VARCHAR));
    auto status_or_type = type_checker_manager_.checkType("oracle.sql.TIMESTAMP", &varchar_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);

    status_or_type = type_checker_manager_.checkType("oracle.sql.TIMESTAMPLTZ", &varchar_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);

    status_or_type = type_checker_manager_.checkType("oracle.sql.TIMESTAMPTZ", &varchar_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportOracleTimestampType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("oracle.sql.TIMESTAMP", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());

    status_or_type = type_checker_manager_.checkType("oracle.sql.TIMESTAMPLTZ", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());

    status_or_type = type_checker_manager_.checkType("oracle.sql.TIMESTAMPTZ", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for microsoft.sql.DateTimeOffset
TEST_F(TypeCheckerTest, SupporSqlServerDateTimeOffsetType) {
    SlotDescriptor varchar_type_slot(0, "varchar_type_slot", TypeDescriptor(TYPE_VARCHAR));
    auto status_or_type = type_checker_manager_.checkType("microsoft.sql.DateTimeOffset", &varchar_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportSqlServerDateTimeOffsetType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("microsoft.sql.DateTimeOffset", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for byte array, oracle.jdbc.OracleBlob, or [B
TEST_F(TypeCheckerTest, SupporByteArrayType) {
    // SlotDescriptor binary_type_slot(0, "binary_type_slot", TypeDescriptor(TYPE_BINARY));
    // auto status_or_type = type_checker_manager_.checkType("byte[]", &binary_type_slot);
    // ASSERT_TRUE(status_or_type.ok());
    // ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARBINARY);

    SlotDescriptor varbinary_type_slot(0, "varbinary_type_slot", TypeDescriptor(TYPE_VARBINARY));
    auto status_or_type = type_checker_manager_.checkType("byte[]", &varbinary_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARBINARY);

    //status_or_type = type_checker_manager_.checkType("oracle.jdbc.OracleBlob", &binary_type_slot);
    //ASSERT_TRUE(status_or_type.ok());
    //ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARBINARY);

    status_or_type = type_checker_manager_.checkType("oracle.jdbc.OracleBlob", &varbinary_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARBINARY);

    //status_or_type = type_checker_manager_.checkType("[B", &binary_type_slot);
    //ASSERT_TRUE(status_or_type.ok());
    //ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARBINARY);

    status_or_type = type_checker_manager_.checkType("[B", &varbinary_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARBINARY);
}

TEST_F(TypeCheckerTest, NotSupportByteArrayType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("byte[]", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());

    status_or_type = type_checker_manager_.checkType("oracle.jdbc.OracleBlob", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());

    status_or_type = type_checker_manager_.checkType("[B", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

// Define unit test for default types
TEST_F(TypeCheckerTest, SupporDefaultType) {
    SlotDescriptor varchar_type_slot(0, "varchar_type_slot", TypeDescriptor(TYPE_VARCHAR));
    auto status_or_type = type_checker_manager_.checkType("unknown java class", &varchar_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}

TEST_F(TypeCheckerTest, NotSupportDefaultType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("unknown java class", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}

} // namespace starrocks
