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

#include <filesystem>

#include "connector/jdbc/type_checker_manager.h"

namespace starrocks {

class TypeCheckerTest : public ::testing::Test {
protected:
    TypeCheckerManager& type_checker_manager_ = TypeCheckerManager::getInstance();
};

// Every element type the JDBC reader can hand over carries its own TYPE_ARRAY rule, all under the
// one java.util.List mapping. Matching has to keep looking past a rule whose element type differs,
// or the first of them answers for all and only its own element type is ever accepted.
TEST_F(TypeCheckerTest, SupportEveryConfiguredArrayElementType) {
    for (auto element_type : {TYPE_VARCHAR, TYPE_BOOLEAN, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_FLOAT, TYPE_DOUBLE,
                              TYPE_DATE, TYPE_DATETIME}) {
        TypeDescriptor array(TYPE_ARRAY);
        array.children.emplace_back(element_type);
        SlotDescriptor slot(0, "element_array", array);
        auto result = type_checker_manager_.checkType("java.util.List", &slot);
        ASSERT_TRUE(result.ok()) << logical_type_to_string(element_type) << ": " << result.status();
        EXPECT_EQ(TYPE_ARRAY, result.value()) << logical_type_to_string(element_type);
    }
}

TEST_F(TypeCheckerTest, RejectUnsupportedArrayElementTypes) {
    for (auto element_type :
         {TYPE_CHAR, TYPE_ARRAY, TYPE_JSON, TYPE_TIME, TYPE_VARBINARY, TYPE_LARGEINT, TYPE_TINYINT, TYPE_DECIMAL32}) {
        TypeDescriptor array(TYPE_ARRAY);
        array.children.emplace_back(element_type);
        SlotDescriptor slot(0, "unsupported_array", array);
        auto result = type_checker_manager_.checkType("java.util.List", &slot);
        ASSERT_FALSE(result.ok()) << logical_type_to_string(element_type);
        // The message names every element type that would have been accepted, not just the first
        // rule's, so it says what to do rather than only what failed. The names are whatever
        // logical_type_to_string spells, which is not the SQL name for either temporal type:
        // TYPE_DATE renders as DATE_V2 and TYPE_DATETIME as TIMESTAMP.
        const auto& message = result.status().message();
        EXPECT_NE(std::string::npos, message.find("ARRAY<VARCHAR>")) << message;
        EXPECT_NE(std::string::npos, message.find("ARRAY<INT>")) << message;
        EXPECT_NE(std::string::npos, message.find("ARRAY<DATE_V2>")) << message;
        EXPECT_NE(std::string::npos, message.find("ARRAY<TIMESTAMP>")) << message;
    }
    SlotDescriptor malformed(0, "no_element_type", TypeDescriptor(TYPE_ARRAY));
    EXPECT_FALSE(type_checker_manager_.checkType("java.util.List", &malformed).ok());
    SlotDescriptor scalar(0, "scalar", TypeDescriptor(TYPE_INT));
    EXPECT_FALSE(type_checker_manager_.checkType("java.util.List", &scalar).ok());
    TypeDescriptor array(TYPE_ARRAY);
    array.children.emplace_back(TYPE_VARCHAR);
    SlotDescriptor slot(0, "string_array", array);
    EXPECT_FALSE(type_checker_manager_.checkType("java.lang.String", &slot).ok());
    EXPECT_FALSE(type_checker_manager_.checkType("org.postgresql.jdbc.PgArray", &slot).ok());
}

TEST_F(TypeCheckerTest, PreserveListScalarFallback) {
    // TYPE_BINARY is not a slot type: TypeDescriptor::get_slot_size() puts it in the
    // "not a real type" group and DCHECKs, so a SlotDescriptor can never carry it.
    for (auto type : {TYPE_VARCHAR, TYPE_VARBINARY}) {
        SlotDescriptor slot(0, "list_as_string", TypeDescriptor(type));
        auto result = type_checker_manager_.checkType("java.util.List", &slot);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(TYPE_VARCHAR, result.value());
    }
}

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

// java.util.UUID. A PostgreSQL JDBC catalog maps a uuid column to VARCHAR(36), a hand-written
// external table may declare it char(36), and the binary declarations that predate both still
// exist -- all four have to be accepted. Every one of them returns TYPE_VARBINARY, because the
// intermediate column the bridge fills is the only shape UDFHelper writes a UUID[] into; the
// scanner casts that to the slot's own type afterwards. Returning TYPE_VARCHAR here instead would
// compile and configure fine and then throw UnsupportedOperationException at read time.
TEST_F(TypeCheckerTest, SupportUUIDType) {
    for (auto slot_type : {TYPE_VARCHAR, TYPE_CHAR, TYPE_BINARY, TYPE_VARBINARY}) {
        SlotDescriptor slot(0, "uuid_slot", TypeDescriptor(slot_type));
        auto result = type_checker_manager_.checkType("java.util.UUID", &slot);
        ASSERT_TRUE(result.ok()) << logical_type_to_string(slot_type) << ": " << result.status();
        EXPECT_EQ(LogicalType::TYPE_VARBINARY, result.value()) << logical_type_to_string(slot_type);
    }
}

TEST_F(TypeCheckerTest, NotSupportUUIDType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_BIGINT));
    auto status_or_type = type_checker_manager_.checkType("java.util.UUID", &unknown_type_slot);
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

    // A JDBC Long may land on a narrower integer slot when an expression is pushed down
    // (e.g. a pushed CAST(... AS SIGNED) whose StarRocks slot is INT/SMALLINT/TINYINT). It is
    // materialized as BIGINT and then cast down to the slot type.
    for (auto narrow : {TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT}) {
        SlotDescriptor narrow_slot(0, "narrow_int_slot", TypeDescriptor(narrow));
        auto narrow_or_type = type_checker_manager_.checkType("java.lang.Long", &narrow_slot);
        ASSERT_TRUE(narrow_or_type.ok());
        ASSERT_EQ(narrow_or_type.value(), LogicalType::TYPE_BIGINT);
    }
}

TEST_F(TypeCheckerTest, NotSupportLongType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_VARCHAR));
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

// Define unit test for java.time.LocalDate
TEST_F(TypeCheckerTest, SupportLocalDateType) {
    SlotDescriptor localdate_type_slot(0, "localdate_type_slot", TypeDescriptor(TYPE_DATE));
    auto status_or_type = type_checker_manager_.checkType("java.time.LocalDate", &localdate_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_VARCHAR);
}
TEST_F(TypeCheckerTest, NotSupportLocalDateType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_TINYINT));
    auto status_or_type = type_checker_manager_.checkType("java.time.LocalDate", &unknown_type_slot);
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

    // MySQL SUM(int) returns DECIMAL (read as BigDecimal) while StarRocks types sum(int) as
    // BIGINT, so a pushed-down sum lands a BigDecimal on a BIGINT slot.
    SlotDescriptor bigint_type_slot(0, "bigint_type_slot", TypeDescriptor(TYPE_BIGINT));
    status_or_type = type_checker_manager_.checkType("java.math.BigDecimal", &bigint_type_slot);
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

// Additional tests for XML configuration support
namespace starrocks {

class TypeCheckerManagerConfigTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directory
        test_dir_ = "/tmp/type_checker_manager_test";
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        // Clean up
        std::filesystem::remove_all(test_dir_);
        unsetenv("STARROCKS_TYPE_CHECKER_CONFIG");
        unsetenv("STARROCKS_HOME");
    }

    std::string test_dir_;
};

// Test that TypeCheckerManager uses hardcoded config by default
TEST_F(TypeCheckerManagerConfigTest, DefaultsToHardcodedConfig) {
    // Ensure no environment variables are set
    unsetenv("STARROCKS_TYPE_CHECKER_CONFIG");
    unsetenv("STARROCKS_HOME");

    // The singleton is already initialized, so we just verify it works
    TypeCheckerManager& manager = TypeCheckerManager::getInstance();

    // Verify a few basic type checkers work
    SlotDescriptor int_slot(0, "test", TypeDescriptor(TYPE_INT));
    auto result = manager.checkType("java.lang.Integer", &int_slot);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value(), TYPE_INT);

    SlotDescriptor varchar_slot(0, "test", TypeDescriptor(TYPE_VARCHAR));
    result = manager.checkType("java.lang.String", &varchar_slot);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value(), TYPE_VARCHAR);
}

// Test backward compatibility - all existing type checkers still work
TEST_F(TypeCheckerManagerConfigTest, BackwardCompatibility) {
    TypeCheckerManager& manager = TypeCheckerManager::getInstance();

    // Test a comprehensive set of type mappings to ensure backward compatibility
    struct TypeTest {
        std::string java_class;
        LogicalType slot_type;
        LogicalType expected_result;
    };

    std::vector<TypeTest> tests = {
            {"java.lang.Byte", TYPE_TINYINT, TYPE_TINYINT},    {"java.lang.Short", TYPE_SMALLINT, TYPE_SMALLINT},
            {"java.lang.Integer", TYPE_INT, TYPE_INT},         {"java.lang.Long", TYPE_BIGINT, TYPE_BIGINT},
            {"java.lang.Boolean", TYPE_BOOLEAN, TYPE_BOOLEAN}, {"java.lang.Float", TYPE_FLOAT, TYPE_FLOAT},
            {"java.lang.Double", TYPE_DOUBLE, TYPE_DOUBLE},    {"java.lang.String", TYPE_VARCHAR, TYPE_VARCHAR},
            {"java.sql.Time", TYPE_TIME, TYPE_TIME},           {"byte[]", TYPE_VARBINARY, TYPE_VARBINARY},
    };

    for (const auto& test : tests) {
        SlotDescriptor slot(0, "test", TypeDescriptor(test.slot_type));
        auto result = manager.checkType(test.java_class, &slot);
        ASSERT_TRUE(result.ok()) << "Failed for " << test.java_class;
        ASSERT_EQ(result.value(), test.expected_result) << "Wrong result for " << test.java_class;
    }
}

} // namespace starrocks
