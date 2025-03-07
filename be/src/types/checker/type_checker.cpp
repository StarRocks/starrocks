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

#include "types/checker/type_checker.h"

#include <fmt/format.h>

namespace starrocks {
StatusOr<LogicalType> ByteTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    switch (type) {
    case TYPE_BOOLEAN:
        return TYPE_BOOLEAN;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return TYPE_TINYINT;
    default:
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is Byte, please set the type to "
                            "one of boolean,tinyint,smallint,int,bigint",
                            slot_desc->col_name()));
    }
}

StatusOr<LogicalType> ClickHouseUnsignedByteTypeChecker::check(const std::string& java_class,
                                                               const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    switch (type) {
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return TYPE_SMALLINT;
    default:
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is UnsignedByte, please set the type to "
                            "one of smallint,int,bigint",
                            slot_desc->col_name()));
    }
}

StatusOr<LogicalType> ShortTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    switch (type) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return TYPE_SMALLINT;
    default:
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is Short, please set the type to "
                            "one of tinyint,smallint,int,bigint",
                            slot_desc->col_name()));
    }
}

StatusOr<LogicalType> ClickHouseUnsignedShortTypeChecker::check(const std::string& java_class,
                                                                const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    switch (type) {
    case TYPE_INT:
    case TYPE_BIGINT:
        return TYPE_INT;
    default:
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is UnsignedShort, please set the type to "
                            "one of int,bigint",
                            slot_desc->col_name()));
    }
}

StatusOr<LogicalType> IntegerTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    switch (type) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return TYPE_INT;
    default:
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is Integer, please set the type to "
                            "one of tinyint,smallint,int,bigint",
                            slot_desc->col_name()));
    }
}

StatusOr<LogicalType> StringTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;

    switch (type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return TYPE_VARCHAR;
    default:
        return Status::NotSupported(fmt::format(
                "Type mismatches on column[{}], JDBC result type is String, please set the type to varchar or char",
                slot_desc->col_name()));
    }
}

StatusOr<LogicalType> ClickHouseUnsignedIntegerTypeChecker::check(const std::string& java_class,
                                                                  const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_BIGINT) {
        return Status::NotSupported(fmt::format(
                "Type mismatches on column[{}], JDBC result type is UnsignedInteger, please set the type to bigint",
                slot_desc->col_name()));
    }
    return TYPE_BIGINT;
}

StatusOr<LogicalType> LongTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_BIGINT) {
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is Long, please set the type to bigint",
                            slot_desc->col_name()));
    }
    return TYPE_BIGINT;
}

StatusOr<LogicalType> BigIntegerTypeChecker::check(const std::string& java_class,
                                                   const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    switch (type) {
    case TYPE_LARGEINT:
    case TYPE_VARCHAR:
        return TYPE_VARCHAR;
    default:
        return Status::NotSupported(fmt::format(
                "Type mismatches on column[{}], JDBC result type is BigInteger, please set the type to largeint",
                slot_desc->col_name()));
    }
}

StatusOr<LogicalType> ClickHouseUnsignedLongTypeChecker::check(const std::string& java_class,
                                                               const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_LARGEINT) {
        return Status::NotSupported(fmt::format(
                "Type mismatches on column[{}], JDBC result type is UnsignedLong, please set the type to largeint",
                slot_desc->col_name()));
    }
    return TYPE_VARCHAR;
}

StatusOr<LogicalType> BooleanTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;

    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return TYPE_BOOLEAN;
    default:
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is Boolean, please set the type to "
                            "one of boolean, smallint, int, bigint",
                            slot_desc->col_name()));
    }
}

StatusOr<LogicalType> FloatTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_FLOAT) {
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is Float, please set the type to float",
                            slot_desc->col_name()));
    }
    return TYPE_FLOAT;
}

StatusOr<LogicalType> DoubleTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;

    switch (type) {
    case TYPE_DOUBLE:
    case TYPE_FLOAT:
        return TYPE_DOUBLE;
    default:
        return Status::NotSupported(fmt::format(
                "Type mismatches on column[{}], JDBC result type is Double, please set the type to double/float",
                slot_desc->col_name()));
    }
}

StatusOr<LogicalType> TimestampTypeChecker::check(const std::string& java_class,
                                                  const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;

    switch (type) {
    case TYPE_DATETIME:
    case TYPE_VARCHAR:
        return TYPE_VARCHAR;
    default:
        return Status::NotSupported(fmt::format(
                "Type mismatches on column[{}], JDBC result type is Timestamp, please set the type to datetime",
                slot_desc->col_name()));
    }
}

StatusOr<LogicalType> DateTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_DATE) {
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is Date, please set the type to date",
                            slot_desc->col_name()));
    }
    return TYPE_VARCHAR;
}

StatusOr<LogicalType> TimeTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_TIME) {
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is Time, please set the type to time",
                            slot_desc->col_name()));
    }
    return TYPE_TIME;
}

StatusOr<LogicalType> LocalDateTimeTypeChecker::check(const std::string& java_class,
                                                      const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_DATETIME) {
        return Status::NotSupported(fmt::format(
                "Type mismatches on column[{}], JDBC result type is LocalDateTime, please set the type to datetime",
                slot_desc->col_name()));
    }
    return TYPE_VARCHAR;
}

StatusOr<LogicalType> BigDecimalTypeChecker::check(const std::string& java_class,
                                                   const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;

    switch (type) {
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_VARCHAR:
    case TYPE_DOUBLE:
        return TYPE_VARCHAR;
    default:
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is BigDecimal, please set the type to "
                            "decimal, double or varchar",
                            slot_desc->col_name()));
    }
}

StatusOr<LogicalType> OracleTimestampClassTypeChecker::check(const std::string& java_class,
                                                             const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_VARCHAR) {
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is {}, please set the "
                            "type to varchar",
                            slot_desc->col_name(), java_class));
    }
    return TYPE_VARCHAR;
}

StatusOr<LogicalType> SqlServerDateTimeOffsetTypeChecker::check(const std::string& java_class,
                                                                const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_VARCHAR) {
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is {}, please set the "
                            "type to varchar",
                            slot_desc->col_name(), java_class));
    }
    return TYPE_VARCHAR;
}

StatusOr<LogicalType> ByteArrayTypeChecker::check(const std::string& java_class,
                                                  const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;

    switch (type) {
    case TYPE_BINARY:
    case TYPE_VARBINARY:
        return TYPE_VARBINARY;
    default:
        return Status::NotSupported(
                fmt::format("Type mismatches on column[{}], JDBC result type is {}, please set the "
                            "type to varbinary",
                            slot_desc->col_name(), java_class));
    }
}

StatusOr<LogicalType> DefaultTypeChecker::check(const std::string& java_class, const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;
    if (type != TYPE_VARCHAR) {
        return Status::NotSupported(
                fmt::format("JDBC result type of column[{}] is [{}], StarRocks does not recognize it, please set "
                            "the type of this column to varchar to avoid information loss.",
                            slot_desc->col_name(), java_class));
    }
    return TYPE_VARCHAR;
}
} // namespace starrocks