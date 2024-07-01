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

class ByteTypeChecker : public TypeChecker {
public:
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class ClickHouseUnsignedByteTypeChecker : public TypeChecker {
public:
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class ShortTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class ClickHouseUnsignedShortTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class IntegerTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

// java.lang.String
class StringTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class ClickHouseUnsignedIntegerTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class LongTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class BigIntegerTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class ClickHouseUnsignedLongTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class BooleanTypeChecker : public TypeChecker {
public:
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class FloatTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class DoubleTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class TimestampTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class DateTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class TimeTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class LocalDateTimeTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class BigDecimalTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class OracleTimestampClassTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class SqlServerDateTimeOffsetTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class ByteArrayTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

class DefaultTypeChecker : public TypeChecker {
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;
};

} // namespace starrocks