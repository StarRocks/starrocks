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

#define DEFINE_TYPE_CHECKER(class_name) \
class class_name : public TypeChecker { \
public: \
    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override; \
};

DEFINE_TYPE_CHECKER(ByteTypeChecker)
DEFINE_TYPE_CHECKER(ClickHouseUnsignedByteTypeChecker)
DEFINE_TYPE_CHECKER(ShortTypeChecker)
DEFINE_TYPE_CHECKER(ClickHouseUnsignedShortTypeChecker)
DEFINE_TYPE_CHECKER(IntegerTypeChecker)
DEFINE_TYPE_CHECKER(StringTypeChecker)
DEFINE_TYPE_CHECKER(ClickHouseUnsignedIntegerTypeChecker)
DEFINE_TYPE_CHECKER(LongTypeChecker)
DEFINE_TYPE_CHECKER(BigIntegerTypeChecker)
DEFINE_TYPE_CHECKER(ClickHouseUnsignedLongTypeChecker)
DEFINE_TYPE_CHECKER(BooleanTypeChecker)
DEFINE_TYPE_CHECKER(FloatTypeChecker)
DEFINE_TYPE_CHECKER(DoubleTypeChecker)
DEFINE_TYPE_CHECKER(TimestampTypeChecker)
DEFINE_TYPE_CHECKER(DateTypeChecker)
DEFINE_TYPE_CHECKER(TimeTypeChecker)
DEFINE_TYPE_CHECKER(LocalDateTimeTypeChecker)
DEFINE_TYPE_CHECKER(BigDecimalTypeChecker)
DEFINE_TYPE_CHECKER(OracleTimestampClassTypeChecker)
DEFINE_TYPE_CHECKER(SqlServerDateTimeOffsetTypeChecker)
DEFINE_TYPE_CHECKER(ByteArrayTypeChecker)
DEFINE_TYPE_CHECKER(DefaultTypeChecker)


} // namespace starrocks