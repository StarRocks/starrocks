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

#include "types/type_checker_manager.h"

namespace starrocks {

TypeCheckerManager::TypeCheckerManager() {
    _default_checker = std::make_unique<DefaultTypeChecker>();
    registerChecker("java.lang.Byte", std::make_unique<ByteTypeChecker>());
    registerChecker("com.clickhouse.data.value.UnsignedByte", std::make_unique<ClickHouseUnsignedByteTypeChecker>());
    registerChecker("java.lang.Short", std::make_unique<ShortTypeChecker>());
    registerChecker("com.clickhouse.data.value.UnsignedShort", std::make_unique<ClickHouseUnsignedShortTypeChecker>());
    registerChecker("java.lang.Integer", std::make_unique<IntegerTypeChecker>());
    registerChecker("java.lang.String", std::make_unique<StringTypeChecker>());
    registerChecker("com.clickhouse.data.value.UnsignedInteger",
                    std::make_unique<ClickHouseUnsignedIntegerTypeChecker>());
    registerChecker("java.lang.Long", std::make_unique<LongTypeChecker>());
    registerChecker("java.math.BigInteger", std::make_unique<BigIntegerTypeChecker>());
    registerChecker("com.clickhouse.data.value.UnsignedLong", std::make_unique<ClickHouseUnsignedLongTypeChecker>());
    registerChecker("java.lang.Boolean", std::make_unique<BooleanTypeChecker>());
    registerChecker("java.lang.Float", std::make_unique<FloatTypeChecker>());
    registerChecker("java.lang.Double", std::make_unique<DoubleTypeChecker>());
    registerChecker("java.sql.Timestamp", std::make_unique<TimestampTypeChecker>());
    registerChecker("java.sql.Date", std::make_unique<DateTypeChecker>());
    registerChecker("java.sql.Time", std::make_unique<TimeTypeChecker>());
    registerChecker("java.time.LocalDateTime", std::make_unique<LocalDateTimeTypeChecker>());
    registerChecker("java.math.BigDecimal", std::make_unique<BigDecimalTypeChecker>());
    registerChecker("oracle.sql.TIMESTAMP", std::make_unique<OracleTimestampClassTypeChecker>());
    registerChecker("oracle.sql.TIMESTAMPLTZ", std::make_unique<OracleTimestampClassTypeChecker>());
    registerChecker("oracle.sql.TIMESTAMPTZ", std::make_unique<OracleTimestampClassTypeChecker>());
    registerChecker("microsoft.sql.DateTimeOffset", std::make_unique<SqlServerDateTimeOffsetTypeChecker>());
    registerChecker("byte[]", std::make_unique<ByteArrayTypeChecker>());
    registerChecker("oracle.jdbc.OracleBlob", std::make_unique<ByteArrayTypeChecker>());
    registerChecker("[B", std::make_unique<ByteArrayTypeChecker>());
}

TypeCheckerManager& TypeCheckerManager::getInstance() {
    static TypeCheckerManager instance;
    return instance;
}

void TypeCheckerManager::registerChecker(const std::string& java_class, std::unique_ptr<TypeChecker> checker) {
    _checkers.emplace(java_class, std::move(checker));
}

StatusOr<LogicalType> TypeCheckerManager::checkType(const std::string& java_class, const SlotDescriptor* slot_desc) {
    auto it = _checkers.find(java_class);
    if (it != _checkers.end()) {
        return it->second->check(java_class, slot_desc);
    }
    return _default_checker->check(java_class, slot_desc);
}

} // namespace starrocks