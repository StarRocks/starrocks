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

#include <cstdlib>

#include "checker/type_checker.h"
#include "checker/type_checker_xml_loader.h"
#include "common/logging.h"

namespace starrocks {

TypeCheckerManager::TypeCheckerManager() : _use_xml_config(false) {
    _default_checker = std::make_unique<DefaultTypeChecker>();

    // Attempt to load from XML configuration file
    // Priority order:
    // 1. Environment variable STARROCKS_TYPE_CHECKER_CONFIG
    // 2. Default location: conf/type_checker_config.xml (relative to BE home)
    // 3. Fallback to hardcoded configuration
    const char* xml_path_env = std::getenv("STARROCKS_TYPE_CHECKER_CONFIG");
    std::string xml_path;

    if (xml_path_env != nullptr) {
        xml_path = xml_path_env;
    } else {
        // Try default location relative to BE home
        const char* be_home = std::getenv("STARROCKS_HOME");
        if (be_home != nullptr) {
            xml_path = std::string(be_home) + "/conf/type_checker_config.xml";
        }
    }

    // Try to load from XML if a path was found
    if (!xml_path.empty() && try_load_from_xml(xml_path)) {
        LOG(INFO) << "TypeCheckerManager initialized from XML configuration: " << xml_path;
        return;
    }

    // Fallback to hardcoded configuration
    LOG(INFO) << "TypeCheckerManager using hardcoded configuration";
    init_hardcoded_checkers();
}

void TypeCheckerManager::init_hardcoded_checkers() {
    registerChecker("java.lang.Byte", std::make_unique<ByteTypeChecker>());
    registerChecker("java.lang.Short", std::make_unique<ShortTypeChecker>());
    registerChecker("java.lang.Integer", std::make_unique<IntegerTypeChecker>());
    registerChecker("java.lang.String", std::make_unique<StringTypeChecker>());
    registerChecker("java.lang.Long", std::make_unique<LongTypeChecker>());
    registerChecker("java.math.BigInteger", std::make_unique<BigIntegerTypeChecker>());
    registerChecker("java.lang.Boolean", std::make_unique<BooleanTypeChecker>());
    registerChecker("java.lang.Float", std::make_unique<FloatTypeChecker>());
    registerChecker("java.lang.Double", std::make_unique<DoubleTypeChecker>());
    registerChecker("java.sql.Timestamp", std::make_unique<TimestampTypeChecker>());
    registerChecker("java.sql.Date", std::make_unique<DateTypeChecker>());
    registerChecker("java.sql.Time", std::make_unique<TimeTypeChecker>());
    registerChecker("java.time.LocalDateTime", std::make_unique<LocalDateTimeTypeChecker>());
    registerChecker("java.time.LocalDate", std::make_unique<LocalDateTypeChecker>());
    registerChecker("java.math.BigDecimal", std::make_unique<BigDecimalTypeChecker>());
    registerChecker("oracle.sql.TIMESTAMP", std::make_unique<OracleTimestampClassTypeChecker>());
    registerChecker("oracle.sql.TIMESTAMPLTZ", std::make_unique<OracleTimestampClassTypeChecker>());
    registerChecker("oracle.sql.TIMESTAMPTZ", std::make_unique<OracleTimestampClassTypeChecker>());
    registerChecker("microsoft.sql.DateTimeOffset", std::make_unique<SqlServerDateTimeOffsetTypeChecker>());
    registerChecker("byte[]", std::make_unique<ByteArrayTypeChecker>());
    registerChecker("oracle.jdbc.OracleBlob", std::make_unique<ByteArrayTypeChecker>());
    registerChecker("[B", std::make_unique<ByteArrayTypeChecker>());
    registerChecker("java.util.UUID", std::make_unique<ByteArrayTypeChecker>());
    
    // ClickHouse unsigned types - use configurable checkers with hardcoded rules
    std::vector<ConfigurableTypeChecker::TypeRule> unsigned_byte_rules = {
            {TYPE_SMALLINT, TYPE_SMALLINT},
            {TYPE_INT, TYPE_SMALLINT},
            {TYPE_BIGINT, TYPE_SMALLINT}
    };
    registerChecker("com.clickhouse.data.value.UnsignedByte", 
                    std::make_unique<ConfigurableTypeChecker>("UnsignedByte", unsigned_byte_rules));
    
    std::vector<ConfigurableTypeChecker::TypeRule> unsigned_short_rules = {
            {TYPE_INT, TYPE_INT},
            {TYPE_BIGINT, TYPE_INT}
    };
    registerChecker("com.clickhouse.data.value.UnsignedShort",
                    std::make_unique<ConfigurableTypeChecker>("UnsignedShort", unsigned_short_rules));
    
    std::vector<ConfigurableTypeChecker::TypeRule> unsigned_int_rules = {
            {TYPE_BIGINT, TYPE_BIGINT}
    };
    registerChecker("com.clickhouse.data.value.UnsignedInteger",
                    std::make_unique<ConfigurableTypeChecker>("UnsignedInteger", unsigned_int_rules));
    
    std::vector<ConfigurableTypeChecker::TypeRule> unsigned_long_rules = {
            {TYPE_LARGEINT, TYPE_VARCHAR}
    };
    registerChecker("com.clickhouse.data.value.UnsignedLong",
                    std::make_unique<ConfigurableTypeChecker>("UnsignedLong", unsigned_long_rules));
}

bool TypeCheckerManager::try_load_from_xml(const std::string& xml_file_path) {
    auto mappings_or = TypeCheckerXMLLoader::load_from_xml(xml_file_path);
    if (!mappings_or.ok()) {
        LOG(WARNING) << "Failed to load type checker configuration from XML: " << mappings_or.status().message();
        return false;
    }

    const auto& mappings = mappings_or.value();
    size_t loaded_count = 0;
    for (const auto& mapping : mappings) {
        auto checker = TypeCheckerXMLLoader::create_checker_from_mapping(mapping);
        if (checker == nullptr) {
            LOG(WARNING) << "Failed to create checker for: " << mapping.java_class;
            continue;
        }
        registerChecker(mapping.java_class, std::move(checker));
        loaded_count++;
    }

    if (loaded_count == 0) {
        LOG(WARNING) << "No valid type checkers were loaded from XML configuration";
        return false;
    }

    _use_xml_config = true;
    return true;
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