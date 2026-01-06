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
    // Default checker for unknown types
    _default_checker = std::make_unique<ConfigurableTypeChecker>("Default",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_VARCHAR, TYPE_VARCHAR}
        });

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
    // All type checkers now use ConfigurableTypeChecker with hardcoded rules
    // These rules match the XML configuration for backward compatibility
    
    // java.lang.Byte
    registerChecker("java.lang.Byte", std::make_unique<ConfigurableTypeChecker>("Byte", 
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_BOOLEAN, TYPE_BOOLEAN},
            {TYPE_TINYINT, TYPE_TINYINT},
            {TYPE_SMALLINT, TYPE_TINYINT},
            {TYPE_INT, TYPE_TINYINT},
            {TYPE_BIGINT, TYPE_TINYINT}
        }));
    
    // java.lang.Short
    registerChecker("java.lang.Short", std::make_unique<ConfigurableTypeChecker>("Short",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_TINYINT, TYPE_SMALLINT},
            {TYPE_SMALLINT, TYPE_SMALLINT},
            {TYPE_INT, TYPE_SMALLINT},
            {TYPE_BIGINT, TYPE_SMALLINT}
        }));
    
    // java.lang.Integer
    registerChecker("java.lang.Integer", std::make_unique<ConfigurableTypeChecker>("Integer",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_TINYINT, TYPE_INT},
            {TYPE_SMALLINT, TYPE_INT},
            {TYPE_INT, TYPE_INT},
            {TYPE_BIGINT, TYPE_INT}
        }));
    
    // java.lang.Long
    registerChecker("java.lang.Long", std::make_unique<ConfigurableTypeChecker>("Long",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_BIGINT, TYPE_BIGINT}
        }));
    
    // java.math.BigInteger
    registerChecker("java.math.BigInteger", std::make_unique<ConfigurableTypeChecker>("BigInteger",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_LARGEINT, TYPE_VARCHAR},
            {TYPE_VARCHAR, TYPE_VARCHAR}
        }));
    
    // java.lang.Boolean
    registerChecker("java.lang.Boolean", std::make_unique<ConfigurableTypeChecker>("Boolean",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_BOOLEAN, TYPE_BOOLEAN},
            {TYPE_SMALLINT, TYPE_BOOLEAN},
            {TYPE_INT, TYPE_BOOLEAN},
            {TYPE_BIGINT, TYPE_BOOLEAN}
        }));
    
    // java.lang.Float
    registerChecker("java.lang.Float", std::make_unique<ConfigurableTypeChecker>("Float",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_FLOAT, TYPE_FLOAT}
        }));
    
    // java.lang.Double
    registerChecker("java.lang.Double", std::make_unique<ConfigurableTypeChecker>("Double",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_DOUBLE, TYPE_DOUBLE},
            {TYPE_FLOAT, TYPE_DOUBLE}
        }));
    
    // java.lang.String
    registerChecker("java.lang.String", std::make_unique<ConfigurableTypeChecker>("String",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_CHAR, TYPE_VARCHAR},
            {TYPE_VARCHAR, TYPE_VARCHAR}
        }));
    
    // java.sql.Timestamp
    registerChecker("java.sql.Timestamp", std::make_unique<ConfigurableTypeChecker>("Timestamp",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_DATETIME, TYPE_VARCHAR},
            {TYPE_VARCHAR, TYPE_VARCHAR}
        }));
    
    // java.sql.Date
    registerChecker("java.sql.Date", std::make_unique<ConfigurableTypeChecker>("Date",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_DATE, TYPE_VARCHAR}
        }));
    
    // java.sql.Time
    registerChecker("java.sql.Time", std::make_unique<ConfigurableTypeChecker>("Time",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_TIME, TYPE_TIME}
        }));
    
    // java.time.LocalDateTime
    registerChecker("java.time.LocalDateTime", std::make_unique<ConfigurableTypeChecker>("LocalDateTime",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_DATETIME, TYPE_VARCHAR}
        }));
    
    // java.time.LocalDate
    registerChecker("java.time.LocalDate", std::make_unique<ConfigurableTypeChecker>("LocalDate",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_DATE, TYPE_VARCHAR}
        }));
    
    // java.math.BigDecimal
    registerChecker("java.math.BigDecimal", std::make_unique<ConfigurableTypeChecker>("BigDecimal",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_DECIMAL32, TYPE_VARCHAR},
            {TYPE_DECIMAL64, TYPE_VARCHAR},
            {TYPE_DECIMAL128, TYPE_VARCHAR},
            {TYPE_DECIMAL256, TYPE_VARCHAR},
            {TYPE_VARCHAR, TYPE_VARCHAR},
            {TYPE_DOUBLE, TYPE_VARCHAR}
        }));
    
    // Oracle TIMESTAMP types
    registerChecker("oracle.sql.TIMESTAMP", std::make_unique<ConfigurableTypeChecker>("OracleTimestamp",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_VARCHAR, TYPE_VARCHAR}
        }));
    
    registerChecker("oracle.sql.TIMESTAMPLTZ", std::make_unique<ConfigurableTypeChecker>("OracleTimestampLTZ",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_VARCHAR, TYPE_VARCHAR}
        }));
    
    registerChecker("oracle.sql.TIMESTAMPTZ", std::make_unique<ConfigurableTypeChecker>("OracleTimestampTZ",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_VARCHAR, TYPE_VARCHAR}
        }));
    
    // microsoft.sql.DateTimeOffset
    registerChecker("microsoft.sql.DateTimeOffset", std::make_unique<ConfigurableTypeChecker>("DateTimeOffset",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_VARCHAR, TYPE_VARCHAR}
        }));
    
    // Binary types
    registerChecker("byte[]", std::make_unique<ConfigurableTypeChecker>("ByteArray",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_BINARY, TYPE_VARBINARY},
            {TYPE_VARBINARY, TYPE_VARBINARY}
        }));
    
    registerChecker("oracle.jdbc.OracleBlob", std::make_unique<ConfigurableTypeChecker>("OracleBlob",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_BINARY, TYPE_VARBINARY},
            {TYPE_VARBINARY, TYPE_VARBINARY}
        }));
    
    registerChecker("[B", std::make_unique<ConfigurableTypeChecker>("ByteArrayB",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_BINARY, TYPE_VARBINARY},
            {TYPE_VARBINARY, TYPE_VARBINARY}
        }));
    
    registerChecker("java.util.UUID", std::make_unique<ConfigurableTypeChecker>("UUID",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_BINARY, TYPE_VARBINARY},
            {TYPE_VARBINARY, TYPE_VARBINARY}
        }));
    
    // ClickHouse unsigned types
    registerChecker("com.clickhouse.data.value.UnsignedByte", std::make_unique<ConfigurableTypeChecker>("UnsignedByte",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_SMALLINT, TYPE_SMALLINT},
            {TYPE_INT, TYPE_SMALLINT},
            {TYPE_BIGINT, TYPE_SMALLINT}
        }));
    
    registerChecker("com.clickhouse.data.value.UnsignedShort", std::make_unique<ConfigurableTypeChecker>("UnsignedShort",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_INT, TYPE_INT},
            {TYPE_BIGINT, TYPE_INT}
        }));
    
    registerChecker("com.clickhouse.data.value.UnsignedInteger", std::make_unique<ConfigurableTypeChecker>("UnsignedInteger",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_BIGINT, TYPE_BIGINT}
        }));
    
    registerChecker("com.clickhouse.data.value.UnsignedLong", std::make_unique<ConfigurableTypeChecker>("UnsignedLong",
        std::vector<ConfigurableTypeChecker::TypeRule>{
            {TYPE_LARGEINT, TYPE_VARCHAR}
        }));
}

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