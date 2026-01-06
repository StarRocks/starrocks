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
    _default_checker = std::make_unique<ConfigurableTypeChecker>(
            "Default", std::vector<ConfigurableTypeChecker::TypeRule>{{TYPE_VARCHAR, TYPE_VARCHAR}});

    // Load type checkers from XML configuration file
    // Location: conf/type_checker_config.xml (relative to BE home)
    std::string xml_path;
    const char* be_home = std::getenv("STARROCKS_HOME");
    if (be_home != nullptr) {
        xml_path = std::string(be_home) + "/conf/type_checker_config.xml";
    } else {
        // Use relative path as fallback
        xml_path = "conf/type_checker_config.xml";
    }

    // Load from XML configuration - this is now mandatory
    if (try_load_from_xml(xml_path)) {
        LOG(INFO) << "TypeCheckerManager initialized from XML configuration: " << xml_path;
    } else {
        LOG(ERROR) << "Failed to load type checker configuration from XML: " << xml_path;
        LOG(ERROR) << "All type checkers must be defined in XML. Please ensure the configuration file exists.";
        // Continue with empty checkers - default checker will handle unknown types
    }
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