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

#include "exec/jdbc/type_checker_manager.h"

#include <cstdlib>
#include <filesystem>

#include "common/logging.h"
#include "exec/jdbc/type_checker.h"
#include "exec/jdbc/type_checker_xml_loader.h"

namespace starrocks {

TypeCheckerManager::TypeCheckerManager() {
    init();
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
        if (mapping.java_class == "*") {
            _default_checker = std::move(checker);
            continue;
        }
        registerChecker(mapping.java_class, std::move(checker));
        loaded_count++;
    }

    if (loaded_count == 0) {
        LOG(WARNING) << "No valid type checkers were loaded from XML configuration";
        return false;
    }

    return true;
}

TypeCheckerManager& TypeCheckerManager::getInstance() {
    static TypeCheckerManager instance;
    return instance;
}

void TypeCheckerManager::init() {
    // Installed under lib/ so package upgrades always overwrite it
    // (conf/ may be preserved for user customizations). Fall back to conf/
    // so the BE can still find the file when running from the source tree.
    const char* be_home = std::getenv("STARROCKS_HOME");
    std::string base = be_home != nullptr ? std::string(be_home) : std::string(".");
    std::string lib_path = base + "/lib/type_checker_config.xml";
    std::string conf_path = base + "/conf/type_checker_config.xml";

    std::string xml_path = std::filesystem::exists(lib_path) ? lib_path : conf_path;
    if (try_load_from_xml(xml_path)) {
        LOG(INFO) << "TypeCheckerManager initialized from XML configuration: " << xml_path;
    } else {
        LOG(ERROR) << "Failed to load type checker configuration from XML: " << xml_path;
    }
}

void TypeCheckerManager::registerChecker(const std::string& java_class, std::unique_ptr<TypeChecker> checker) {
    _checkers.emplace(java_class, std::move(checker));
}

StatusOr<LogicalType> TypeCheckerManager::checkType(const std::string& java_class, const SlotDescriptor* slot_desc) {
    auto it = _checkers.find(java_class);
    if (it != _checkers.end()) {
        return it->second->check(java_class, slot_desc);
    }
    if (_default_checker != nullptr) {
        return _default_checker->check(java_class, slot_desc);
    }
    return Status::NotFound("No type checker found for Java class: " + java_class);
}

} // namespace starrocks
