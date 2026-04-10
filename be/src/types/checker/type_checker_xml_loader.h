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

#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "types/checker/type_checker.h"

namespace starrocks {

/**
 * TypeCheckerXMLLoader - Loads type checker configurations from XML files using libxml2.
 * 
 * This class provides functionality to parse XML configuration files that define
 * type validation rules for Java-to-StarRocks type mappings.
 * 
 * XML Format:
 * <?xml version="1.0" encoding="UTF-8"?>
 * <type-checkers>
 *   <type-mapping java_class="java.lang.String" display_name="String">
 *     <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
 *   </type-mapping>
 * </type-checkers>
 * 
 * All type checkers are now defined via XML with configurable type rules.
 * The loader uses libxml2 for robust XML parsing.
 */
class TypeCheckerXMLLoader {
public:
    struct TypeMapping {
        std::string java_class;
        std::string display_name;
        std::vector<ConfigurableTypeChecker::TypeRule> rules;
        bool is_configurable; // Always true now
    };

    /**
     * Load type checker mappings from an XML file using libxml2.
     * 
     * @param xml_file_path Path to the XML configuration file
     * @return StatusOr containing a vector of TypeMapping on success, or error Status on failure
     */
    static StatusOr<std::vector<TypeMapping>> load_from_xml(const std::string& xml_file_path);

    /**
     * Create a type checker instance from a type mapping.
     * All type checkers are now ConfigurableTypeChecker instances.
     * 
     * @param mapping The type mapping configuration
     * @return Unique pointer to TypeChecker instance
     */
    static std::unique_ptr<TypeChecker> create_checker_from_mapping(const TypeMapping& mapping);

private:
    /**
     * Parse LogicalType from string.
     * 
     * @param type_str String representation of LogicalType (e.g., "TYPE_INT")
     * @return LogicalType enum value, or TYPE_UNKNOWN if invalid
     */
    static LogicalType parse_logical_type(const std::string& type_str);
};

} // namespace starrocks
