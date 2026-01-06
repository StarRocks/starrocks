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
 * TypeCheckerXMLLoader - Loads type checker configurations from XML files.
 * 
 * This class provides functionality to parse XML configuration files that define
 * mappings between Java class names and their corresponding type checker implementations.
 * 
 * XML Format:
 * <?xml version="1.0" encoding="UTF-8"?>
 * <type-checkers>
 *   <type-mapping java_class="java.lang.String" checker="StringTypeChecker"/>
 *   ...
 * </type-checkers>
 * 
 * The loader supports:
 * - Dynamic type checker registration from XML
 * - Fallback to hardcoded configurations if XML is unavailable
 * - Validation of XML structure and content
 * - Error reporting for malformed XML or missing files
 */
class TypeCheckerXMLLoader {
public:
    struct TypeMapping {
        std::string java_class;
        std::string checker_name;
    };

    /**
     * Load type checker mappings from an XML file.
     * 
     * @param xml_file_path Path to the XML configuration file
     * @return StatusOr containing a vector of TypeMapping on success, or error Status on failure
     */
    static StatusOr<std::vector<TypeMapping>> load_from_xml(const std::string& xml_file_path);

    /**
     * Create a type checker instance based on the checker name.
     * 
     * @param checker_name Name of the checker class (e.g., "ByteTypeChecker")
     * @return Unique pointer to TypeChecker instance, or nullptr if checker name is unknown
     */
    static std::unique_ptr<TypeChecker> create_checker(const std::string& checker_name);

private:
    /**
     * Parse XML content and extract type mappings.
     * 
     * @param xml_content The XML content as a string
     * @return StatusOr containing a vector of TypeMapping on success, or error Status on failure
     */
    static StatusOr<std::vector<TypeMapping>> parse_xml_content(const std::string& xml_content);

    /**
     * Extract attribute value from an XML tag.
     * 
     * @param line The XML line containing the tag
     * @param attr_name The attribute name to extract
     * @return The attribute value, or empty string if not found
     */
    static std::string extract_attribute(const std::string& line, const std::string& attr_name);

    /**
     * Trim whitespace from both ends of a string.
     * 
     * @param str The string to trim
     * @return Trimmed string
     */
    static std::string trim(const std::string& str);
};

} // namespace starrocks
