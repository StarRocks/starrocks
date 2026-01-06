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

#include "types/checker/type_checker_xml_loader.h"

#include <algorithm>
#include <cctype>

namespace starrocks {

StatusOr<std::vector<TypeCheckerXMLLoader::TypeMapping>> TypeCheckerXMLLoader::load_from_xml(
        const std::string& xml_file_path) {
    std::ifstream file(xml_file_path);
    if (!file.is_open()) {
        return Status::NotFound(strings::Substitute("XML configuration file not found: $0", xml_file_path));
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string xml_content = buffer.str();
    file.close();

    return parse_xml_content(xml_content);
}

StatusOr<std::vector<TypeCheckerXMLLoader::TypeMapping>> TypeCheckerXMLLoader::parse_xml_content(
        const std::string& xml_content) {
    std::vector<TypeMapping> mappings;
    std::istringstream stream(xml_content);
    std::string line;
    bool in_type_checkers = false;

    while (std::getline(stream, line)) {
        std::string trimmed_line = trim(line);

        // Check for opening tag
        if (trimmed_line.find("<type-checkers>") != std::string::npos) {
            in_type_checkers = true;
            continue;
        }

        // Check for closing tag
        if (trimmed_line.find("</type-checkers>") != std::string::npos) {
            in_type_checkers = false;
            continue;
        }

        // Skip comments and empty lines
        if (trimmed_line.empty() || trimmed_line.find("<!--") != std::string::npos ||
            trimmed_line.find("<?xml") != std::string::npos) {
            continue;
        }

        // Parse type-mapping elements
        if (in_type_checkers && trimmed_line.find("<type-mapping") != std::string::npos) {
            std::string java_class = extract_attribute(trimmed_line, "java_class");
            std::string checker = extract_attribute(trimmed_line, "checker");

            if (java_class.empty() || checker.empty()) {
                return Status::InvalidArgument(
                        strings::Substitute("Invalid type-mapping element: $0", trimmed_line));
            }

            mappings.push_back({java_class, checker});
        }
    }

    if (mappings.empty()) {
        return Status::InvalidArgument("No valid type mappings found in XML configuration");
    }

    return mappings;
}

std::string TypeCheckerXMLLoader::extract_attribute(const std::string& line, const std::string& attr_name) {
    std::string search_pattern = attr_name + "=\"";
    size_t start_pos = line.find(search_pattern);
    if (start_pos == std::string::npos) {
        return "";
    }

    start_pos += search_pattern.length();
    size_t end_pos = line.find("\"", start_pos);
    if (end_pos == std::string::npos) {
        return "";
    }

    return line.substr(start_pos, end_pos - start_pos);
}

std::string TypeCheckerXMLLoader::trim(const std::string& str) {
    auto start = std::find_if_not(str.begin(), str.end(), [](unsigned char ch) { return std::isspace(ch); });
    auto end = std::find_if_not(str.rbegin(), str.rend(), [](unsigned char ch) { return std::isspace(ch); }).base();
    return (start < end) ? std::string(start, end) : "";
}

std::unique_ptr<TypeChecker> TypeCheckerXMLLoader::create_checker(const std::string& checker_name) {
    // Factory method to create type checker instances based on checker name.
    // Note: This uses an if-else chain for simplicity and clarity.
    // If the number of checker types grows significantly, consider refactoring
    // to use a static map<string, factory_function> pattern.
    if (checker_name == "ByteTypeChecker") {
        return std::make_unique<ByteTypeChecker>();
    } else if (checker_name == "ClickHouseUnsignedByteTypeChecker") {
        return std::make_unique<ClickHouseUnsignedByteTypeChecker>();
    } else if (checker_name == "ShortTypeChecker") {
        return std::make_unique<ShortTypeChecker>();
    } else if (checker_name == "ClickHouseUnsignedShortTypeChecker") {
        return std::make_unique<ClickHouseUnsignedShortTypeChecker>();
    } else if (checker_name == "IntegerTypeChecker") {
        return std::make_unique<IntegerTypeChecker>();
    } else if (checker_name == "StringTypeChecker") {
        return std::make_unique<StringTypeChecker>();
    } else if (checker_name == "ClickHouseUnsignedIntegerTypeChecker") {
        return std::make_unique<ClickHouseUnsignedIntegerTypeChecker>();
    } else if (checker_name == "LongTypeChecker") {
        return std::make_unique<LongTypeChecker>();
    } else if (checker_name == "BigIntegerTypeChecker") {
        return std::make_unique<BigIntegerTypeChecker>();
    } else if (checker_name == "ClickHouseUnsignedLongTypeChecker") {
        return std::make_unique<ClickHouseUnsignedLongTypeChecker>();
    } else if (checker_name == "BooleanTypeChecker") {
        return std::make_unique<BooleanTypeChecker>();
    } else if (checker_name == "FloatTypeChecker") {
        return std::make_unique<FloatTypeChecker>();
    } else if (checker_name == "DoubleTypeChecker") {
        return std::make_unique<DoubleTypeChecker>();
    } else if (checker_name == "TimestampTypeChecker") {
        return std::make_unique<TimestampTypeChecker>();
    } else if (checker_name == "DateTypeChecker") {
        return std::make_unique<DateTypeChecker>();
    } else if (checker_name == "TimeTypeChecker") {
        return std::make_unique<TimeTypeChecker>();
    } else if (checker_name == "LocalDateTimeTypeChecker") {
        return std::make_unique<LocalDateTimeTypeChecker>();
    } else if (checker_name == "LocalDateTypeChecker") {
        return std::make_unique<LocalDateTypeChecker>();
    } else if (checker_name == "BigDecimalTypeChecker") {
        return std::make_unique<BigDecimalTypeChecker>();
    } else if (checker_name == "OracleTimestampClassTypeChecker") {
        return std::make_unique<OracleTimestampClassTypeChecker>();
    } else if (checker_name == "SqlServerDateTimeOffsetTypeChecker") {
        return std::make_unique<SqlServerDateTimeOffsetTypeChecker>();
    } else if (checker_name == "ByteArrayTypeChecker") {
        return std::make_unique<ByteArrayTypeChecker>();
    } else if (checker_name == "DefaultTypeChecker") {
        return std::make_unique<DefaultTypeChecker>();
    }

    return nullptr;
}

} // namespace starrocks
