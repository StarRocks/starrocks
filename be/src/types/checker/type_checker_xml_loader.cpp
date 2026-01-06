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

#include <libxml/parser.h>
#include <libxml/tree.h>

#include <unordered_map>

namespace starrocks {

StatusOr<std::vector<TypeCheckerXMLLoader::TypeMapping>> TypeCheckerXMLLoader::load_from_xml(
        const std::string& xml_file_path) {
    // Initialize libxml2
    xmlInitParser();

    // Parse the XML file
    xmlDocPtr doc = xmlReadFile(xml_file_path.c_str(), nullptr, 0);
    if (doc == nullptr) {
        xmlCleanupParser();
        return Status::NotFound(strings::Substitute("Failed to parse XML configuration file: $0", xml_file_path));
    }

    // Get root element
    xmlNode* root = xmlDocGetRootElement(doc);
    if (root == nullptr || xmlStrcmp(root->name, BAD_CAST "type-checkers") != 0) {
        xmlFreeDoc(doc);
        xmlCleanupParser();
        return Status::InvalidArgument("Invalid XML: root element must be <type-checkers>");
    }

    std::vector<TypeMapping> mappings;

    // Iterate through type-mapping elements
    for (xmlNode* node = root->children; node != nullptr; node = node->next) {
        if (node->type != XML_ELEMENT_NODE || xmlStrcmp(node->name, BAD_CAST "type-mapping") != 0) {
            continue;
        }

        // Get java_class attribute
        xmlChar* java_class_attr = xmlGetProp(node, BAD_CAST "java_class");
        if (java_class_attr == nullptr) {
            xmlFreeDoc(doc);
            xmlCleanupParser();
            return Status::InvalidArgument("type-mapping element missing java_class attribute");
        }
        std::string java_class = reinterpret_cast<const char*>(java_class_attr);
        xmlFree(java_class_attr);

        // Get display_name attribute
        xmlChar* display_name_attr = xmlGetProp(node, BAD_CAST "display_name");
        if (display_name_attr == nullptr) {
            xmlFreeDoc(doc);
            xmlCleanupParser();
            return Status::InvalidArgument(
                    strings::Substitute("type-mapping for $0 missing display_name attribute", java_class));
        }
        std::string display_name = reinterpret_cast<const char*>(display_name_attr);
        xmlFree(display_name_attr);

        TypeMapping mapping;
        mapping.java_class = java_class;
        mapping.display_name = display_name;
        mapping.is_configurable = true;

        // Parse type-rule children
        for (xmlNode* rule_node = node->children; rule_node != nullptr; rule_node = rule_node->next) {
            if (rule_node->type != XML_ELEMENT_NODE || xmlStrcmp(rule_node->name, BAD_CAST "type-rule") != 0) {
                continue;
            }

            // Get allowed_type attribute
            xmlChar* allowed_type_attr = xmlGetProp(rule_node, BAD_CAST "allowed_type");
            if (allowed_type_attr == nullptr) {
                xmlFreeDoc(doc);
                xmlCleanupParser();
                return Status::InvalidArgument("type-rule missing allowed_type attribute");
            }
            std::string allowed_type_str = reinterpret_cast<const char*>(allowed_type_attr);
            xmlFree(allowed_type_attr);

            // Get return_type attribute
            xmlChar* return_type_attr = xmlGetProp(rule_node, BAD_CAST "return_type");
            if (return_type_attr == nullptr) {
                xmlFreeDoc(doc);
                xmlCleanupParser();
                return Status::InvalidArgument("type-rule missing return_type attribute");
            }
            std::string return_type_str = reinterpret_cast<const char*>(return_type_attr);
            xmlFree(return_type_attr);

            // Parse LogicalType values
            LogicalType allowed_type = parse_logical_type(allowed_type_str);
            LogicalType return_type = parse_logical_type(return_type_str);

            if (allowed_type == TYPE_UNKNOWN || return_type == TYPE_UNKNOWN) {
                xmlFreeDoc(doc);
                xmlCleanupParser();
                return Status::InvalidArgument(strings::Substitute(
                        "Invalid logical type in type-rule: allowed=$0, return=$1", allowed_type_str, return_type_str));
            }

            ConfigurableTypeChecker::TypeRule rule;
            rule.allowed_type = allowed_type;
            rule.return_type = return_type;
            mapping.rules.push_back(rule);
        }

        if (mapping.rules.empty()) {
            xmlFreeDoc(doc);
            xmlCleanupParser();
            return Status::InvalidArgument(
                    strings::Substitute("type-mapping for $0 has no type-rule children", java_class));
        }

        mappings.push_back(mapping);
    }

    // Clean up
    xmlFreeDoc(doc);
    xmlCleanupParser();

    if (mappings.empty()) {
        return Status::InvalidArgument("No valid type mappings found in XML configuration");
    }

    return mappings;
}

LogicalType TypeCheckerXMLLoader::parse_logical_type(const std::string& type_str) {
    static const std::unordered_map<std::string, LogicalType> type_map = {
            {"TYPE_BOOLEAN", TYPE_BOOLEAN},
            {"TYPE_TINYINT", TYPE_TINYINT},
            {"TYPE_SMALLINT", TYPE_SMALLINT},
            {"TYPE_INT", TYPE_INT},
            {"TYPE_BIGINT", TYPE_BIGINT},
            {"TYPE_LARGEINT", TYPE_LARGEINT},
            {"TYPE_FLOAT", TYPE_FLOAT},
            {"TYPE_DOUBLE", TYPE_DOUBLE},
            {"TYPE_VARCHAR", TYPE_VARCHAR},
            {"TYPE_CHAR", TYPE_CHAR},
            {"TYPE_VARBINARY", TYPE_VARBINARY},
            {"TYPE_BINARY", TYPE_BINARY},
            {"TYPE_DATE", TYPE_DATE},
            {"TYPE_DATETIME", TYPE_DATETIME},
            {"TYPE_TIME", TYPE_TIME},
            {"TYPE_DECIMAL32", TYPE_DECIMAL32},
            {"TYPE_DECIMAL64", TYPE_DECIMAL64},
            {"TYPE_DECIMAL128", TYPE_DECIMAL128},
            {"TYPE_DECIMAL256", TYPE_DECIMAL256},
    };

    auto it = type_map.find(type_str);
    return (it != type_map.end()) ? it->second : TYPE_UNKNOWN;
}

std::unique_ptr<TypeChecker> TypeCheckerXMLLoader::create_checker_from_mapping(const TypeMapping& mapping) {
    // All type checkers are now configurable
    return std::make_unique<ConfigurableTypeChecker>(mapping.display_name, mapping.rules);
}

} // namespace starrocks
