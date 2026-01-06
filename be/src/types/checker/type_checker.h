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

/**
 * Type Checker Framework
 * 
 * This file defines the type checker interface and concrete implementations for validating
 * and converting Java types to StarRocks logical types. The framework supports both:
 * 
 * 1. XML Configuration-based Type Mapping (Recommended)
 *    - Type mappings can be defined in XML configuration files
 *    - Location specified via STARROCKS_TYPE_CHECKER_CONFIG environment variable
 *    - Default location: $STARROCKS_HOME/conf/type_checker_config.xml
 *    - Provides dynamic type registration without recompilation
 *    - See type_checker_xml_loader.h for XML format details
 * 
 * 2. Hardcoded Type Mapping (Backward Compatible)
 *    - Falls back to hardcoded type checkers if XML is unavailable
 *    - Ensures backward compatibility with existing deployments
 *    - No configuration required
 * 
 * Usage Example:
 *   TypeCheckerManager& manager = TypeCheckerManager::getInstance();
 *   StatusOr<LogicalType> result = manager.checkType("java.lang.Integer", slot_desc);
 * 
 * Type Checker Responsibilities:
 *   - Validate compatibility between Java class types and StarRocks slot types
 *   - Return the appropriate StarRocks LogicalType for the conversion
 *   - Provide clear error messages when type mismatches occur
 * 
 * Supported Java Types:
 *   - Primitive types: Byte, Short, Integer, Long, Boolean, Float, Double
 *   - String types: String
 *   - Temporal types: Date, Time, Timestamp, LocalDate, LocalDateTime
 *   - Numeric types: BigInteger, BigDecimal
 *   - Binary types: byte[], ByteArray
 *   - Database-specific types: Oracle, ClickHouse, SQL Server extensions
 */

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

/**
 * ConfigurableTypeChecker - XML-configurable type checker
 * 
 * This checker allows type validation rules to be specified in XML configuration
 * instead of requiring separate C++ classes for each type variant.
 * 
 * Type checking rules specify:
 * - Allowed input types (what StarRocks types can accept this Java type)
 * - Return type mapping (what LogicalType to return for each allowed type)
 * - Display name for error messages
 * 
 * All type checkers in StarRocks now use this configurable approach, with
 * rules defined in XML configuration files.
 */
class ConfigurableTypeChecker : public TypeChecker {
public:
    struct TypeRule {
        LogicalType allowed_type; // Input type that's allowed
        LogicalType return_type;  // What to return when this type is matched
    };

    ConfigurableTypeChecker(const std::string& display_name, const std::vector<TypeRule>& rules)
            : _display_name(display_name), _rules(rules) {}

    StatusOr<LogicalType> check(const std::string& java_class, const SlotDescriptor* slot_desc) const override;

private:
    std::string _display_name;
    std::vector<TypeRule> _rules;
};

} // namespace starrocks