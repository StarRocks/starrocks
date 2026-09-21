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

#include "connector/jdbc/type_checker.h"

#include <fmt/format.h>

namespace starrocks {

// ConfigurableTypeChecker implementation
StatusOr<LogicalType> ConfigurableTypeChecker::check(const std::string& java_class,
                                                     const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;

    // Check if the slot type matches any of our configured rules. A Java class may carry several
    // rules for one allowed_type that differ only in element_type -- java.util.List carries one
    // per array element type the reader can build -- so an element mismatch has to keep looking
    // rather than decide here: the first such rule would otherwise answer for all of them and
    // reject every element type but its own.
    std::vector<LogicalType> allowed_element_types;
    for (const auto& rule : _rules) {
        if (type != rule.allowed_type) {
            continue;
        }
        if (rule.element_type == TYPE_UNKNOWN) {
            return rule.return_type;
        }
        const auto& children = slot_desc->type().children;
        if (children.size() == 1 && children[0].type == rule.element_type) {
            return rule.return_type;
        }
        allowed_element_types.emplace_back(rule.element_type);
    }

    if (!allowed_element_types.empty()) {
        std::string allowed;
        for (auto element_type : allowed_element_types) {
            if (!allowed.empty()) {
                allowed += ", ";
            }
            allowed += fmt::format("{}<{}>", logical_type_to_string(type), logical_type_to_string(element_type));
        }
        return Status::NotSupported(fmt::format("Unsupported element type on column[{}]: {} accepts only {}",
                                                slot_desc->col_name(), _display_name, allowed));
    }

    auto err_msg = fmt::format(
            "Type mismatches on column[{}] type:{}, JDBC result type is {}, check configuration "
            "for allowed types",
            slot_desc->col_name(), logical_type_to_string(type), java_class);
    // No matching rule found - generate error message
    return Status::NotSupported(std::move(err_msg));
}

} // namespace starrocks
