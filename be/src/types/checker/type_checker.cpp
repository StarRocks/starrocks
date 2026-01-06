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

#include "types/checker/type_checker.h"

#include <fmt/format.h>

namespace starrocks {

// ConfigurableTypeChecker implementation
StatusOr<LogicalType> ConfigurableTypeChecker::check(const std::string& java_class,
                                                     const SlotDescriptor* slot_desc) const {
    auto type = slot_desc->type().type;

    // Check if the slot type matches any of our configured rules
    for (const auto& rule : _rules) {
        if (type == rule.allowed_type) {
            return rule.return_type;
        }
    }

    // No matching rule found - generate error message
    return Status::NotSupported(
            fmt::format("Type mismatches on column[{}], JDBC result type is {}, check configuration for allowed types",
                        slot_desc->col_name(), _display_name));
}

} // namespace starrocks
