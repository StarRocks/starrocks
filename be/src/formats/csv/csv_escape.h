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

#include <string>

namespace starrocks::csv {

// Escape a CSV field according to RFC 4180 rules:
// - If the field contains the column delimiter, double quotes, or newlines (CR/LF),
//   the field must be enclosed in double quotes
// - Any double quotes within the field must be escaped by doubling them
// - Returns the original string if no escaping is needed
inline std::string escape_csv_field(const std::string& field, const std::string& column_delimiter) {
    // Check if the field needs quoting
    bool needs_quoting = false;

    // Check for double quotes, newlines, or column delimiter
    for (char c : field) {
        if (c == '"' || c == '\n' || c == '\r') {
            needs_quoting = true;
            break;
        }
    }

    // Check if field contains the column delimiter
    if (!needs_quoting && field.find(column_delimiter) != std::string::npos) {
        needs_quoting = true;
    }

    if (!needs_quoting) {
        return field;
    }

    // Build the escaped field
    std::string escaped;
    escaped.reserve(field.size() + 2); // At minimum, we add two quotes
    escaped.push_back('"');

    for (char c : field) {
        if (c == '"') {
            // Escape double quotes by doubling them
            escaped.push_back('"');
            escaped.push_back('"');
        } else {
            escaped.push_back(c);
        }
    }

    escaped.push_back('"');
    return escaped;
}

} // namespace starrocks::csv
