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

#include <cstdint>
#include <string>

namespace starrocks {

struct CSVParseOptions {
    std::string row_delimiter;
    std::string column_delimiter;
    int64_t skip_header;
    bool trim_space;
    char escape;
    char enclose;
    CSVParseOptions(const std::string& row_delimiter_, const std::string& column_delimiter_, int64_t skip_header_ = 0,
                    bool trim_space_ = false, char escape_ = 0, char enclose_ = 0) {
        row_delimiter = row_delimiter_;
        column_delimiter = column_delimiter_;
        skip_header = skip_header_;
        trim_space = trim_space_;
        escape = escape_;
        enclose = enclose_;
    }
    CSVParseOptions() {
        row_delimiter = '\n';
        column_delimiter = ',';
        skip_header = false;
        trim_space = false;
        escape = 0;
        enclose = 0;
    }
};

} // namespace starrocks
