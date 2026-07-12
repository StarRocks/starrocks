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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/parse_util.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "base/string/parse_util.h"

#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <limits>
#include <unordered_map>

#include "base/string/string_parser.hpp"

namespace starrocks {

StatusOr<int64_t> ParseUtil::parse_mem_spec(const std::string& mem_spec_str, const int64_t memory_limit) {
    bool is_percent = false;
    if (mem_spec_str.empty()) {
        return 0;
    }

    // Assume last character indicates unit or percent.
    int32_t number_str_len = mem_spec_str.size() - 1;
    int64_t multiplier = -1;

    // Look for accepted suffix character.
    switch (*mem_spec_str.rbegin()) {
    case 't':
    case 'T':
        // Terabytes.
        multiplier = 1024L * 1024L * 1024L * 1024L;
        break;
    case 'g':
    case 'G':
        // Gigabytes.
        multiplier = 1024L * 1024L * 1024L;
        break;
    case 'm':
    case 'M':
        // Megabytes.
        multiplier = 1024L * 1024L;
        break;
    case 'k':
    case 'K':
        // Kilobytes
        multiplier = 1024L;
        break;
    case 'b':
    case 'B':
        break;
    case '%':
        is_percent = true;
        break;
    default:
        // No unit was given. Default to bytes.
        number_str_len = mem_spec_str.size();
        break;
    }

    StringParser::ParseResult result;
    int64_t bytes;

    if (multiplier != -1) {
        // Parse float - MB or GB
        auto limit_val = StringParser::string_to_float<double>(mem_spec_str.data(), number_str_len, &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument(fmt::format("Parse mem string: {}", mem_spec_str));
        }

        bytes = multiplier * limit_val;
    } else {
        // Parse int - bytes or percent
        auto limit_val = StringParser::string_to_int<int64_t>(mem_spec_str.data(), number_str_len, &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument(fmt::format("Parse mem string: {}", mem_spec_str));
        }

        if (is_percent) {
            bytes = (static_cast<double>(limit_val) / 100.0) * memory_limit;
        } else {
            bytes = limit_val;
        }
    }

    return bytes;
}

int64_t ParseUtil::parse_data_size(const std::string& value_str) {
    if (value_str.empty()) return 0;

    // Trim leading/trailing spaces
    size_t start = value_str.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return 0;
    size_t end = value_str.find_last_not_of(" \t\n\r");
    std::string s = value_str.substr(start, end - start + 1);

    // Find where the number ends
    size_t idx = 0;
    bool dot_found = false;
    while (idx < s.size() && (std::isdigit(static_cast<unsigned char>(s[idx])) || (!dot_found && s[idx] == '.'))) {
        if (s[idx] == '.') dot_found = true;
        ++idx;
    }

    double num = 0;
    try {
        num = std::stod(s.substr(0, idx));
    } catch (...) {
        return 0;
    }

    // Extract and normalize unit
    std::string unit = s.substr(idx);
    unit.erase(std::remove_if(unit.begin(), unit.end(), [](unsigned char c) { return std::isspace(c); }), unit.end());
    std::transform(unit.begin(), unit.end(), unit.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });

    static const std::unordered_map<std::string, int64_t> unit_map = {{"", 1LL},
                                                                      {"B", 1LL},
                                                                      {"K", 1024LL},
                                                                      {"KB", 1024LL},
                                                                      {"M", 1024LL * 1024},
                                                                      {"MB", 1024LL * 1024},
                                                                      {"G", 1024LL * 1024 * 1024},
                                                                      {"GB", 1024LL * 1024 * 1024},
                                                                      {"T", 1024LL * 1024 * 1024 * 1024},
                                                                      {"TB", 1024LL * 1024 * 1024 * 1024},
                                                                      {"P", 1024LL * 1024 * 1024 * 1024 * 1024},
                                                                      {"PB", 1024LL * 1024 * 1024 * 1024 * 1024}};

    auto it = unit_map.find(unit);
    if (it == unit_map.end()) return 0;

    double result = num * static_cast<double>(it->second);
    if (result >= 0x1p63 || result < static_cast<double>(std::numeric_limits<int64_t>::min())) {
        return 0;
    }
    return static_cast<int64_t>(result);
}

} // namespace starrocks
