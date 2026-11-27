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

#include "base/string/slice.h"

namespace starrocks {

const std::string LIKE_FN_NAME = "like";

// Returns the smallest string greater than every string that starts with |prefix_s|, comparing bytes as unsigned, or
// an empty string when there is none (|prefix_s| is empty or all 0xFF).
inline std::string get_next_prefix(const Slice& prefix_s) {
    std::string next_prefix = prefix_s.to_string();

    for (int i = next_prefix.length() - 1; i >= 0; i--) {
        if (static_cast<unsigned char>(next_prefix[i]) != 0xFF) {
            next_prefix[i]++;
            next_prefix.resize(i + 1);
            return next_prefix;
        }
    }
    return "";
}

enum class InvertedIndexQueryType {
    UNKNOWN_QUERY = -1,
    EQUAL_QUERY = 0,
    LESS_THAN_QUERY = 1,
    LESS_EQUAL_QUERY = 2,
    GREATER_THAN_QUERY = 3,
    GREATER_EQUAL_QUERY = 4,
    MATCH_WILDCARD_QUERY = 5,
    MATCH_FUZZY_QUERY = 6,
    MATCH_ALL_QUERY = 7,
    MATCH_PHRASE_QUERY = 8,
    MATCH_ANY_QUERY = 9
};

} // namespace starrocks
