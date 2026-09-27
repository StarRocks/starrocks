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

#include <simdjson.h>

#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"

namespace starrocks {

struct SimpleJsonPath {
    std::string key; // key of a json object
    int idx;         // array index of a json array, -1 means not set, -2 means *
    bool is_valid;   // true if the path is successfully parsed

    SimpleJsonPath(std::string key_, int idx_, bool is_valid_) : key(std::move(key_)), idx(idx_), is_valid(is_valid_) {}

    std::string to_string() const {
        std::stringstream ss;
        if (!is_valid) {
            return "INVALID";
        }
        if (!key.empty()) {
            ss << key;
        }
        if (idx == -2) {
            ss << "[*]";
        } else if (idx > -1) {
            ss << "[" << idx << "]";
        }
        return ss.str();
    }

    std::string debug_string() const {
        std::stringstream ss;
        ss << "key: " << key << ", idx: " << idx << ", valid: " << is_valid;
        return ss.str();
    }
};

Status parse_simple_json_paths(const std::string& path_string, std::vector<SimpleJsonPath>* parsed_paths);

// extract_from_object extracts value from object according to the json path.
// Now, we do not support complete functions of json path.
Status extract_from_object(simdjson::ondemand::object& obj, const std::vector<SimpleJsonPath>& jsonpath,
                           simdjson::ondemand::value* value) noexcept;

// jsonpaths_to_string serializes json paths to std::string. Setting sub_index serializes partial json paths.
std::string jsonpaths_to_string(const std::vector<SimpleJsonPath>& jsonpaths, size_t sub_index = -1);

template <typename ValueType>
std::string_view to_json_string_limited(ValueType&& val, size_t limit) {
    std::string_view sv = simdjson::to_json_string(std::forward<ValueType>(val));
    if (sv.size() > limit) {
        return sv.substr(0, limit);
    }
    return sv;
}

} // namespace starrocks
