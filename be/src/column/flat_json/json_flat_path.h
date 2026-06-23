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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/phmap/phmap.h"
#include "common/logging.h"
#include "types/logical_type.h"

namespace arangodb::velocypack {}

namespace starrocks {
namespace vpack = arangodb::velocypack;

#ifndef NDEBUG
template <typename K, typename V>
using FlatJsonHashMap = std::unordered_map<K, V>;
#else
template <typename K, typename V>
using FlatJsonHashMap = phmap::flat_hash_map<K, V>;
#endif

class JsonFlatPath {
public:
    using OP = uint8_t;
    static const OP OP_INCLUDE = 0;
    static const OP OP_EXCLUDE = 1;   // for compaction remove extract json
    static const OP OP_IGNORE = 2;    // for merge and read middle json
    static const OP OP_ROOT = 3;      // to mark new root
    static const OP OP_NEW_LEVEL = 4; // for merge flat json use, to mark the path is need

    // for express flat path
    int index = -1; // flat paths array index, only use for leaf, to find column
    LogicalType type = LogicalType::TYPE_JSON;
    bool remain = false;
    OP op = OP_INCLUDE; // merge flat json use, to mark the path is need
    FlatJsonHashMap<std::string_view, std::unique_ptr<JsonFlatPath>> children;

    // derived stats
    // json compatible type
    uint8_t json_type = 31; // JSON_NULL_TYPE_BITS
    // column path hit count, some json may be null or none, so hit use to record the actual value
    // e.g: {"a": 1, "b": 2}, path "$.c" not exist, so hit is 0
    uint32_t hits = 0;
    // for json-uint, json-uint is uint64_t, check the maximum value and downgrade to bigint
    uint64_t max_uint = 0;
    // same key may appear many times in json, so we need avoid duplicate compute hits
    uint32_t last_row = -1;
    uint32_t multi_times = 0;
    uint32_t base_type_count = 0; // for count the base type, e.g: int, double, string
    uint32_t object_count = 0;    // for count the object type

    JsonFlatPath() = default;
    JsonFlatPath(JsonFlatPath&&) = default;
    JsonFlatPath(const JsonFlatPath& rhs) = default;
    ~JsonFlatPath() = default;

    // return the leaf node
    // @info: string_view is not safe memory use, must be careful plz
    static JsonFlatPath* normalize_from_path(const std::string_view& path, JsonFlatPath* root);

    // set new root, other path will set to exclude, the node must include the root path
    static void set_root(const std::string_view& new_root_path, JsonFlatPath* node);

    static std::string debug_flat_json(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                                       bool has_remain) {
        if (paths.empty()) {
            return "[]";
        }
        DCHECK_EQ(paths.size(), types.size());
        std::ostringstream ss;
        ss << "[";
        size_t i = 0;
        for (; i < paths.size() - 1; i++) {
            ss << paths[i] << "(" << type_to_string(types[i]) << "), ";
        }
        ss << paths[i] << "(" << type_to_string(types[i]) << ")";
        ss << (has_remain ? "]" : "}");
        return ss.str();
    }

    static std::pair<std::string_view, std::string_view> split_path(const std::string_view& path);
};

} // namespace starrocks
