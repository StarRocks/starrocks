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

#include "column/flat_json/json_flat_path.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace starrocks {

std::pair<std::string_view, std::string_view> JsonFlatPath::split_path(const std::string_view& path) {
    // A quoted level like "a.b" is a single key whose interior '.' is part of the name (the FE wraps a
    // key containing the '.' separator in quotes, see SubfieldAccessPathNormalizer.formatJsonPath);
    // resume the separator search after the closing quote, then strip the wrapping quotes so the level
    // key equals the raw vpack object key, which carries none.
    size_t scan = (!path.empty() && path.front() == '"') ? path.find('"', 1) : 0;
    size_t dot = path.find('.', scan);
    std::string_view key = path.substr(0, dot);
    std::string_view next = (dot == std::string_view::npos) ? std::string_view{} : path.substr(dot + 1);
    if (key.size() >= 2 && key.front() == '"' && key.back() == '"') {
        key = key.substr(1, key.size() - 2); // strip the wrapping quotes
    }
    return {key, next};
}

JsonFlatPath* JsonFlatPath::normalize_from_path(const std::string_view& path, JsonFlatPath* root) {
    if (path.empty()) {
        return root;
    }
    auto [key, next] = split_path(path);
    auto [iter, inserted] = root->children.try_emplace(key);
    if (inserted) {
        iter->second = std::make_unique<JsonFlatPath>();
    }
    return normalize_from_path(next, iter->second.get());
}

/*
* to mark new root
*              root(Ig)
*           /       |       \
*        a(Ex)     b(Ig)    c(Ex)
*        /         /    \      \   
*      any     b1(Ex) b2(N)     any
*                    /    \
*                b3(IN)   b4(IN)
*/
void JsonFlatPath::set_root(const std::string_view& new_root_path, JsonFlatPath* node) {
    node->op = OP_IGNORE;
    if (new_root_path.empty()) {
        node->op = OP_ROOT;
        return;
    }
    auto [key, next] = split_path(new_root_path);

    auto iter = node->children.begin();
    for (; iter != node->children.end(); iter++) {
        iter->second->op = OP_EXCLUDE;
        if (iter->first == key) {
            set_root(next, iter->second.get());
        }
    }
}

} // namespace starrocks
