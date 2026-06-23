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

#include "util/json_flat_path.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace starrocks {

std::pair<std::string_view, std::string_view> JsonFlatPath::split_path(const std::string_view& path) {
    size_t pos = 0;
    pos = path.find('.', pos);
    std::string_view key;
    std::string_view next;
    if (pos == std::string::npos) {
        key = path;
    } else {
        key = path.substr(0, pos);
        next = path.substr(pos + 1);
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
