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

#include "storage/flexible_partial_update.h"

#include <algorithm>

namespace starrocks {

std::string ColumnSetDict::canonical_key(const std::vector<std::string>& sorted_names) {
    // sorted_names is already sorted/uniqued; join with '\0' (cannot appear in a name).
    std::string key;
    size_t total = 0;
    for (const auto& n : sorted_names) {
        total += n.size() + 1;
    }
    key.reserve(total);
    for (const auto& n : sorted_names) {
        key.append(n);
        key.push_back('\0');
    }
    return key;
}

ColumnSetId ColumnSetDict::intern(std::vector<std::string> names) {
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    std::string key = canonical_key(names);

    std::lock_guard<std::mutex> l(_mu);
    auto it = _index.find(key);
    if (it != _index.end()) {
        return it->second;
    }
    if (_sets.size() >= kMaxColumnSets) {
        // Out of set-id space. Caller should treat this as "fall back to the union" --
        // i.e. degrade this row to the homogeneous union partial update (set-id 0 is the
        // first interned set; we cannot mint a new one). Returning the invalid id lets
        // the scanner map it to the union without corrupting the dictionary.
        return kInvalidColumnSetId;
    }
    auto id = static_cast<ColumnSetId>(_sets.size());
    _sets.emplace_back(std::move(names));
    _index.emplace(std::move(key), id);
    return id;
}

void ColumnSetDict::populate_from_snapshot(const std::vector<std::vector<std::string>>& sets) {
    std::lock_guard<std::mutex> l(_mu);
    if (!_sets.empty()) {
        // Already populated (the coordinator broadcasts the same snapshot on every sender's eos;
        // any one of them may arrive first). Idempotent no-op.
        return;
    }
    _sets.reserve(sets.size());
    for (auto names : sets) {
        // Canonicalize identically to intern() so the canonical_key index resolves, and so the
        // set-id order matches the coordinator (which interned canonicalized sets).
        std::sort(names.begin(), names.end());
        names.erase(std::unique(names.begin(), names.end()), names.end());
        std::string key = canonical_key(names);
        auto id = static_cast<ColumnSetId>(_sets.size());
        _index.emplace(std::move(key), id);
        _sets.emplace_back(std::move(names));
    }
}

FlexiblePartialUpdateRegistry* FlexiblePartialUpdateRegistry::instance() {
    static FlexiblePartialUpdateRegistry s_instance;
    return &s_instance;
}

ColumnSetDictPtr FlexiblePartialUpdateRegistry::get_or_create(int64_t txn_id) {
    std::lock_guard<std::mutex> l(_mu);
    auto it = _by_txn.find(txn_id);
    if (it != _by_txn.end()) {
        return it->second;
    }
    auto dict = std::make_shared<ColumnSetDict>();
    _by_txn.emplace(txn_id, dict);
    return dict;
}

ColumnSetDictPtr FlexiblePartialUpdateRegistry::get(int64_t txn_id) {
    std::lock_guard<std::mutex> l(_mu);
    auto it = _by_txn.find(txn_id);
    return it != _by_txn.end() ? it->second : nullptr;
}

void FlexiblePartialUpdateRegistry::erase(int64_t txn_id) {
    std::lock_guard<std::mutex> l(_mu);
    _by_txn.erase(txn_id);
}

} // namespace starrocks
