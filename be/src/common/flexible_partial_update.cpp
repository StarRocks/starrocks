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

#include "common/flexible_partial_update.h"

#include <fmt/format.h>

#include <algorithm>

namespace starrocks {

std::string ColumnSetDict::canonical_key(const std::vector<std::string>& sorted_names) {
    // The names are sorted and unique; '\0' cannot appear in a column name.
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

void ColumnSetDict::canonicalize(std::vector<std::string>* names) {
    std::sort(names->begin(), names->end());
    names->erase(std::unique(names->begin(), names->end()), names->end());
}

ColumnSetId ColumnSetDict::intern(std::vector<std::string> names) {
    canonicalize(&names);
    std::string key = canonical_key(names);

    std::lock_guard<std::mutex> l(_mu);
    auto it = _index.find(key);
    if (it != _index.end()) {
        return it->second;
    }
    if (_sets.size() >= kMaxColumnSets) {
        return kInvalidColumnSetId;
    }
    auto id = static_cast<ColumnSetId>(_sets.size());
    _sets.emplace_back(std::move(names));
    _index.emplace(std::move(key), id);
    return id;
}

Status ColumnSetDict::merge_snapshot(const std::vector<std::vector<std::string>>& sets) {
    if (sets.size() > kMaxColumnSets) {
        return Status::InvalidArgument(fmt::format("column-set dictionary has {} entries, more than the limit {}",
                                                   sets.size(), kMaxColumnSets));
    }
    std::lock_guard<std::mutex> l(_mu);
    for (size_t i = 0; i < sets.size(); ++i) {
        auto names = sets[i];
        canonicalize(&names);
        if (i < _sets.size()) {
            if (_sets[i] != names) {
                return Status::InternalError(
                        fmt::format("flexible partial update: column-set dictionaries disagree on set-id {}", i));
            }
            continue;
        }
        std::string key = canonical_key(names);
        if (!_index.emplace(std::move(key), static_cast<ColumnSetId>(i)).second) {
            return Status::InternalError(
                    fmt::format("flexible partial update: column-set dictionary repeats a set at set-id {}", i));
        }
        _sets.emplace_back(std::move(names));
    }
    return Status::OK();
}

FlexiblePartialUpdateRegistry* FlexiblePartialUpdateRegistry::instance() {
    static FlexiblePartialUpdateRegistry s_instance;
    return &s_instance;
}

ColumnSetDictPtr FlexiblePartialUpdateRegistry::retain(int64_t txn_id) {
    std::lock_guard<std::mutex> l(_mu);
    auto& entry = _by_txn[txn_id];
    if (entry.dict == nullptr) {
        entry.dict = std::make_shared<ColumnSetDict>();
    }
    ++entry.refs;
    return entry.dict;
}

void FlexiblePartialUpdateRegistry::release(int64_t txn_id) {
    std::lock_guard<std::mutex> l(_mu);
    auto it = _by_txn.find(txn_id);
    if (it == _by_txn.end()) {
        return;
    }
    if (it->second.refs > 0) {
        --it->second.refs;
    }
    if (it->second.refs == 0) {
        _by_txn.erase(it);
    }
}

ColumnSetDictPtr FlexiblePartialUpdateRegistry::get_or_create(int64_t txn_id) {
    std::lock_guard<std::mutex> l(_mu);
    auto& entry = _by_txn[txn_id];
    if (entry.dict == nullptr) {
        entry.dict = std::make_shared<ColumnSetDict>();
    }
    return entry.dict;
}

ColumnSetDictPtr FlexiblePartialUpdateRegistry::get(int64_t txn_id) {
    std::lock_guard<std::mutex> l(_mu);
    auto it = _by_txn.find(txn_id);
    return it != _by_txn.end() ? it->second.dict : nullptr;
}

void FlexiblePartialUpdateRegistry::erase(int64_t txn_id) {
    std::lock_guard<std::mutex> l(_mu);
    _by_txn.erase(txn_id);
}

} // namespace starrocks
