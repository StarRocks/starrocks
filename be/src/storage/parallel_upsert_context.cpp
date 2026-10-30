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

#include "storage/parallel_upsert_context.h"

#include "storage/primary_index.h"

namespace starrocks {

void ParallelUpsertContext::add_replaced(const std::vector<uint64_t>& old_values) {
    std::lock_guard<std::mutex> l(_mutex);
    for (uint64_t old : old_values) {
        if (old != NullIndexValue) {
            (*_deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
        }
    }
}

void ParallelUpsertContext::add_replaced(const IndexValue* old_values, size_t n) {
    std::lock_guard<std::mutex> l(_mutex);
    for (size_t i = 0; i < n; ++i) {
        const uint64_t old = old_values[i].get_value();
        if (old != NullIndexValue) {
            (*_deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
        }
    }
}

void ParallelUpsertContext::add_delete(uint32_t rssid, uint32_t rowid) {
    std::lock_guard<std::mutex> l(_mutex);
    (*_deletes)[rssid].push_back(rowid);
}

} // namespace starrocks
