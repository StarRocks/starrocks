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

#include <algorithm>
#include <cstddef>
#include <numeric>
#include <vector>

namespace starrocks::lake {

// Weighted max-min allocation: redistribute shares that exceed a tablet's full SST demand.
// Fixed bytes (memtables, delete vectors and metadata) must be subtracted before calling this.
struct PkIndexCacheShare {
    size_t demand = 0;
    double frequency = 1;
    size_t target = 0;
};

inline void allocate_pk_index_cache_shares(size_t budget, std::vector<PkIndexCacheShare>* shares) {
    std::vector<size_t> order(shares->size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return (*shares)[a].demand / (*shares)[a].frequency < (*shares)[b].demand / (*shares)[b].frequency;
    });
    double total_frequency = 0;
    for (auto& share : *shares) {
        total_frequency += share.frequency;
        share.target = 0;
    }
    for (size_t i : order) {
        auto& share = (*shares)[i];
        const auto target = std::min(
                budget, static_cast<size_t>(static_cast<long double>(budget) * share.frequency / total_frequency));
        share.target = std::min(share.demand, target);
        budget -= share.target;
        total_frequency -= share.frequency;
    }
}

} // namespace starrocks::lake
