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
#include <atomic>
#include <cstdint>

namespace starrocks::pipeline {

struct ConnectorScanOperatorMemShareArbitrator {
    static constexpr double kChunkBufferMemRatio = 0.5;
    static constexpr int64_t kDefaultDataSourceMemBytes = 64 * 1024 * 1024;
    // Fraction of `scan_mem_limit` handed out as equal per-node floors before the remainder is split
    // proportionally to the observed per-chunk-source cost. A purely proportional split gives the
    // node with the most expensive chunk sources the most memory, which lets it run even more of
    // them and report an even higher total; a cheap but hot node can be squeezed towards zero even
    // though it is the one that would have finished early and returned its share.
    static constexpr double kNodeFloorRatio = 0.5;

    int64_t query_mem_limit = 0;
    int64_t scan_mem_limit = 0;
    // Scan nodes that have not finished yet. The floors are divided among these rather than among
    // all the nodes in the plan, so a node that has run to completion stops holding a reservation
    // that the nodes still scanning could use.
    std::atomic<int> active_scan_node_number = 1;
    std::atomic<int64_t> total_chunk_source_mem_bytes = 0;

    ConnectorScanOperatorMemShareArbitrator(int64_t query_mem_limit, int connector_scan_node_number,
                                            int64_t default_data_source_mem_bytes = kDefaultDataSourceMemBytes);

    int64_t set_scan_mem_ratio(double mem_ratio) {
        scan_mem_limit = std::max<int64_t>(1, query_mem_limit * mem_ratio);
        return scan_mem_limit;
    }

    int64_t update_chunk_source_mem_bytes(int64_t old_value, int64_t new_value);
};

} // namespace starrocks::pipeline
