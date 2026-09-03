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

#include "compute_env/query/connector_scan_mem_share_arbitrator.h"

namespace starrocks::pipeline {

ConnectorScanOperatorMemShareArbitrator::ConnectorScanOperatorMemShareArbitrator(int64_t query_mem_limit,
                                                                                 int connector_scan_node_number,
                                                                                 int64_t default_data_source_mem_bytes)
        : query_mem_limit(query_mem_limit),
          scan_mem_limit(query_mem_limit),
          active_scan_node_number(std::max(1, connector_scan_node_number)),
          total_chunk_source_mem_bytes(connector_scan_node_number * default_data_source_mem_bytes) {}

int64_t ConnectorScanOperatorMemShareArbitrator::update_chunk_source_mem_bytes(int64_t old_value, int64_t new_value) {
    int64_t diff = new_value - old_value;
    int64_t total = total_chunk_source_mem_bytes.fetch_add(diff) + diff;
    if (new_value == 0) {
        // This scan node has finished and handed its share back, so stop reserving a floor for it.
        // Reached once per scan node, from the last of its scan operators to close.
        active_scan_node_number.fetch_sub(1, std::memory_order_relaxed);
        return 0;
    }
    if (total <= 0) return scan_mem_limit;

    // Reserve an equal floor for every connector scan node first, then split what is left in
    // proportion to the reported per-chunk-source cost. The total handed out is unchanged:
    //   sum_i(floor + remaining * v_i / total) == floor * N + remaining == scan_mem_limit
    // With a single scan node, or with nodes reporting equal cost, this returns exactly the same
    // share as the proportional-only split, so the common plans are unaffected. It only differs
    // when the reported costs are skewed, which is the case the proportional split handles badly.
    const int64_t active_nodes = std::max(1, active_scan_node_number.load(std::memory_order_relaxed));
    const int64_t floor_per_node = static_cast<int64_t>(scan_mem_limit * kNodeFloorRatio) / active_nodes;
    const int64_t remaining = scan_mem_limit - floor_per_node * active_nodes;
    return floor_per_node + remaining * (new_value * 1.0 / std::max(total, new_value));
}

} // namespace starrocks::pipeline
