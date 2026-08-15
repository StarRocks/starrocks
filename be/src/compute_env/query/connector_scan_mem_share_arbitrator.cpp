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
          total_chunk_source_mem_bytes(connector_scan_node_number * default_data_source_mem_bytes) {}

int64_t ConnectorScanOperatorMemShareArbitrator::update_chunk_source_mem_bytes(int64_t old_value, int64_t new_value) {
    int64_t diff = new_value - old_value;
    int64_t total = total_chunk_source_mem_bytes.fetch_add(diff) + diff;
    if (new_value == 0) return 0;
    if (total <= 0) return scan_mem_limit;
    return scan_mem_limit * (new_value * 1.0 / std::max(total, new_value));
}

} // namespace starrocks::pipeline
