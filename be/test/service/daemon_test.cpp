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

#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "base/utility/defer_op.h"
#include "runtime/process_memory_metrics.h"

namespace starrocks {

// Defined in service/daemon.cpp, which has no header for it because only the
// daemon threads there call it.
std::string dump_memory_tracker();

// This log line is what an operator reads first when a BE's RSS grows unexpectedly,
// and its field list is hardcoded rather than derived from the registered trackers,
// so a tracker that exists but is missing here stays invisible. Pin the whole list
// in emission order, and check that vector_index carries its own metric value.
TEST(DumpMemoryTrackerTest, DumpsEveryMemTracker) {
    auto* metrics = ProcessMemoryMetrics::instance();
    const int64_t saved = metrics->vector_index_mem_bytes.value();
    metrics->vector_index_mem_bytes.set_value(987654321);
    DeferOp restore([&]() { metrics->vector_index_mem_bytes.set_value(saved); });

    const std::string dump = dump_memory_tracker();

    EXPECT_EQ(0, dump.rfind("Current memory statistics:", 0));
    EXPECT_NE(std::string::npos, dump.find(" vector_index(987654321)")) << dump;

    std::istringstream expected_fields(
            "process query_pool load metadata compaction schema_change page_cache update passthrough clone "
            "consistency datacache jit brpc_iobuf replication vector_index jemalloc_active jemalloc_allocated "
            "jemalloc_metadata jemalloc_rss");
    size_t pos = 0;
    for (std::string field; expected_fields >> field;) {
        pos = dump.find(" " + field + "(", pos);
        ASSERT_NE(std::string::npos, pos) << field << " missing or out of order in: " << dump;
        pos += field.size();
    }
}

} // namespace starrocks
