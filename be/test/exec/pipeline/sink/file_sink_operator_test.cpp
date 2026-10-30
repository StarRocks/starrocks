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

#include "exec/pipeline/sink/file_sink_operator.h"

#include <memory>

#include "exec/runtime/query_context.h"
#include "gtest/gtest.h"
#include "runtime/mem_tracker.h"
#include "runtime/query_statistics.h"

namespace starrocks::pipeline {

// SELECT INTO OUTFILE reports the query statistics through the file sink. They must include what the
// upstream BEs sent through exchange, not only the local BE's counters.
TEST(FileSinkOperatorTest, query_statistic_merges_upstream) {
    auto parent = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    QueryContext ctx;
    ctx.init_mem_tracker(parent->limit(), parent.get());
    ctx.set_final_sink();

    auto& query_runtime_state = ctx.query_runtime_state();
    query_runtime_state.incr_cpu_cost(17);
    query_runtime_state.incr_cur_scan_rows_num(5);
    query_runtime_state.incr_cur_scan_bytes(7);
    query_runtime_state.update_scan_stats(100, 5, 7);

    PQueryStatistics upstream;
    upstream.set_cpu_cost_ns(100);
    upstream.set_scan_rows(20);
    upstream.set_scan_bytes(30);
    auto* item = upstream.add_stats_items();
    item->set_table_id(100);
    item->set_scan_rows(20);
    item->set_scan_bytes(30);
    ctx.maintained_query_recv()->insert(upstream, 0);

    PQueryStatistics result;
    build_file_sink_query_statistic(&ctx, 42)->to_pb(&result);
    EXPECT_EQ(117, result.cpu_cost_ns());
    EXPECT_EQ(25, result.scan_rows());
    EXPECT_EQ(37, result.scan_bytes());
    EXPECT_EQ(42, result.returned_rows());
    ASSERT_EQ(1, result.stats_items_size());
    EXPECT_EQ(25, result.stats_items(0).scan_rows());
    EXPECT_EQ(37, result.stats_items(0).scan_bytes());
}

} // namespace starrocks::pipeline
