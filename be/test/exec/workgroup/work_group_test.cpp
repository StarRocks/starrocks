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

#include "exec/workgroup/work_group.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "gen_cpp/StatusCode_types.h"

namespace starrocks::workgroup {
namespace {

TWorkGroup make_big_query_workgroup(int64_t cpu_second_limit, int64_t scan_rows_limit) {
    TWorkGroup twg;
    twg.__set_id(101);
    twg.__set_name("big_query_wg");
    twg.__set_version(WorkGroup::DEFAULT_VERSION);
    if (cpu_second_limit > 0) {
        twg.__set_big_query_cpu_second_limit(cpu_second_limit);
    }
    if (scan_rows_limit > 0) {
        twg.__set_big_query_scan_rows_limit(scan_rows_limit);
    }
    return twg;
}

} // namespace

PARALLEL_TEST(WorkGroupTest, check_big_query_rejects_cpu_runtime_over_limit) {
    WorkGroup workgroup(make_big_query_workgroup(2, 0));

    WorkGroupQueryStats stats;
    stats.cpu_runtime_ns = 2'000'000'001L;

    Status status = workgroup.check_big_query(stats);
    EXPECT_EQ(TStatusCode::BIG_QUERY_CPU_SECOND_LIMIT_EXCEEDED, status.code());
    EXPECT_EQ(1, workgroup.bigquery_count());
}

PARALLEL_TEST(WorkGroupTest, check_big_query_uses_workgroup_scan_limit_when_query_limit_absent) {
    WorkGroup workgroup(make_big_query_workgroup(0, 100));

    WorkGroupQueryStats stats;
    stats.scan_rows = 101;

    Status status = workgroup.check_big_query(stats);
    EXPECT_EQ(TStatusCode::BIG_QUERY_SCAN_ROWS_LIMIT_EXCEEDED, status.code());
    EXPECT_EQ(1, workgroup.bigquery_count());
}

PARALLEL_TEST(WorkGroupTest, check_big_query_uses_query_scan_limit_when_present) {
    WorkGroup workgroup(make_big_query_workgroup(0, 100));

    WorkGroupQueryStats stats;
    stats.scan_rows = 150;
    stats.scan_rows_limit = 200;
    ASSERT_OK(workgroup.check_big_query(stats));
    EXPECT_EQ(0, workgroup.bigquery_count());

    stats.scan_rows = 201;
    Status status = workgroup.check_big_query(stats);
    EXPECT_EQ(TStatusCode::BIG_QUERY_SCAN_ROWS_LIMIT_EXCEEDED, status.code());
    EXPECT_EQ(1, workgroup.bigquery_count());
}

PARALLEL_TEST(WorkGroupTest, sched_entity_updates_workgroup_owned_sched_state) {
    WorkGroup workgroup("sched_wg", 201, WorkGroup::DEFAULT_VERSION, 2, 0.5, 10, 1.0, WorkGroupType::WG_NORMAL,
                        WorkGroup::DEFAULT_MEM_POOL);

    workgroup.driver_sched_entity()->incr_runtime_ns(1'000);

    EXPECT_EQ(500, workgroup.driver_sched_state().vruntime_ns());
    EXPECT_EQ(1'000, workgroup.driver_sched_state().runtime_ns(workgroup.cpu_weight()));
    EXPECT_EQ(1'000, workgroup.driver_sched_state().unadjusted_runtime_ns());
    EXPECT_EQ(workgroup.driver_sched_state().vruntime_ns(), workgroup.driver_sched_entity()->vruntime_ns());
    EXPECT_EQ(workgroup.driver_sched_state().unadjusted_runtime_ns(),
              workgroup.driver_sched_entity()->unadjusted_runtime_ns());
}

PARALLEL_TEST(WorkGroupTest, sched_states_are_independent) {
    WorkGroup workgroup("sched_wg", 202, WorkGroup::DEFAULT_VERSION, 1, 0.5, 10, 1.0, WorkGroupType::WG_NORMAL,
                        WorkGroup::DEFAULT_MEM_POOL);

    workgroup.driver_sched_entity()->incr_runtime_ns(100);
    workgroup.scan_sched_entity()->incr_runtime_ns(200);
    workgroup.connector_scan_sched_entity()->incr_runtime_ns(300);

    EXPECT_EQ(100, workgroup.driver_sched_state().vruntime_ns());
    EXPECT_EQ(200, workgroup.scan_sched_state().vruntime_ns());
    EXPECT_EQ(300, workgroup.connector_scan_sched_state().vruntime_ns());
    EXPECT_EQ(100, workgroup.driver_sched_state().unadjusted_runtime_ns());
    EXPECT_EQ(200, workgroup.scan_sched_state().unadjusted_runtime_ns());
    EXPECT_EQ(300, workgroup.connector_scan_sched_state().unadjusted_runtime_ns());
}

PARALLEL_TEST(WorkGroupTest, sched_entity_adjust_runtime_does_not_change_unadjusted_runtime) {
    WorkGroup workgroup("sched_wg", 203, WorkGroup::DEFAULT_VERSION, 4, 0.5, 10, 1.0, WorkGroupType::WG_NORMAL,
                        WorkGroup::DEFAULT_MEM_POOL);

    workgroup.driver_sched_entity()->incr_runtime_ns(800);
    workgroup.driver_sched_entity()->adjust_runtime_ns(400);

    EXPECT_EQ(300, workgroup.driver_sched_state().vruntime_ns());
    EXPECT_EQ(800, workgroup.driver_sched_state().unadjusted_runtime_ns());
}

PARALLEL_TEST(WorkGroupTest, sched_state_preserves_growth_runtime_marks) {
    WorkGroup workgroup("sched_wg", 204, WorkGroup::DEFAULT_VERSION, 1, 0.5, 10, 1.0, WorkGroupType::WG_NORMAL,
                        WorkGroup::DEFAULT_MEM_POOL);

    auto* sched_entity = workgroup.driver_sched_entity();
    sched_entity->incr_runtime_ns(100);
    sched_entity->mark_curr_runtime_ns();
    EXPECT_EQ(100, workgroup.driver_sched_state().growth_runtime_ns());

    sched_entity->mark_last_runtime_ns();
    EXPECT_EQ(0, workgroup.driver_sched_state().growth_runtime_ns());

    sched_entity->incr_runtime_ns(50);
    sched_entity->mark_curr_runtime_ns();
    EXPECT_EQ(50, workgroup.driver_sched_state().growth_runtime_ns());
}

} // namespace starrocks::workgroup
