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

#include <chrono>
#include <thread>

#include "common/object_pool.h"
#include "exec/runtime/query_runtime_state.h"
#include "runtime/mem_tracker.h"
#include "runtime/query_statistics.h"

namespace starrocks::pipeline {

namespace {

const QueryStatisticsItemPB* find_scan_stats_item(const PQueryStatistics& statistics, int64_t table_id) {
    for (const auto& stats_item : statistics.stats_items()) {
        if (stats_item.table_id() == table_id) {
            return &stats_item;
        }
    }
    return nullptr;
}

} // namespace

TEST(QueryRuntimeStateTest, StoresQueryId) {
    QueryRuntimeState state;
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;

    state.set_query_id(query_id);

    EXPECT_EQ(1, state.query_id().hi);
    EXPECT_EQ(2, state.query_id().lo);
}

TEST(QueryRuntimeStateTest, StoresQueryMemTrackerReference) {
    QueryRuntimeState state;
    MemTracker tracker(-1);

    state.set_query_mem_tracker(&tracker);

    EXPECT_EQ(&tracker, state.query_mem_tracker());
    const auto& const_state = state;
    EXPECT_EQ(&tracker, const_state.query_mem_tracker());
}

TEST(QueryRuntimeStateTest, QueryScopedServicesDefaultToUnset) {
    QueryRuntimeState state;

    EXPECT_EQ(nullptr, state.object_pool());
    EXPECT_EQ(nullptr, state.connector_scan_mem_tracker());
    EXPECT_EQ(nullptr, state.connector_scan_operator_mem_share_arbitrator());
    EXPECT_EQ(nullptr, state.global_late_materialization_ctx_mgr());
    EXPECT_EQ(0, state.static_query_mem_limit());
}

TEST(QueryRuntimeStateTest, StoresQueryScopedServiceReferences) {
    QueryRuntimeState state;
    ObjectPool pool;
    MemTracker connector_scan_tracker(-1);
    // Opaque to ExecRuntime: only the pointer round-trip is observable here.
    auto* arbitrator = reinterpret_cast<ConnectorScanOperatorMemShareArbitrator*>(0x10);
    auto* glm_ctx_mgr = reinterpret_cast<GlobalLateMaterilizationContextMgr*>(0x20);

    state.set_object_pool(&pool);
    state.set_connector_scan_mem_tracker(&connector_scan_tracker);
    state.set_connector_scan_operator_mem_share_arbitrator(arbitrator);
    state.set_global_late_materialization_ctx_mgr(glm_ctx_mgr);
    state.set_static_query_mem_limit(1024);

    EXPECT_EQ(&pool, state.object_pool());
    EXPECT_EQ(&connector_scan_tracker, state.connector_scan_mem_tracker());
    EXPECT_EQ(arbitrator, state.connector_scan_operator_mem_share_arbitrator());
    EXPECT_EQ(glm_ctx_mgr, state.global_late_materialization_ctx_mgr());
    EXPECT_EQ(1024, state.static_query_mem_limit());
}

TEST(QueryRuntimeStateTest, TracksQueryAndDeliveryExpiry) {
    QueryRuntimeState state;

    EXPECT_EQ(QueryRuntimeState::DEFAULT_EXPIRE_SECONDS, state.get_query_expire_seconds());

    state.set_delivery_expire_seconds(1);
    state.set_query_expire_seconds(1);
    state.extend_delivery_lifetime();
    state.extend_query_lifetime();

    EXPECT_FALSE(state.is_delivery_expired());
    EXPECT_FALSE(state.is_query_expired());
}

TEST(QueryRuntimeStateTest, ExpiresDeliveryAndQueryIndependently) {
    QueryRuntimeState state;

    state.set_delivery_expire_seconds(0);
    state.set_query_expire_seconds(1);
    state.extend_delivery_lifetime();
    state.extend_query_lifetime();
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    EXPECT_TRUE(state.is_delivery_expired());
    EXPECT_FALSE(state.is_query_expired());

    state.set_delivery_expire_seconds(1);
    state.set_query_expire_seconds(0);
    state.extend_delivery_lifetime();
    state.extend_query_lifetime();
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    EXPECT_FALSE(state.is_delivery_expired());
    EXPECT_TRUE(state.is_query_expired());
}

TEST(QueryRuntimeStateTest, TracksRegisteredNodeExecStats) {
    QueryRuntimeState state;
    state.init_node_exec_stats({10, 20});

    EXPECT_TRUE(state.need_record_exec_stats(10));
    EXPECT_TRUE(state.need_record_exec_stats(20));
    EXPECT_FALSE(state.need_record_exec_stats(30));

    state.update_push_rows_stats(10, 7);
    state.update_pull_rows_stats(10, 5);
    state.update_pred_filter_stats(10, 3);
    state.update_index_filter_stats(10, 2);
    state.update_rf_filter_stats(10, 1);
    state.update_push_rows_stats(30, 100);

    const auto& stats = state.node_exec_stats();
    auto node10_stats = stats.find(10);
    ASSERT_NE(stats.end(), node10_stats);
    EXPECT_EQ(7, node10_stats->second->push_rows.load());
    EXPECT_EQ(5, node10_stats->second->pull_rows.load());
    EXPECT_EQ(3, node10_stats->second->pred_filter_rows.load());
    EXPECT_EQ(2, node10_stats->second->index_filter_rows.load());
    EXPECT_EQ(1, node10_stats->second->rf_filter_rows.load());
    EXPECT_EQ(stats.end(), stats.find(30));
}

TEST(QueryRuntimeStateTest, ExposesNodeExecStatsForDeltaConsumption) {
    QueryRuntimeState state;
    state.init_node_exec_stats({10});
    state.update_push_rows_stats(10, 7);
    state.update_pull_rows_stats(10, 5);
    state.update_pred_filter_stats(10, 3);
    state.update_index_filter_stats(10, 2);
    state.update_rf_filter_stats(10, 1);

    auto& stats = state.node_exec_stats();
    auto node10_stats = stats.find(10);
    ASSERT_NE(stats.end(), node10_stats);
    EXPECT_EQ(7, node10_stats->second->push_rows.exchange(0));
    EXPECT_EQ(5, node10_stats->second->pull_rows.exchange(0));
    EXPECT_EQ(3, node10_stats->second->pred_filter_rows.exchange(0));
    EXPECT_EQ(2, node10_stats->second->index_filter_rows.exchange(0));
    EXPECT_EQ(1, node10_stats->second->rf_filter_rows.exchange(0));

    EXPECT_EQ(0, node10_stats->second->push_rows.load());
    EXPECT_EQ(0, node10_stats->second->pull_rows.load());
    EXPECT_EQ(0, node10_stats->second->pred_filter_rows.load());
    EXPECT_EQ(0, node10_stats->second->index_filter_rows.load());
    EXPECT_EQ(0, node10_stats->second->rf_filter_rows.load());
}

TEST(QueryRuntimeStateTest, AppliesOperatorExecStatsSnapshot) {
    QueryRuntimeState state;
    state.init_node_exec_stats({10, 20});

    OperatorExecStatsSnapshot snapshot;
    snapshot.require_registered_plan_node = true;
    snapshot.plan_node_id = 10;
    snapshot.update_push_rows = true;
    snapshot.push_rows = 7;
    snapshot.update_pull_rows = true;
    snapshot.pull_rows = 5;
    snapshot.update_pred_filter_rows = true;
    snapshot.pred_filter_rows = 3;
    snapshot.update_rf_filter_rows = true;
    snapshot.rf_filter_rows = 1;
    state.update_operator_exec_stats(snapshot);

    snapshot.plan_node_id = 30;
    snapshot.push_rows = 100;
    state.update_operator_exec_stats(snapshot);

    state.update_pull_rows_stats(20, 100);
    OperatorExecStatsSnapshot force_set_snapshot;
    force_set_snapshot.plan_node_id = 20;
    force_set_snapshot.update_pull_rows = true;
    force_set_snapshot.force_set_pull_rows = true;
    force_set_snapshot.pull_rows = 6;
    state.update_operator_exec_stats(force_set_snapshot);

    const auto& stats = state.node_exec_stats();
    auto node10_stats = stats.find(10);
    ASSERT_NE(stats.end(), node10_stats);
    EXPECT_EQ(7, node10_stats->second->push_rows.load());
    EXPECT_EQ(5, node10_stats->second->pull_rows.load());
    EXPECT_EQ(3, node10_stats->second->pred_filter_rows.load());
    EXPECT_EQ(0, node10_stats->second->index_filter_rows.load());
    EXPECT_EQ(1, node10_stats->second->rf_filter_rows.load());

    auto node20_stats = stats.find(20);
    ASSERT_NE(stats.end(), node20_stats);
    EXPECT_EQ(6, node20_stats->second->pull_rows.load());
    EXPECT_EQ(stats.end(), stats.find(30));
}

TEST(QueryRuntimeStateTest, TracksCpuAndScanStats) {
    QueryRuntimeState state;

    state.incr_cpu_cost(7);
    state.incr_cpu_cost(11);
    EXPECT_EQ(18, state.cpu_cost());
    EXPECT_EQ(18, state.consume_delta_cpu_cost());
    EXPECT_EQ(0, state.consume_delta_cpu_cost());
    EXPECT_EQ(18, state.cpu_cost());

    state.incr_cur_scan_rows_num(5);
    state.incr_cur_scan_bytes(7);
    state.update_scan_stats(100, 5, 7);
    state.incr_cur_scan_rows_num(3);
    state.incr_cur_scan_bytes(4);
    state.update_scan_stats(100, 3, 4);
    state.incr_cur_scan_rows_num(11);
    state.incr_cur_scan_bytes(13);
    state.update_scan_stats(200, 11, 13);

    EXPECT_EQ(19, state.cur_scan_rows_num());
    EXPECT_EQ(24, state.get_scan_bytes());

    QueryStatistics delta_statistics;
    state.consume_delta_scan_stats(&delta_statistics);
    PQueryStatistics delta_pb;
    delta_statistics.to_pb(&delta_pb);
    ASSERT_EQ(2, delta_pb.stats_items_size());
    EXPECT_EQ(19, delta_pb.scan_rows());
    EXPECT_EQ(24, delta_pb.scan_bytes());
    auto* table100_delta = find_scan_stats_item(delta_pb, 100);
    ASSERT_NE(nullptr, table100_delta);
    EXPECT_EQ(8, table100_delta->scan_rows());
    EXPECT_EQ(11, table100_delta->scan_bytes());
    auto* table200_delta = find_scan_stats_item(delta_pb, 200);
    ASSERT_NE(nullptr, table200_delta);
    EXPECT_EQ(11, table200_delta->scan_rows());
    EXPECT_EQ(13, table200_delta->scan_bytes());

    QueryStatistics second_delta_statistics;
    state.consume_delta_scan_stats(&second_delta_statistics);
    PQueryStatistics second_delta_pb;
    second_delta_statistics.to_pb(&second_delta_pb);
    EXPECT_EQ(0, second_delta_pb.scan_rows());
    EXPECT_EQ(0, second_delta_pb.scan_bytes());
    EXPECT_EQ(0, second_delta_pb.stats_items_size());

    QueryStatistics total_statistics;
    state.add_total_scan_stats(&total_statistics);
    PQueryStatistics total_pb;
    total_statistics.to_pb(&total_pb);
    ASSERT_EQ(2, total_pb.stats_items_size());
    EXPECT_EQ(19, total_pb.scan_rows());
    EXPECT_EQ(24, total_pb.scan_bytes());
    auto* table100_total = find_scan_stats_item(total_pb, 100);
    ASSERT_NE(nullptr, table100_total);
    EXPECT_EQ(8, table100_total->scan_rows());
    EXPECT_EQ(11, table100_total->scan_bytes());
    auto* table200_total = find_scan_stats_item(total_pb, 200);
    ASSERT_NE(nullptr, table200_total);
    EXPECT_EQ(11, table200_total->scan_rows());
    EXPECT_EQ(13, table200_total->scan_bytes());
}

} // namespace starrocks::pipeline
