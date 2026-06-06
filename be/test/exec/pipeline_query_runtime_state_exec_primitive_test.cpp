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

#include "exec/pipeline/primitives/query_runtime_state.h"

namespace starrocks::pipeline {

TEST(QueryRuntimeStateTest, StoresQueryId) {
    QueryRuntimeState state;
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;

    state.set_query_id(query_id);

    EXPECT_EQ(1, state.query_id().hi);
    EXPECT_EQ(2, state.query_id().lo);
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

} // namespace starrocks::pipeline
