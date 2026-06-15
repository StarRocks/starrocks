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

#include <chrono>
#include <memory>
#include <random>

#include "base/testutil/assert.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/query_context_manager.h"
#include "gtest/gtest.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_filter_query_lifecycle.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {

TEST(QueryContextManagerTest, QueryCancelReleasesRunningQueryTokenOnce) {
    auto query_ctx = QueryContext::create();
    auto workgroup = std::make_shared<workgroup::WorkGroup>("wg", 1, 0, 1, -1, 0, 1.0, TWorkGroupType::WG_NORMAL, "");
    ASSIGN_OR_ASSERT_FAIL(auto token, workgroup->acquire_running_query_token(true));
    query_ctx->_wg_running_query_token_ptr = std::move(token);
    query_ctx->_wg_running_query_token_atomic_ptr = query_ctx->_wg_running_query_token_ptr.get();
    ASSERT_EQ(1, workgroup->num_running_queries());

    query_ctx->cancel(Status::Cancelled("cancelled by fe"), true);

    EXPECT_TRUE(query_ctx->is_cancelled());
    EXPECT_TRUE(query_ctx->is_cancelled_by_fe());
    EXPECT_EQ(nullptr, query_ctx->_wg_running_query_token_ptr);
    EXPECT_EQ(nullptr, query_ctx->_wg_running_query_token_atomic_ptr.load());
    EXPECT_EQ(0, workgroup->num_running_queries());

    query_ctx->cancel(Status::Cancelled("duplicate cancel"), true);
    EXPECT_EQ(0, workgroup->num_running_queries());
}

TEST(QueryContextManagerTest, testSingleThreadOperations) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    {
        auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
        ASSERT_TRUE(query_ctx_mgr->init().ok());
        for (int i = 0; i < 100; ++i) {
            TUniqueId query_id;
            query_id.hi = 100;
            query_id.lo = i;
            ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
            ASSERT_TRUE(query_ctx != nullptr);
            query_ctx->query_runtime_state().set_delivery_expire_seconds(60);
            query_ctx->query_runtime_state().set_query_expire_seconds(300);
            query_ctx->set_total_fragments(1);
            query_ctx->query_runtime_state().extend_delivery_lifetime();
            query_ctx->query_runtime_state().extend_query_lifetime();
            query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_EQ(query_ctx->query_id(), query_id);
            ASSERT_FALSE(query_ctx->query_runtime_state().is_delivery_expired());
            ASSERT_FALSE(query_ctx->query_runtime_state().is_query_expired());
            ASSERT_FALSE(query_ctx->has_no_active_instances());
            ASSERT_FALSE(query_ctx->is_dead());
        }
        for (int i = 1; i < 10; ++i) {
            TUniqueId query_id;
            query_id.hi = 100;
            query_id.lo = i;
            auto query_ctx = query_ctx_mgr->get(query_id);
            ASSERT_TRUE(query_ctx);
            ASSERT_EQ(query_ctx->query_id(), query_id);
            ASSERT_FALSE(query_ctx->query_runtime_state().is_delivery_expired());
            ASSERT_FALSE(query_ctx->query_runtime_state().is_query_expired());
            ASSERT_FALSE(query_ctx->has_no_active_instances());
            ASSERT_FALSE(query_ctx->is_dead());
            query_ctx->count_down_fragment();
            ASSERT_TRUE(query_ctx->has_no_active_instances());
            ASSERT_TRUE(query_ctx->is_dead());
            query_ctx_mgr->remove(query_id);
            query_ctx = query_ctx_mgr->get(query_id);
            ASSERT_FALSE(query_ctx);
        }
    }

    {
        auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
        ASSERT_TRUE(query_ctx_mgr->init().ok());
        TUniqueId query_id;
        query_id.hi = 100;
        query_id.lo = 1;
        ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
        query_ctx->set_total_fragments(8);
        query_ctx->query_runtime_state().set_delivery_expire_seconds(60);
        query_ctx->query_runtime_state().set_query_expire_seconds(300);
        query_ctx->query_runtime_state().extend_delivery_lifetime();
        query_ctx->query_runtime_state().extend_query_lifetime();
        query_ctx->count_down_fragment();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

        for (int i = 0; i < 7; ++i) {
            ASSIGN_OR_ASSERT_FAIL(auto* tmp_query_ctx, query_ctx_mgr->get_or_register(query_id));
            tmp_query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_TRUE(tmp_query_ctx != nullptr);
        }
        auto retained_query_ctx = query_ctx_mgr->get(query_id);
        ASSERT_TRUE(retained_query_ctx != nullptr);
        for (int i = 0; i < 7; ++i) {
            query_ctx->count_down_fragment();
        }
        ASSERT_TRUE(query_ctx->has_no_active_instances());
        ASSERT_TRUE(query_ctx->is_dead());
        query_ctx_mgr->remove(query_id);
        ASSERT_TRUE(query_ctx_mgr->get(query_id) == nullptr);
    }

    {
        auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
        ASSERT_TRUE(query_ctx_mgr->init().ok());
        TUniqueId query_id;
        query_id.hi = 100;
        query_id.lo = 2;
        ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
        query_ctx->set_total_fragments(8);
        query_ctx->query_runtime_state().set_delivery_expire_seconds(60);
        query_ctx->query_runtime_state().set_query_expire_seconds(300);
        query_ctx->query_runtime_state().extend_delivery_lifetime();
        query_ctx->query_runtime_state().extend_query_lifetime();
        query_ctx->count_down_fragment();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

        for (int i = 0; i < 3; ++i) {
            ASSIGN_OR_ASSERT_FAIL(auto* tmp_query_ctx, query_ctx_mgr->get_or_register(query_id));
            tmp_query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_TRUE(tmp_query_ctx != nullptr);
        }
        for (int i = 0; i < 3; ++i) {
            query_ctx->count_down_fragment();
        }
        ASSERT_TRUE(query_ctx->has_no_active_instances());
        ASSERT_FALSE(query_ctx->is_dead());
        query_ctx_mgr->remove(query_id);
        ASSERT_TRUE(query_ctx_mgr->get(query_id) != nullptr);
        for (int i = 0; i < 3; ++i) {
            ASSIGN_OR_ASSERT_FAIL(auto* tmp_query_ctx, query_ctx_mgr->get_or_register(query_id));
            tmp_query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_TRUE(query_ctx != nullptr);
            tmp_query_ctx->count_down_fragment();
            query_ctx_mgr->remove(query_id);
            ASSERT_TRUE(tmp_query_ctx->has_no_active_instances());
            ASSERT_FALSE(tmp_query_ctx->is_dead());
            ASSERT_TRUE(query_ctx_mgr->get(query_id) != nullptr);
        }
        ASSIGN_OR_ASSERT_FAIL(query_ctx, query_ctx_mgr->get_or_register(query_id));
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
        auto retained_query_ctx = query_ctx_mgr->get(query_id);
        ASSERT_TRUE(retained_query_ctx != nullptr);
        query_ctx->count_down_fragment();
        ASSERT_TRUE(retained_query_ctx->is_dead());
        query_ctx_mgr->remove(query_id);
        ASSERT_TRUE(query_ctx_mgr->get(query_id) == nullptr);
    }

    {
        auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
        ASSERT_TRUE(query_ctx_mgr->init().ok());
        TUniqueId query_id;
        query_id.hi = 100;
        query_id.lo = 3;
        ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
        query_ctx->set_total_fragments(8);
        query_ctx->query_runtime_state().set_delivery_expire_seconds(60);
        query_ctx->query_runtime_state().set_query_expire_seconds(300);
        query_ctx->query_runtime_state().extend_delivery_lifetime();
        query_ctx->query_runtime_state().extend_query_lifetime();
        query_ctx->count_down_fragment();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
        for (int i = 0; i < 3; ++i) {
            ASSIGN_OR_ASSERT_FAIL(auto* tmp_query_ctx, query_ctx_mgr->get_or_register(query_id));
            tmp_query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_TRUE(tmp_query_ctx != nullptr);
        }
        for (int i = 0; i < 3; ++i) {
            query_ctx->count_down_fragment();
        }
        ASSERT_TRUE(query_ctx->has_no_active_instances());
        ASSERT_FALSE(query_ctx->is_dead());
        query_ctx_mgr->remove(query_id);
        ASSERT_TRUE(query_ctx_mgr->get(query_id) != nullptr);
        query_ctx->query_runtime_state().set_delivery_expire_seconds(1);
        query_ctx->query_runtime_state().extend_delivery_lifetime();
        query_ctx->query_runtime_state().extend_query_lifetime();
        sleep(2);
        ASSERT_TRUE(query_ctx_mgr->get(query_id) == nullptr);
    }

    {
        auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
        ASSERT_TRUE(query_ctx_mgr->init().ok());
        TUniqueId query_id;
        query_id.hi = 100;
        query_id.lo = 3;
        ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
        query_ctx->set_total_fragments(8);
        query_ctx->query_runtime_state().set_delivery_expire_seconds(60);
        query_ctx->query_runtime_state().set_query_expire_seconds(300);
        query_ctx->query_runtime_state().extend_delivery_lifetime();
        query_ctx->query_runtime_state().extend_query_lifetime();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
        // port query_ctx to second map
        query_ctx->count_down_fragment();

        // Single thread cannot reproduce query context registration success,
        // while this query context is in the second case. So let's simulate it here.
        query_ctx->increment_num_fragments();

        // cancel
        query_ctx->cancel(Status::Cancelled("cannelled"), true);

        ASSERT_TRUE(query_ctx_mgr->get_or_register(query_id).status().is_cancelled());

        ASSERT_TRUE(query_ctx_mgr->get(query_id) != nullptr);
    }
}

TEST(QueryContextManagerTest, testMulitiThreadOperations) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
    ASSERT_TRUE(query_ctx_mgr->init().ok());
    TUniqueId query_id;
    query_id.lo = 100;
    query_id.hi = 2;
    ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
    query_ctx->set_total_fragments(202);
    query_ctx->query_runtime_state().set_delivery_expire_seconds(60);
    query_ctx->query_runtime_state().set_query_expire_seconds(300);
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
    query_ctx->query_runtime_state().extend_delivery_lifetime();
    query_ctx->query_runtime_state().extend_query_lifetime();
    query_ctx->count_down_fragment();
    query_ctx_mgr->remove(query_id);
    ASSERT_TRUE(query_ctx->has_no_active_instances());
    ASSERT_FALSE(query_ctx->is_dead());
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&query_ctx_mgr, &query_id, &parent_mem_tracker]() {
            std::random_device rd;
            std::uniform_int_distribution<int> dist(1, 100);
            for (int k = 0; k < 20; ++k) {
                ASSIGN_OR_ASSERT_FAIL(auto* query_ctx, query_ctx_mgr->get_or_register(query_id));
                query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
                ASSERT_TRUE(query_ctx != nullptr);
                ASSERT_FALSE(query_ctx->query_runtime_state().is_delivery_expired());
                ASSERT_FALSE(query_ctx->query_runtime_state().is_query_expired());
                ASSERT_FALSE(query_ctx->is_dead());
                std::this_thread::sleep_for(std::chrono::milliseconds(dist(rd)));
                query_ctx->count_down_fragment();
                query_ctx_mgr->remove(query_id);
            }
        });
    }
    for (int i = 0; i < 10; ++i) {
        threads[i].join();
    }

    ASSIGN_OR_ASSERT_FAIL(query_ctx, query_ctx_mgr->get_or_register(query_id));
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
    auto retained_query_ctx = query_ctx_mgr->get(query_id);
    ASSERT_TRUE(retained_query_ctx != nullptr);
    query_ctx->count_down_fragment();
    ASSERT_TRUE(retained_query_ctx->is_dead());
    query_ctx_mgr->remove(query_id);
    ASSERT_TRUE(query_ctx_mgr->get(query_id) == nullptr);
}

QueryContext* gen_query_ctx(MemTracker* parent_mem_tracker, QueryContextManager* query_ctx_mgr, int64_t query_id_hi,
                            int64_t query_id_lo, size_t total_fragments, size_t delivery_expire_seconds,
                            size_t query_expire_seconds) {
    TUniqueId query_id;
    query_id.hi = query_id_hi;
    query_id.lo = query_id_lo;

    auto res = query_ctx_mgr->get_or_register(query_id);
    if (!res.ok()) {
        return nullptr;
    }
    auto* query_ctx = res.value();
    query_ctx->set_total_fragments(total_fragments);
    query_ctx->query_runtime_state().set_delivery_expire_seconds(delivery_expire_seconds);
    query_ctx->query_runtime_state().set_query_expire_seconds(query_expire_seconds);
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker);
    query_ctx->query_runtime_state().extend_delivery_lifetime();
    query_ctx->query_runtime_state().extend_query_lifetime();
    query_ctx->count_down_fragment();

    return query_ctx;
}

TEST(QueryContextManagerTest, testSetWorkgroup) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
    ASSERT_TRUE(query_ctx_mgr->init().ok());

    workgroup::WorkGroupPtr wg = std::make_shared<workgroup::WorkGroup>(
            "wg1", 1, 1, 1, 1, 1 /* concurrency_limit */, 1.0 /* spill_mem_limit_threshold */,
            workgroup::WorkGroupType::WG_NORMAL, workgroup::WorkGroup::DEFAULT_MEM_POOL);

    auto* query_ctx1 = gen_query_ctx(parent_mem_tracker.get(), query_ctx_mgr.get(), 0, 1, 3, 60, 300);
    auto* query_ctx_overloaded = gen_query_ctx(parent_mem_tracker.get(), query_ctx_mgr.get(), 1, 2, 3, 60, 300);
    const auto query_id1 = query_ctx1->query_id();

    /// Case 1: When all the fragments have come and finished, wg.num_running_queries should become to zero.
    ASSERT_OK(query_ctx1->init_query_once(wg.get(), false));
    ASSERT_OK(query_ctx1->init_query_once(wg.get(), false)); // None-first invocations have no side-effects.
    ASSERT_ERROR(query_ctx_overloaded->init_query_once(wg.get(), false)); // Exceed concurrency_limit.
    ASSERT_EQ(1, wg->num_running_queries());
    ASSERT_EQ(1, wg->concurrency_overflow_count());
    // All the fragments comes.
    for (int i = 1; i < query_ctx1->total_fragments(); ++i) {
        ASSIGN_OR_ASSERT_FAIL(auto* cur_query_ctx, query_ctx_mgr->get_or_register(query_id1));
        ASSERT_EQ(query_ctx1, cur_query_ctx);
    }
    auto retained_query_ctx1 = query_ctx_mgr->get(query_id1);
    ASSERT_TRUE(retained_query_ctx1 != nullptr);
    while (!query_ctx1->has_no_active_instances()) {
        query_ctx1->count_down_fragment();
    }
    ASSERT_TRUE(retained_query_ctx1->is_dead()); // All the fragments have come and finished.
    retained_query_ctx1.reset();
    ASSERT_TRUE(query_ctx_mgr->get(query_id1) == nullptr);
    ASSERT_EQ(0, wg->num_running_queries());

    /// Case 2: When some fragments don't come but delivery timeout has expired,
    /// wg.num_running_queries should also become to zero.
    auto* query_ctx2 =
            gen_query_ctx(parent_mem_tracker.get(), query_ctx_mgr.get(), 3, 4, 3, 0 /* delivery_timeout */, 300);
    const auto query_id2 = query_ctx2->query_id();
    ASSERT_OK(query_ctx2->init_query_once(wg.get(), false));
    ASSERT_OK(query_ctx2->init_query_once(wg.get(), false)); // None-first invocations have no side-effects.
    ASSERT_EQ(1, wg->num_running_queries());
    for (int i = 2; i < query_ctx2->total_fragments(); ++i) {
        ASSIGN_OR_ASSERT_FAIL(auto* cur_query_ctx, query_ctx_mgr->get_or_register(query_id2));
        ASSERT_EQ(query_ctx2, cur_query_ctx);
    }
    while (!query_ctx2->has_no_active_instances()) {
        query_ctx2->count_down_fragment();
    }
    ASSERT_FALSE(query_ctx2->is_dead());
    query_ctx_mgr->remove(query_id2);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_FALSE(query_ctx_mgr->remove(query_id2)); // Trigger _clean_slot.
    ASSERT_EQ(0, wg->num_running_queries());
}

TEST(QueryContextManagerTest, testReadStats) {
    QueryContext ctx;
    ctx.incr_read_stats(100, 200);
    ASSERT_EQ(100, ctx.get_read_local_cnt());
    ASSERT_EQ(200, ctx.get_read_remote_cnt());
}

class MockRuntimeFilterQueryLifecycle final : public RuntimeFilterQueryLifecycle {
public:
    void close_query(const TUniqueId& query_id) override {
        ++num_closed_queries;
        last_query_id = query_id;
    }

    int num_closed_queries = 0;
    TUniqueId last_query_id;
};

TEST(QueryContextManagerTest, testRuntimeFilterCoordinatorClosedOnQueryContextDestruction) {
    RuntimeFilterCache cache(1);
    MockRuntimeFilterQueryLifecycle lifecycle;
    RuntimeServices runtime_services;
    runtime_services.runtime_filter_query_lifecycle = &lifecycle;
    runtime_services.runtime_filter_cache = &cache;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    TUniqueId query_id;
    query_id.hi = 1000;
    query_id.lo = 2000;
    {
        auto query_ctx = QueryContext::create();
        query_ctx->set_query_id(query_id);
        query_ctx->set_query_execution_services(&query_execution_services);
        query_ctx->set_is_runtime_filter_coordinator(true);
    }

    ASSERT_EQ(1, lifecycle.num_closed_queries);
    ASSERT_EQ(query_id, lifecycle.last_query_id);
}

TEST(QueryContextManagerTest, testNonRuntimeFilterCoordinatorDoesNotCloseRuntimeFilterQuery) {
    RuntimeFilterCache cache(1);
    MockRuntimeFilterQueryLifecycle lifecycle;
    RuntimeServices runtime_services;
    runtime_services.runtime_filter_query_lifecycle = &lifecycle;
    runtime_services.runtime_filter_cache = &cache;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    TUniqueId query_id;
    query_id.hi = 3000;
    query_id.lo = 4000;
    {
        auto query_ctx = QueryContext::create();
        query_ctx->set_query_id(query_id);
        query_ctx->set_query_execution_services(&query_execution_services);
    }

    ASSERT_EQ(0, lifecycle.num_closed_queries);
}

TEST(QueryContextManagerTest, testRuntimeFilterCoordinatorToleratesMissingLifecycle) {
    RuntimeFilterCache cache(1);
    RuntimeServices runtime_services;
    runtime_services.runtime_filter_cache = &cache;
    QueryExecutionServices query_execution_services;
    query_execution_services.runtime = &runtime_services;

    TUniqueId query_id;
    query_id.hi = 5000;
    query_id.lo = 6000;
    {
        auto query_ctx = QueryContext::create();
        query_ctx->set_query_id(query_id);
        query_ctx->set_query_execution_services(&query_execution_services);
        query_ctx->set_is_runtime_filter_coordinator(true);
    }
}

class MockQueryLifecycle final : public QueryLifecycle {
public:
    void on_query_releasable(const TUniqueId& query_id) override {
        ++num_releasable_queries;
        last_query_id = query_id;
    }

    int num_releasable_queries = 0;
    TUniqueId last_query_id;
};

TEST(QueryContextManagerTest, testQueryLifecycleNotifiedOnlyWhenLastFragmentFinishes) {
    MockQueryLifecycle lifecycle;
    QueryContext ctx;
    TUniqueId query_id;
    query_id.hi = 10;
    query_id.lo = 20;
    ctx.set_query_id(query_id);
    ctx.set_query_lifecycle(&lifecycle);
    ctx.increment_num_fragments();
    ctx.increment_num_fragments();

    ctx.count_down_fragment();
    ASSERT_EQ(1, ctx.num_active_fragments());
    ASSERT_EQ(0, lifecycle.num_releasable_queries);

    ctx.count_down_fragment();
    ASSERT_EQ(0, ctx.num_active_fragments());
    ASSERT_EQ(1, lifecycle.num_releasable_queries);
    ASSERT_EQ(query_id, lifecycle.last_query_id);
}

class DroppingQueryLifecycle final : public QueryLifecycle {
public:
    explicit DroppingQueryLifecycle(QueryContextPtr* query_ctx) : _query_ctx(query_ctx) {}

    void on_query_releasable(const TUniqueId& query_id) override {
        last_query_id = query_id;
        _query_ctx->reset();
        query_ctx_alive_during_callback = !weak_query_ctx.expired();
    }

    QueryContextPtr* _query_ctx;
    std::weak_ptr<QueryContext> weak_query_ctx;
    bool query_ctx_alive_during_callback = false;
    TUniqueId last_query_id;
};

TEST(QueryContextManagerTest, testCountDownFragmentKeepsSelfAliveDuringLifecycleCallback) {
    auto query_ctx = QueryContext::create();
    auto* raw_query_ctx = query_ctx.get();
    TUniqueId query_id;
    query_id.hi = 30;
    query_id.lo = 40;
    query_ctx->set_query_id(query_id);
    query_ctx->increment_num_fragments();

    DroppingQueryLifecycle lifecycle(&query_ctx);
    lifecycle.weak_query_ctx = query_ctx;
    query_ctx->set_query_lifecycle(&lifecycle);

    raw_query_ctx->count_down_fragment();

    ASSERT_EQ(query_id, lifecycle.last_query_id);
    ASSERT_TRUE(lifecycle.query_ctx_alive_during_callback);
    ASSERT_EQ(nullptr, query_ctx);
    ASSERT_TRUE(lifecycle.weak_query_ctx.expired());
}

TEST(QueryContextManagerTest, testQueryStatisticsUsesQueryRuntimeStateExecStats) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    QueryContext ctx;
    TUniqueId query_id;
    query_id.hi = 3;
    query_id.lo = 4;
    ctx.set_query_id(query_id);
    ctx.init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
    ctx.query_runtime_state().init_node_exec_stats({10});
    ctx.query_runtime_state().update_push_rows_stats(10, 7);
    ctx.query_runtime_state().update_pull_rows_stats(10, 5);
    ctx.query_runtime_state().update_pred_filter_stats(10, 3);
    ctx.query_runtime_state().update_index_filter_stats(10, 2);
    ctx.query_runtime_state().update_rf_filter_stats(10, 1);

    auto intermediate_stats = ctx.intermediate_query_statistic(0);
    ASSERT_NE(nullptr, intermediate_stats);
    PQueryStatistics intermediate_pb;
    intermediate_stats->to_pb(&intermediate_pb);
    ASSERT_EQ(1, intermediate_pb.node_exec_stats_items_size());
    const auto& intermediate_item = intermediate_pb.node_exec_stats_items(0);
    EXPECT_EQ(10, intermediate_item.node_id());
    EXPECT_EQ(7, intermediate_item.push_rows());
    EXPECT_EQ(5, intermediate_item.pull_rows());
    EXPECT_EQ(3, intermediate_item.pred_filter_rows());
    EXPECT_EQ(2, intermediate_item.index_filter_rows());
    EXPECT_EQ(1, intermediate_item.rf_filter_rows());

    auto snapshot_stats = ctx.snapshot_query_statistic();
    ASSERT_NE(nullptr, snapshot_stats);
    PQueryStatistics snapshot_pb;
    snapshot_stats->to_pb(&snapshot_pb);
    ASSERT_EQ(1, snapshot_pb.node_exec_stats_items_size());
    const auto& snapshot_item = snapshot_pb.node_exec_stats_items(0);
    EXPECT_EQ(10, snapshot_item.node_id());
    EXPECT_EQ(0, snapshot_item.push_rows());
    EXPECT_EQ(0, snapshot_item.pull_rows());
    EXPECT_EQ(0, snapshot_item.pred_filter_rows());
    EXPECT_EQ(0, snapshot_item.index_filter_rows());
    EXPECT_EQ(0, snapshot_item.rf_filter_rows());
}

TEST(QueryContextManagerTest, testQueryStatisticsUsesQueryRuntimeStateCpuAndScanStats) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    QueryContext ctx;
    ctx.init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

    auto& query_runtime_state = ctx.query_runtime_state();
    query_runtime_state.incr_cpu_cost(17);
    query_runtime_state.incr_cur_scan_rows_num(5);
    query_runtime_state.incr_cur_scan_bytes(7);
    query_runtime_state.update_scan_stats(100, 5, 7);
    query_runtime_state.incr_cur_scan_rows_num(3);
    query_runtime_state.incr_cur_scan_bytes(4);
    query_runtime_state.update_scan_stats(100, 3, 4);

    auto intermediate_stats = ctx.intermediate_query_statistic(0);
    ASSERT_NE(nullptr, intermediate_stats);
    PQueryStatistics intermediate_pb;
    intermediate_stats->to_pb(&intermediate_pb);
    EXPECT_EQ(17, intermediate_pb.cpu_cost_ns());
    EXPECT_EQ(8, intermediate_pb.scan_rows());
    EXPECT_EQ(11, intermediate_pb.scan_bytes());
    ASSERT_EQ(1, intermediate_pb.stats_items_size());
    EXPECT_EQ(100, intermediate_pb.stats_items(0).table_id());
    EXPECT_EQ(8, intermediate_pb.stats_items(0).scan_rows());
    EXPECT_EQ(11, intermediate_pb.stats_items(0).scan_bytes());

    auto second_intermediate_stats = ctx.intermediate_query_statistic(0);
    ASSERT_NE(nullptr, second_intermediate_stats);
    PQueryStatistics second_intermediate_pb;
    second_intermediate_stats->to_pb(&second_intermediate_pb);
    EXPECT_EQ(0, second_intermediate_pb.cpu_cost_ns());
    EXPECT_EQ(0, second_intermediate_pb.scan_rows());
    EXPECT_EQ(0, second_intermediate_pb.scan_bytes());
    EXPECT_EQ(0, second_intermediate_pb.stats_items_size());

    auto snapshot_stats = ctx.snapshot_query_statistic();
    ASSERT_NE(nullptr, snapshot_stats);
    PQueryStatistics snapshot_pb;
    snapshot_stats->to_pb(&snapshot_pb);
    EXPECT_EQ(17, snapshot_pb.cpu_cost_ns());
    EXPECT_EQ(8, snapshot_pb.scan_rows());
    EXPECT_EQ(11, snapshot_pb.scan_bytes());
    ASSERT_EQ(1, snapshot_pb.stats_items_size());
    EXPECT_EQ(100, snapshot_pb.stats_items(0).table_id());
    EXPECT_EQ(8, snapshot_pb.stats_items(0).scan_rows());
    EXPECT_EQ(11, snapshot_pb.stats_items(0).scan_bytes());
}

TEST(QueryContextManagerTest, testAttachRuntimeStateWiresQueryRuntimeState) {
    auto query_ctx = std::make_shared<QueryContext>();
    TUniqueId query_id;
    query_id.hi = 3;
    query_id.lo = 4;
    query_ctx->set_query_id(query_id);
    query_ctx->query_runtime_state().set_delivery_expire_seconds(60);
    query_ctx->query_runtime_state().set_query_expire_seconds(30);
    query_ctx->query_runtime_state().extend_delivery_lifetime();
    query_ctx->query_runtime_state().extend_query_lifetime();

    RuntimeState runtime_state;
    query_ctx->attach_to_runtime_state(&runtime_state);

    ASSERT_EQ(query_ctx.get(), runtime_state.query_ctx());
    ASSERT_EQ(&query_ctx->query_runtime_state(), runtime_state.query_runtime_state());
    EXPECT_EQ(query_ctx->object_pool(), runtime_state.global_obj_pool());
    EXPECT_EQ(3, runtime_state.query_runtime_state()->query_id().hi);
    EXPECT_EQ(4, runtime_state.query_runtime_state()->query_id().lo);
    EXPECT_FALSE(runtime_state.query_runtime_state()->is_delivery_expired());
    EXPECT_FALSE(runtime_state.query_runtime_state()->is_query_expired());
    EXPECT_EQ(30, runtime_state.query_runtime_state()->get_query_expire_seconds());
}

TEST(QueryContextManagerTest, testInitMemTrackerWiresQueryRuntimeStateMemTracker) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    QueryContext query_ctx;
    TUniqueId query_id;
    query_id.hi = 5;
    query_id.lo = 6;
    query_ctx.set_query_id(query_id);

    query_ctx.init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

    ASSERT_NE(nullptr, query_ctx.mem_tracker());
    EXPECT_EQ(query_ctx.mem_tracker().get(), query_ctx.query_runtime_state().query_mem_tracker());
}

} // namespace starrocks::pipeline
