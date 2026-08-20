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

#include "exec/pipeline/query_context.h"
#include "exec/workgroup/work_group.h"
#include "gtest/gtest.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

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
            query_ctx->set_delivery_expire_seconds(60);
            query_ctx->set_query_expire_seconds(300);
            query_ctx->set_total_fragments(1);
            query_ctx->extend_delivery_lifetime();
            query_ctx->extend_query_lifetime();
            query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_EQ(query_ctx->query_id(), query_id);
            ASSERT_FALSE(query_ctx->is_delivery_expired());
            ASSERT_FALSE(query_ctx->is_query_expired());
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
            ASSERT_FALSE(query_ctx->is_delivery_expired());
            ASSERT_FALSE(query_ctx->is_query_expired());
            ASSERT_FALSE(query_ctx->has_no_active_instances());
            ASSERT_FALSE(query_ctx->is_dead());
            query_ctx->count_down_fragments();
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
        query_ctx->set_delivery_expire_seconds(60);
        query_ctx->set_query_expire_seconds(300);
        query_ctx->extend_delivery_lifetime();
        query_ctx->extend_query_lifetime();
        query_ctx->count_down_fragments();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

        for (int i = 0; i < 7; ++i) {
            ASSIGN_OR_ASSERT_FAIL(auto* tmp_query_ctx, query_ctx_mgr->get_or_register(query_id));
            tmp_query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_TRUE(tmp_query_ctx != nullptr);
        }
        for (int i = 0; i < 7; ++i) {
            query_ctx->count_down_fragments();
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
        query_ctx->set_delivery_expire_seconds(60);
        query_ctx->set_query_expire_seconds(300);
        query_ctx->extend_delivery_lifetime();
        query_ctx->extend_query_lifetime();
        query_ctx->count_down_fragments();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

        for (int i = 0; i < 3; ++i) {
            ASSIGN_OR_ASSERT_FAIL(auto* tmp_query_ctx, query_ctx_mgr->get_or_register(query_id));
            tmp_query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_TRUE(tmp_query_ctx != nullptr);
        }
        for (int i = 0; i < 3; ++i) {
            query_ctx->count_down_fragments();
        }
        ASSERT_TRUE(query_ctx->has_no_active_instances());
        ASSERT_FALSE(query_ctx->is_dead());
        query_ctx_mgr->remove(query_id);
        ASSERT_TRUE(query_ctx_mgr->get(query_id) != nullptr);
        for (int i = 0; i < 3; ++i) {
            ASSIGN_OR_ASSERT_FAIL(auto* tmp_query_ctx, query_ctx_mgr->get_or_register(query_id));
            tmp_query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_TRUE(query_ctx != nullptr);
            tmp_query_ctx->count_down_fragments();
            query_ctx_mgr->remove(query_id);
            ASSERT_TRUE(tmp_query_ctx->has_no_active_instances());
            ASSERT_FALSE(tmp_query_ctx->is_dead());
            ASSERT_TRUE(query_ctx_mgr->get(query_id) != nullptr);
        }
        ASSIGN_OR_ASSERT_FAIL(query_ctx, query_ctx_mgr->get_or_register(query_id));
        query_ctx->count_down_fragments();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
        ASSERT_TRUE(query_ctx->is_dead());
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
        query_ctx->set_delivery_expire_seconds(60);
        query_ctx->set_query_expire_seconds(300);
        query_ctx->extend_delivery_lifetime();
        query_ctx->extend_query_lifetime();
        query_ctx->count_down_fragments();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
        for (int i = 0; i < 3; ++i) {
            ASSIGN_OR_ASSERT_FAIL(auto* tmp_query_ctx, query_ctx_mgr->get_or_register(query_id));
            tmp_query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_TRUE(tmp_query_ctx != nullptr);
        }
        for (int i = 0; i < 3; ++i) {
            query_ctx->count_down_fragments();
        }
        ASSERT_TRUE(query_ctx->has_no_active_instances());
        ASSERT_FALSE(query_ctx->is_dead());
        query_ctx_mgr->remove(query_id);
        ASSERT_TRUE(query_ctx_mgr->get(query_id) != nullptr);
        query_ctx->set_delivery_expire_seconds(1);
        query_ctx->extend_delivery_lifetime();
        query_ctx->extend_query_lifetime();
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
        query_ctx->set_delivery_expire_seconds(60);
        query_ctx->set_query_expire_seconds(300);
        query_ctx->extend_delivery_lifetime();
        query_ctx->extend_query_lifetime();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
        // port query_ctx to second map
        query_ctx->count_down_fragments(query_ctx_mgr.get());

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
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(300);
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->count_down_fragments();
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
                ASSERT_FALSE(query_ctx->is_delivery_expired());
                ASSERT_FALSE(query_ctx->is_query_expired());
                ASSERT_FALSE(query_ctx->is_dead());
                std::this_thread::sleep_for(std::chrono::milliseconds(dist(rd)));
                query_ctx->count_down_fragments();
                query_ctx_mgr->remove(query_id);
            }
        });
    }
    for (int i = 0; i < 10; ++i) {
        threads[i].join();
    }

    ASSIGN_OR_ASSERT_FAIL(query_ctx, query_ctx_mgr->get_or_register(query_id));
    query_ctx->count_down_fragments();
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
    ASSERT_TRUE(query_ctx->is_dead());
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
    query_ctx->set_delivery_expire_seconds(delivery_expire_seconds);
    query_ctx->set_query_expire_seconds(query_expire_seconds);
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->count_down_fragments();

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
    while (!query_ctx1->has_no_active_instances()) {
        query_ctx1->count_down_fragments();
    }
    ASSERT_TRUE(query_ctx1->is_dead()); // All the fragments have come and finished.
    query_ctx_mgr->remove(query_id1);
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
        query_ctx2->count_down_fragments();
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

    // Deltas drain once consumed, while totals stay intact.
    EXPECT_EQ(100, ctx.consume_delta_read_local_cnt());
    EXPECT_EQ(200, ctx.consume_delta_read_remote_cnt());
    EXPECT_EQ(0, ctx.consume_delta_read_local_cnt());
    EXPECT_EQ(0, ctx.consume_delta_read_remote_cnt());
    EXPECT_EQ(100, ctx.get_read_local_cnt());
    EXPECT_EQ(200, ctx.get_read_remote_cnt());

    ctx.incr_read_stats(2, 4);
    EXPECT_EQ(2, ctx.consume_delta_read_local_cnt());
    EXPECT_EQ(4, ctx.consume_delta_read_remote_cnt());
    EXPECT_EQ(102, ctx.get_read_local_cnt());
    EXPECT_EQ(204, ctx.get_read_remote_cnt());
}

TEST(QueryContextManagerTest, testIntermediateQueryStatisticCarriesReadStats) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 1073741824L, "parent", nullptr);
    QueryContext ctx;
    ctx.init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

    ctx.incr_read_stats(100, 200);

    auto intermediate_stats = ctx.intermediate_query_statistic(0);
    ASSERT_NE(nullptr, intermediate_stats);
    PQueryStatistics intermediate_pb;
    intermediate_stats->to_pb(&intermediate_pb);
    EXPECT_EQ(100, intermediate_pb.read_local_cnt());
    EXPECT_EQ(200, intermediate_pb.read_remote_cnt());

    // The delta has been drained, so a second report carries nothing.
    auto second_intermediate_stats = ctx.intermediate_query_statistic(0);
    ASSERT_NE(nullptr, second_intermediate_stats);
    PQueryStatistics second_intermediate_pb;
    second_intermediate_stats->to_pb(&second_intermediate_pb);
    EXPECT_EQ(0, second_intermediate_pb.read_local_cnt());
    EXPECT_EQ(0, second_intermediate_pb.read_remote_cnt());

    // Totals used by the final-sink path are unaffected by delta consumption.
    EXPECT_EQ(100, ctx.get_read_local_cnt());
    EXPECT_EQ(200, ctx.get_read_remote_cnt());
}

// The reserve limit is the early-warning line the AUTO spill trigger probes through
// try_mem_reserve, so it must always stay below the hard limit the query is checked against.
class QueryContextReserveLimitTest : public ::testing::Test {
protected:
    static constexpr int64_t kParentMemLimit = 100L * 1024 * 1024 * 1024; // 100GB
    static constexpr int64_t kQueryMemLimit = 64L * 1024 * 1024;          // 64MB
    static constexpr int64_t kBigQueryMemLimit = 128L * 1024 * 1024;      // 128MB
    static constexpr double kSpillMemReserveRatio = 0.8;

    // The production code truncates the double product, so the expectations must do the same.
    static int64_t expected_reserve_limit(int64_t limit) { return limit * kSpillMemReserveRatio; }

    void SetUp() override {
        _parent_mem_tracker =
                std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, kParentMemLimit, "parent", nullptr);
        // init_mem_tracker runs under std::call_once, so every case needs a fresh QueryContext.
        _query_ctx = std::make_unique<QueryContext>();
        TUniqueId query_id;
        query_id.hi = 7;
        query_id.lo = 8;
        _query_ctx->set_query_id(query_id);
    }

    std::shared_ptr<MemTracker> _parent_mem_tracker;
    std::unique_ptr<QueryContext> _query_ctx;
};

TEST_F(QueryContextReserveLimitTest, ReserveLimitHonorsSmallQueryMemLimit) {
    _query_ctx->init_mem_tracker(kQueryMemLimit, _parent_mem_tracker.get(), -1, kSpillMemReserveRatio);

    auto tracker = _query_ctx->mem_tracker();
    ASSERT_NE(nullptr, tracker);
    EXPECT_EQ(kQueryMemLimit, tracker->limit());
    EXPECT_EQ(expected_reserve_limit(kQueryMemLimit), tracker->reserve_limit());
    // The early-warning line must sit below the hard limit, otherwise the reservation probe never
    // fails and an AUTO mode query OOMs instead of spilling.
    EXPECT_LT(tracker->reserve_limit(), tracker->limit());
}

TEST_F(QueryContextReserveLimitTest, ReserveLimitFallsBackToParentWhenNoQueryLimit) {
    _query_ctx->init_mem_tracker(-1, _parent_mem_tracker.get(), -1, kSpillMemReserveRatio);

    auto tracker = _query_ctx->mem_tracker();
    ASSERT_NE(nullptr, tracker);
    EXPECT_EQ(expected_reserve_limit(kParentMemLimit), tracker->reserve_limit());
}

TEST_F(QueryContextReserveLimitTest, ReserveLimitUnsetWhenRatioAbsent) {
    // Spill is disabled, so no reserve limit is set and the reservation path falls back to limit().
    _query_ctx->init_mem_tracker(kQueryMemLimit, _parent_mem_tracker.get());

    auto tracker = _query_ctx->mem_tracker();
    ASSERT_NE(nullptr, tracker);
    EXPECT_EQ(-1, tracker->reserve_limit());
    EXPECT_EQ(kQueryMemLimit, tracker->limit());
}

TEST_F(QueryContextReserveLimitTest, ReserveLimitHonorsBigQueryMemLimit) {
    auto wg = std::make_shared<workgroup::WorkGroup>("wg", 1, 0, 1, -1, 0, 1.0, TWorkGroupType::WG_NORMAL, "");
    _query_ctx->init_mem_tracker(-1, _parent_mem_tracker.get(), kBigQueryMemLimit, kSpillMemReserveRatio, wg.get());

    auto tracker = _query_ctx->mem_tracker();
    ASSERT_NE(nullptr, tracker);
    EXPECT_EQ(MemTrackerType::RESOURCE_GROUP_BIG_QUERY, tracker->type());
    EXPECT_EQ(kBigQueryMemLimit, tracker->limit());
    EXPECT_EQ(expected_reserve_limit(kBigQueryMemLimit), tracker->reserve_limit());
    EXPECT_LT(tracker->reserve_limit(), tracker->limit());
}

TEST_F(QueryContextReserveLimitTest, ReserveLimitTakesMinOfAllLimits) {
    auto wg = std::make_shared<workgroup::WorkGroup>("wg", 1, 0, 1, -1, 0, 1.0, TWorkGroupType::WG_NORMAL, "");
    _query_ctx->init_mem_tracker(kQueryMemLimit, _parent_mem_tracker.get(), kBigQueryMemLimit, kSpillMemReserveRatio,
                                 wg.get());

    auto tracker = _query_ctx->mem_tracker();
    ASSERT_NE(nullptr, tracker);
    EXPECT_EQ(kQueryMemLimit, tracker->limit());
    EXPECT_EQ(expected_reserve_limit(kQueryMemLimit), tracker->reserve_limit());
    EXPECT_LT(tracker->reserve_limit(), tracker->limit());
}

TEST_F(QueryContextReserveLimitTest, StaticQueryMemLimitTakesMinOfAllLimits) {
    auto wg = std::make_shared<workgroup::WorkGroup>("wg", 1, 0, 1, -1, 0, 1.0, TWorkGroupType::WG_NORMAL, "");
    _query_ctx->init_mem_tracker(kQueryMemLimit, _parent_mem_tracker.get(), kBigQueryMemLimit, kSpillMemReserveRatio,
                                 wg.get());

    // Computing the effective limit earlier must not change its value.
    EXPECT_EQ(kQueryMemLimit, _query_ctx->get_static_query_mem_limit());
}

} // namespace starrocks::pipeline
