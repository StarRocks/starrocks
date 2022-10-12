// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <chrono>
#include <random>

#include "exec/pipeline/query_context.h"
#include "gtest/gtest.h"

namespace starrocks {
namespace pipeline {
TEST(QueryContextManagerTest, testSingleThreadOperations) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTracker::QUERY_POOL, 1073741824L, "parent", nullptr);
    {
        auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
        ASSERT_TRUE(query_ctx_mgr->init().ok());
        for (int i = 0; i < 100; ++i) {
            TUniqueId query_id;
            query_id.hi = 100;
            query_id.lo = i;
            auto* query_ctx = query_ctx_mgr->get_or_register(query_id);
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
        auto* query_ctx = query_ctx_mgr->get_or_register(query_id);
        query_ctx->set_total_fragments(8);
        query_ctx->set_delivery_expire_seconds(60);
        query_ctx->set_query_expire_seconds(300);
        query_ctx->extend_delivery_lifetime();
        query_ctx->extend_query_lifetime();
        query_ctx->count_down_fragments();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

        for (int i = 0; i < 7; ++i) {
            auto* tmp_query_ctx = query_ctx_mgr->get_or_register(query_id);
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
        auto* query_ctx = query_ctx_mgr->get_or_register(query_id);
        query_ctx->set_total_fragments(8);
        query_ctx->set_delivery_expire_seconds(60);
        query_ctx->set_query_expire_seconds(300);
        query_ctx->extend_delivery_lifetime();
        query_ctx->extend_query_lifetime();
        query_ctx->count_down_fragments();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());

        for (int i = 0; i < 3; ++i) {
            auto* tmp_query_ctx = query_ctx_mgr->get_or_register(query_id);
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
            auto* tmp_query_ctx = query_ctx_mgr->get_or_register(query_id);
            tmp_query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
            ASSERT_TRUE(query_ctx != nullptr);
            tmp_query_ctx->count_down_fragments();
            query_ctx_mgr->remove(query_id);
            ASSERT_TRUE(tmp_query_ctx->has_no_active_instances());
            ASSERT_FALSE(tmp_query_ctx->is_dead());
            ASSERT_TRUE(query_ctx_mgr->get(query_id) != nullptr);
        }
        query_ctx = query_ctx_mgr->get_or_register(query_id);
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
        auto* query_ctx = query_ctx_mgr->get_or_register(query_id);
        query_ctx->set_total_fragments(8);
        query_ctx->set_delivery_expire_seconds(60);
        query_ctx->set_query_expire_seconds(300);
        query_ctx->extend_delivery_lifetime();
        query_ctx->extend_query_lifetime();
        query_ctx->count_down_fragments();
        query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
        for (int i = 0; i < 3; ++i) {
            auto* tmp_query_ctx = query_ctx_mgr->get_or_register(query_id);
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
}

TEST(QueryContextManagerTest, testMulitiThreadOperations) {
    auto parent_mem_tracker = std::make_shared<MemTracker>(MemTracker::QUERY_POOL, 1073741824L, "parent", nullptr);
    auto query_ctx_mgr = std::make_shared<QueryContextManager>(6);
    ASSERT_TRUE(query_ctx_mgr->init().ok());
    TUniqueId query_id;
    query_id.lo = 100;
    query_id.hi = 2;
    auto* query_ctx = query_ctx_mgr->get_or_register(query_id);
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
                auto* query_ctx = query_ctx_mgr->get_or_register(query_id);
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

    query_ctx = query_ctx_mgr->get_or_register(query_id);
    query_ctx->count_down_fragments();
    query_ctx->init_mem_tracker(parent_mem_tracker->limit(), parent_mem_tracker.get());
    ASSERT_TRUE(query_ctx->is_dead());
    query_ctx_mgr->remove(query_id);
    ASSERT_TRUE(query_ctx_mgr->get(query_id) == nullptr);
}
} // namespace pipeline
} // namespace starrocks
