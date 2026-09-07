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

#include "exec/pipeline/ai/ai_query_memory_account.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/runtime/query_context.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

bool ai_memory_test_env_initialized() {
    return true;
}

MemTracker* ai_memory_test_process_tracker() {
    return nullptr;
}

class AIQueryMemoryAccountTest : public ::testing::Test {
protected:
    void SetUp() override {
        tls_mem_tracker = nullptr;
        CurrentThread::set_mem_tracker_source(ai_memory_test_env_initialized, ai_memory_test_process_tracker);
    }

    void TearDown() override {
        CurrentThread::set_mem_tracker_source(nullptr, nullptr);
        tls_mem_tracker = nullptr;
    }

    struct Harness {
        explicit Harness(int64_t process_limit = -1, int64_t workgroup_limit = -1, int64_t query_limit = -1)
                : process_tracker(std::make_shared<MemTracker>(MemTrackerType::PROCESS, process_limit, "process")),
                  query_pool_tracker(std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, -1, "query_pool",
                                                                  process_tracker.get())),
                  workgroup(std::make_shared<workgroup::WorkGroup>("wg", 1, 1, 1, -1, 0, 1.0, TWorkGroupType::WG_NORMAL,
                                                                   "ai_test_pool")),
                  query_tracker(std::make_shared<MemTracker>(MemTrackerType::QUERY, query_limit, "query")),
                  query_context(QueryContext::create()) {
            workgroup->_shared_mem_tracker = query_pool_tracker;
            workgroup->_mem_tracker = std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP, workgroup_limit,
                                                                   "wg", query_pool_tracker.get());
            query_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY, query_limit, "query",
                                                         workgroup->_mem_tracker.get());
            fragment_runtime_state.set_workgroup(workgroup);
            runtime_state.set_fragment_runtime_state(&fragment_runtime_state);
            runtime_state.set_query_mem_tracker(query_tracker);
            query_context->attach_to_runtime_state(&runtime_state);
        }

        std::shared_ptr<AIQueryMemoryAccount> create_account() {
            auto account_or = AIQueryMemoryAccount::create(runtime_state);
            EXPECT_TRUE(account_or.ok()) << account_or.status();
            return account_or.ok() ? std::move(account_or).value() : nullptr;
        }

        std::shared_ptr<MemTracker> process_tracker;
        std::shared_ptr<MemTracker> query_pool_tracker;
        workgroup::WorkGroupPtr workgroup;
        std::shared_ptr<MemTracker> query_tracker;
        QueryContextPtr query_context;
        FragmentRuntimeState fragment_runtime_state;
        RuntimeState runtime_state;
    };
};

TEST_F(AIQueryMemoryAccountTest, CreateRequiresACompleteRuntimeSnapshot) {
    auto lifetime = std::make_shared<QueryContextLifetime>();
    auto process_tracker = std::make_shared<MemTracker>(MemTrackerType::PROCESS, -1, "process");
    auto query_pool_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, -1, "query_pool");
    auto workgroup = std::make_shared<workgroup::WorkGroup>("wg", 1, 1, 1, -1, 0, 1.0, TWorkGroupType::WG_NORMAL,
                                                            "ai_test_pool");
    FragmentRuntimeState fragment_runtime_state;
    RuntimeState runtime_state;

    auto account_or = AIQueryMemoryAccount::create(runtime_state);
    EXPECT_TRUE(account_or.status().is_invalid_argument()) << account_or.status();

    runtime_state.set_query_ctx_lifetime(lifetime);
    account_or = AIQueryMemoryAccount::create(runtime_state);
    EXPECT_TRUE(account_or.status().is_invalid_argument()) << account_or.status();

    runtime_state.set_fragment_runtime_state(&fragment_runtime_state);
    account_or = AIQueryMemoryAccount::create(runtime_state);
    EXPECT_TRUE(account_or.status().is_invalid_argument()) << account_or.status();

    fragment_runtime_state.set_workgroup(workgroup);
    account_or = AIQueryMemoryAccount::create(runtime_state);
    EXPECT_TRUE(account_or.status().is_invalid_argument()) << account_or.status();

    workgroup->_shared_mem_tracker = query_pool_tracker;
    workgroup->_mem_tracker =
            std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP, -1, "wg", query_pool_tracker.get());
    account_or = AIQueryMemoryAccount::create(runtime_state);
    EXPECT_TRUE(account_or.status().is_invalid_argument()) << account_or.status();

    auto wrong_parent = std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, -1, "wrong_parent");
    auto query_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY, -1, "query", wrong_parent.get());
    runtime_state.set_query_mem_tracker(query_tracker);
    account_or = AIQueryMemoryAccount::create(runtime_state);
    EXPECT_TRUE(account_or.status().is_invalid_argument()) << account_or.status();

    query_tracker = std::make_shared<MemTracker>(MemTrackerType::QUERY, -1, "query", workgroup->mem_tracker());
    runtime_state.set_query_mem_tracker(query_tracker);
    account_or = AIQueryMemoryAccount::create(runtime_state);
    ASSERT_TRUE(account_or.status().is_invalid_argument()) << account_or.status();

    auto rooted_query_pool =
            std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, -1, "query_pool", process_tracker.get());
    auto rooted_workgroup = std::make_shared<workgroup::WorkGroup>("rooted_wg", 2, 2, 2, -1, 0, 1.0,
                                                                   TWorkGroupType::WG_NORMAL, "ai_test_pool");
    rooted_workgroup->_shared_mem_tracker = rooted_query_pool;
    rooted_workgroup->_mem_tracker =
            std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP, -1, "wg", rooted_query_pool.get());
    auto rooted_query_tracker =
            std::make_shared<MemTracker>(MemTrackerType::QUERY, -1, "query", rooted_workgroup->mem_tracker());
    fragment_runtime_state.set_workgroup(rooted_workgroup);
    runtime_state.set_query_mem_tracker(rooted_query_tracker);
    account_or = AIQueryMemoryAccount::create(runtime_state);
    ASSERT_TRUE(account_or.ok()) << account_or.status();
    EXPECT_EQ(0, (*account_or)->reserved_bytes());
}

TEST_F(AIQueryMemoryAccountTest, ConcurrentReserveAndReleaseBalancesTheNonRootTrackerChain) {
    Harness harness;
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();
    constexpr int kThreadCount = 8;
    constexpr int kIterations = 2000;
    constexpr size_t kBytes = 7;
    std::atomic<bool> all_reserved{true};
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (int thread = 0; thread < kThreadCount; ++thread) {
        threads.emplace_back([&] {
            for (int iteration = 0; iteration < kIterations; ++iteration) {
                if (!hooks.reserve(kBytes)) {
                    all_reserved.store(false, std::memory_order_relaxed);
                    return;
                }
                hooks.release(kBytes);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_TRUE(all_reserved.load(std::memory_order_relaxed));
    EXPECT_EQ(0, account->reserved_bytes());
    EXPECT_EQ(0, harness.query_tracker->consumption());
    EXPECT_EQ(0, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(0, harness.query_pool_tracker->consumption());
    EXPECT_EQ(0, harness.process_tracker->consumption());
}

TEST_F(AIQueryMemoryAccountTest, MemoryContextCopiesMovesAndFinalDestructionPreserveAmbientScope) {
    Harness harness;
    MemTracker ambient(-1, "ambient");
    CurrentThreadMemTrackerSetter ambient_scope(&ambient);
    ASSERT_EQ(&ambient, tls_mem_tracker);
    std::atomic<int> accounts_destroyed_in_physical_scope{0};
    std::atomic<int> control_blocks_deallocated_in_physical_scope{0};
    std::atomic<bool> context_destroyed_in_physical_scope{false};
    std::atomic<int> context_destroy_callbacks{0};
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("AIQueryMemoryAccount::destroy:in_physical_scope", [&](void*) {
        if (tls_mem_tracker == harness.process_tracker.get()) {
            accounts_destroyed_in_physical_scope.fetch_add(1, std::memory_order_relaxed);
        }
    });
    sync_point->SetCallBack("AIQueryMemoryAccount::control_block_deallocate:in_physical_scope", [&](void*) {
        if (tls_mem_tracker == harness.process_tracker.get()) {
            control_blocks_deallocated_in_physical_scope.fetch_add(1, std::memory_order_relaxed);
        }
    });
    sync_point->SetCallBack("AIQueryMemoryAccount::context_owner_destroy:in_physical_scope", [&](void*) {
        context_destroy_callbacks.fetch_add(1, std::memory_order_relaxed);
        context_destroyed_in_physical_scope.store(tls_mem_tracker == harness.process_tracker.get(),
                                                  std::memory_order_relaxed);
    });
    sync_point->EnableProcessing();
    DeferOp cleanup([&] {
        sync_point->DisableProcessing();
        sync_point->ClearCallBack("AIQueryMemoryAccount::destroy:in_physical_scope");
        sync_point->ClearCallBack("AIQueryMemoryAccount::control_block_deallocate:in_physical_scope");
        sync_point->ClearCallBack("AIQueryMemoryAccount::context_owner_destroy:in_physical_scope");
        sync_point->ClearTrace();
    });

    auto account_without_hooks = harness.create_account();
    ASSERT_NE(nullptr, account_without_hooks);
    ASSERT_EQ(1, account_without_hooks.use_count());
    std::weak_ptr<AIQueryMemoryAccount> weak_account_without_hooks = account_without_hooks;
    ASSERT_EQ(&ambient, tls_mem_tracker);
    account_without_hooks.reset();
    ASSERT_TRUE(weak_account_without_hooks.expired());
    EXPECT_EQ(1, accounts_destroyed_in_physical_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(0, control_blocks_deallocated_in_physical_scope.load(std::memory_order_relaxed));
    weak_account_without_hooks.reset();
    EXPECT_EQ(1, control_blocks_deallocated_in_physical_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(&ambient, tls_mem_tracker);

    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();
    ASSERT_EQ(2, account.use_count());
    ASSERT_TRUE(hooks);
    bool action_ran = false;
    hooks.run_in_physical_scope([](void* context) { *static_cast<bool*>(context) = true; }, &action_ran);
    ASSERT_TRUE(action_ran);
    ASSERT_EQ(&ambient, tls_mem_tracker);

    AIMemoryContext copied_hooks = hooks;
    AIMemoryContext moved_hooks = std::move(copied_hooks);
    EXPECT_FALSE(copied_hooks);
    hooks = {};
    account.reset();

    MemTracker thread_ambient(-1, "thread_ambient");
    std::atomic<bool> ambient_restored{false};
    std::thread destroyer([hooks = std::move(moved_hooks), &thread_ambient, &ambient_restored]() mutable {
        CurrentThreadMemTrackerSetter ambient_scope(&thread_ambient);
        hooks = {};
        ambient_restored.store(tls_mem_tracker == &thread_ambient, std::memory_order_relaxed);
    });
    destroyer.join();

    EXPECT_TRUE(context_destroyed_in_physical_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(1, context_destroy_callbacks.load(std::memory_order_relaxed));
    EXPECT_EQ(2, accounts_destroyed_in_physical_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(2, control_blocks_deallocated_in_physical_scope.load(std::memory_order_relaxed));
    EXPECT_TRUE(ambient_restored.load(std::memory_order_relaxed));
}

TEST_F(AIQueryMemoryAccountTest, QueryLimitFailureRollsBackAncestorsWithoutLosingPriorReservation) {
    Harness harness(-1, 100, 10);
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();

    ASSERT_TRUE(hooks.reserve(8));
    EXPECT_FALSE(hooks.reserve(3));
    EXPECT_EQ(8, account->reserved_bytes());
    EXPECT_EQ(8, harness.query_tracker->consumption());
    EXPECT_EQ(8, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(8, harness.query_pool_tracker->consumption());
    EXPECT_EQ(0, harness.process_tracker->consumption());

    hooks.release(8);
    EXPECT_EQ(0, account->reserved_bytes());
    EXPECT_EQ(0, harness.query_tracker->consumption());
    EXPECT_EQ(0, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(0, harness.query_pool_tracker->consumption());
    EXPECT_EQ(0, harness.process_tracker->consumption());
}

TEST_F(AIQueryMemoryAccountTest, WorkGroupLimitFailureRollsBackItsParent) {
    Harness harness(-1, 10, 100);
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();

    EXPECT_FALSE(hooks.reserve(11));
    EXPECT_EQ(0, account->reserved_bytes());
    EXPECT_EQ(0, harness.query_tracker->consumption());
    EXPECT_EQ(0, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(0, harness.query_pool_tracker->consumption());
    EXPECT_EQ(0, harness.process_tracker->consumption());
}

TEST_F(AIQueryMemoryAccountTest, RejectsBytesAndAggregateBalanceBeyondInt64Max) {
    static_assert(sizeof(size_t) >= sizeof(int64_t));
    Harness harness;
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();
    constexpr size_t kInt64Max = static_cast<size_t>(std::numeric_limits<int64_t>::max());

    EXPECT_FALSE(hooks.reserve(kInt64Max + 1));
    ASSERT_TRUE(hooks.reserve(kInt64Max));
    EXPECT_FALSE(hooks.reserve(1));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(), account->reserved_bytes());

    hooks.release(kInt64Max);
    EXPECT_EQ(0, account->reserved_bytes());
    EXPECT_EQ(0, harness.query_tracker->consumption());
    EXPECT_EQ(0, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(0, harness.query_pool_tracker->consumption());
    EXPECT_EQ(0, harness.process_tracker->consumption());
}

TEST_F(AIQueryMemoryAccountTest, ReclassifiesPhysicallyAllocatedBytesWithoutDoubleCountingTheRoot) {
    Harness harness;
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();

    harness.process_tracker->consume(40);
    ASSERT_TRUE(hooks.reserve(8));
    EXPECT_EQ(8, account->reserved_bytes());
    EXPECT_EQ(8, harness.query_tracker->consumption());
    EXPECT_EQ(8, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(8, harness.query_pool_tracker->consumption());
    EXPECT_EQ(40, harness.process_tracker->consumption());

    hooks.release(8);
    EXPECT_EQ(0, account->reserved_bytes());
    EXPECT_EQ(0, harness.query_tracker->consumption());
    EXPECT_EQ(0, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(0, harness.query_pool_tracker->consumption());
    EXPECT_EQ(40, harness.process_tracker->consumption());
    harness.process_tracker->release(40);
}

TEST_F(AIQueryMemoryAccountTest, RootAtLimitAcceptsAlreadyAllocatedBytesWithoutChargingThemTwice) {
    Harness harness(10, 100, 100);
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();

    harness.process_tracker->consume(10);
    ASSERT_TRUE(hooks.reserve(8));
    EXPECT_EQ(8, harness.query_tracker->consumption());
    EXPECT_EQ(8, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(8, harness.query_pool_tracker->consumption());
    EXPECT_EQ(10, harness.process_tracker->consumption());

    hooks.release(8);
    harness.process_tracker->release(10);
}

TEST_F(AIQueryMemoryAccountTest, AlreadyExceededRootRejectsWithoutMutatingChildren) {
    Harness harness(10, 100, 100);
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();

    harness.process_tracker->consume(11);
    EXPECT_FALSE(hooks.reserve(1));
    EXPECT_EQ(0, account->reserved_bytes());
    EXPECT_EQ(0, harness.query_tracker->consumption());
    EXPECT_EQ(0, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(0, harness.query_pool_tracker->consumption());
    EXPECT_EQ(11, harness.process_tracker->consumption());
    harness.process_tracker->release(11);
}

TEST_F(AIQueryMemoryAccountTest, ExpiredQueryRejectsNewReservationsButExistingReservationStillReleases) {
    Harness harness;
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();
    std::weak_ptr<QueryContext> weak_query_context = harness.query_context;

    ASSERT_TRUE(hooks.reserve(32));
    harness.query_context.reset();
    ASSERT_TRUE(weak_query_context.expired());

    EXPECT_FALSE(hooks.reserve(1));
    EXPECT_EQ(32, account->reserved_bytes());
    EXPECT_NO_THROW(hooks.release(32));
    EXPECT_EQ(0, account->reserved_bytes());
    EXPECT_EQ(0, harness.query_tracker->consumption());
    EXPECT_EQ(0, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(0, harness.query_pool_tracker->consumption());
    EXPECT_EQ(0, harness.process_tracker->consumption());
}

TEST_F(AIQueryMemoryAccountTest, MemoryContextKeepsTrackerParentsAliveWithoutPinningQueryContext) {
    AIMemoryContext hooks;
    std::weak_ptr<AIQueryMemoryAccount> weak_account;
    std::weak_ptr<QueryContext> weak_query_context;
    std::weak_ptr<MemTracker> weak_query_tracker;
    std::weak_ptr<workgroup::WorkGroup> weak_workgroup;
    std::weak_ptr<MemTracker> weak_query_pool_tracker;
    std::weak_ptr<MemTracker> weak_process_tracker;
    std::shared_ptr<MemTracker> process_owner;

    {
        Harness harness;
        auto account = harness.create_account();
        ASSERT_NE(nullptr, account);
        hooks = account->memory_context();
        ASSERT_TRUE(hooks.reserve(21));
        weak_account = account;
        weak_query_context = harness.query_context;
        weak_query_tracker = harness.query_tracker;
        weak_workgroup = harness.workgroup;
        weak_query_pool_tracker = harness.query_pool_tracker;
        weak_process_tracker = harness.process_tracker;
        process_owner = harness.process_tracker;

        harness.query_context.reset();
        harness.runtime_state.set_query_mem_tracker(nullptr);
        harness.fragment_runtime_state.set_workgroup(nullptr);
        harness.query_tracker.reset();
        harness.workgroup.reset();
        harness.query_pool_tracker.reset();
        harness.process_tracker.reset();
        account.reset();
    }

    EXPECT_TRUE(weak_query_context.expired());
    ASSERT_FALSE(weak_account.expired());
    ASSERT_FALSE(weak_query_tracker.expired());
    ASSERT_FALSE(weak_workgroup.expired());
    ASSERT_FALSE(weak_query_pool_tracker.expired());
    ASSERT_FALSE(weak_process_tracker.expired());
    EXPECT_EQ(21, weak_query_tracker.lock()->consumption());
    EXPECT_EQ(21, weak_workgroup.lock()->mem_tracker()->consumption());
    EXPECT_EQ(21, weak_query_pool_tracker.lock()->consumption());
    EXPECT_EQ(0, weak_process_tracker.lock()->consumption());

    hooks.release(21);
    EXPECT_EQ(0, weak_query_tracker.lock()->consumption());
    EXPECT_EQ(0, weak_workgroup.lock()->mem_tracker()->consumption());
    EXPECT_EQ(0, weak_query_pool_tracker.lock()->consumption());
    EXPECT_EQ(0, weak_process_tracker.lock()->consumption());

    hooks = {};
    EXPECT_TRUE(weak_account.expired());
    EXPECT_TRUE(weak_query_tracker.expired());
    EXPECT_TRUE(weak_workgroup.expired());
    EXPECT_TRUE(weak_query_pool_tracker.expired());
    EXPECT_FALSE(weak_process_tracker.expired());
    process_owner.reset();
    EXPECT_TRUE(weak_process_tracker.expired());
}

TEST_F(AIQueryMemoryAccountTest, InvalidAndDuplicateReleaseCannotUnderflowTrackers) {
    Harness harness;
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();

    ASSERT_TRUE(hooks.reserve(10));
    EXPECT_NO_THROW(hooks.release(11));
    EXPECT_EQ(10, account->reserved_bytes());
    EXPECT_EQ(10, harness.query_tracker->consumption());

    hooks.release(10);
    EXPECT_NO_THROW(hooks.release(10));
    EXPECT_EQ(0, account->reserved_bytes());
    EXPECT_EQ(0, harness.query_tracker->consumption());
    EXPECT_EQ(0, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(0, harness.query_pool_tracker->consumption());
    EXPECT_EQ(0, harness.process_tracker->consumption());
}

TEST_F(AIQueryMemoryAccountTest, FinalExternalContextDestructionDrainsResidualReservation) {
    Harness harness;
    auto account = harness.create_account();
    ASSERT_NE(nullptr, account);
    AIMemoryContext hooks = account->memory_context();

    ASSERT_TRUE(hooks.reserve(21));
    ASSERT_EQ(21, account->reserved_bytes());
    account.reset();

    std::thread final_owner([hooks = std::move(hooks)]() mutable { hooks = {}; });
    final_owner.join();

    EXPECT_EQ(0, harness.query_tracker->consumption());
    EXPECT_EQ(0, harness.workgroup->mem_tracker()->consumption());
    EXPECT_EQ(0, harness.query_pool_tracker->consumption());
    EXPECT_EQ(0, harness.process_tracker->consumption());
}

} // namespace
} // namespace starrocks::pipeline
