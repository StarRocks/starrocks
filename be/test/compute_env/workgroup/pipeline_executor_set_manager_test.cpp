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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "common/statusor.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/work_group.h"
#include "compute_env/workgroup/work_group_manager.h"
#include "exec/pipeline/primitives/driver_executor.h"
#include "exec/pipeline/primitives/driver_queue.h"
#include "exec/pipeline/primitives/pipeline_metrics.h"

namespace starrocks::workgroup {

// ---------------------------------------------------------------------------
// Helper utilities
// ---------------------------------------------------------------------------

static TWorkGroup make_exclusive_twg(int64_t id, int32_t exclusive_cpu_cores) {
    TWorkGroup twg;
    twg.__set_id(id);
    twg.__set_version(1);
    twg.__set_name("exclusive_wg_" + std::to_string(id));
    twg.__set_exclusive_cpu_cores(exclusive_cpu_cores);
    twg.__set_mem_limit(0.5);
    return twg;
}

static TWorkGroupOp make_create_op(const TWorkGroup& twg) {
    TWorkGroupOp op;
    op.__set_workgroup(twg);
    op.__set_op_type(TWorkGroupOpType::WORKGROUP_OP_CREATE);
    return op;
}

class FakeDriverQueue final : public pipeline::DriverQueue {
public:
    FakeDriverQueue() : DriverQueue(nullptr) {}

    void close() override {}
    void put_back(pipeline::DriverRawPtr driver) override {}
    void put_back(const std::vector<pipeline::DriverRawPtr>& drivers) override {}
    void put_back_from_executor(pipeline::DriverRawPtr driver) override {}
    StatusOr<pipeline::DriverRawPtr> take(bool block) override { return nullptr; }
    void cancel(pipeline::DriverRawPtr driver) override {}
    void update_statistics(pipeline::DriverRawPtr driver) override {}
    size_t size() const override { return 0; }
    bool should_yield(pipeline::DriverRawPtr driver, int64_t unaccounted_runtime_ns) const override { return false; }
};

class FakeDriverExecutor final : public pipeline::DriverExecutor {
public:
    explicit FakeDriverExecutor(std::string name) : DriverExecutor(std::move(name)) {}

    void initialize(int32_t num_threads) override {
        initialized_num_threads = num_threads;
        current_num_threads = num_threads;
    }
    void change_num_threads(int32_t num_threads) override { current_num_threads = num_threads; }
    void submit(pipeline::DriverRawPtr) override {}
    void cancel(pipeline::DriverRawPtr) override {}
    void close() override { closed = true; }
    void report_exec_state(pipeline::QueryContext*, pipeline::FragmentContext*, const Status&, bool) override {}
    void report_audit_statistics(pipeline::QueryContext*, pipeline::FragmentContext*) override {}
    void report_audit_statistics_on_failure(pipeline::QueryContext*, pipeline::FragmentContext*) override {}
    void iterate_immutable_blocking_driver(const pipeline::ConstDriverConsumer&) const override {}
    void bind_cpus(const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) override {
        bound_cpuids = cpuids;
        bound_borrowed_cpuids = borrowed_cpuids;
    }
    Status update_exec_state_report_max_threads(int max_threads) override {
        exec_state_report_max_threads = max_threads;
        return Status::OK();
    }
    Status update_priority_exec_state_report_max_threads(int max_threads) override {
        priority_exec_state_report_max_threads = max_threads;
        return Status::OK();
    }

    int32_t initialized_num_threads = 0;
    int32_t current_num_threads = 0;
    int exec_state_report_max_threads = 0;
    int priority_exec_state_report_max_threads = 0;
    bool closed = false;
    CpuUtil::CpuIds bound_cpuids;
    std::vector<CpuUtil::CpuIds> bound_borrowed_cpuids;
};

struct DriverExecutorFactoryCall {
    std::string name;
    CpuUtil::CpuIds cpuids;
    std::vector<CpuUtil::CpuIds> borrowed_cpuids;
    uint32_t num_driver_threads = 0;
    pipeline::PipelineExecutorMetrics* metrics = nullptr;
};

static DriverExecutorFactory make_fake_driver_executor_factory(
        std::vector<DriverExecutorFactoryCall>* calls = nullptr) {
    return [calls](const std::string& name, const CpuUtil::CpuIds& cpuids,
                   const std::vector<CpuUtil::CpuIds>& borrowed_cpuids, uint32_t num_driver_threads,
                   pipeline::PipelineExecutorMetrics* metrics, const WorkGroupSchedulePolicy& schedule_policy)
                   -> StatusOr<std::unique_ptr<pipeline::DriverExecutor>> {
        (void)schedule_policy;
        if (calls != nullptr) {
            calls->push_back({name, cpuids, borrowed_cpuids, num_driver_threads, metrics});
        }
        std::unique_ptr<pipeline::DriverExecutor> executor = std::make_unique<FakeDriverExecutor>(name);
        return executor;
    };
}

static WorkGroupManager::DriverQueueFactory make_fake_driver_queue_factory() {
    return [](pipeline::DriverQueueMetrics*) { return std::make_unique<FakeDriverQueue>(); };
}

static DriverExecutorFactory make_fail_after_shared_driver_executor_factory(
        std::vector<DriverExecutorFactoryCall>* calls) {
    return [calls](const std::string& name, const CpuUtil::CpuIds& cpuids,
                   const std::vector<CpuUtil::CpuIds>& borrowed_cpuids, uint32_t num_driver_threads,
                   pipeline::PipelineExecutorMetrics* metrics, const WorkGroupSchedulePolicy& schedule_policy)
                   -> StatusOr<std::unique_ptr<pipeline::DriverExecutor>> {
        (void)schedule_policy;
        calls->push_back({name, cpuids, borrowed_cpuids, num_driver_threads, metrics});
        if (calls->size() > 1) {
            return Status::InternalError("injected driver executor creation failure");
        }
        std::unique_ptr<pipeline::DriverExecutor> executor = std::make_unique<FakeDriverExecutor>(name);
        return executor;
    };
}

static FakeDriverExecutor* TEST_fake_driver_executor(const PipelineExecutorSet* exec_set) {
    auto* driver_exec = dynamic_cast<FakeDriverExecutor*>(exec_set->driver_executor());
    EXPECT_NE(nullptr, driver_exec);
    return driver_exec;
}

// ---------------------------------------------------------------------------
// ExecutorsManagerTest
//
// Tests that do NOT require executor threads to be running.  The manager is
// constructed with null metrics and empty CPUIDs so start() is never called.
// The lambdas passed to for_each_executors only count visits and never
// dereference sub-executor members (which would be null in un-started sets).
// ---------------------------------------------------------------------------

TEST(ExecutorsManagerTest, for_each_executors_visits_only_shared_when_no_exclusive_workgroups) {
    // With null metrics and empty CPUIDs, no exclusive executor can be created,
    // so for_each_executors should visit exactly the one shared executor set.
    PipelineExecutorSetConfig config(8, 2, 2, 4, CpuUtil::CpuIds{}, false, false, nullptr);
    auto manager = std::make_unique<WorkGroupManager>(config, nullptr, make_fake_driver_queue_factory(),
                                                      make_fake_driver_executor_factory());

    int visit_count = 0;
    manager->for_each_executors([&visit_count](PipelineExecutorSet&) { ++visit_count; });

    ASSERT_EQ(1, visit_count);

    manager->destroy();
}

TEST(ExecutorsManagerTest, for_each_executors_still_one_when_shared_workgroups_exist) {
    // Workgroups that cannot obtain exclusive CPUs (empty total_cpuids) use the
    // shared executor set, so the visit count stays 1.
    PipelineExecutorSetConfig config(8, 2, 2, 4, CpuUtil::CpuIds{}, false, false, nullptr);
    auto manager = std::make_unique<WorkGroupManager>(config, nullptr, make_fake_driver_queue_factory(),
                                                      make_fake_driver_executor_factory());

    // Add two workgroups that would request exclusive cores but cannot get them.
    TWorkGroup twg1 = make_exclusive_twg(10, 4);
    TWorkGroup twg2 = make_exclusive_twg(11, 2);
    manager->apply({make_create_op(twg1), make_create_op(twg2)});

    int visit_count = 0;
    manager->for_each_executors([&visit_count](PipelineExecutorSet&) { ++visit_count; });

    // Both workgroups fall back to the shared executor because total_cpuids is
    // empty, so still only 1 executor set is visited.
    ASSERT_EQ(1, visit_count);

    manager->destroy();
}

// ---------------------------------------------------------------------------
// ExecutorsManagerIntegrationTest
//
// Tests that require fully started executor sets, including scan thread pools.
// Using 8 total cores with actual CPUIDs and a real PipelineExecutorMetrics so
// that start() and exclusive-executor creation succeed.
//
// Config layout:
//   total_cores = 8, cpuids = {0..7}
//   initial num_total_connector_scan_threads = 4
//
// After applying an exclusive workgroup with exclusive_cpu_cores = 4:
//   exclusive executor: cpuids = {0,1,2,3}
//   shared executor  : cpuids = {4,5,6,7}
//   num_connector_scan_threads per set = max(1, 4 * 4/8) = 2 (before any change)
// ---------------------------------------------------------------------------

class ExecutorsManagerIntegrationTest : public ::testing::Test {
protected:
    static constexpr uint32_t kTotalCores = 8;
    static constexpr uint32_t kInitConnScanThreads = 4;

    void SetUp() override {
        _metrics = std::make_unique<pipeline::PipelineExecutorMetrics>();
        CpuUtil::CpuIds cpuids{0, 1, 2, 3, 4, 5, 6, 7};
        // enable_bind_cpus=false → thread pools are created without CPU pinning,
        // which avoids requiring actual core-binding privileges in CI.
        _config = std::make_unique<PipelineExecutorSetConfig>(kTotalCores, 2, 2, kInitConnScanThreads, cpuids, false,
                                                              false, _metrics.get());
        _manager =
                std::make_unique<WorkGroupManager>(*_config, nullptr, make_fake_driver_queue_factory(),
                                                   make_fake_driver_executor_factory(&_driver_executor_factory_calls));
        ASSERT_OK(_manager->start());
    }

    void TearDown() override {
        _manager->close();
        _manager->destroy();
        // _manager and _config must die before _metrics so that thread pools
        // (and their executor-metric references) are fully cleaned up first.
        _manager.reset();
        _config.reset();
        _metrics.reset();
    }

    // Apply a CREATE op for a workgroup with the given number of exclusive cores.
    void apply_exclusive_workgroup(int64_t id, int32_t exclusive_cores) {
        _manager->apply({make_create_op(make_exclusive_twg(id, exclusive_cores))});
    }

    // Return the exclusive PipelineExecutorSet for any workgroup that has one,
    // or nullptr if none exists.
    PipelineExecutorSet* find_exclusive_executor() const {
        PipelineExecutorSet* result = nullptr;
        _manager->for_each_workgroup([&result](const WorkGroup& wg) {
            if (wg.exclusive_executors() != nullptr) {
                result = wg.exclusive_executors();
            }
        });
        return result;
    }

    // Return the max_threads of the normal exec_state_report thread pool owned by the executor set.
    static int TEST_exec_state_report_pool_max_threads(const PipelineExecutorSet* exec_set) {
        auto* driver_exec = TEST_fake_driver_executor(exec_set);
        if (driver_exec == nullptr) return -1;
        return driver_exec->exec_state_report_max_threads;
    }

    // Return the max_threads of the priority exec_state_report thread pool owned by the executor set.
    static int TEST_priority_exec_state_report_pool_max_threads(const PipelineExecutorSet* exec_set) {
        auto* driver_exec = TEST_fake_driver_executor(exec_set);
        if (driver_exec == nullptr) return -1;
        return driver_exec->priority_exec_state_report_max_threads;
    }

    std::unique_ptr<pipeline::PipelineExecutorMetrics> _metrics;
    std::unique_ptr<PipelineExecutorSetConfig> _config;
    std::unique_ptr<WorkGroupManager> _manager;
    std::vector<DriverExecutorFactoryCall> _driver_executor_factory_calls;
};

TEST_F(ExecutorsManagerIntegrationTest, start_shared_executors_uses_injected_driver_executor_factory) {
    ASSERT_EQ(1, _driver_executor_factory_calls.size());
    EXPECT_EQ("com", _driver_executor_factory_calls[0].name);
    EXPECT_EQ((CpuUtil::CpuIds{0, 1, 2, 3, 4, 5, 6, 7}), _driver_executor_factory_calls[0].cpuids);
    EXPECT_TRUE(_driver_executor_factory_calls[0].borrowed_cpuids.empty());
    EXPECT_EQ(2u, _driver_executor_factory_calls[0].num_driver_threads);
    EXPECT_EQ(_metrics.get(), _driver_executor_factory_calls[0].metrics);

    auto* driver_exec = TEST_fake_driver_executor(_manager->shared_executors());
    ASSERT_NE(nullptr, driver_exec);
    EXPECT_EQ(2, driver_exec->initialized_num_threads);
}

// With no workgroups, only the shared executor set exists → 1 visit.
TEST_F(ExecutorsManagerIntegrationTest, for_each_executors_visits_only_shared_by_default) {
    int visit_count = 0;
    _manager->for_each_executors([&visit_count](PipelineExecutorSet&) { ++visit_count; });
    ASSERT_EQ(1, visit_count);
}

// After adding an exclusive workgroup (which successfully obtains dedicated CPUs
// because total_cpuids is non-empty), for_each_executors should visit both the
// exclusive and the shared executor set.
TEST_F(ExecutorsManagerIntegrationTest, for_each_executors_visits_both_shared_and_exclusive) {
    const size_t old_num_factory_calls = _driver_executor_factory_calls.size();
    apply_exclusive_workgroup(200, 4);

    int visit_count = 0;
    _manager->for_each_executors([&visit_count](PipelineExecutorSet&) { ++visit_count; });

    // 1 exclusive + 1 shared
    ASSERT_EQ(2, visit_count);
    ASSERT_NE(nullptr, find_exclusive_executor());
    ASSERT_EQ(old_num_factory_calls + 1, _driver_executor_factory_calls.size());
    const auto& call = _driver_executor_factory_calls.back();
    EXPECT_EQ("200", call.name);
    EXPECT_EQ((CpuUtil::CpuIds{0, 1, 2, 3}), call.cpuids);
    EXPECT_TRUE(call.borrowed_cpuids.empty());
    EXPECT_EQ(1u, call.num_driver_threads);
}

TEST(ExecutorsManagerIntegrationTestStandalone, exclusive_driver_executor_factory_failure_falls_back_to_shared) {
    auto metrics = std::make_unique<pipeline::PipelineExecutorMetrics>();
    CpuUtil::CpuIds cpuids{0, 1, 2, 3, 4, 5, 6, 7};
    PipelineExecutorSetConfig config(8, 2, 2, 4, cpuids, false, false, metrics.get());
    std::vector<DriverExecutorFactoryCall> calls;
    auto manager = std::make_unique<WorkGroupManager>(config, nullptr, make_fake_driver_queue_factory(),
                                                      make_fail_after_shared_driver_executor_factory(&calls));
    ASSERT_OK(manager->start());

    TWorkGroup twg = make_exclusive_twg(210, 4);
    manager->apply({make_create_op(twg)});

    int visit_count = 0;
    manager->for_each_executors([&visit_count](PipelineExecutorSet&) { ++visit_count; });
    ASSERT_EQ(1, visit_count);
    ASSERT_EQ(2, calls.size());
    EXPECT_EQ("210", calls.back().name);

    PipelineExecutorSet* exclusive = nullptr;
    PipelineExecutorSet* assigned = nullptr;
    manager->for_each_workgroup([&exclusive, &assigned](const WorkGroup& wg) {
        if (wg.id() == 210) {
            exclusive = wg.exclusive_executors();
            assigned = wg.executors();
        }
    });
    EXPECT_EQ(nullptr, exclusive);
    EXPECT_EQ(manager->shared_executors(), assigned);

    manager->close();
    manager->destroy();
}

// change_num_connector_scan_threads propagates the new thread-count to the
// shared executor set when no exclusive workgroup exists.
TEST_F(ExecutorsManagerIntegrationTest, change_num_connector_scan_threads_updates_shared_executor) {
    // All 8 CPUs belong to shared initially: max(1, 4 * 8/8) = 4.
    ASSERT_EQ(4u, _manager->shared_executors()->num_connector_scan_threads());

    _manager->change_num_connector_scan_threads(8);

    // Shared still holds all 8 CPUs: max(1, 8 * 8/8) = 8.
    ASSERT_EQ(8u, _manager->shared_executors()->num_connector_scan_threads());
}

// After change_num_connector_scan_threads, the new thread-count is reflected
// both by the shared executor set and by every exclusive executor set.
TEST_F(ExecutorsManagerIntegrationTest, change_num_connector_scan_threads_propagates_to_exclusive_executor) {
    apply_exclusive_workgroup(201, 4);
    // exclusive: cpuids={0..3}, shared: cpuids={4..7}
    // Before change: max(1, 4 * 4/8) = 2 for both.
    ASSERT_EQ(2u, _manager->shared_executors()->num_connector_scan_threads());

    PipelineExecutorSet* excl_exec = find_exclusive_executor();
    ASSERT_NE(nullptr, excl_exec);
    ASSERT_EQ(2u, excl_exec->num_connector_scan_threads());

    _manager->change_num_connector_scan_threads(8);

    // After change: max(1, 8 * 4/8) = 4 for both executor sets.
    ASSERT_EQ(4u, _manager->shared_executors()->num_connector_scan_threads());
    ASSERT_EQ(4u, excl_exec->num_connector_scan_threads());
}

// Calling change_num_connector_scan_threads with the same value as the current
// config is a no-op (the guard in change_num_connector_scan_threads prevents
// unnecessary notifications).  Subsequent calls with a different value still
// take effect.
TEST_F(ExecutorsManagerIntegrationTest, change_num_connector_scan_threads_no_op_when_same_value) {
    // All 8 CPUs belong to shared: max(1, 4 * 8/8) = 4.
    ASSERT_EQ(4u, _manager->shared_executors()->num_connector_scan_threads());

    // Call with the same initial value — should not change anything.
    _manager->change_num_connector_scan_threads(kInitConnScanThreads);
    ASSERT_EQ(4u, _manager->shared_executors()->num_connector_scan_threads());

    // A subsequent call with a new value must still take effect.
    _manager->change_num_connector_scan_threads(8);
    ASSERT_EQ(8u, _manager->shared_executors()->num_connector_scan_threads());

    // Calling again with the same new value remains a no-op.
    _manager->change_num_connector_scan_threads(8);
    ASSERT_EQ(8u, _manager->shared_executors()->num_connector_scan_threads());
}

// ---------------------------------------------------------------------------
// exec_state_report thread-pool size tests
//
// These tests mirror the SQL integration test in
// test/sql/test_exec_state_report_threadpool_size/T/test_exec_state_report_threadpool_size
// and verify that change_exec_state_report_max_threads /
// change_priority_exec_state_report_max_threads propagate to the correct
// thread pools in both the shared and exclusive executor sets.
//
// Key difference from connector-scan thread tests: exec_state_reporter pools
// use the raw config value directly (no proportional-to-cpu-cores scaling),
// so shared and exclusive executor sets always get the same per-pool value.
// ---------------------------------------------------------------------------

// change_exec_state_report_max_threads updates the shared executor's normal
// pool when no exclusive workgroup exists.
TEST_F(ExecutorsManagerIntegrationTest, change_exec_state_report_max_threads_updates_shared_executor) {
    // Establish a known baseline (config default is 2).
    _manager->change_exec_state_report_max_threads(2);
    ASSERT_EQ(2, TEST_exec_state_report_pool_max_threads(_manager->shared_executors()));

    _manager->change_exec_state_report_max_threads(4);

    ASSERT_EQ(4, TEST_exec_state_report_pool_max_threads(_manager->shared_executors()));
}

// change_exec_state_report_max_threads propagates to both the shared and the
// exclusive executor set.  Unlike connector-scan threads there is no
// proportional-to-cpu-cores scaling: both sets receive the raw config value.
TEST_F(ExecutorsManagerIntegrationTest, change_exec_state_report_max_threads_propagates_to_exclusive_executor) {
    apply_exclusive_workgroup(300, 4);
    // exclusive: cpuids={0..3}, shared: cpuids={4..7}

    _manager->change_exec_state_report_max_threads(2);
    ASSERT_EQ(2, TEST_exec_state_report_pool_max_threads(_manager->shared_executors()));

    PipelineExecutorSet* excl_exec = find_exclusive_executor();
    ASSERT_NE(nullptr, excl_exec);
    ASSERT_EQ(2, TEST_exec_state_report_pool_max_threads(excl_exec));

    _manager->change_exec_state_report_max_threads(4);

    // Both shared and exclusive receive the full config value — no cpu-fraction scaling.
    ASSERT_EQ(4, TEST_exec_state_report_pool_max_threads(_manager->shared_executors()));
    ASSERT_EQ(4, TEST_exec_state_report_pool_max_threads(excl_exec));
}

// change_priority_exec_state_report_max_threads updates the shared executor's
// priority pool when no exclusive workgroup exists.
TEST_F(ExecutorsManagerIntegrationTest, change_priority_exec_state_report_max_threads_updates_shared_executor) {
    _manager->change_priority_exec_state_report_max_threads(2);
    ASSERT_EQ(2, TEST_priority_exec_state_report_pool_max_threads(_manager->shared_executors()));

    _manager->change_priority_exec_state_report_max_threads(6);

    ASSERT_EQ(6, TEST_priority_exec_state_report_pool_max_threads(_manager->shared_executors()));
}

// change_priority_exec_state_report_max_threads propagates to both the shared
// and the exclusive executor set with the raw config value.
TEST_F(ExecutorsManagerIntegrationTest,
       change_priority_exec_state_report_max_threads_propagates_to_exclusive_executor) {
    apply_exclusive_workgroup(301, 4);

    _manager->change_priority_exec_state_report_max_threads(2);
    ASSERT_EQ(2, TEST_priority_exec_state_report_pool_max_threads(_manager->shared_executors()));

    PipelineExecutorSet* excl_exec = find_exclusive_executor();
    ASSERT_NE(nullptr, excl_exec);
    ASSERT_EQ(2, TEST_priority_exec_state_report_pool_max_threads(excl_exec));

    _manager->change_priority_exec_state_report_max_threads(6);

    ASSERT_EQ(6, TEST_priority_exec_state_report_pool_max_threads(_manager->shared_executors()));
    ASSERT_EQ(6, TEST_priority_exec_state_report_pool_max_threads(excl_exec));
}

// Mirrors the SQL test's three-phase scenario (default→enlarge→shrink) for
// the normal exec_state_report pool with both shared and exclusive executor
// sets present.
TEST_F(ExecutorsManagerIntegrationTest, change_exec_state_report_max_threads_enlarge_then_shrink) {
    apply_exclusive_workgroup(302, 4);
    PipelineExecutorSet* excl_exec = find_exclusive_executor();
    ASSERT_NE(nullptr, excl_exec);

    // Phase 1 – default (2 per pool, matching config default).
    _manager->change_exec_state_report_max_threads(2);
    ASSERT_EQ(2, TEST_exec_state_report_pool_max_threads(_manager->shared_executors()));
    ASSERT_EQ(2, TEST_exec_state_report_pool_max_threads(excl_exec));

    // Phase 2 – enlarge to 4.
    _manager->change_exec_state_report_max_threads(4);
    ASSERT_EQ(4, TEST_exec_state_report_pool_max_threads(_manager->shared_executors()));
    ASSERT_EQ(4, TEST_exec_state_report_pool_max_threads(excl_exec));

    // Phase 3 – shrink back to 2.
    _manager->change_exec_state_report_max_threads(2);
    ASSERT_EQ(2, TEST_exec_state_report_pool_max_threads(_manager->shared_executors()));
    ASSERT_EQ(2, TEST_exec_state_report_pool_max_threads(excl_exec));
}

// Mirrors the SQL test's three-phase scenario for the priority
// exec_state_report pool with both shared and exclusive executor sets present.
TEST_F(ExecutorsManagerIntegrationTest, change_priority_exec_state_report_max_threads_enlarge_then_shrink) {
    apply_exclusive_workgroup(303, 4);
    PipelineExecutorSet* excl_exec = find_exclusive_executor();
    ASSERT_NE(nullptr, excl_exec);

    // Phase 1 – default (2 per pool).
    _manager->change_priority_exec_state_report_max_threads(2);
    ASSERT_EQ(2, TEST_priority_exec_state_report_pool_max_threads(_manager->shared_executors()));
    ASSERT_EQ(2, TEST_priority_exec_state_report_pool_max_threads(excl_exec));

    // Phase 2 – enlarge to 6 (matching the SQL test's priority_exec_state_report_max_threads = 6).
    _manager->change_priority_exec_state_report_max_threads(6);
    ASSERT_EQ(6, TEST_priority_exec_state_report_pool_max_threads(_manager->shared_executors()));
    ASSERT_EQ(6, TEST_priority_exec_state_report_pool_max_threads(excl_exec));

    // Phase 3 – shrink back to 2.
    _manager->change_priority_exec_state_report_max_threads(2);
    ASSERT_EQ(2, TEST_priority_exec_state_report_pool_max_threads(_manager->shared_executors()));
    ASSERT_EQ(2, TEST_priority_exec_state_report_pool_max_threads(excl_exec));
}

} // namespace starrocks::workgroup
