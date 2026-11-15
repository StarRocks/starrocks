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

#include "compute_env/workgroup/work_group_manager.h"

#include <gtest/gtest.h>

#include <limits>
#include <optional>

#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "common/statusor.h"
#include "compute_env/workgroup/work_group.h"
#include "exec_primitive/pipeline/primitives/driver_executor.h"
#include "exec_primitive/pipeline/primitives/driver_queue.h"
#include "runtime/mem_tracker.h"

namespace starrocks::workgroup {
TWorkGroup create_twg(const int64_t id, const int64_t version, const std::string& name, const std::string& mem_pool,
                      const double mem_limit) {
    TWorkGroup twg;
    twg.__set_id(id);
    twg.__set_version(version);
    twg.__set_name(name);
    twg.__set_mem_pool(mem_pool);
    twg.__set_mem_limit(mem_limit);
    return twg;
}

TWorkGroupOp make_twg_op(const TWorkGroup& twg, const TWorkGroupOpType::type op_type) {
    TWorkGroupOp op;
    op.__set_workgroup(twg);
    op.__set_op_type(op_type);
    return op;
}

// In an anonymous namespace, since other test files linked into the same binary define their own FakeDriverQueue.
namespace {
class FakeDriverQueue final : public pipeline::DriverQueue {
public:
    FakeDriverQueue() : DriverQueue(nullptr) {}

    void close() override {}
    void put_back(const pipeline::DriverRawPtr driver) override {}
    void put_back(const std::vector<pipeline::DriverRawPtr>& drivers) override {}
    void put_back_from_executor(const pipeline::DriverRawPtr driver) override {}
    StatusOr<pipeline::DriverRawPtr> take(const bool block) override { return nullptr; }
    void cancel(pipeline::DriverRawPtr driver) override {}
    void update_statistics(const pipeline::DriverRawPtr driver) override {}
    size_t size() const override { return 0; }
    bool should_yield(const pipeline::DriverRawPtr driver, int64_t unaccounted_runtime_ns) const override {
        return false;
    }
    std::vector<pipeline::DriverQueueLevelStats> level_stats() const override { return stats; }

    std::vector<pipeline::DriverQueueLevelStats> stats;
};
} // namespace

WorkGroupManager::DriverQueueFactory make_fake_driver_queue_factory(int* create_count = nullptr) {
    return [create_count](pipeline::DriverQueueMetrics*) {
        if (create_count != nullptr) {
            ++(*create_count);
        }
        return std::make_unique<FakeDriverQueue>();
    };
}

DriverExecutorFactory unused_driver_executor_factory() {
    return [](const std::string& name, const CpuUtil::CpuIds& cpuids,
              const std::vector<CpuUtil::CpuIds>& borrowed_cpuids, uint32_t num_driver_threads,
              pipeline::PipelineExecutorMetrics* metrics,
              const WorkGroupSchedulePolicy& schedule_policy) -> StatusOr<std::unique_ptr<pipeline::DriverExecutor>> {
        (void)name;
        (void)cpuids;
        (void)borrowed_cpuids;
        (void)num_driver_threads;
        (void)metrics;
        (void)schedule_policy;
        return Status::InternalError("unexpected driver executor creation");
    };
}

PARALLEL_TEST(WorkGroupManagerTest, injects_driver_queue_factory_for_accepted_workgroups) {
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    int create_count = 0;
    WorkGroupManager manager(config, nullptr, make_fake_driver_queue_factory(&create_count),
                             unused_driver_executor_factory());

    auto accepted = std::make_shared<WorkGroup>(create_twg(120, 2, "wg120", WorkGroup::DEFAULT_MEM_POOL, 0.5));
    manager.add_workgroup(accepted);

    EXPECT_EQ(1, create_count);
    EXPECT_NE(nullptr, accepted->driver_sched_entity()->queue());

    auto stale = create_twg(120, 1, "wg120_stale", WorkGroup::DEFAULT_MEM_POOL, 0.5);
    manager.apply({make_twg_op(stale, TWorkGroupOpType::WORKGROUP_OP_CREATE)});

    EXPECT_EQ(1, create_count);

    manager.destroy();
}

PARALLEL_TEST(WorkGroupManagerTest, driver_queue_level_metrics) {
    MetricRegistry registry("test");
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    auto driver_queue_factory = [](pipeline::DriverQueueMetrics*) {
        auto queue = std::make_unique<FakeDriverQueue>();
        queue->stats = {{}, {.accu_time_ns = 300, .wait_time_ns = 200, .schedule_count = 2}};
        return queue;
    };
    WorkGroupManager manager(config, &registry, driver_queue_factory, unused_driver_executor_factory());

    auto wg = std::make_shared<WorkGroup>(create_twg(130, 1, "wg130", WorkGroup::DEFAULT_MEM_POOL, 0.5));
    manager.add_workgroup(wg);
    manager.update_metrics();

    auto get_value = [&registry](const std::string& name, const std::string& level) -> std::optional<int64_t> {
        auto* metric = registry.get_metric(name, MetricLabels().add("name", "wg130").add("level", level));
        if (metric == nullptr) {
            return std::nullopt;
        }
        return std::stoll(metric->to_string());
    };
    EXPECT_EQ(0, get_value("resource_group_driver_queue_level_accu_time_ns", "0"));
    EXPECT_EQ(300, get_value("resource_group_driver_queue_level_accu_time_ns", "1"));
    EXPECT_EQ(200, get_value("resource_group_driver_queue_level_wait_time_ns", "1"));
    EXPECT_EQ(2, get_value("resource_group_driver_queue_level_schedule_count", "1"));
    EXPECT_EQ(std::nullopt, get_value("resource_group_driver_queue_level_accu_time_ns", "2"));

    manager.destroy();
}

PARALLEL_TEST(WorkGroupManagerTest, driver_queue_level_counters_survive_workgroup_lifecycle) {
    MetricRegistry registry("test");
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    std::vector<FakeDriverQueue*> queues;
    auto factory = [&queues](pipeline::DriverQueueMetrics*) {
        auto queue = std::make_unique<FakeDriverQueue>();
        queue->stats.resize(2);
        queues.push_back(queue.get());
        return queue;
    };
    WorkGroupManager manager(config, &registry, factory, unused_driver_executor_factory());
    manager.set_workgroup_expiration_time(std::chrono::seconds(0));
    const std::string name = "wg_queue_lifecycle";
    auto twg = create_twg(131, 1, name, WorkGroup::DEFAULT_MEM_POOL, 0.5);
    auto old_wg = std::make_shared<WorkGroup>(twg);
    manager.add_workgroup(old_wg);
    old_wg->incr_num_running_drivers();

    auto get_value = [&registry, &name](const std::string& metric_name) -> std::optional<int64_t> {
        auto* metric = registry.get_metric(metric_name, MetricLabels().add("name", name).add("level", "1"));
        return metric == nullptr ? std::nullopt : std::optional<int64_t>(std::stoll(metric->to_string()));
    };
    auto check_values = [&](int64_t accu_time, int64_t wait_time, int64_t schedule_count) {
        EXPECT_EQ(accu_time, get_value("resource_group_driver_queue_level_accu_time_ns"));
        EXPECT_EQ(wait_time, get_value("resource_group_driver_queue_level_wait_time_ns"));
        EXPECT_EQ(schedule_count, get_value("resource_group_driver_queue_level_schedule_count"));
    };
    queues[0]->stats[1] = {.accu_time_ns = 300, .wait_time_ns = 200, .schedule_count = 2};
    manager.update_metrics();
    check_values(300, 200, 2);

    // The old queue has unsampled work. The new queue can already exceed the previous sample,
    // so comparing successive samples to detect a reset would undercount it.
    queues[0]->stats[1] = {.accu_time_ns = 400, .wait_time_ns = 350, .schedule_count = 3};
    twg.version = 2;
    manager.add_workgroup(std::make_shared<WorkGroup>(twg));
    queues[1]->stats[1] = {.accu_time_ns = 250, .wait_time_ns = 500, .schedule_count = 5};
    manager.update_metrics();
    check_values(250, 850, 8);
    manager.update_metrics();
    check_values(250, 850, 8);

    // A draining version can still run after ALTER. Its increments belong to the same series.
    queues[0]->stats[1].wait_time_ns = 450;
    queues[0]->stats[1].schedule_count = 4;
    queues[1]->stats[1].wait_time_ns = 700;
    queues[1]->stats[1].schedule_count = 7;
    manager.apply({});
    manager.update_metrics();
    check_values(250, 1150, 11);

    // Retiring the queue must preserve its final increments, without counting them twice.
    queues[0]->stats[1].wait_time_ns = 475;
    queues[0]->stats[1].schedule_count = 5;
    old_wg->decr_num_running_drivers();
    manager.apply({});
    manager.update_metrics();
    check_values(250, 1175, 12);

    queues[1]->stats[1].wait_time_ns = 750;
    queues[1]->stats[1].schedule_count = 8;
    twg.version = 3;
    manager.apply({make_twg_op(twg, TWorkGroupOpType::WORKGROUP_OP_DELETE)});
    manager.apply({});
    manager.update_metrics();
    check_values(0, 1225, 13);
    manager.update_metrics();
    check_values(0, 1225, 13);

    // DROP/CREATE with a new ID and the same name reuses the registered metric series.
    auto recreated = create_twg(132, 1, name, WorkGroup::DEFAULT_MEM_POOL, 0.5);
    manager.add_workgroup(std::make_shared<WorkGroup>(recreated));
    queues[2]->stats[1] = {.accu_time_ns = 150, .wait_time_ns = 75, .schedule_count = 1};
    manager.update_metrics();
    check_values(150, 1300, 14);

    queues[2]->stats[1].wait_time_ns = 175;
    queues[2]->stats[1].schedule_count = 2;
    manager.destroy();
    manager.update_metrics();
    check_values(0, 1400, 15);
}

PARALLEL_TEST(WorkGroupManagerTest, driver_queue_level_counter_totals_saturate) {
    MetricRegistry registry("test");
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    int create_count = 0;
    std::vector<FakeDriverQueue*> queues;
    auto factory = [&create_count, &queues](pipeline::DriverQueueMetrics*) {
        auto queue = std::make_unique<FakeDriverQueue>();
        const auto value = create_count++ == 0 ? std::numeric_limits<int64_t>::max() - 1 : 10;
        queue->stats = {{.accu_time_ns = 0, .wait_time_ns = value, .schedule_count = value}};
        queues.push_back(queue.get());
        return queue;
    };
    WorkGroupManager manager(config, &registry, factory, unused_driver_executor_factory());
    manager.add_workgroup(
            std::make_shared<WorkGroup>(create_twg(133, 1, "wg_queue_limit", WorkGroup::DEFAULT_MEM_POOL, 0.5)));
    manager.add_workgroup(
            std::make_shared<WorkGroup>(create_twg(133, 2, "wg_queue_limit", WorkGroup::DEFAULT_MEM_POOL, 0.5)));
    manager.update_metrics();
    auto check_saturated = [&] {
        for (const auto* name :
             {"resource_group_driver_queue_level_wait_time_ns", "resource_group_driver_queue_level_schedule_count"}) {
            auto* metric = registry.get_metric(name, MetricLabels().add("name", "wg_queue_limit").add("level", "0"));
            ASSERT_NE(nullptr, metric);
            EXPECT_EQ(std::to_string(std::numeric_limits<int64_t>::max()), metric->to_string());
        }
    };
    check_saturated();
    for (auto* queue : queues) queue->stats[0] = {.wait_time_ns = -1, .schedule_count = -1};
    manager.update_metrics();
    check_saturated();
    // A complete wrap can make the queue's raw counters positive again; the exported total remains saturated.
    for (auto* queue : queues) queue->stats[0] = {.wait_time_ns = 1, .schedule_count = 1};
    manager.update_metrics();
    check_saturated();
    manager.destroy();
}

TEST(WorkGroupManagerTest, default_workgroup_reverts_after_temporary_manager_destroy) {
    const auto previous_default = WorkGroup::default_workgroup();
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    WorkGroupManager original_manager(config, nullptr, make_fake_driver_queue_factory(),
                                      unused_driver_executor_factory());
    DefaultWorkGroupInitialization original_init(&original_manager, 1);
    const auto original_default = original_manager.get_default_workgroup();
    ASSERT_EQ(original_default, WorkGroup::default_workgroup());

    WorkGroupManager temporary_manager(config, nullptr, make_fake_driver_queue_factory(),
                                       unused_driver_executor_factory());
    DefaultWorkGroupInitialization temporary_init(&temporary_manager, 2);
    const auto temporary_default = temporary_manager.get_default_workgroup();
    ASSERT_EQ(temporary_default, WorkGroup::default_workgroup());
    ASSERT_NE(original_default.get(), temporary_default.get());

    temporary_manager.destroy();
    EXPECT_EQ(original_default, WorkGroup::default_workgroup());

    original_manager.destroy();
    EXPECT_EQ(previous_default, WorkGroup::default_workgroup());
}

PARALLEL_TEST(WorkGroupManagerTest, add_workgroups_different_mem_pools) {
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    auto _manager = std::make_unique<WorkGroupManager>(config, nullptr, make_fake_driver_queue_factory(),
                                                       unused_driver_executor_factory());

    {
        auto wg1 = std::make_shared<WorkGroup>(create_twg(102, 1, "wg", "test_pool", 0.5));
        auto wg2 = std::make_shared<WorkGroup>(create_twg(103, 1, "wg2", "test_pool1", 0.5));
        auto wg3 = std::make_shared<WorkGroup>(create_twg(104, 1, "wg3", WorkGroup::DEFAULT_MEM_POOL, 0.5));

        _manager->add_workgroup(wg1);
        _manager->add_workgroup(wg2);
        _manager->add_workgroup(wg3);

        auto workgroups = _manager->list_workgroups();

        ASSERT_EQ(3, workgroups.size());
        EXPECT_NE(wg2->mem_tracker()->parent(), wg1->mem_tracker()->parent());
        EXPECT_NE(wg2->mem_tracker()->parent(), wg3->mem_tracker()->parent());
        EXPECT_NE(wg1->mem_tracker()->parent(), wg3->mem_tracker()->parent());

        EXPECT_EQ(wg1->mem_tracker()->parent()->type(), MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL);
        EXPECT_EQ(wg2->mem_tracker()->parent()->type(), MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL);
        EXPECT_EQ(wg3->mem_tracker()->parent()->type(), MemTrackerType::QUERY_POOL);
    }
    _manager->destroy();
}

PARALLEL_TEST(WorkGroupManagerTest, add_workgroups_same_mem_pools) {
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    auto _manager = std::make_unique<WorkGroupManager>(config, nullptr, make_fake_driver_queue_factory(),
                                                       unused_driver_executor_factory());

    {
        auto wg1 = std::make_shared<WorkGroup>(create_twg(105, 1, "wg5", "test_pool", 0.5));
        auto wg2 = std::make_shared<WorkGroup>(create_twg(106, 1, "wg6", "test_pool", 0.5));
        auto wg3 = std::make_shared<WorkGroup>(create_twg(107, 1, "wg7", WorkGroup::DEFAULT_MEM_POOL, 0.5));

        _manager->add_workgroup(wg1);
        _manager->add_workgroup(wg2);
        _manager->add_workgroup(wg3);

        auto workgroups = _manager->list_workgroups();

        ASSERT_EQ(3, workgroups.size());
        EXPECT_EQ(wg2->mem_tracker()->parent(), wg1->mem_tracker()->parent());
        EXPECT_EQ(wg2->mem_tracker()->parent()->type(), MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL);
        EXPECT_EQ(wg2->mem_limit_bytes(), wg2->mem_tracker()->parent()->limit());

        EXPECT_NE(wg2->mem_tracker()->parent(), wg3->mem_tracker()->parent());
        EXPECT_EQ(wg3->mem_tracker()->parent()->type(), MemTrackerType::QUERY_POOL);
    }
    _manager->destroy();
}

PARALLEL_TEST(WorkGroupManagerTest, test_if_unused_memory_pools_are_cleaned_up) {
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    auto _manager = std::make_unique<WorkGroupManager>(config, nullptr, make_fake_driver_queue_factory(),
                                                       unused_driver_executor_factory());
    _manager->set_workgroup_expiration_time(std::chrono::seconds(0));
    {
        auto twg1 = create_twg(110, 1, "wg110", "test_pool_2", 0.5);
        auto twg2 = create_twg(111, 1, "wg111", "test_pool_2", 0.5);
        auto twg3 = create_twg(112, 1, "wg112", WorkGroup::DEFAULT_MEM_POOL, 0.5);

        std::vector create_operations{
                make_twg_op(twg1, TWorkGroupOpType::WORKGROUP_OP_CREATE),
                make_twg_op(twg2, TWorkGroupOpType::WORKGROUP_OP_CREATE),
                make_twg_op(twg3, TWorkGroupOpType::WORKGROUP_OP_CREATE),
        };

        _manager->apply(create_operations);

        EXPECT_EQ(_manager->list_memory_pools().size(), 1);

        // Version must be strictly larger, otherwise workgroup will not be deleted
        twg1.version++;
        twg2.version++;

        std::vector delete_operations{make_twg_op(twg1, TWorkGroupOpType::WORKGROUP_OP_DELETE),
                                      make_twg_op(twg2, TWorkGroupOpType::WORKGROUP_OP_DELETE)};

        _manager->apply(delete_operations);
        // The expired workgroups will only get erased in the next call to apply
        _manager->apply({});

        EXPECT_EQ(_manager->list_memory_pools().size(), 0);
    }
    _manager->destroy();
}
} // namespace starrocks::workgroup
