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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "common/config_exec_flow_fwd.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "common/system/cpu_info.h"
#include "common/thread/priority_thread_pool.hpp"
#include "compute_env/query/query_runtime_state.h"
#include "compute_env/workgroup/work_group_manager.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/fragment_context_manager.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_driver.h"
#include "exec/runtime/schedule/event_scheduler.h"
#include "exec_primitive/pipeline/operator.h"
#include "exec_primitive/pipeline/operator_factory.h"
#include "exec_primitive/pipeline/primitives/driver_executor.h"
#include "exec_primitive/pipeline/primitives/driver_queue.h"
#include "exec_primitive/pipeline/primitives/pipeline_metrics.h"
#include "exec_primitive/pipeline/scan/morsel_queue_factory_base.h"
#include "exec_primitive/pipeline/source_operator.h"
#include "runtime/query_context_lifetime.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {
namespace {

class NoopDriverQueue final : public DriverQueue {
public:
    NoopDriverQueue() : DriverQueue(nullptr) {}

    void close() override {}
    void put_back(const DriverRawPtr driver) override {
        queued_drivers.push_back(driver);
        driver->set_in_ready(true);
        driver->set_in_queue(this);
    }
    void put_back(const std::vector<DriverRawPtr>& drivers) override {
        for (auto* driver : drivers) {
            put_back(driver);
        }
    }
    void put_back_from_executor(const DriverRawPtr driver) override { put_back(driver); }
    StatusOr<DriverRawPtr> take(const bool block) override { return nullptr; }
    void cancel(DriverRawPtr driver) override {}
    void update_statistics(const DriverRawPtr driver) override {}
    size_t size() const override { return queued_drivers.size(); }
    bool should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const override { return false; }

    std::vector<DriverRawPtr> queued_drivers;
};

class RecordingDriverExecutor final : public DriverExecutor {
public:
    RecordingDriverExecutor() : DriverExecutor("recording") {}

    void submit(DriverRawPtr driver) override {}
    void cancel(DriverRawPtr driver) override {}
    void close() override {}
    void report_exec_state(QueryContext* query_ctx, FragmentContext* fragment_ctx, const Status& status,
                           bool done) override {
        if (fragment_ctx != nullptr && fragment_ctx->runtime_state() != nullptr) {
            auto* instance_profile = fragment_ctx->runtime_state()->runtime_profile();
            if (instance_profile != nullptr) {
                ObjectPool obj_pool;
                std::vector<RuntimeProfile*> pipeline_profiles;
                instance_profile->get_children(&pipeline_profiles);
                for (auto* pipeline_profile : pipeline_profiles) {
                    std::vector<RuntimeProfile*> driver_profiles;
                    pipeline_profile->get_children(&driver_profiles);
                    if (!driver_profiles.empty()) {
                        (void)RuntimeProfile::merge_isomorphic_profiles(&obj_pool, driver_profiles);
                        merged_profile_count.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }
        report_exec_state_count.fetch_add(1, std::memory_order_release);
    }
    void report_audit_statistics(QueryContext* query_ctx, FragmentContext* fragment_ctx) override {}
    void report_audit_statistics_on_failure(QueryContext* query_ctx, FragmentContext* fragment_ctx) override {
        ++audit_failure_count;
        last_query_ctx = query_ctx;
        last_fragment_ctx = fragment_ctx;
    }
    void iterate_immutable_blocking_driver(const ConstDriverConsumer& call) const override {}
    void bind_cpus(const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) override {}

    std::atomic<int> report_exec_state_count{0};
    std::atomic<int> merged_profile_count{0};
    int audit_failure_count = 0;
    QueryContext* last_query_ctx = nullptr;
    FragmentContext* last_fragment_ctx = nullptr;
};

// Liveness tracker shared between the MorselQueueFactory stand-in and the
// Pipeline stand-in so the pipeline teardown can observe factory liveness without
// dereferencing freed memory itself.
struct MorselFactoryLiveness {
    bool factory_alive = true;
    bool factory_destroyed_before_pipeline = false;
};

// Stands in for the MorselQueueFactory owned by FragmentContext::_morsel_queue_factories.
// In production a scan operator's ConnectorChunkSource::close() calls into this
// object during fragment teardown.
class TrackingMorselQueueFactory final : public MorselQueueFactory {
public:
    explicit TrackingMorselQueueFactory(std::shared_ptr<MorselFactoryLiveness> live) : _live(std::move(live)) {}
    ~TrackingMorselQueueFactory() override { _live->factory_alive = false; }

    MorselQueue* create(int /*driver_sequence*/) override { return nullptr; }
    size_t size() const override { return 0; }
    size_t num_original_morsels() const override { return 0; }
    bool is_shared() const override { return false; }
    bool could_local_shuffle() const override { return false; }

private:
    std::shared_ptr<MorselFactoryLiveness> _live;
};

// Stands in for the operator subtree owned by FragmentContext::_pipelines. The
// real teardown chain is ~PipelineDriver -> ~ScanOperator ->
// ConnectorChunkSource::close() -> MorselQueueFactory::mark_split_source_morsel_finished();
// this destructor observes whether the factory is still alive at that point.
class TrackingPipeline final : public Pipeline {
public:
    explicit TrackingPipeline(std::shared_ptr<MorselFactoryLiveness> live)
            : Pipeline(0, OpFactories{}, nullptr), _live(std::move(live)) {}
    ~TrackingPipeline() override {
        if (!_live->factory_alive) {
            _live->factory_destroyed_before_pipeline = true;
        }
    }

private:
    std::shared_ptr<MorselFactoryLiveness> _live;
};

std::shared_ptr<FragmentContext> make_fragment_context(const TUniqueId& query_id, const TUniqueId& fragment_id) {
    TQueryOptions query_options;
    query_options.query_type = TQueryType::SELECT;

    auto fragment_ctx = std::make_shared<FragmentContext>();
    fragment_ctx->set_query_id(query_id);
    fragment_ctx->set_fragment_instance_id(fragment_id);
    fragment_ctx->set_runtime_state(std::make_shared<RuntimeState>(fragment_id, query_options, TQueryGlobals{},
                                                                   static_cast<const QueryExecutionServices*>(nullptr),
                                                                   static_cast<ExecEnv*>(nullptr)));
    return fragment_ctx;
}

TEST(FragmentContextManagerExecRuntimeTest, RegisterGetAndUnregisterFragments) {
    FragmentContextManager manager;
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    TUniqueId fragment_id;
    fragment_id.hi = 3;
    fragment_id.lo = 4;

    auto fragment_ctx = make_fragment_context(query_id, fragment_id);
    ASSERT_OK(manager.register_ctx(fragment_id, fragment_ctx));
    EXPECT_EQ(fragment_ctx, manager.get(fragment_id));

    auto duplicate_ctx = make_fragment_context(query_id, fragment_id);
    EXPECT_FALSE(manager.register_ctx(fragment_id, duplicate_ctx).ok());

    manager.unregister(fragment_id);
    EXPECT_EQ(nullptr, manager.get(fragment_id));
}

TEST(FragmentContextExecRuntimeTest, FailureAuditUsesInjectedExecutionServicesWithoutExecEnv) {
    CpuInfo::init();

    RecordingDriverExecutor* recording_executor = nullptr;
    workgroup::PipelineExecutorSetConfig config{
            1, 1, 1, 1, CpuUtil::CpuIds{}, false, false, PipelineExecutorMetrics::instance()};
    workgroup::WorkGroupManager manager(
            config, nullptr, [](DriverQueueMetrics*) { return std::make_unique<NoopDriverQueue>(); },
            [&recording_executor](const std::string& name, const CpuUtil::CpuIds& cpuids,
                                  const std::vector<CpuUtil::CpuIds>& borrowed_cpuids, uint32_t num_driver_threads,
                                  PipelineExecutorMetrics* metrics,
                                  const workgroup::WorkGroupSchedulePolicy& schedule_policy)
                    -> StatusOr<std::unique_ptr<DriverExecutor>> {
                (void)name;
                (void)cpuids;
                (void)borrowed_cpuids;
                (void)num_driver_threads;
                (void)metrics;
                (void)schedule_policy;
                auto executor = std::make_unique<RecordingDriverExecutor>();
                recording_executor = executor.get();
                return std::move(executor);
            });
    ASSERT_OK(manager.start());

    ExecutionEnv execution_services;
    execution_services.workgroup_manager = &manager;
    QueryExecutionServices query_execution_services;
    query_execution_services.execution = &execution_services;

    FragmentContext fragment_ctx;
    auto runtime_state = std::make_shared<RuntimeState>(TQueryGlobals{});
    runtime_state->set_query_execution_services(&query_execution_services);
    auto* query_ctx = reinterpret_cast<QueryContext*>(static_cast<uintptr_t>(0x1234));
    runtime_state->set_query_ctx(query_ctx, nullptr, nullptr);
    runtime_state->set_fragment_ctx(&fragment_ctx, &fragment_ctx.fragment_runtime_state());
    runtime_state->set_fragment_dict_state(fragment_ctx.dict_state());
    fragment_ctx.set_runtime_state(std::move(runtime_state));

    fragment_ctx.set_final_status(Status::InternalError("injected failure"));

    ASSERT_NE(nullptr, recording_executor);
    EXPECT_EQ(1, recording_executor->audit_failure_count);
    EXPECT_EQ(query_ctx, recording_executor->last_query_ctx);
    EXPECT_EQ(&fragment_ctx, recording_executor->last_fragment_ctx);
    manager.close();
}

// Regression test for the scan-teardown use-after-free that crashed CN nodes:
// ConnectorChunkSource::close() runs from ~ScanOperator while FragmentContext's
// _pipelines are being destroyed, and calls back into a MorselQueueFactory owned
// by _morsel_queue_factories. The factory must therefore be destroyed AFTER the
// pipelines, i.e. declared before them. This pins that ordering so a future
// reshuffle of the member declarations cannot silently reintroduce the UAF.
TEST(FragmentContextExecRuntimeTest, MorselQueueFactoryOutlivesPipelines) {
    TUniqueId query_id;
    query_id.hi = 10;
    query_id.lo = 20;
    TUniqueId fragment_id;
    fragment_id.hi = 30;
    fragment_id.lo = 40;

    auto live = std::make_shared<MorselFactoryLiveness>();
    {
        auto fragment_ctx = make_fragment_context(query_id, fragment_id);
        fragment_ctx->morsel_queue_factories().emplace(0, std::make_unique<TrackingMorselQueueFactory>(live));

        Pipelines pipelines;
        pipelines.emplace_back(std::make_shared<TrackingPipeline>(live));
        fragment_ctx->set_pipelines(ExecutionGroups{}, std::move(pipelines));
    } // ~FragmentContext destroys members in reverse declaration order here.

    EXPECT_FALSE(live->factory_destroyed_before_pipeline)
            << "MorselQueueFactory was destroyed before the pipelines; scan teardown would use-after-free";
}

class DummySourceOperator final : public SourceOperator {
public:
    DummySourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "dummy_source", plan_node_id, false, driver_sequence) {}

    bool has_output() const override { return false; }
    bool is_finished() const override { return false; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
};

class DummySourceOperatorFactory final : public SourceOperatorFactory {
public:
    DummySourceOperatorFactory(int32_t id, int32_t plan_node_id, size_t dop, AdaptiveState adaptive_state)
            : SourceOperatorFactory(id, "dummy_source", plan_node_id), _adaptive_state(adaptive_state) {
        set_degree_of_parallelism(dop);
    }

    AdaptiveState adaptive_initial_state() const override { return _adaptive_state; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<DummySourceOperator>(this, id(), plan_node_id(), driver_sequence);
    }

private:
    AdaptiveState _adaptive_state;
};

class DummySinkOperator final : public Operator {
public:
    DummySinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "dummy_sink", plan_node_id, false, driver_sequence) {}

    bool has_output() const override { return false; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return false; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

class DummySinkOperatorFactory final : public OperatorFactory {
public:
    DummySinkOperatorFactory(int32_t id, int32_t plan_node_id) : OperatorFactory(id, "dummy_sink", plan_node_id) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<DummySinkOperator>(this, id(), plan_node_id(), driver_sequence);
    }
};

void instantiate_test_drivers(FragmentContext* fragment_ctx, QueryRuntimeState* query_runtime_state,
                              Pipeline* pipeline) {
    size_t dop = pipeline->degree_of_parallelism();
    auto& drivers = pipeline->mutable_drivers();
    drivers.reserve(dop);
    for (size_t i = 0; i < dop; ++i) {
        auto operators = pipeline->create_operators(dop, i);
        auto driver = std::make_shared<PipelineDriver>(
                std::move(operators), query_runtime_state, &fragment_ctx->fragment_runtime_state(),
                pipeline->pipeline_event(), pipeline, nullptr, fragment_ctx->next_driver_id());
        pipeline->setup_drivers_profile(driver);
        drivers.emplace_back(std::move(driver));
    }
}

TEST(FragmentContextExecRuntimeTest, ReportExecStateSkipsUnpublishedAdaptivePipeline) {
    CpuInfo::init();

    RecordingDriverExecutor* recording_executor = nullptr;
    workgroup::PipelineExecutorSetConfig config{
            1, 1, 1, 1, CpuUtil::CpuIds{}, false, false, PipelineExecutorMetrics::instance()};
    workgroup::WorkGroupManager manager(
            config, nullptr, [](DriverQueueMetrics*) { return std::make_unique<NoopDriverQueue>(); },
            [&recording_executor](
                    const std::string&, const CpuUtil::CpuIds&, const std::vector<CpuUtil::CpuIds>&, uint32_t,
                    PipelineExecutorMetrics*,
                    const workgroup::WorkGroupSchedulePolicy&) -> StatusOr<std::unique_ptr<DriverExecutor>> {
                auto executor = std::make_unique<RecordingDriverExecutor>();
                recording_executor = executor.get();
                return std::move(executor);
            });
    ASSERT_OK(manager.start());
    auto default_wg = std::make_shared<workgroup::WorkGroup>(
            "default_wg", workgroup::WorkGroup::DEFAULT_WG_ID, workgroup::WorkGroup::DEFAULT_VERSION, 1, 1.0, 0, 1.0,
            workgroup::WorkGroupType::WG_DEFAULT, workgroup::WorkGroup::DEFAULT_MEM_POOL);
    default_wg->set_shared_executors(manager.shared_executors());

    PriorityThreadPool prepare_pool("ut_prepare", 2, 16);
    ExecutionEnv execution_services;
    execution_services.workgroup_manager = &manager;
    execution_services.pipeline_prepare_pool = &prepare_pool;
    QueryExecutionServices query_execution_services;
    query_execution_services.execution = &execution_services;

    QueryRuntimeState query_runtime_state;
    query_runtime_state.set_enable_profile();
    query_runtime_state.set_runtime_profile_report_interval(0);

    FragmentContext fragment_ctx;
    auto runtime_state = std::make_shared<RuntimeState>(TQueryGlobals{});
    runtime_state->set_query_execution_services(&query_execution_services);
    auto* query_ctx = reinterpret_cast<QueryContext*>(static_cast<uintptr_t>(0x1234));
    runtime_state->set_query_ctx(query_ctx, &query_runtime_state, nullptr);
    runtime_state->set_fragment_ctx(&fragment_ctx, &fragment_ctx.fragment_runtime_state());
    runtime_state->set_fragment_dict_state(fragment_ctx.dict_state());
    fragment_ctx.set_runtime_state(std::move(runtime_state));
    auto* state = fragment_ctx.runtime_state();
    fragment_ctx.set_workgroup(default_wg);

    auto exec_group = std::make_shared<NormalExecutionGroup>();
    OpFactories p0_ops{
            std::make_shared<DummySourceOperatorFactory>(1, 10, 2, SourceOperatorFactory::AdaptiveState::ACTIVE),
            std::make_shared<DummySinkOperatorFactory>(2, 20)};
    OpFactories p1_ops{
            std::make_shared<DummySourceOperatorFactory>(3, 30, 2, SourceOperatorFactory::AdaptiveState::INACTIVE),
            std::make_shared<DummySinkOperatorFactory>(4, 40)};
    auto p0 = std::make_shared<Pipeline>(0, std::move(p0_ops), exec_group.get());
    auto p1 = std::make_shared<Pipeline>(1, std::move(p1_ops), exec_group.get());
    exec_group->add_pipeline(p0.get());
    exec_group->add_pipeline(p1.get());
    fragment_ctx.set_pipelines({exec_group}, {p0, p1});

    ASSERT_OK(fragment_ctx.prepare_all_pipelines());
    p0->setup_pipeline_profile(state);
    instantiate_test_drivers(&fragment_ctx, &query_runtime_state, p0.get());
    ASSERT_OK(fragment_ctx.prepare_active_drivers());

    EXPECT_TRUE(p0->is_drivers_prepared());
    EXPECT_FALSE(p1->is_drivers_prepared());

    // Simulate adaptive DOP instantiating drivers for p1 before prepare/prepare_local_state finish.
    // Like instantiate_pipeline_drivers(), the pipeline profile is attached at instantiation time.
    p1->setup_pipeline_profile(state);
    instantiate_test_drivers(&fragment_ctx, &query_runtime_state, p1.get());
    ASSERT_EQ(2, p1->drivers().size());
    ASSERT_FALSE(p1->drivers()[0]->local_prepare_is_done());

    // Reporting and driver iteration must skip unprepared p1 without crashing or blocking p0's report.
    fragment_ctx.report_exec_state_if_necessary();
    ASSERT_NE(nullptr, recording_executor);
    EXPECT_EQ(1, recording_executor->report_exec_state_count.load());

    size_t observed_drivers = 0;
    fragment_ctx.iterate_drivers([&](const DriverPtr& driver) {
        EXPECT_TRUE(driver->local_prepare_is_done());
        ++observed_drivers;
    });
    EXPECT_EQ(2, observed_drivers);
    EXPECT_NE(nullptr, state->runtime_profile()->get_child("Pipeline (id=0)"));
    // p1's profile is already attached; only the drivers are skipped until they are prepared.
    EXPECT_NE(nullptr, state->runtime_profile()->get_child("Pipeline (id=1)"));

    // Once p1 finishes prepare + prepare_local_state and marks prepared, it becomes visible to reporting and iteration.
    for (const auto& driver : p1->drivers()) {
        ASSERT_OK(driver->prepare(state));
        ASSERT_OK(driver->prepare_local_state(state));
    }
    p1->set_drivers_prepared(true);
    EXPECT_TRUE(p1->is_drivers_prepared());
    EXPECT_EQ(2, p1->drivers().size());

    fragment_ctx.report_exec_state_if_necessary();
    EXPECT_EQ(2, recording_executor->report_exec_state_count.load());

    observed_drivers = 0;
    fragment_ctx.iterate_drivers([&](const DriverPtr& driver) {
        EXPECT_TRUE(driver->local_prepare_is_done());
        ++observed_drivers;
    });
    EXPECT_EQ(4, observed_drivers);

    prepare_pool.shutdown();
    prepare_pool.join();
    manager.close();
}

TEST(FragmentContextExecRuntimeTest, ConcurrentAdaptivePublishAndProfileReport) {
    CpuInfo::init();

    RecordingDriverExecutor* recording_executor = nullptr;
    workgroup::PipelineExecutorSetConfig config{
            1, 1, 1, 1, CpuUtil::CpuIds{}, false, false, PipelineExecutorMetrics::instance()};
    workgroup::WorkGroupManager manager(
            config, nullptr, [](DriverQueueMetrics*) { return std::make_unique<NoopDriverQueue>(); },
            [&recording_executor](
                    const std::string&, const CpuUtil::CpuIds&, const std::vector<CpuUtil::CpuIds>&, uint32_t,
                    PipelineExecutorMetrics*,
                    const workgroup::WorkGroupSchedulePolicy&) -> StatusOr<std::unique_ptr<DriverExecutor>> {
                auto executor = std::make_unique<RecordingDriverExecutor>();
                recording_executor = executor.get();
                return std::move(executor);
            });
    ASSERT_OK(manager.start());
    auto default_wg = std::make_shared<workgroup::WorkGroup>(
            "default_wg", workgroup::WorkGroup::DEFAULT_WG_ID, workgroup::WorkGroup::DEFAULT_VERSION, 1, 1.0, 0, 1.0,
            workgroup::WorkGroupType::WG_DEFAULT, workgroup::WorkGroup::DEFAULT_MEM_POOL);
    default_wg->set_shared_executors(manager.shared_executors());

    PriorityThreadPool prepare_pool("ut_prepare_race", 2, 64);
    ExecutionEnv execution_services;
    execution_services.workgroup_manager = &manager;
    execution_services.pipeline_prepare_pool = &prepare_pool;
    QueryExecutionServices query_execution_services;
    query_execution_services.execution = &execution_services;

    for (int round = 0; round < 32; ++round) {
        QueryRuntimeState query_runtime_state;
        query_runtime_state.set_enable_profile();
        query_runtime_state.set_runtime_profile_report_interval(0);

        FragmentContext fragment_ctx;
        auto runtime_state = std::make_shared<RuntimeState>(TQueryGlobals{});
        runtime_state->set_query_execution_services(&query_execution_services);
        auto* query_ctx = reinterpret_cast<QueryContext*>(static_cast<uintptr_t>(0x1234));
        runtime_state->set_query_ctx(query_ctx, &query_runtime_state, nullptr);
        runtime_state->set_fragment_ctx(&fragment_ctx, &fragment_ctx.fragment_runtime_state());
        runtime_state->set_fragment_dict_state(fragment_ctx.dict_state());
        fragment_ctx.set_runtime_state(std::move(runtime_state));
        auto* state = fragment_ctx.runtime_state();
        fragment_ctx.set_workgroup(default_wg);

        auto exec_group = std::make_shared<NormalExecutionGroup>();
        OpFactories p0_ops{
                std::make_shared<DummySourceOperatorFactory>(1, 10, 2, SourceOperatorFactory::AdaptiveState::ACTIVE),
                std::make_shared<DummySinkOperatorFactory>(2, 20)};
        OpFactories p1_ops{
                std::make_shared<DummySourceOperatorFactory>(3, 30, 8, SourceOperatorFactory::AdaptiveState::INACTIVE),
                std::make_shared<DummySinkOperatorFactory>(4, 40)};
        auto p0 = std::make_shared<Pipeline>(0, std::move(p0_ops), exec_group.get());
        auto p1 = std::make_shared<Pipeline>(1, std::move(p1_ops), exec_group.get());
        exec_group->add_pipeline(p0.get());
        exec_group->add_pipeline(p1.get());
        fragment_ctx.set_pipelines({exec_group}, {p0, p1});

        ASSERT_OK(fragment_ctx.prepare_all_pipelines());
        p0->setup_pipeline_profile(state);
        instantiate_test_drivers(&fragment_ctx, &query_runtime_state, p0.get());
        ASSERT_OK(fragment_ctx.prepare_active_drivers());

        ASSERT_NE(nullptr, recording_executor);
        const int base_merges = recording_executor->merged_profile_count.load(std::memory_order_acquire);

        std::atomic<bool> stop{false};
        std::thread reporter([&]() {
            while (!stop.load(std::memory_order_acquire)) {
                fragment_ctx.report_exec_state_if_necessary();
                fragment_ctx.iterate_drivers(
                        [](const DriverPtr& driver) { EXPECT_TRUE(driver->local_prepare_is_done()); });
                std::this_thread::yield();
            }
        });

        p1->setup_pipeline_profile(state);
        instantiate_test_drivers(&fragment_ctx, &query_runtime_state, p1.get());

        // Ensure the reporter thread executes real profile merging on the instantiated (but unprepared) p1.
        const int during_prepare_target =
                recording_executor->report_exec_state_count.load(std::memory_order_acquire) + 1;
        while (recording_executor->report_exec_state_count.load(std::memory_order_acquire) < during_prepare_target) {
            std::this_thread::yield();
        }
        EXPECT_GT(recording_executor->merged_profile_count.load(std::memory_order_acquire), base_merges);

        for (const auto& driver : p1->drivers()) {
            ASSERT_OK(driver->prepare(state));
            ASSERT_OK(driver->prepare_local_state(state));
        }
        p1->set_drivers_prepared(true);

        // Wait for the reporter thread itself to observe the published pipeline at least once.
        const int post_publish_target = recording_executor->report_exec_state_count.load(std::memory_order_acquire) + 1;
        while (recording_executor->report_exec_state_count.load(std::memory_order_acquire) < post_publish_target) {
            std::this_thread::yield();
        }
        stop.store(true, std::memory_order_release);
        reporter.join();

        size_t total_visible = 0;
        fragment_ctx.iterate_drivers([&](const DriverPtr& driver) {
            EXPECT_TRUE(driver->local_prepare_is_done());
            ++total_visible;
        });
        EXPECT_EQ(10, total_visible);
    }

    prepare_pool.shutdown();
    prepare_pool.join();
    manager.close();
}

TEST(FragmentContextExecRuntimeTest, CancelDuringAdaptivePrepareWakesDriverOnBlock) {
    CpuInfo::init();

    RecordingDriverExecutor* recording_executor = nullptr;
    workgroup::PipelineExecutorSetConfig config{
            1, 1, 1, 1, CpuUtil::CpuIds{}, false, false, PipelineExecutorMetrics::instance()};
    workgroup::WorkGroupManager manager(
            config, nullptr, [](DriverQueueMetrics*) { return std::make_unique<NoopDriverQueue>(); },
            [&recording_executor](
                    const std::string&, const CpuUtil::CpuIds&, const std::vector<CpuUtil::CpuIds>&, uint32_t,
                    PipelineExecutorMetrics*,
                    const workgroup::WorkGroupSchedulePolicy&) -> StatusOr<std::unique_ptr<DriverExecutor>> {
                auto executor = std::make_unique<RecordingDriverExecutor>();
                recording_executor = executor.get();
                return std::move(executor);
            });
    ASSERT_OK(manager.start());
    auto default_wg = std::make_shared<workgroup::WorkGroup>(
            "default_wg", workgroup::WorkGroup::DEFAULT_WG_ID, workgroup::WorkGroup::DEFAULT_VERSION, 1, 1.0, 0, 1.0,
            workgroup::WorkGroupType::WG_DEFAULT, workgroup::WorkGroup::DEFAULT_MEM_POOL);
    default_wg->set_shared_executors(manager.shared_executors());

    PriorityThreadPool prepare_pool("ut_cancel_prepare", 2, 16);
    ExecutionEnv execution_services;
    execution_services.workgroup_manager = &manager;
    execution_services.pipeline_prepare_pool = &prepare_pool;
    QueryExecutionServices query_execution_services;
    query_execution_services.execution = &execution_services;

    QueryRuntimeState query_runtime_state;
    auto query_lifetime = std::make_shared<QueryContextLifetime>();

    FragmentContext fragment_ctx;
    auto runtime_state = std::make_shared<RuntimeState>(TQueryGlobals{});
    runtime_state->set_query_execution_services(&query_execution_services);
    auto* query_ctx = reinterpret_cast<QueryContext*>(static_cast<uintptr_t>(0x1234));
    runtime_state->set_query_ctx(query_ctx, &query_runtime_state, nullptr);
    runtime_state->set_query_ctx_lifetime(query_lifetime);
    runtime_state->set_fragment_ctx(&fragment_ctx, &fragment_ctx.fragment_runtime_state());
    runtime_state->set_fragment_dict_state(fragment_ctx.dict_state());
    runtime_state->set_enable_event_scheduler(true);
    fragment_ctx.set_runtime_state(std::move(runtime_state));
    auto* state = fragment_ctx.runtime_state();
    fragment_ctx.set_workgroup(default_wg);
    fragment_ctx.init_event_scheduler();

    NoopDriverQueue recording_queue;
    fragment_ctx.event_scheduler()->attach_queue(&recording_queue);

    auto exec_group = std::make_shared<NormalExecutionGroup>();
    OpFactories p0_ops{
            std::make_shared<DummySourceOperatorFactory>(1, 10, 1, SourceOperatorFactory::AdaptiveState::ACTIVE),
            std::make_shared<DummySinkOperatorFactory>(2, 20)};
    OpFactories p1_ops{
            std::make_shared<DummySourceOperatorFactory>(3, 30, 1, SourceOperatorFactory::AdaptiveState::INACTIVE),
            std::make_shared<DummySinkOperatorFactory>(4, 40)};
    auto p0 = std::make_shared<Pipeline>(0, std::move(p0_ops), exec_group.get());
    auto p1 = std::make_shared<Pipeline>(1, std::move(p1_ops), exec_group.get());
    exec_group->add_pipeline(p0.get());
    exec_group->add_pipeline(p1.get());
    fragment_ctx.set_pipelines({exec_group}, {p0, p1});

    ASSERT_OK(fragment_ctx.prepare_all_pipelines());
    instantiate_test_drivers(&fragment_ctx, &query_runtime_state, p0.get());
    p0->drivers()[0]->set_observer(fragment_ctx.event_scheduler()->create_driver_observer(p0->drivers()[0].get()));
    p0->drivers()[0]->assign_observer();
    ASSERT_OK(fragment_ctx.prepare_active_drivers());

    // Instantiate adaptive pipeline p1 with an observer, then cancel before p1 finishes prepare.
    instantiate_test_drivers(&fragment_ctx, &query_runtime_state, p1.get());
    ASSERT_EQ(1, p1->drivers().size());
    auto p1_driver = p1->drivers()[0];
    p1_driver->set_observer(fragment_ctx.event_scheduler()->create_driver_observer(p1_driver.get()));
    p1_driver->assign_observer();
    ASSERT_FALSE(p1->is_drivers_prepared());

    fragment_ctx.cancel(Status::Cancelled("cancel during adaptive prepare"));
    ASSERT_TRUE(state->is_cancelled());
    EXPECT_FALSE(p1_driver->need_check_reschedule());

    // Finish prepare after cancellation and simulate submit() parking the empty-input driver in EventScheduler.
    ASSERT_OK(p1_driver->prepare(state));
    ASSERT_OK(p1_driver->prepare_local_state(state));
    p1->set_drivers_prepared(true);

    ASSERT_FALSE(p1_driver->source_operator()->has_output());
    ASSERT_FALSE(p1_driver->source_operator()->is_finished());
    p1_driver->set_driver_state(DriverState::INPUT_EMPTY);
    fragment_ctx.event_scheduler()->add_blocked_driver(p1_driver.get());

    ASSERT_EQ(1, recording_queue.queued_drivers.size());
    EXPECT_EQ(p1_driver.get(), recording_queue.queued_drivers.back());
    EXPECT_FALSE(p1_driver->is_in_blocked());
    ASSERT_NE(nullptr, recording_executor);

    prepare_pool.shutdown();
    prepare_pool.join();
    manager.close();
}

} // namespace
} // namespace starrocks::pipeline
