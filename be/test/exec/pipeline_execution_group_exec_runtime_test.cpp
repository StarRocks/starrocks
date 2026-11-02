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
#include <vector>

#include "base/testutil/assert.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_driver.h"
#include "exec_primitive/pipeline/operator.h"
#include "exec_primitive/pipeline/operator_factory.h"
#include "exec_primitive/pipeline/primitives/driver_executor.h"
#include "exec_primitive/pipeline/primitives/execution_group_lifecycle.h"
#include "exec_primitive/pipeline/scan/fixed_morsel_queue.h"
#include "exec_primitive/pipeline/source_operator.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

class TestSourceOperator final : public SourceOperator {
public:
    TestSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "test_source", plan_node_id, false, driver_sequence) {}

    bool has_output() const override { return false; }
    bool is_finished() const override { return false; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
};

class ActiveSourceOperatorFactory final : public SourceOperatorFactory {
public:
    explicit ActiveSourceOperatorFactory(size_t dop) : SourceOperatorFactory(1, "test_source", 10) {
        set_degree_of_parallelism(dop);
    }

    AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSourceOperator>(this, id(), plan_node_id(), driver_sequence);
    }
};

// Only the group scheduler/lifecycle is concrete here. This source models a driver-owned
// task queue; lake-reader + blocking aggregate execution requires SQL integration tests.
class GroupedMockSourceOperator final : public SourceOperator {
public:
    GroupedMockSourceOperator(OperatorFactory* factory, int32_t sequence)
            : SourceOperator(factory, 1, "grouped_mock", 10, false, sequence) {
        Morsels tasks;
        if (sequence < 3) {
            for (int i = 0; i < 2; ++i) tasks.emplace_back(std::make_unique<ScanMorsel>(10, TScanRange{}));
        }
        queue = std::make_unique<FixedMorselQueue>(std::move(tasks));
    }
    bool has_output() const override { return queue != nullptr && !queue->empty(); }
    bool is_finished() const override { return !has_output(); }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState*) override {
        ASSIGN_OR_RETURN(auto task, queue->try_get());
        if (task != nullptr) ++consumed_tasks;
        return nullptr;
    }
    Status set_finished(RuntimeState*) override {
        queue.reset();
        return Status::OK();
    }
    Status set_cancelled(RuntimeState*) override {
        ++cancel_count;
        queue.reset();
        return Status::OK();
    }
    void close(RuntimeState*) override {
        ++close_count;
        queue.reset();
    }
    std::unique_ptr<FixedMorselQueue> queue;
    int consumed_tasks = 0;
    int cancel_count = 0;
    int close_count = 0;
};

class GroupedMockSourceFactory final : public SourceOperatorFactory {
public:
    GroupedMockSourceFactory() : SourceOperatorFactory(1, "grouped_mock", 10) { set_degree_of_parallelism(4); }
    AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }
    OperatorPtr create(int32_t, int32_t sequence) override {
        return std::make_shared<GroupedMockSourceOperator>(this, sequence);
    }
    void close(RuntimeState* state) override {
        ++close_count;
        SourceOperatorFactory::close(state);
    }
    int close_count = 0;
};

class TestSinkOperator final : public Operator {
public:
    TestSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "test_sink", plan_node_id, false, driver_sequence) {}

    bool has_output() const override { return false; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return false; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

class TestSinkOperatorFactory final : public OperatorFactory {
public:
    TestSinkOperatorFactory() : OperatorFactory(2, "test_sink", 20) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSinkOperator>(this, id(), plan_node_id(), driver_sequence);
    }
};

class RecordingDriverExecutor final : public DriverExecutor {
public:
    RecordingDriverExecutor() : DriverExecutor("recording") {}

    void submit(DriverRawPtr driver) override { submitted_driver_ids.push_back(driver->driver_id()); }
    void cancel(DriverRawPtr driver) override {}
    void close() override {}
    void report_exec_state(QueryContext* query_ctx, FragmentContext* fragment_ctx, const Status& status,
                           bool done) override {}
    void report_audit_statistics(QueryContext* query_ctx, FragmentContext* fragment_ctx) override {}
    void report_audit_statistics_on_failure(QueryContext* query_ctx, FragmentContext* fragment_ctx) override {}
    void iterate_immutable_blocking_driver(const ConstDriverConsumer& call) const override {}
    void bind_cpus(const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) override {}

    std::vector<int32_t> submitted_driver_ids;
};

class RecordingExecutionGroupLifecycle final : public ExecutionGroupLifecycle {
public:
    void on_execution_group_finished() override { ++num_finished_groups; }

    size_t num_finished_groups = 0;
};

void add_drivers(Pipeline* pipeline, size_t dop) {
    for (size_t i = 0; i < dop; ++i) {
        auto operators = pipeline->create_operators(dop, i);
        pipeline->mutable_drivers().emplace_back(std::make_shared<PipelineDriver>(
                operators, nullptr, nullptr, pipeline->pipeline_event(), pipeline, nullptr, i));
    }
}

} // namespace

TEST(ExecutionGroupExecRuntimeTest, NotifiesLifecycleOnceWhenAllPipelinesFinish) {
    NormalExecutionGroup group;
    RecordingExecutionGroupLifecycle lifecycle;
    group.attach_execution_group_lifecycle(&lifecycle);

    Pipeline pipeline1(1, {}, &group);
    Pipeline pipeline2(2, {}, &group);
    group.add_pipeline(&pipeline1);
    group.add_pipeline(&pipeline2);

    group.count_down_pipeline();
    EXPECT_EQ(0, lifecycle.num_finished_groups);

    group.count_down_pipeline();
    EXPECT_EQ(1, lifecycle.num_finished_groups);

    group.count_down_pipeline();
    EXPECT_EQ(1, lifecycle.num_finished_groups);
}

TEST(ExecutionGroupExecRuntimeTest, ColocateGroupSubmitsPhysicalDopThenNextDrivers) {
    constexpr size_t kPhysicalDop = 2;
    constexpr size_t kLogicalDop = 4;

    ColocateExecutionGroup group(kPhysicalDop);
    RecordingDriverExecutor executor;
    group.attach_driver_executor(&executor);

    OpFactories factories;
    factories.emplace_back(std::make_shared<ActiveSourceOperatorFactory>(kLogicalDop));
    factories.emplace_back(std::make_shared<TestSinkOperatorFactory>());

    Pipeline pipeline(0, factories, &group);
    add_drivers(&pipeline, kLogicalDop);
    group.add_pipeline(&pipeline);

    RuntimeState runtime_state;
    ASSERT_OK(group.prepare_pipelines(&runtime_state));

    group.submit_active_drivers();
    EXPECT_EQ((std::vector<int32_t>{0, 1}), executor.submitted_driver_ids);

    group.submit_next_driver();
    EXPECT_EQ((std::vector<int32_t>{0, 1, 2}), executor.submitted_driver_ids);

    group.submit_next_driver();
    EXPECT_EQ((std::vector<int32_t>{0, 1, 2, 3}), executor.submitted_driver_ids);

    group.submit_next_driver();
    EXPECT_EQ((std::vector<int32_t>{0, 1, 2, 3}), executor.submitted_driver_ids);
}

TEST(ExecutionGroupExecRuntimeTest, GroupedPaddingRetainsPhysicalConcurrencyAndCancelCleanup) {
    // Three real C groups plus one empty padded logical driver, physical DOP two.
    ColocateExecutionGroup group(2);
    RecordingDriverExecutor executor;
    RecordingExecutionGroupLifecycle lifecycle;
    group.attach_driver_executor(&executor);
    group.attach_execution_group_lifecycle(&lifecycle);
    auto source_factory = std::make_shared<GroupedMockSourceFactory>();
    OpFactories factories{source_factory, std::make_shared<TestSinkOperatorFactory>()};
    Pipeline pipeline(0, factories, &group);
    add_drivers(&pipeline, 4);
    group.add_pipeline(&pipeline);
    RuntimeState state;
    ASSERT_OK(group.prepare_pipelines(&state));
    EXPECT_EQ(4, group.total_logical_dop());
    group.submit_active_drivers();
    ASSERT_EQ((std::vector<int32_t>{0, 1}), executor.submitted_driver_ids);
    auto source = [&](int sequence) {
        return dynamic_cast<GroupedMockSourceOperator*>(pipeline.mutable_drivers()[sequence]->operators()[0].get());
    };
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(source(i)->has_output());
        ASSERT_TRUE(source(i)->pull_chunk(&state).ok());
        ASSERT_FALSE(source(i)->is_finished()); // one remaining task is not group EOS
        ASSERT_TRUE(source(i)->pull_chunk(&state).ok());
        EXPECT_EQ(2, source(i)->consumed_tasks);
        EXPECT_TRUE(source(i)->is_finished());
        group.submit_next_driver();
        EXPECT_EQ(3 + i, executor.submitted_driver_ids.size());
    }
    EXPECT_EQ((std::vector<int32_t>{0, 1, 2, 3}), executor.submitted_driver_ids);
    EXPECT_TRUE(source(3)->is_finished()); // a truly empty padded queue
    EXPECT_EQ(0, source(3)->consumed_tasks);
    ASSERT_TRUE(source(2)->has_output());
    for (int i = 0; i < 4; ++i) {
        ASSERT_OK(source(i)->set_finished(&state));
        ASSERT_OK(source(i)->set_cancelled(&state));
        source(i)->close(&state);
        EXPECT_EQ(nullptr, source(i)->queue);
        EXPECT_EQ(1, source(i)->cancel_count);
        EXPECT_EQ(1, source(i)->close_count);
    }
    group.count_down_pipeline();
    EXPECT_EQ(1, lifecycle.num_finished_groups);
    group.count_down_pipeline();
    EXPECT_EQ(1, lifecycle.num_finished_groups);
    group.close(&state);
    EXPECT_EQ(1, source_factory->close_count);
}

} // namespace starrocks::pipeline
