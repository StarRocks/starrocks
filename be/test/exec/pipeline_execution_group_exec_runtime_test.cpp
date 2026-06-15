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
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_factory.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/primitives/driver_executor.h"
#include "exec/pipeline/primitives/execution_group_lifecycle.h"
#include "exec/pipeline/source_operator.h"
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

} // namespace starrocks::pipeline
