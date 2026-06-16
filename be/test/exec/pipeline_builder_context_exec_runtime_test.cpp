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
#include <unordered_map>

#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_factory.h"
#include "exec/pipeline/source_operator.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_builder_context.h"
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

class TestSourceOperatorFactory final : public SourceOperatorFactory {
public:
    TestSourceOperatorFactory(int32_t id, int32_t plan_node_id, size_t dop)
            : SourceOperatorFactory(id, "test_source", plan_node_id) {
        set_degree_of_parallelism(dop);
    }

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
    TestSinkOperatorFactory(int32_t id, int32_t plan_node_id) : OperatorFactory(id, "test_sink", plan_node_id) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSinkOperator>(this, id(), plan_node_id(), driver_sequence);
    }
};

std::unique_ptr<FragmentContext> make_fragment_context() {
    auto fragment_context = std::make_unique<FragmentContext>();
    fragment_context->set_runtime_state(std::make_shared<RuntimeState>(TQueryGlobals{}));
    return fragment_context;
}

} // namespace

TEST(PipelineBuilderContextExecRuntimeTest, BuildsPipelinesAndKeepsCountersInContext) {
    auto fragment_context = make_fragment_context();
    PipelineBuilderContext context(fragment_context.get(), 4, 2);

    EXPECT_EQ(4, context.degree_of_parallelism());
    EXPECT_EQ(2, context.data_sink_dop());
    EXPECT_FALSE(context.has_aggregation);
    context.has_aggregation = true;
    EXPECT_TRUE(context.has_aggregation);
    EXPECT_EQ(0, context.next_operator_id());
    EXPECT_EQ(1, context.next_operator_id());

    OpFactories factories;
    factories.emplace_back(std::make_shared<TestSourceOperatorFactory>(10, 100, 3));
    factories.emplace_back(std::make_shared<TestSinkOperatorFactory>(11, 100));
    context.add_pipeline(factories);

    EXPECT_EQ(0, context.last_pipeline()->get_id());
    EXPECT_EQ(3, context.last_pipeline()->degree_of_parallelism());

    PipelineBuilder builder(context);
    auto [execution_groups, pipelines] = builder.build();
    ASSERT_EQ(1, execution_groups.size());
    ASSERT_EQ(1, pipelines.size());
    EXPECT_EQ(0, pipelines[0]->get_id());
}

TEST(PipelineBuilderContextExecRuntimeTest, ResolvesColocateGroupsWithoutChangingNormalFallback) {
    auto fragment_context = make_fragment_context();
    PipelineBuilderContext context(fragment_context.get(), 4, 2);
    auto* normal_group = context.find_exec_group_by_plan_node_id(999);

    auto colocate_group = std::make_shared<ColocateExecutionGroup>(2);
    colocate_group->add_plan_node_id(10);
    std::unordered_map<int32_t, ExecutionGroupPtr> groups;
    groups.emplace(1, colocate_group);

    context.init_colocate_groups(std::move(groups));

    EXPECT_EQ(colocate_group.get(), context.find_exec_group_by_plan_node_id(10));
    EXPECT_EQ(normal_group, context.find_exec_group_by_plan_node_id(999));
}

TEST(PipelineBuilderContextExecRuntimeTest, InheritsSourceProperties) {
    auto fragment_context = make_fragment_context();
    PipelineBuilderContext context(fragment_context.get(), 4, 2);

    TestSourceOperatorFactory upstream(10, 100, 6);
    upstream.set_could_local_shuffle(false);
    upstream.set_partition_type(TPartitionType::RANDOM);

    TestSourceOperatorFactory downstream(11, 100, 1);
    context.inherit_upstream_source_properties(&downstream, &upstream);

    EXPECT_EQ(6, downstream.degree_of_parallelism());
    EXPECT_FALSE(downstream.could_local_shuffle());
    EXPECT_EQ(TPartitionType::RANDOM, downstream.partition_type());
}

} // namespace starrocks::pipeline
