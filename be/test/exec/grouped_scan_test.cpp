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

#include "common/object_pool.h"
#include "exec/connector_scan_node.h"
#include "exec/exec_node.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {
namespace {
namespace {
class GroupedQueueScanNode final : public ScanNode {
public:
    GroupedQueueScanNode(ObjectPool* pool, const TPlanNode& node, const DescriptorTbl& desc)
            : ScanNode(pool, node, desc) {}
    Status set_scan_ranges(const std::vector<TScanRangeParams>&) override { return Status::OK(); }
};

TScanRangeParams grouped_queue_task(int group, int tablet = 77) {
    TScanRangeParams result;
    TInternalScanRange internal;
    internal.__set_tablet_id(tablet);
    internal.__set_version("42");
    internal.__set_bucket_sequence(group);
    result.scan_range.__set_internal_scan_range(internal);
    TGroupedRangeTask task;
    task.__set_group_id(group);
    task.__set_key_range(TTabletRange{});
    task.__set_sort_key_unique_ids({100, 101, 102});
    result.scan_range.__set_grouped_range_task(task);
    return result;
}
} // namespace

TEST(GroupedScanQueueTest, grouped_queues_are_independent_fixed_and_padding_terminates) {
    ObjectPool pool;
    DescriptorTbl desc;
    TPlanNode plan;
    plan.__set_node_type(TPlanNodeType::LAKE_GROUPED_SCAN_NODE);
    GroupedQueueScanNode node(&pool, plan, desc);
    TScanRangeParams padding;
    padding.__set_empty(true);
    std::map<int32_t, std::vector<TScanRangeParams>> ranges{{0, {grouped_queue_task(0), grouped_queue_task(0, 78)}},
                                                            {1, {grouped_queue_task(1)}},
                                                            {2, {padding}},
                                                            {3, {}}};
    ASSIGN_OR_ABORT(auto factory,
                    node.convert_scan_range_to_morsel_queue_factory({}, ranges, 1, 4, true, true,
                                                                    TTabletInternalParallelMode::FORCE_SPLIT, true));
    ASSERT_FALSE(factory->is_shared());
    EXPECT_FALSE(factory->could_local_shuffle());
    ASSERT_EQ(4, factory->size());
    EXPECT_NE(factory->create(0), factory->create(1));
    for (int i = 0; i < 4; ++i) {
        auto* queue = factory->create(i);
        EXPECT_EQ(MorselQueue::FIXED, queue->type());
        EXPECT_FALSE(queue->has_more());
        int count = 0;
        while (!queue->empty()) {
            ASSIGN_OR_ABORT(auto morsel, queue->try_get());
            ASSERT_NE(nullptr, morsel);
            EXPECT_EQ(i, morsel->get_scan_range()->grouped_range_task.group_id);
            ++count;
        }
        EXPECT_EQ(i == 0 ? 2 : i == 1 ? 1 : 0, count);
    }
}

TEST(GroupedScanQueueTest, grouped_requests_reject_global_unowned_or_cross_driver_groups) {
    ObjectPool pool;
    DescriptorTbl desc;
    TPlanNode plan;
    plan.__set_node_type(TPlanNodeType::LAKE_GROUPED_SCAN_NODE);
    GroupedQueueScanNode node(&pool, plan, desc);
    auto check = [&](const std::vector<TScanRangeParams>& global,
                     const std::map<int32_t, std::vector<TScanRangeParams>>& drivers, bool colocate = true) {
        auto result = node.convert_scan_range_to_morsel_queue_factory(global, drivers, 1, 2, colocate, false,
                                                                      TTabletInternalParallelMode::AUTO, true);
        EXPECT_FALSE(result.ok());
    };
    check({grouped_queue_task(0)}, {});
    check({grouped_queue_task(0)}, {{0, {grouped_queue_task(0)}}});
    check({}, {});
    check({}, {{0, {grouped_queue_task(0)}}}, false);
    check({}, {{0, {grouped_queue_task(0), grouped_queue_task(1)}}});
    check({}, {{0, {grouped_queue_task(0)}}, {1, {grouped_queue_task(0)}}});
    auto missing = grouped_queue_task(0);
    missing.scan_range.__isset.grouped_range_task = false;
    check({}, {{0, {missing}}});
    auto mismatch = grouped_queue_task(0);
    mismatch.scan_range.internal_scan_range.__set_bucket_sequence(1);
    check({}, {{0, {mismatch}}});
    auto streaming = grouped_queue_task(0);
    streaming.__set_has_more(true);
    check({}, {{0, {streaming}}});
    check({}, {{2, {grouped_queue_task(0)}}});
}

TEST(MorselPrimitiveTest, GroupedEnvelopeSurvivesCopiesAndKeepsSameTabletTasksDistinct) {
    TInternalScanRange internal;
    internal.__set_tablet_id(77);
    internal.__set_version("42");
    internal.__set_bucket_sequence(0);
    TGroupedRangeTask task;
    task.__set_group_id(0);
    TTuple cut;
    for (auto literal : {"5", "0", "0"}) {
        TScalarType scalar;
        scalar.__set_type(TPrimitiveType::BIGINT);
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        node.__set_scalar_type(scalar);
        TTypeDesc type;
        type.__set_types({node});
        type.__isset.types = true;
        TVariant value;
        value.__set_type(type);
        value.__set_variant_type(TVariantType::NORMAL_VALUE);
        value.__set_value(literal);
        cut.values.push_back(value);
    }
    TTabletRange clip;
    clip.__set_upper_bound(cut);
    clip.__set_upper_bound_included(false);
    task.__set_key_range(clip);
    task.__set_sort_key_unique_ids({100, 101, 102});
    TScanRangeParams first;
    first.scan_range.__set_internal_scan_range(internal);
    first.scan_range.__set_grouped_range_task(task);
    auto second = first;
    second.scan_range.grouped_range_task.__set_group_id(1);
    second.scan_range.grouped_range_task.key_range.__isset.upper_bound = false;
    second.scan_range.grouped_range_task.key_range.__set_lower_bound(cut);
    second.scan_range.grouped_range_task.key_range.__set_lower_bound_included(true);
    second.scan_range.internal_scan_range.__set_bucket_sequence(1);
    Morsels morsels;
    bool has_more = false;
    ScanMorsel::build_scan_morsels(9, {first, second}, true, &morsels, &has_more);
    ASSERT_EQ(2, morsels.size());
    first.scan_range.grouped_range_task.__set_group_id(99);
    for (int i = 0; i < 2; ++i) {
        auto* range = morsels[i]->get_scan_range();
        EXPECT_TRUE(range->__isset.grouped_range_task);
        EXPECT_EQ(i, range->grouped_range_task.group_id);
        EXPECT_EQ(i == 0, range->grouped_range_task.key_range.__isset.upper_bound);
        EXPECT_EQ(i == 1, range->grouped_range_task.key_range.__isset.lower_bound);
        const auto& bound = i == 0 ? range->grouped_range_task.key_range.upper_bound
                                   : range->grouped_range_task.key_range.lower_bound;
        EXPECT_EQ(cut, bound);
        EXPECT_EQ((std::vector<int32_t>{100, 101, 102}), range->grouped_range_task.sort_key_unique_ids);
        EXPECT_EQ(77, range->internal_scan_range.tablet_id);
        EXPECT_EQ("42", range->internal_scan_range.version);
        EXPECT_EQ(i, morsels[i]->owner_id());
    }
    EXPECT_FALSE(has_more);
}

TEST(MorselPrimitiveTest, GroupedPaddingHasNoSyntheticTabletAndTerminates) {
    TScanRangeParams marker;
    marker.__set_empty(true);
    Morsels morsels;
    bool has_more = true;
    ScanMorsel::build_scan_morsels(9, {marker}, true, &morsels, &has_more);
    EXPECT_TRUE(morsels.empty());
    EXPECT_FALSE(has_more);
    auto queue = std::make_unique<FixedMorselQueue>(std::move(morsels));
    EXPECT_TRUE(queue->empty());
    EXPECT_FALSE(queue->has_more());
    auto next = queue->try_get();
    ASSERT_TRUE(next.ok());
    EXPECT_EQ(nullptr, next.value());
}

// Factory recognition and scan collection preserve the distinct fail-closed protocol type.
TEST(GroupedScanQueueTest, RecognizesGroupedNodeAndRejectsUnknownNode) {
    RuntimeState state{TQueryGlobals{}};
    ObjectPool pool;
    DescriptorTbl desc;
    TPlanNode plan;
    plan.__set_node_type(TPlanNodeType::LAKE_GROUPED_SCAN_NODE);
    plan.__set_limit(-1);
    plan.__set_lake_scan_node(TLakeScanNode{});
    ExecNode* node = nullptr;
    ASSERT_OK(ExecNode::create_vectorized_node(&state, &pool, plan, desc, &node));
    ASSERT_NE(nullptr, dynamic_cast<ConnectorScanNode*>(node));
    EXPECT_EQ(TPlanNodeType::LAKE_GROUPED_SCAN_NODE, node->type());
    std::vector<ExecNode*> scans;
    node->collect_scan_nodes(&scans);
    ASSERT_EQ(1, scans.size());
    EXPECT_EQ(node, scans[0]);
    plan.__set_node_type(static_cast<TPlanNodeType::type>(999));
    EXPECT_TRUE(ExecNode::create_vectorized_node(&state, &pool, plan, desc, &node).is_internal_error());
}
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
    void report_exec_state(QueryContext* query_ctx, FragmentContext* fragment_ctx, const Status& status, bool done,
                           bool attach_profile) override {}
    void report_audit_statistics(QueryContext* query_ctx, FragmentContext* fragment_ctx) override {}
    size_t activate_parked_driver(const ConstDriverPredicator&) override { return 0; }
    size_t calculate_parked_driver(const ConstDriverPredicator&) const override { return 0; }
    void report_epoch(ExecEnv*, QueryContext*, std::vector<FragmentContext*>) override {}
    void iterate_immutable_blocking_driver(const ConstDriverConsumer& call) const override {}
    void bind_cpus(const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) override {}

    std::vector<int32_t> submitted_driver_ids;
};

void add_grouped_drivers(Pipeline* pipeline, size_t dop) {
    for (size_t i = 0; i < dop; ++i) {
        auto operators = pipeline->create_operators(dop, i);
        pipeline->drivers().emplace_back(std::make_shared<PipelineDriver>(operators, nullptr, nullptr, pipeline, i));
    }
}
TEST(GroupedScanQueueTest, GroupedPaddingRetainsPhysicalConcurrencyAndCancelCleanup) {
    // Three real C groups plus one empty padded logical driver, physical DOP two.
    ColocateExecutionGroup group(2);
    RecordingDriverExecutor executor;
    group.attach_driver_executor(&executor);
    auto source_factory = std::make_shared<GroupedMockSourceFactory>();
    OpFactories factories{source_factory, std::make_shared<TestSinkOperatorFactory>()};
    Pipeline pipeline(0, factories, &group);
    add_grouped_drivers(&pipeline, 4);
    group.add_pipeline(&pipeline);
    RuntimeState state;
    ASSERT_OK(group.prepare_pipelines(&state));
    EXPECT_EQ(4, group.total_logical_dop());
    group.submit_active_drivers();
    ASSERT_EQ((std::vector<int32_t>{0, 1}), executor.submitted_driver_ids);
    auto source = [&](int sequence) {
        return dynamic_cast<GroupedMockSourceOperator*>(pipeline.drivers()[sequence]->operators()[0].get());
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
    group.close(&state);
    EXPECT_EQ(1, source_factory->close_count);
}

} // namespace
} // namespace starrocks::pipeline
