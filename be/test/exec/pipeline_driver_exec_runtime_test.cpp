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

#include "compute_env/pipeline/driver_scan_operator.h"
#include "exec/pipeline/primitives/event.h"
#include "exec/pipeline/scan/morsel_queue.h"
#include "exec/pipeline/scan/ticketed_morsel_queue.h"
#include "exec/pipeline/source_operator.h"
#include "exec/runtime/fragment_runtime_state.h"
#include "exec/runtime/pipeline_driver.h"
#include "exec/runtime/query_runtime_state.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

namespace {

class TestTicketedMorselQueue : public MorselQueue, public TicketedMorselQueue {
public:
    bool empty() const override { return true; }
    StatusOr<MorselPtr> try_get() override { return nullptr; }
    std::string name() const override { return "test_ticketed_morsel_queue"; }
    Type type() const override { return DYNAMIC; }

    void set_ticket_checker(const SplitMorselTicketCheckerPtr& ticket_checker) override {
        attached_ticket_checker = ticket_checker;
    }
    bool could_attch_ticket_checker() const override { return true; }

    SplitMorselTicketCheckerPtr attached_ticket_checker;
};

class PolicyTicketedMorselQueue final : public TestTicketedMorselQueue {
public:
    bool should_attach_ticket_checker([[maybe_unused]] bool cache_enabled) const override { return true; }
};

class TestDriverScanSourceOperator final : public SourceOperator, public DriverScanOperator {
public:
    TestDriverScanSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "test_driver_scan_source", plan_node_id, false, driver_sequence) {}

    bool has_output() const override { return false; }
    bool is_finished() const override { return false; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }

    workgroup::ScanSchedEntityType sched_entity_type() const override { return workgroup::ScanSchedEntityType::OLAP; }
    void set_scan_executor(workgroup::ScanExecutor* scan_executor) override { _scan_executor = scan_executor; }
    void set_workgroup(workgroup::WorkGroupPtr wg) override { _workgroup = std::move(wg); }
    void set_query_ctx(const QueryContextPtr& query_ctx) override { _query_ctx = query_ctx; }
    void set_cache_context(const query_cache::ScanCacheContextPtr& cache_context) override {
        _cache_context = cache_context;
    }
    void set_ticket_checker(const SplitMorselTicketCheckerPtr& ticket_checker) override {
        _ticket_checker = ticket_checker;
    }

    int64_t get_last_scan_rows_num() override { return 0; }
    int64_t get_last_scan_bytes() override { return 0; }
    int64_t get_scan_table_id() const override { return 0; }

    void begin_driver_process() override {}
    void end_driver_process(DriverState driver_state) override {}
    void end_pull_chunk(int64_t time_ns) override {}

    const SplitMorselTicketCheckerPtr& ticket_checker() const { return _ticket_checker; }

private:
    workgroup::ScanExecutor* _scan_executor = nullptr;
    workgroup::WorkGroupPtr _workgroup;
    QueryContextPtr _query_ctx;
    query_cache::ScanCacheContextPtr _cache_context;
    SplitMorselTicketCheckerPtr _ticket_checker;
};

class TestDriverScanSourceOperatorFactory final : public SourceOperatorFactory {
public:
    TestDriverScanSourceOperatorFactory() : SourceOperatorFactory(1, "test_driver_scan_source", 10) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestDriverScanSourceOperator>(this, _id, _plan_node_id, driver_sequence);
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
        return std::make_shared<TestSinkOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

struct DriverPrepareHarness {
    TestDriverScanSourceOperatorFactory source_factory;
    TestSinkOperatorFactory sink_factory;
    QueryRuntimeState query_runtime_state;
    FragmentRuntimeState fragment_runtime_state;
    RuntimeState runtime_state{TQueryGlobals{}};
    EventPtr pipeline_event = Event::create_event();
    Operators operators;
    std::shared_ptr<TestDriverScanSourceOperator> source;
    std::unique_ptr<PipelineDriver> driver;

    explicit DriverPrepareHarness(bool enable_cache)
            : source(std::static_pointer_cast<TestDriverScanSourceOperator>(source_factory.create(1, 0))) {
        fragment_runtime_state.set_enable_cache(enable_cache);
        runtime_state.set_query_runtime_state(&query_runtime_state);
        runtime_state.set_fragment_runtime_state(&fragment_runtime_state);
        operators.emplace_back(source);
        operators.emplace_back(sink_factory.create(1, 0));
        driver = std::make_unique<PipelineDriver>(operators, &query_runtime_state, &fragment_runtime_state,
                                                  pipeline_event.get(), nullptr, nullptr, -1);
    }
};

} // namespace

TEST(PipelineDriverExecRuntimeTest, DefaultTicketedQueueAttachesOnlyWhenCacheEnabled) {
    {
        DriverPrepareHarness harness(false);
        TestTicketedMorselQueue morsel_queue;
        harness.driver->set_morsel_queue(&morsel_queue);

        ASSERT_TRUE(harness.driver->prepare(&harness.runtime_state).ok());

        EXPECT_EQ(nullptr, harness.source->ticket_checker());
        EXPECT_EQ(nullptr, morsel_queue.attached_ticket_checker);
    }

    {
        DriverPrepareHarness harness(true);
        TestTicketedMorselQueue morsel_queue;
        harness.driver->set_morsel_queue(&morsel_queue);

        ASSERT_TRUE(harness.driver->prepare(&harness.runtime_state).ok());

        ASSERT_NE(nullptr, harness.source->ticket_checker());
        EXPECT_EQ(harness.source->ticket_checker(), morsel_queue.attached_ticket_checker);
    }
}

TEST(PipelineDriverExecRuntimeTest, TicketedQueuePolicyControlsAttachmentWithoutConcreteQueueType) {
    DriverPrepareHarness harness(false);
    PolicyTicketedMorselQueue morsel_queue;
    harness.driver->set_morsel_queue(&morsel_queue);

    ASSERT_TRUE(harness.driver->prepare(&harness.runtime_state).ok());

    ASSERT_NE(nullptr, harness.source->ticket_checker());
    EXPECT_EQ(harness.source->ticket_checker(), morsel_queue.attached_ticket_checker);
}

} // namespace starrocks::pipeline
