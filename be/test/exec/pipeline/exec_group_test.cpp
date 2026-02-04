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

#include "base/testutil/assert.h"
#include "exec/chunk_buffer_memory_manager.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/operator.h"
#include "runtime/runtime_state.h"
#include "util/threadpool.h"

namespace starrocks::pipeline {
struct GroupTaskRunner : public Runnable {
    GroupTaskRunner(ColocateExecutionGroup* group) : _group(group) {}
    ~GroupTaskRunner() override = default;
    void run() override { _group->submit_active_drivers(); }
    ColocateExecutionGroup* _group;
};

class MockedDriverExecutor : public DriverExecutor {
public:
    MockedDriverExecutor(ThreadPool* tp, ColocateExecutionGroup* group)
            : DriverExecutor("mocked"), _tp(tp), _group(group) {}
    ~MockedDriverExecutor() override = default;
    void submit(DriverRawPtr driver) override { (void)_tp->submit(std::make_shared<GroupTaskRunner>(_group)); }
    void cancel(DriverRawPtr driver) override {}
    void close() override { _tp->shutdown(); }

    void report_exec_state(QueryContext* query_ctx, FragmentContext* fragment_ctx, const Status& status, bool done,
                           bool attach_profile) override {}

    void report_audit_statistics(QueryContext* query_ctx, FragmentContext* fragment_ctx) override {}

    void iterate_immutable_blocking_driver(const ConstDriverConsumer& call) const override {}

    size_t activate_parked_driver(const ConstDriverPredicator& predicate_func) override { return 0; }

    void report_epoch(ExecEnv* exec_env, QueryContext* query_ctx,
                      std::vector<FragmentContext*> fragment_ctxs) override {}

    size_t calculate_parked_driver(const ConstDriverPredicator& predicate_func) const override { return 0; }

    void bind_cpus(const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) override {}

private:
    ThreadPool* _tp;
    ColocateExecutionGroup* _group;
};

TEST(ExecutionGroupTest, SubmitRaceConditionTest) {
    std::unique_ptr<ThreadPool> tp;
    ASSERT_OK(ThreadPoolBuilder("mock").set_max_threads(4).build(&tp));
    RuntimeState dummy;
    ColocateExecutionGroup group(1);
    MockedDriverExecutor executor(tp.get(), &group);
    group.attach_driver_executor(&executor);

    OpFactories factories;
    auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(100, 1024);
    auto source = std::make_shared<LocalExchangeSourceOperatorFactory>(1, 1, mem_mgr);
    source->set_degree_of_parallelism(100);
    factories.emplace_back(source);
    factories.emplace_back(std::make_shared<NoopSinkOperatorFactory>(2, 0));
    Pipeline pipeline(0, factories, &group);

    for (size_t i = 0; i < 100; ++i) {
        Operators ops_with_sink;
        ops_with_sink.emplace_back(factories[0]->create(100, i));
        ops_with_sink.emplace_back(factories[1]->create(100, i));
        pipeline.drivers().emplace_back(
                std::make_shared<PipelineDriver>(ops_with_sink, nullptr, nullptr, &pipeline, -1));
    }

    group.add_pipeline(&pipeline);
    // skip prepare step
    group._submit_drivers = std::make_unique<std::atomic<int>[]>(1);

    group.submit_active_drivers();

    executor.close();
}
} // namespace starrocks::pipeline