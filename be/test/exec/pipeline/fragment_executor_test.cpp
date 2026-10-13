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

#include "exec/pipeline/fragment_executor.h"

#include <gtest/gtest.h>
#include <stdexcept>
#include <thread>
#include "common/config.h"
#include "exec/exec_node.h"
#include "exec/pipeline/driver_limiter.h"
#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_execution_params.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/workgroup/pipeline_executor_set.h"
#include "exec/workgroup/work_group.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

class FragmentExecutorPartitionTest : public ::testing::Test {
public:
    std::shared_ptr<RuntimeState> _make_runtime_state(QueryContext* query_ctx, DescriptorTbl* desc_tbl) {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        auto state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, ExecEnv::GetInstance());
        TUniqueId query_id;
        state->init_mem_trackers(query_id);
        state->set_query_ctx(query_ctx);
        state->set_desc_tbl(desc_tbl);
        return state;
    }
};

namespace {

class DriverLimitSchedulePolicy final : public workgroup::WorkGroupSchedulePolicy {
public:
    bool should_yield(const workgroup::WorkGroup*) const override { return false; }
    size_t num_workgroups() const override { return 1; }
};

class DriverLimitSourceFactory final : public pipeline::SourceOperatorFactory {
public:
    DriverLimitSourceFactory(pipeline::DriverLimiter* limiter, int id, int dop)
            : SourceOperatorFactory(id, "driver_limit_test", id), _limiter(limiter) {
        set_degree_of_parallelism(dop);
    }

    AdaptiveState adaptive_initial_state() const override {
        return initially_active ? AdaptiveState::ACTIVE : AdaptiveState::INACTIVE;
    }

    bool with_morsels() const override { return needs_morsels; }

    void close(RuntimeState* state) override {
        if (needs_morsels) {
            EXPECT_NE(nullptr, morsel_queue_factory());
        }
        ++closes;
        SourceOperatorFactory::close(state);
    }

    pipeline::OperatorPtr create(int32_t dop, int32_t driver_sequence) override {
        ++instances;
        reserved_during_instantiation = _limiter->num_total_drivers();
        fragment_owns_token_during_instantiation = fragment->_driver_token != nullptr;
        if (finish_during_instantiation) {
            // Exercise the terminal-state token reset from another thread before any driver is published.
            std::thread finish([this] { fragment->cancel(Status::Cancelled("QueryFinished")); });
            finish.join();
            EXPECT_EQ(reserved_during_instantiation, _limiter->num_total_drivers());
            EXPECT_EQ(TStatusCode::TOO_MANY_TASKS, _limiter->try_acquire(1).status().code());
        }
        if (fail_instantiation) {
            throw std::runtime_error("injected driver instantiation failure");
        }
        return std::make_shared<pipeline::EmptySetOperator>(this, _id, _plan_node_id, driver_sequence);
    }

    int instances = 0;
    int closes = 0;
    int reserved_during_instantiation = 0;
    bool initially_active = true;
    bool needs_morsels = false;
    bool finish_during_instantiation = false;
    bool fail_instantiation = false;
    bool fragment_owns_token_during_instantiation = false;
    pipeline::FragmentContext* fragment = nullptr;

private:
    pipeline::DriverLimiter* _limiter;
};

class DriverLimitPlanNode final : public PipelineNode {
public:
    DriverLimitPlanNode(ObjectPool* pool, const TPlanNode& node, const DescriptorTbl& desc,
                        std::vector<std::shared_ptr<DriverLimitSourceFactory>> sources, bool colocate)
            : PipelineNode(pool, node, desc), _sources(std::move(sources)), _colocate(colocate) {}

    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override {
        pipeline::ExecutionGroupPtr group;
        if (_colocate) {
            group = std::make_shared<pipeline::ColocateExecutionGroup>(1);
            context->init_colocate_groups({{0, group}});
        }
        for (const auto& source : _sources) {
            pipeline::OpFactories operators{source};
            if (group) {
                context->add_pipeline(operators, group.get());
            } else {
                context->add_pipeline(operators);
            }
        }
        return pipeline::OpFactories{_sources.back()};
    }

private:
    std::vector<std::shared_ptr<DriverLimitSourceFactory>> _sources;
    bool _colocate;
};

} // namespace

TEST_F(FragmentExecutorPartitionTest, DriverAdmissionPrecedesInstantiation) {
    // Keep all requests tiny: at most four physical drivers, including the rejected cases.
    struct Scenario {
        const char* name;
        int limit;
        int num_pipelines;
        int dop;
        bool colocate;
        bool fail_instantiation;
        bool finish_during_instantiation;
        bool initially_active;
        bool needs_morsels = false;
    };
    const Scenario scenarios[] = {
            {"reject", 0, 1, 1, false, false, false, true},
            {"admit", 1, 1, 1, false, false, false, true},
            {"instantiation_failure", 1, 1, 1, false, true, false, true},
            {"terminal_state_during_instantiation", 1, 1, 1, false, true, true, true},
            {"multi_pipeline_reject", 3, 2, 2, false, false, false, true},
            {"multi_pipeline_admit", 4, 2, 2, false, false, false, true},
            {"colocate_reject", 1, 2, 2, true, false, false, true},
            {"colocate_admit", 2, 2, 2, true, false, false, true},
            {"inactive_pipeline_reject", 0, 1, 1, false, false, false, false},
            {"inactive_pipeline_admit", 1, 1, 1, false, false, false, false},
            {"morsel_pipeline_reject", 1, 1, 2, false, false, false, true, true},
            {"morsel_pipeline_admit", 2, 1, 2, false, false, false, true, true},
    };
    for (const auto& scenario : scenarios) {
        SCOPED_TRACE(scenario.name);
        const int expected_dop = scenario.dop * (scenario.colocate ? 1 : scenario.num_pipelines);
        ComputeEnv compute_env;
        compute_env._driver_limiter = std::make_unique<pipeline::DriverLimiter>(scenario.limit);
        auto* limiter = compute_env.driver_limiter();
        ExecEnv exec_env;
        exec_env.set_compute_env(&compute_env);
        auto query_ctx = std::make_shared<QueryContext>();
        workgroup::PipelineExecutorSetConfig executor_config{1, 1, 1, 1, {}, false, false, nullptr};
        DriverLimitSchedulePolicy schedule_policy;
        workgroup::PipelineExecutorSet executors(executor_config, "driver_limit_test", {}, {}, schedule_policy);
        auto wg = std::make_shared<workgroup::WorkGroup>("driver_limit_test", 1, 0, 1, 1.0, 0, 1.0,
                                                         TWorkGroupType::WG_NORMAL, "");
        wg->set_shared_executors(&executors);
        std::vector<std::shared_ptr<DriverLimitSourceFactory>> sources;
        for (int i = 0; i < scenario.num_pipelines; ++i) {
            auto source = std::make_shared<DriverLimitSourceFactory>(limiter, i, scenario.dop);
            source->fail_instantiation = scenario.fail_instantiation;
            source->finish_during_instantiation = scenario.finish_during_instantiation;
            source->initially_active = scenario.initially_active;
            source->needs_morsels = scenario.needs_morsels;
            sources.emplace_back(std::move(source));
        }
        {
            FragmentExecutor executor;
            executor._query_ctx = query_ctx.get();
            executor._wg = wg;
            executor._fragment_ctx = std::make_shared<pipeline::FragmentContext>();
            executor._fragment_ctx->set_workgroup(wg);
            for (const auto& source : sources) {
                source->fragment = executor._fragment_ctx.get();
                if (source->needs_morsels) {
                    executor._fragment_ctx->morsel_queue_factories()[source->plan_node_id()] =
                            std::make_unique<pipeline::SharedMorselQueueFactory>(
                                    std::make_unique<pipeline::FixedMorselQueue>(pipeline::Morsels{}), scenario.dop);
                }
            }
            auto state = _make_runtime_state(query_ctx.get(), nullptr);
            executor._fragment_ctx->set_runtime_state(std::move(state));
            auto* runtime_state = executor._fragment_ctx->runtime_state();
            executor._fragment_ctx->attach_to_runtime_state(runtime_state);
            DescriptorTbl desc;
            TPlanNode node;
            node.__set_node_id(0);
            node.__set_node_type(TPlanNodeType::EMPTY_SET_NODE);
            node.__set_limit(-1);
            executor._fragment_ctx->plan() = runtime_state->obj_pool()->add(
                    new DriverLimitPlanNode(runtime_state->obj_pool(), node, desc, sources, scenario.colocate));

            TExecPlanFragmentParams params;
            params.__set_backend_num(0);
            params.__set_pipeline_dop(scenario.dop);
            TPlanFragmentExecParams exec_params;
            exec_params.__set_sender_id(0);
            params.__set_params(exec_params);
            pipeline::UnifiedExecPlanFragmentParams request(params, params);
            if (scenario.fail_instantiation) {
                EXPECT_THROW(executor._prepare_pipeline_driver(&exec_env, request), std::runtime_error);
                EXPECT_EQ(1, sources.front()->instances);
                EXPECT_EQ(expected_dop, sources.front()->reserved_during_instantiation);
                EXPECT_FALSE(sources.front()->fragment_owns_token_during_instantiation);
                // Unwinding must release the local reservation even while the fragment is still alive.
                EXPECT_EQ(0, limiter->num_total_drivers());
                EXPECT_TRUE(limiter->try_acquire(scenario.limit).ok());
            } else {
                auto status = executor._prepare_pipeline_driver(&exec_env, request);
                ASSERT_EQ(expected_dop, executor._fragment_ctx->total_dop());
                if (scenario.limit < expected_dop) {
                    EXPECT_EQ(TStatusCode::TOO_MANY_TASKS, status.code()) << status;
                    EXPECT_NE(std::string::npos, status.message().find("BE has overloaded with pipeline drivers"));
                    for (const auto& source : sources) {
                        EXPECT_EQ(0, source->instances);
                    }
                    executor._fragment_ctx->iterate_pipeline(
                            [](pipeline::Pipeline* p) { EXPECT_TRUE(p->drivers().empty()); });
                    EXPECT_EQ(0, limiter->num_total_drivers());
                } else {
                    ASSERT_OK(status);
                    for (const auto& source : sources) {
                        if (scenario.initially_active) {
                            EXPECT_EQ(scenario.dop, source->instances);
                            EXPECT_EQ(expected_dop, source->reserved_during_instantiation);
                            EXPECT_FALSE(source->fragment_owns_token_during_instantiation);
                        } else {
                            EXPECT_EQ(0, source->instances);
                            EXPECT_NE(nullptr, source->_group_initialize_event);
                        }
                    }
                    EXPECT_EQ(expected_dop, limiter->num_total_drivers());
                    ASSERT_NE(nullptr, executor._fragment_ctx->_driver_token);
                }
            }
        }
        // Both rejection before instantiation and normal teardown must close the prepared factories.
        for (const auto& source : sources) {
            EXPECT_EQ(1, source->closes);
        }
        EXPECT_EQ(0, limiter->num_total_drivers());
        EXPECT_TRUE(limiter->try_acquire(scenario.limit).ok());
    }
}

} // namespace starrocks::pipeline
