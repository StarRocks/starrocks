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

#include "orchestration/fragment_executor.h"

#include <stdexcept>
#include <thread>

// Third-party headers.
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "common/config_exec_fwd.h"
#include "compute_env/compute_env.h"
#include "compute_env/pipeline/driver_limiter.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/work_group.h"
#include "compute_env/workgroup/work_group_schedule_policy.h"
#include "exec/exec_env.h"
#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/fragment_execution_params.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/morsel_queue_factory.h"
#include "exec/pipeline_node.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_builder_context.h"
#include "exec/runtime_compat/runtime_state_helper.h"
#include "exec_primitive/pipeline/scan/fixed_morsel_queue.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"
#include "runtime/descriptors.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"

namespace starrocks::orchestration {

using pipeline::QueryContext;

class FragmentExecutorPartitionTest : public ::testing::Test {
public:
    void SetUp() override { _exec_env = ExecEnv::GetInstance(); }

    std::shared_ptr<RuntimeState> _make_runtime_state(QueryContext* query_ctx, DescriptorTbl* desc_tbl) {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        auto rs = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
        TUniqueId query_id;
        rs->init_mem_trackers(query_id);
        rs->set_query_ctx(query_ctx, &query_ctx->query_runtime_state(), query_ctx->object_pool());
        rs->set_desc_tbl(desc_tbl);
        return rs;
    }

    static TScanRangeParams _make_scan_range(TTableId table_id, int64_t partition_id) {
        TScanRangeParams params;
        TScanRange& sr = params.scan_range;
        sr.__isset.hdfs_scan_range = true;
        sr.hdfs_scan_range.__set_table_id(table_id);
        sr.hdfs_scan_range.__set_partition_id(partition_id);
        THdfsPartition pv;
        pv.__set_file_format(THdfsFileFormat::TEXT);
        THdfsPartitionLocation loc;
        loc.__set_suffix("");
        pv.__set_location(loc);
        sr.hdfs_scan_range.__set_partition_value(pv);
        return params;
    }

protected:
    ExecEnv* _exec_env = nullptr;
};

// Regression test for a heap-use-after-free in add_scan_ranges_partition_values.
//
// HiveTableDescriptor::_partition_id_to_desc_map is shared across all fragment
// instances of the same query (the descriptor itself lives in the query-level
// ObjectPool via DescriptorTbl). The partition descriptors stored in that map
// must therefore outlive any single fragment instance.
//
// Before the fix, add_scan_ranges_partition_values allocated partition descriptors
// from `runtime_state->obj_pool()` (per-fragment). When one fragment finished and
// tore down its pool, the descriptors it inserted into the shared map were freed,
// leaving dangling pointers. A sibling fragment then hit UAF on the duplicate-check
// path `partition->thrift_partition_key_exprs() != old_partition->...`.
//
// The fix switches the allocation to `RuntimeStateHelper::global_obj_pool(rs)`
// (query-level). Together with the earlier refactor that made HdfsPartitionDescriptor
// thrift-only (no fragment-bound ExprContexts on the descriptor), this lets the
// descriptor live for the whole query without smuggling fragment-scoped state.
//
// This test asserts: after a fragment-level RuntimeState (and its per-fragment
// pool) is destroyed, the partition descriptor it inserted via this helper remains
// alive in the query-level pool and its thrift fields are still readable. Under
// ASAN, reading those fields after the fragment's pool dies would otherwise be
// reported as heap-use-after-free.
TEST_F(FragmentExecutorPartitionTest, PartitionDescriptorOutlivesFragmentPool) {
    constexpr TTableId kTableId = 100;
    constexpr int64_t kPartitionId = 42;

    TDescriptorTable thrift_tbl;
    {
        TTableDescriptor tt;
        tt.id = kTableId;
        tt.tableType = TTableType::HDFS_TABLE;
        thrift_tbl.tableDescriptors.push_back(tt);
    }

    // The QueryContext owns the per-query ObjectPool; the partition descriptor must
    // ultimately end up here so it survives fragment teardown.
    auto query_ctx = std::make_shared<QueryContext>();

    DescriptorTbl* desc_tbl = nullptr;
    {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        RuntimeState bootstrap_rs(fragment_id, query_options, query_globals, _exec_env);
        ASSERT_OK(DescriptorTbl::create(&bootstrap_rs, query_ctx->object_pool(), thrift_tbl, &desc_tbl,
                                        config::vector_chunk_size));
    }

    auto* table_desc = desc_tbl->get_table_descriptor(kTableId);
    ASSERT_NE(nullptr, table_desc);
    auto* hive_table = down_cast<HiveTableDescriptor*>(table_desc);

    std::vector<TScanRangeParams> scan_ranges = {_make_scan_range(kTableId, kPartitionId)};

    // Run add_scan_ranges_partition_values inside a fragment-level scope. The
    // RuntimeState (and its per-fragment obj_pool) is destroyed when the scope
    // exits, simulating one fragment finishing while a sibling continues.
    {
        auto rs = _make_runtime_state(query_ctx.get(), desc_tbl);
        // Sanity: the contract being asserted only holds when the two pools really
        // differ. If a future refactor unified them, the assertion below would not
        // tell us anything useful, so guard it.
        ASSERT_NE(rs->obj_pool(), RuntimeStateHelper::global_obj_pool(rs.get()));

        ASSERT_OK(FragmentExecutor::add_scan_ranges_partition_values(rs.get(), scan_ranges));
    }

    // After the fragment is gone, the partition descriptor must still be reachable
    // through the shared map and its heap-owned fields must not have been freed.
    HdfsPartitionDescriptor* partition = hive_table->get_partition(kPartitionId);
    ASSERT_NE(nullptr, partition);
    // Reading the vector trips ASAN if the descriptor's backing memory was freed.
    (void)partition->thrift_partition_key_exprs().size();
}

// Guards the final-sink classification for every sink type that decomposes to a
// ConnectorSinkOperator: the last connector sinker reports audit statistics through
// QueryContext::final_query_statistic(), which DCHECKs is_final_sink(). Dropping any of
// these types from FragmentExecutor::is_final_sink_type would abort debug builds and
// violate final_query_statistic()'s precondition in release builds (issue #62262). The
// connector-sink unit tests call set_final_sink() manually, so they cannot catch such a
// regression.
TEST(FragmentExecutorFinalSinkTest, connector_backed_sink_types_are_final_sinks) {
    EXPECT_TRUE(FragmentExecutor::is_final_sink_type(TDataSinkType::ICEBERG_TABLE_SINK));
    EXPECT_TRUE(FragmentExecutor::is_final_sink_type(TDataSinkType::ICEBERG_DELETE_SINK));
    EXPECT_TRUE(FragmentExecutor::is_final_sink_type(TDataSinkType::ICEBERG_ROW_DELTA_SINK));
    EXPECT_TRUE(FragmentExecutor::is_final_sink_type(TDataSinkType::HIVE_TABLE_SINK));
    EXPECT_TRUE(FragmentExecutor::is_final_sink_type(TDataSinkType::TABLE_FUNCTION_TABLE_SINK));

    // Exchange sinks hand off to a downstream fragment and must NOT be classified as final.
    EXPECT_FALSE(FragmentExecutor::is_final_sink_type(TDataSinkType::DATA_STREAM_SINK));
}

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

} // namespace starrocks::orchestration
