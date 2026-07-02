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
#include <utility>

#include "base/statusor.h"
#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "common/config_exec_fwd.h"
#include "exec/analytic_node.h"
#include "exec/chunk_buffer_memory_manager.h"
#include "exec/empty_set_node.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline_node.h"
#include "exec/runtime/fragment_context_manager.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/query_context_manager.h"
#include "gutil/casts.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {
namespace {

constexpr size_t kPipelineDop = 3;

TExpr make_slot_ref_expr(TTupleId tuple_id, TSlotId slot_id) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_type(gen_type_desc(TPrimitiveType::INT));
    node.__set_num_children(0);
    node.__set_is_nullable(true);

    TSlotRef slot_ref;
    slot_ref.__set_tuple_id(tuple_id);
    slot_ref.__set_slot_id(slot_id);
    node.__set_slot_ref(slot_ref);

    TExpr expr;
    expr.nodes.emplace_back(std::move(node));
    return expr;
}

TPlanNode make_base_plan_node(TPlanNodeType::type node_type, TPlanNodeId node_id, TTupleId tuple_id, int num_children) {
    TPlanNode tnode;
    tnode.__set_node_id(node_id);
    tnode.__set_node_type(node_type);
    tnode.__set_num_children(num_children);
    tnode.__set_limit(-1);
    tnode.row_tuples.push_back(tuple_id);
    return tnode;
}

class FixedDopSourceNode final : public PipelineNode {
public:
    FixedDopSourceNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs, size_t dop)
            : PipelineNode(pool, tnode, descs), _dop(dop) {}

    StatusOr<pipeline::OpFactories> decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override {
        auto mem_mgr = std::make_shared<pipeline::ChunkBufferMemoryManager>(_dop, 1024 * 1024);
        auto source = std::make_shared<pipeline::LocalExchangeSourceOperatorFactory>(context->next_operator_id(), id(),
                                                                                     mem_mgr);
        source->set_runtime_state(context->runtime_state());
        source->set_degree_of_parallelism(_dop);
        return pipeline::OpFactories{std::move(source)};
    }

private:
    size_t _dop;
};

class AnalyticNodePipelineTest : public ::testing::Test {
public:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();
        _query_id = generate_uuid();
        _fragment_id = generate_uuid();

        ASSIGN_OR_ASSERT_FAIL(_query_ctx, _exec_env->query_context_mgr()->get_or_register(_query_id));
        _query_ctx->set_query_id(_query_id);
        _query_ctx->set_total_fragments(1);
        _query_ctx->query_runtime_state().set_delivery_expire_seconds(60);
        _query_ctx->query_runtime_state().set_query_expire_seconds(60);
        _query_ctx->query_runtime_state().extend_delivery_lifetime();
        _query_ctx->query_runtime_state().extend_query_lifetime();
        _query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                     GlobalEnv::GetInstance()->query_pool_mem_tracker());

        // FragmentContextManager::get() only returns an already-registered fragment (nullptr otherwise),
        // so the fragment context has to be created and registered explicitly before it can be used.
        _fragment_ctx = std::make_shared<pipeline::FragmentContext>();
        _fragment_ctx->set_query_id(_query_id);
        _fragment_ctx->set_fragment_instance_id(_fragment_id);
        _fragment_ctx->set_runtime_state(
                std::make_unique<RuntimeState>(_query_id, _fragment_id, _query_options, _query_globals,
                                               &_exec_env->query_execution_services(), _exec_env));
        ASSERT_OK(_query_ctx->fragment_mgr()->register_ctx(_fragment_id, _fragment_ctx));

        _runtime_state = _fragment_ctx->runtime_state();
        _runtime_state->set_chunk_size(config::vector_chunk_size);
        _runtime_state->init_mem_trackers(_query_ctx->mem_tracker());
        _query_ctx->attach_to_runtime_state(_runtime_state);
        _runtime_state->set_fragment_ctx(_fragment_ctx.get(), &_fragment_ctx->fragment_runtime_state());
        _runtime_state->set_fragment_dict_state(_fragment_ctx->dict_state());

        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        tuple_desc_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("c1").column_pos(0).build());
        tuple_desc_builder.build(&desc_tbl_builder);
        TDescriptorTable thrift_desc_tbl = desc_tbl_builder.desc_tbl();
        _tuple_id = thrift_desc_tbl.tupleDescriptors[0].id;
        _slot_id = thrift_desc_tbl.slotDescriptors[0].id;

        ASSERT_OK(DescriptorTbl::create(_runtime_state, _runtime_state->obj_pool(), thrift_desc_tbl, &_desc_tbl,
                                        config::vector_chunk_size));
        _runtime_state->set_desc_tbl(_desc_tbl);
    }

    void TearDown() override {
        if (_query_ctx != nullptr) {
            _query_ctx->fragment_mgr()->unregister(_fragment_id);
            _fragment_ctx.reset();
            _runtime_state = nullptr;
            _query_ctx->count_down_fragment();
        }
    }

protected:
    TPlanNode make_analytic_plan_node(bool force_merge_sort) const {
        TPlanNode tnode = make_base_plan_node(TPlanNodeType::ANALYTIC_EVAL_NODE, 2, _tuple_id, 1);
        TExpr partition_expr = make_slot_ref_expr(_tuple_id, _slot_id);

        tnode.analytic_node.__set_partition_exprs({partition_expr});
        tnode.analytic_node.__set_order_by_exprs({partition_expr});
        tnode.analytic_node.__set_analytic_functions({});
        tnode.analytic_node.__set_intermediate_tuple_id(_tuple_id);
        tnode.analytic_node.__set_output_tuple_id(_tuple_id);
        tnode.analytic_node.__set_force_merge_sort(force_merge_sort);
        return tnode;
    }

    pipeline::Pipelines decompose_analytic(bool force_merge_sort, pipeline::OpFactories* analytic_source_ops) {
        TPlanNode child_tnode = make_base_plan_node(TPlanNodeType::EMPTY_SET_NODE, 1, _tuple_id, 0);
        EmptySetNode child(_runtime_state->obj_pool(), child_tnode, *_desc_tbl);

        TPlanNode analytic_tnode = make_analytic_plan_node(force_merge_sort);
        AnalyticNode analytic(_runtime_state->obj_pool(), analytic_tnode, *_desc_tbl);
        analytic.add_child(&child);
        CHECK_OK(analytic.init(analytic_tnode, _runtime_state));

        pipeline::PipelineBuilderContext context(_fragment_ctx.get(), kPipelineDop, kPipelineDop);
        ASSIGN_OR_ABORT(*analytic_source_ops, analytic.decompose_to_pipeline(&context));
        return context.pipelines();
    }

    pipeline::Pipelines decompose_analytic_with_child(ExecNode* child, bool force_merge_sort,
                                                      pipeline::OpFactories* analytic_source_ops) {
        TPlanNode analytic_tnode = make_analytic_plan_node(force_merge_sort);
        AnalyticNode analytic(_runtime_state->obj_pool(), analytic_tnode, *_desc_tbl);
        analytic.add_child(child);
        CHECK_OK(analytic.init(analytic_tnode, _runtime_state));

        pipeline::PipelineBuilderContext context(_fragment_ctx.get(), kPipelineDop, kPipelineDop);
        ASSIGN_OR_ABORT(*analytic_source_ops, analytic.decompose_to_pipeline(&context));
        return context.pipelines();
    }

    ExecEnv* _exec_env = nullptr;
    TUniqueId _query_id;
    TUniqueId _fragment_id;
    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    pipeline::QueryContext* _query_ctx = nullptr;
    pipeline::FragmentContextPtr _fragment_ctx = nullptr;
    RuntimeState* _runtime_state = nullptr;
    DescriptorTbl* _desc_tbl = nullptr;
    TTupleId _tuple_id = 0;
    TSlotId _slot_id = 0;
};

TEST_F(AnalyticNodePipelineTest, ForceMergeSortUsesOrderedPartitionExchange) {
    pipeline::OpFactories analytic_source_ops;
    auto pipelines = decompose_analytic(true, &analytic_source_ops);

    ASSERT_EQ(2, pipelines.size());
    auto* local_exchange_source =
            down_cast<pipeline::LocalExchangeSourceOperatorFactory*>(pipelines.back()->source_operator_factory());
    ASSERT_NE(nullptr, local_exchange_source->exchanger());
    EXPECT_EQ("OrderedPartition", local_exchange_source->exchanger()->name());

    ASSERT_EQ(1, analytic_source_ops.size());
    auto* analytic_source = down_cast<pipeline::SourceOperatorFactory*>(analytic_source_ops.front().get());
    EXPECT_EQ(kPipelineDop, analytic_source->degree_of_parallelism());
}

TEST_F(AnalyticNodePipelineTest, AnalyticWithoutForceMergeSortDoesNotUseOrderedPartitionExchangeWithoutForce) {
    pipeline::OpFactories analytic_source_ops;
    auto pipelines = decompose_analytic(false, &analytic_source_ops);

    ASSERT_EQ(1, pipelines.size());
    EXPECT_EQ("empty_set", pipelines.front()->source_operator_factory()->get_raw_name());

    ASSERT_EQ(1, analytic_source_ops.size());
    auto* analytic_source = down_cast<pipeline::SourceOperatorFactory*>(analytic_source_ops.front().get());
    EXPECT_EQ(1, analytic_source->degree_of_parallelism());
}

TEST_F(AnalyticNodePipelineTest, ForceMergeSortSkipsOrderedPartitionExchangeWhenUpstreamParallel) {
    TPlanNode child_tnode = make_base_plan_node(TPlanNodeType::EMPTY_SET_NODE, 1, _tuple_id, 0);
    FixedDopSourceNode child(_runtime_state->obj_pool(), child_tnode, *_desc_tbl, kPipelineDop);

    pipeline::OpFactories analytic_source_ops;
    auto pipelines = decompose_analytic_with_child(&child, true, &analytic_source_ops);

    ASSERT_EQ(1, pipelines.size());
    auto* child_source =
            down_cast<pipeline::LocalExchangeSourceOperatorFactory*>(pipelines.front()->source_operator_factory());
    EXPECT_EQ(nullptr, child_source->exchanger());

    ASSERT_EQ(1, analytic_source_ops.size());
    auto* analytic_source = down_cast<pipeline::SourceOperatorFactory*>(analytic_source_ops.front().get());
    EXPECT_EQ(kPipelineDop, analytic_source->degree_of_parallelism());
}
} // namespace
} // namespace starrocks
