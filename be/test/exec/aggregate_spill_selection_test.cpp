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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "exec/aggregate/aggregate_blocking_node.h"
#include "exec/aggregate/distinct_blocking_node.h"
#include "exec/empty_set_node.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_builder_context.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

struct SelectedAggregateOperators {
    std::string source;
    std::string sink;
    size_t pipelines;
};

class AggregateSpillSelectionTest : public ::testing::Test {
protected:
    // Exercise the real node decomposition rather than duplicating its selection predicate.
    // No operators are prepared: these tests check which complete sink/source pair the BE
    // builds, including the extra spill-process pipeline, before any input reaches the sink.
    SelectedAggregateOperators select(const std::vector<TypeDescriptor>& key_types, bool distinct,
                                      int64_t spillable_mask, bool partitionwise = false, bool enable_spill = true,
                                      int64_t limit = -1) {
        TSpillOptions spill_options;
        spill_options.__set_spillable_operator_mask(spillable_mask);
        spill_options.__set_spill_partitionwise_agg(partitionwise);
        TQueryOptions options;
        options.__set_enable_spill(enable_spill);
        options.__set_spill_options(spill_options);
        pipeline::FragmentContext fragment;
        fragment.set_runtime_state(std::make_shared<RuntimeState>(TUniqueId{}, options, TQueryGlobals{}, nullptr));
        auto* state = fragment.runtime_state();
        state->set_chunk_size(4096);

        TDescriptorTableBuilder descriptor_builder;
        TTupleDescriptorBuilder tuple_builder;
        // A global aggregate still has a valid input tuple, but no grouping expressions.
        auto input_types = key_types.empty() ? std::vector<TypeDescriptor>{TypeDescriptor(TYPE_INT)} : key_types;
        for (const auto& type : input_types) {
            tuple_builder.add_slot(TSlotDescriptorBuilder().type(type).nullable(true).build());
        }
        tuple_builder.build(&descriptor_builder);
        auto thrift_descriptors = descriptor_builder.desc_tbl();
        DescriptorTbl* descriptors = nullptr;
        CHECK_OK(DescriptorTbl::create(state, state->obj_pool(), thrift_descriptors, &descriptors, 4096));
        state->set_desc_tbl(descriptors);
        const auto tuple_id = thrift_descriptors.tupleDescriptors[0].id;

        TPlanNode child_plan;
        child_plan.__set_node_id(0);
        child_plan.__set_node_type(TPlanNodeType::EMPTY_SET_NODE);
        child_plan.__set_limit(-1);
        child_plan.row_tuples = {tuple_id};
        EmptySetNode child(state->obj_pool(), child_plan, *descriptors);

        std::vector<TExpr> keys;
        for (size_t i = 0; i < key_types.size(); ++i) {
            TExprNode ref;
            ref.__set_node_type(TExprNodeType::SLOT_REF);
            ref.__set_type(key_types[i].to_thrift());
            ref.__set_num_children(0);
            ref.__set_is_nullable(true);
            TSlotRef slot;
            slot.__set_tuple_id(tuple_id);
            slot.__set_slot_id(thrift_descriptors.slotDescriptors[i].id);
            ref.__set_slot_ref(slot);
            TExpr key;
            key.nodes.emplace_back(std::move(ref));
            keys.emplace_back(std::move(key));
        }
        TAggregationNode agg;
        agg.__set_grouping_exprs(keys);
        agg.__set_aggregate_functions({});
        agg.__set_need_finalize(true);
        agg.__set_use_streaming_preaggregation(false);
        agg.__set_intermediate_tuple_id(tuple_id);
        agg.__set_output_tuple_id(tuple_id);
        TPlanNode plan;
        plan.__set_node_id(1);
        plan.__set_node_type(TPlanNodeType::AGGREGATION_NODE);
        plan.__set_num_children(1);
        plan.__set_limit(limit);
        plan.row_tuples = {tuple_id};
        plan.__set_agg_node(agg);

        std::unique_ptr<AggregateBaseNode> node;
        if (distinct) {
            node = std::make_unique<DistinctBlockingNode>(state->obj_pool(), plan, *descriptors);
        } else {
            node = std::make_unique<AggregateBlockingNode>(state->obj_pool(), plan, *descriptors);
        }
        node->add_child(&child);
        CHECK_OK(node->init(plan, state));
        pipeline::PipelineBuilderContext context(&fragment, 1, 1);
        ASSIGN_OR_ABORT(auto sources, node->decompose_to_pipeline(&context));
        CHECK_GE(sources.size(), 1);
        auto pipelines = context.pipelines();
        return {sources[0]->get_raw_name(), pipelines.back()->sink_operator_factory()->get_raw_name(),
                pipelines.size()};
    }

    static TypeDescriptor map_type() {
        return TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT));
    }

    void expect_selection(const SelectedAggregateOperators& operators, bool distinct, bool spill, bool partitionwise) {
        const std::string prefix = partitionwise ? "spillable_partitionwise_" : (spill ? "spillable_" : "");
        const std::string kind = partitionwise ? (distinct ? "distinct" : "agg")
                                               : (distinct ? "aggregate_distinct_blocking" : "aggregate_blocking");
        EXPECT_EQ(prefix + kind + "_source", operators.source);
        EXPECT_EQ(prefix + kind + "_sink", operators.sink);
        EXPECT_EQ(spill ? 2 : 1, operators.pipelines);
    }
};

TEST_F(AggregateSpillSelectionTest, MapAndNestedMapUsePartitionwiseSpill) {
    const auto map = map_type();
    const std::vector<std::vector<TypeDescriptor>> keys = {
            {map},
            {TypeDescriptor::create_array_type(map)},
            {TypeDescriptor::create_struct_type({"id", "attributes"}, {TypeDescriptor(TYPE_INT), map})},
            {TypeDescriptor(TYPE_INT), map},
    };
    for (bool distinct : {false, true}) {
        for (const auto& key_types : keys) {
            SCOPED_TRACE(distinct ? "distinct" : "aggregate");
            SCOPED_TRACE(key_types.back().debug_string());
            expect_selection(select(key_types, distinct, distinct ? 4 : 2), distinct, true, true);
        }
    }
}

TEST_F(AggregateSpillSelectionTest, OrderableKeysKeepSortedSpillUnlessExplicitlyPartitionwise) {
    const std::vector<TypeDescriptor> keys = {
            TypeDescriptor(TYPE_INT),
            TypeDescriptor::create_varchar_type(20),
            TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT)),
            TypeDescriptor::create_struct_type(
                    {"id", "values"},
                    {TypeDescriptor(TYPE_INT), TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT))}),
    };
    for (bool distinct : {false, true}) {
        for (const auto& type : keys) {
            SCOPED_TRACE(distinct ? "distinct" : "aggregate");
            SCOPED_TRACE(type.debug_string());
            expect_selection(select({type}, distinct, distinct ? 4 : 2), distinct, true, false);
            // In particular, DISTINCT's mask alone must honor the explicit partitionwise flag.
            expect_selection(select({type}, distinct, distinct ? 4 : 2, true), distinct, true, true);
        }
    }
}

TEST_F(AggregateSpillSelectionTest, AutomaticFallbackHonorsEachOperatorMask) {
    for (bool distinct : {false, true}) {
        for (int64_t mask : {0, 2, 4}) {
            SCOPED_TRACE(distinct ? "distinct" : "aggregate");
            SCOPED_TRACE(mask);
            const bool spill = mask == (distinct ? 4 : 2);
            expect_selection(select({map_type()}, distinct, mask), distinct, spill, spill);
            expect_selection(select({map_type()}, distinct, mask, true), distinct, spill, spill);
        }
        expect_selection(select({map_type()}, distinct, 6, false, false), distinct, false, false);
    }
}

TEST_F(AggregateSpillSelectionTest, GlobalAggregateAndSmallLimitKeepExistingNonSpillBehavior) {
    expect_selection(select({}, false, 2), false, false, false);
    expect_selection(select({map_type()}, false, 2, false, true, 1), false, false, false);
}

} // namespace
} // namespace starrocks
