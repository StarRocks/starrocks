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

#include <atomic>

#include "base/utility/defer_op.h"
#include "column/column_helper.h"
#include "common/config_exec_flow_fwd.h"
#include "common/object_pool.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregator.h"
#include "exprs/expr_context.h"
#include "exprs/literal.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

// Which key representation an aggregation ran with leaves no trace in the plan and has no counter
// of its own, so it could previously only be inferred from hash table bytes per group. These tests
// cover the `HashVariant` profile line that makes it visible.
class AggregatorHashVariantTest : public ::testing::Test {
protected:
    static constexpr size_t kChunkSize = 4096;

    void SetUp() override {
        _state.set_chunk_size(kChunkSize);
        _tnode.__set_node_id(1);
        _tnode.__set_node_type(TPlanNodeType::AGGREGATION_NODE);
        _tnode.__set_num_children(1);
        _tnode.__set_limit(-1);
        TAggregationNode agg_node;
        agg_node.__set_use_streaming_preaggregation(true);
        _tnode.__set_agg_node(agg_node);
    }

    static ColumnType column_type(LogicalType ltype, bool nullable) {
        return ColumnType{TypeDescriptor(ltype), nullable};
    }

    // _init_agg_hash_variant() never evaluates the group-by expressions; it only needs the list to
    // be non-empty, which is how _report_hash_variant() tells a grouped aggregation from a global
    // one. A literal per column is therefore enough, and an unprepared ExprContext destructs
    // cleanly.
    ExprContext* make_group_by_ctx() {
        auto* literal = _pool.add(
                new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1), TYPE_BIGINT_DESC));
        return _pool.add(new ExprContext(literal));
    }

    // Everything _init_agg_hash_variant() reads, without the descriptor table and plan fragment a
    // real prepare() also needs. The aggregator is marked closed because its destructor otherwise
    // tears down state that was never built.
    AggregatorPtr make_aggregator(const std::vector<ColumnType>& group_by_types) {
        auto factory = std::make_shared<AggregatorFactory>(_tnode);
        auto aggregator = factory->get_or_create(0);
        aggregator->_state = &_state;
        aggregator->_agg_stat = &_agg_stat;
        aggregator->_runtime_profile = &_profile;
        aggregator->_group_by_types = group_by_types;
        aggregator->_ranges.resize(group_by_types.size());
        for (size_t i = 0; i < group_by_types.size(); ++i) {
            aggregator->_group_by_expr_ctxs.emplace_back(make_group_by_ctx());
        }
        aggregator->_is_closed = true;
        return aggregator;
    }

    std::string reported_variant() { return _profile.get_info_string("HashVariant").value_or(""); }

    RuntimeState _state;
    RuntimeProfile _profile{"AggregatorHashVariantTest"};
    AggStatistics _agg_stat{&_profile};
    TPlanNode _tnode;
    ObjectPool _pool;
};

// The name has to come from the same enum the variant switches on, or the profile would report a
// container the query is not running.
TEST_F(AggregatorHashVariantTest, VariantNamesMatchTheEnum) {
    _state.set_chunk_size(kChunkSize);

    AggHashMapVariant map_variant;
    const std::pair<AggHashMapVariant::Type, const char*> map_cases[] = {
            {AggHashMapVariant::Type::phase1_slice, "phase1_slice"},
            {AggHashMapVariant::Type::phase1_slice_fx16, "phase1_slice_fx16"},
            {AggHashMapVariant::Type::phase2_slice_fx8, "phase2_slice_fx8"},
            {AggHashMapVariant::Type::phase1_slice_cx4, "phase1_slice_cx4"},
            {AggHashMapVariant::Type::phase1_slice_two_level, "phase1_slice_two_level"},
    };
    for (const auto& [type, name] : map_cases) {
        map_variant.init(&_state, type, &_agg_stat);
        EXPECT_STREQ(name, map_variant.type_name());
        EXPECT_EQ(type, map_variant.type());
    }

    AggHashSetVariant set_variant;
    const std::pair<AggHashSetVariant::Type, const char*> set_cases[] = {
            {AggHashSetVariant::Type::phase1_slice, "phase1_slice"},
            {AggHashSetVariant::Type::phase1_slice_fx16, "phase1_slice_fx16"},
            {AggHashSetVariant::Type::phase1_slice_two_level, "phase1_slice_two_level"},
    };
    for (const auto& [type, name] : set_cases) {
        set_variant.init(&_state, type, &_agg_stat);
        EXPECT_STREQ(name, set_variant.type_name());
        EXPECT_EQ(type, set_variant.type());
    }
}

// Three nullable INTs serialize to 15 bytes, which is the widest shape the fixed-size ladder
// covers today.
TEST_F(AggregatorHashVariantTest, GroupedAggregationReportsItsVariant) {
    auto aggregator =
            make_aggregator({column_type(TYPE_INT, true), column_type(TYPE_INT, true), column_type(TYPE_INT, true)});
    aggregator->_init_agg_hash_variant(aggregator->_hash_map_variant);

    EXPECT_STREQ("phase1_slice_fx16", aggregator->_hash_map_variant.type_name());
    EXPECT_EQ("phase1_slice_fx16", reported_variant());
}

// Five nullable BIGINTs are 45 bytes: past the top of the ladder, so the serialized fallback --
// the case this line exists to make visible.
TEST_F(AggregatorHashVariantTest, WideKeyReportsSerialized) {
    auto aggregator = make_aggregator({column_type(TYPE_BIGINT, true), column_type(TYPE_BIGINT, true),
                                       column_type(TYPE_BIGINT, true), column_type(TYPE_BIGINT, true),
                                       column_type(TYPE_BIGINT, true)});
    aggregator->_init_agg_hash_variant(aggregator->_hash_map_variant);

    EXPECT_EQ("phase1_slice", reported_variant());
}

// The variant is not fixed at init: once the table crosses the two-level threshold the serialized
// and string rungs migrate to their two-level twins, and the sink keeps polling for that on every
// chunk afterwards. A profile that still named the one-level table would point a reader at the
// wrong memory behaviour.
TEST_F(AggregatorHashVariantTest, TwoLevelConversionRefreshesReportedVariant) {
    auto aggregator = make_aggregator({column_type(TYPE_BIGINT, true), column_type(TYPE_BIGINT, true),
                                       column_type(TYPE_BIGINT, true), column_type(TYPE_BIGINT, true),
                                       column_type(TYPE_BIGINT, true)});
    aggregator->_init_agg_hash_variant(aggregator->_hash_map_variant);
    ASSERT_EQ("phase1_slice", reported_variant());

    aggregator->_mem_pool = std::make_unique<MemPool>();

    const int64_t saved_threshold = config::two_level_memory_threshold;
    DeferOp restore([&]() { config::two_level_memory_threshold = saved_threshold; });
    config::two_level_memory_threshold = 0;

    aggregator->try_convert_to_two_level_map();
    EXPECT_STREQ("phase1_slice_two_level", aggregator->_hash_map_variant.type_name());
    EXPECT_EQ("phase1_slice_two_level", reported_variant());

    // Still over the threshold, so the sink keeps calling; nothing converts any more and the
    // report must stay put rather than be rewritten on every chunk.
    aggregator->try_convert_to_two_level_map();
    EXPECT_EQ("phase1_slice_two_level", reported_variant());
}

TEST_F(AggregatorHashVariantTest, TwoLevelSetConversionRefreshesReportedVariant) {
    auto aggregator = make_aggregator({column_type(TYPE_BIGINT, true), column_type(TYPE_BIGINT, true),
                                       column_type(TYPE_BIGINT, true), column_type(TYPE_BIGINT, true),
                                       column_type(TYPE_BIGINT, true)});
    aggregator->_is_only_group_by_columns = true;
    aggregator->_init_agg_hash_variant(aggregator->_hash_set_variant);
    ASSERT_EQ("phase1_slice", reported_variant());

    aggregator->_mem_pool = std::make_unique<MemPool>();

    const int64_t saved_threshold = config::two_level_memory_threshold;
    DeferOp restore([&]() { config::two_level_memory_threshold = saved_threshold; });
    config::two_level_memory_threshold = 0;

    aggregator->try_convert_to_two_level_set();
    EXPECT_STREQ("phase1_slice_two_level", aggregator->_hash_set_variant.type_name());
    EXPECT_EQ("phase1_slice_two_level", reported_variant());
}

TEST_F(AggregatorHashVariantTest, TwoLevelInt32ConversionRefreshesReportedVariant) {
    auto aggregator = make_aggregator({column_type(TYPE_INT, false)});
    aggregator->_init_agg_hash_variant(aggregator->_hash_map_variant);
    ASSERT_EQ("phase1_int32", reported_variant());

    aggregator->_mem_pool = std::make_unique<MemPool>();

    const int64_t saved_threshold = config::two_level_memory_threshold;
    DeferOp restore([&]() { config::two_level_memory_threshold = saved_threshold; });
    config::two_level_memory_threshold = 0;

    aggregator->try_convert_to_two_level_map();
    EXPECT_STREQ("phase1_int32_two_level", aggregator->_hash_map_variant.type_name());
    EXPECT_EQ("phase1_int32_two_level", reported_variant());

    aggregator->try_convert_to_two_level_map();
    EXPECT_EQ("phase1_int32_two_level", reported_variant());
}

TEST_F(AggregatorHashVariantTest, TwoLevelInt32SetConversionRefreshesReportedVariant) {
    auto aggregator = make_aggregator({column_type(TYPE_INT, false)});
    aggregator->_is_only_group_by_columns = true;
    aggregator->_init_agg_hash_variant(aggregator->_hash_set_variant);
    ASSERT_EQ("phase1_int32", reported_variant());

    aggregator->_mem_pool = std::make_unique<MemPool>();

    const int64_t saved_threshold = config::two_level_memory_threshold;
    DeferOp restore([&]() { config::two_level_memory_threshold = saved_threshold; });
    config::two_level_memory_threshold = 0;

    aggregator->try_convert_to_two_level_set();
    EXPECT_STREQ("phase1_int32_two_level", aggregator->_hash_set_variant.type_name());
    EXPECT_EQ("phase1_int32_two_level", reported_variant());
}

// Exercises AggHashMapWithSerializedKeyFixedSize::compute_agg_prefetch through Aggregator's
// translation unit (be/src/exec/aggregator.cpp) across nullable and non-nullable fixed-size
// keys and all HTBuildOp instantiations (allocate, process_limit < limit and >= limit, and
// fill_not_found).
TEST_F(AggregatorHashVariantTest, FixedSizePrefetchAllBuildPaths) {
    constexpr size_t kRows = 32;
    const double saved_ratio = config::agg_prefetch_l2_ratio;
    DeferOp restore_ratio([&]() { config::agg_prefetch_l2_ratio = saved_ratio; });
    config::agg_prefetch_l2_ratio = 0.0;

    auto run_case = [&](const std::vector<ColumnType>& types, const char* expected_variant) {
        auto aggregator = make_aggregator(types);
        aggregator->_init_agg_hash_variant(aggregator->_hash_map_variant);
        ASSERT_STREQ(expected_variant, aggregator->_hash_map_variant.type_name());

        aggregator->_mem_pool = std::make_unique<MemPool>();
        aggregator->_state_allocator.pool = aggregator->_mem_pool.get();
        aggregator->_state_allocator.aggregate_key_size = 32;
        aggregator->_tmp_agg_states.resize(kRows);

        Columns group_by_columns;
        for (size_t col_idx = 0; col_idx < types.size(); ++col_idx) {
            auto col = ColumnHelper::create_column(types[col_idx].result_type, types[col_idx].is_nullable);
            for (size_t row = 0; row < kRows; ++row) {
                if (types[col_idx].is_nullable && row == 0 && col_idx == 0) {
                    col->append_nulls(1);
                } else {
                    const int64_t val = static_cast<int64_t>(row * 3 + col_idx * 2 + 1);
                    switch (types[col_idx].result_type.type) {
                    case TYPE_TINYINT:
                        col->append_datum(Datum(static_cast<int8_t>(val)));
                        break;
                    case TYPE_SMALLINT:
                        col->append_datum(Datum(static_cast<int16_t>(val)));
                        break;
                    case TYPE_INT:
                        col->append_datum(Datum(static_cast<int32_t>(val)));
                        break;
                    case TYPE_BIGINT:
                        col->append_datum(Datum(static_cast<int64_t>(val)));
                        break;
                    default:
                        FAIL() << "Unexpected column type in test";
                    }
                }
            }
            group_by_columns.emplace_back(std::move(col));
        }
        aggregator->_group_by_columns = group_by_columns;

        // 1. HTBuildOp::allocate (inserts first kRows / 2 distinct groups via compute_agg_prefetch).
        aggregator->build_hash_map(kRows / 2, false);
        EXPECT_EQ(kRows / 2, aggregator->_hash_map_variant.size());

        // 2. HTBuildOp::process_limit (rows 0..23 hit hash_table_size < extra->limits, rows 24..31 hit >= limits).
        constexpr size_t kLimit = (kRows * 3) / 4;
        aggregator->_params->enable_pipeline_share_limit = true;
        aggregator->_limit = kLimit;
        std::atomic<int64_t> shared_limit_countdown{static_cast<int64_t>(kRows)};
        aggregator->build_hash_map(kRows, shared_limit_countdown, true);
        EXPECT_EQ(kLimit, aggregator->_hash_map_variant.size());
        for (size_t i = 0; i < kLimit; ++i) {
            EXPECT_EQ(0, aggregator->_streaming_selection[i]);
        }
        for (size_t i = kLimit; i < kRows; ++i) {
            EXPECT_EQ(1, aggregator->_streaming_selection[i]);
        }

        // 3. HTBuildOp::fill_not_found (build_hash_map_with_selection).
        aggregator->build_hash_map_with_selection(kRows);
        for (size_t i = 0; i < kLimit; ++i) {
            EXPECT_EQ(0, aggregator->_streaming_selection[i]);
        }
        for (size_t i = kLimit; i < kRows; ++i) {
            EXPECT_EQ(1, aggregator->_streaming_selection[i]);
        }

        // 4. HTBuildOp<true, true, false> (build_hash_map_with_selection_and_allocation allocates remaining rows).
        aggregator->build_hash_map_with_selection_and_allocation(kRows, false);
        EXPECT_EQ(kRows, aggregator->_hash_map_variant.size());
    };

    run_case({column_type(TYPE_TINYINT, true), column_type(TYPE_TINYINT, false)}, "phase1_slice_fx4");
    run_case({column_type(TYPE_INT, true), column_type(TYPE_SMALLINT, false)}, "phase1_slice_fx8");
    run_case({column_type(TYPE_INT, true), column_type(TYPE_INT, true), column_type(TYPE_INT, true)},
             "phase1_slice_fx16");
    run_case({column_type(TYPE_BIGINT, false), column_type(TYPE_INT, false)}, "phase1_slice_fx16");
}

// An aggregate with no grouping keys accumulates into a single state and never touches the hash
// table, so its variant field holds nothing but the enum's default. Reporting that would read as
// a missed optimization on exactly the nodes where none was possible.
TEST_F(AggregatorHashVariantTest, GlobalAggregationReportsNothing) {
    auto aggregator = make_aggregator({});
    aggregator->_init_agg_hash_variant(aggregator->_hash_map_variant);

    EXPECT_FALSE(_profile.get_info_string("HashVariant").has_value());
}

} // namespace starrocks
