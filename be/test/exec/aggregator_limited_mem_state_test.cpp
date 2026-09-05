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
//

#include <gtest/gtest.h>

#include "common/config_exec_flow_fwd.h"
#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_streaming_sink_operator.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Unit tests for LimitedMemAggState::clamp_budget, the guard behind the streaming
// pre-aggregation OOM fix.
//
// AggregateStreamingSinkOperator / AggregateDistinctStreamingSinkOperator::set_execute_mode()
// latch the LIMITED_MEM budget into LimitedMemAggState::limited_memory_size exactly once (the
// driver's enter_low_memory_mode() is a one-shot latch). has_limited() then evaluates
// `limited_memory_size > 0 && memory_usage() >= limited_memory_size`. If the budget were
// latched to 0, has_limited() would be false forever, degrading LIMITED_MEM back to AUTO with
// no memory cap and letting a high-cardinality group-by OOM. clamp_budget() computes
// min(usage, cap) but floors the result at 1 so the budget is never 0.
class LimitedMemAggStateTest : public ::testing::Test {};

// Representative cap: config::streaming_agg_limited_memory_size default = 128 MiB.
static constexpr int64_t kCap = 128LL * 1024 * 1024;

// Heart of the fix: a 0 usage must be floored to 1, never left at 0.
TEST_F(LimitedMemAggStateTest, zero_usage_is_floored_to_one) {
    EXPECT_EQ(1u, LimitedMemAggState::clamp_budget(0, kCap));
}

// Positive usage below the cap passes through unchanged.
TEST_F(LimitedMemAggStateTest, usage_below_cap_passthrough) {
    EXPECT_EQ(1u, LimitedMemAggState::clamp_budget(1, kCap));
    // 24 = empty phmap flat_hash_map dump_bound (sizeof(size_t) * 3); 256 = default fixed hash map.
    EXPECT_EQ(24u, LimitedMemAggState::clamp_budget(24, kCap));
    EXPECT_EQ(256u, LimitedMemAggState::clamp_budget(256, kCap));
    EXPECT_EQ(static_cast<size_t>(kCap - 1), LimitedMemAggState::clamp_budget(kCap - 1, kCap));
}

// Usage at or above the cap is clamped down to the cap.
TEST_F(LimitedMemAggStateTest, usage_at_or_above_cap_clamped) {
    EXPECT_EQ(static_cast<size_t>(kCap), LimitedMemAggState::clamp_budget(kCap, kCap));
    EXPECT_EQ(static_cast<size_t>(kCap), LimitedMemAggState::clamp_budget(kCap + 1, kCap));
    EXPECT_EQ(static_cast<size_t>(kCap), LimitedMemAggState::clamp_budget(kCap * 4, kCap));
}

// The invariant that actually prevents the OOM regression: the budget is always > 0, so
// has_limited() can never be permanently disabled, whatever memory usage is reported.
TEST_F(LimitedMemAggStateTest, budget_is_always_positive) {
    const int64_t usages[] = {0, 1, 24, 256, kCap - 1, kCap, kCap + 1, kCap * 10};
    for (int64_t usage : usages) {
        EXPECT_GT(LimitedMemAggState::clamp_budget(usage, kCap), 0u) << "usage=" << usage;
    }
}

} // namespace starrocks

namespace starrocks::pipeline {

// Operator-level tests: drive the real AggregateStreamingSinkOperator /
// AggregateDistinctStreamingSinkOperator::set_execute_mode() implementations, i.e. the code
// the pipeline driver invokes when it downgrades a releaseable sink under memory pressure.
// The aggregator is not prepare()d (that needs a full plan-fragment context); instead the few
// fields set_execute_mode() reads are set directly — tests compile with -fno-access-control.
class StreamingAggSinkExecuteModeTest : public ::testing::Test {
protected:
    void SetUp() override {
        _state.set_chunk_size(4096);
        _tnode.__set_node_id(1);
        _tnode.__set_node_type(TPlanNodeType::AGGREGATION_NODE);
        _tnode.__set_num_children(1);
        _tnode.__set_limit(-1);
        TAggregationNode agg_node;
        agg_node.__set_use_streaming_preaggregation(true);
        _tnode.__set_agg_node(agg_node);
    }

    RuntimeState _state;
    RuntimeProfile _profile{"test"};
    AggStatistics _agg_stat{&_profile};
    TPlanNode _tnode;
    // AggregateStreamingSinkOperatorFactory keeps a reference to this vector.
    std::vector<RuntimeFilterBuildDescriptor*> _no_runtime_filters;
};

TEST_F(StreamingAggSinkExecuteModeTest, streaming_sink_latches_positive_capped_budget) {
    auto aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);
    AggregateStreamingSinkOperatorFactory factory(1, 1, aggregator_factory, _no_runtime_filters);
    auto op = factory.create(1, 0);
    auto aggregator = aggregator_factory->get_or_create(0);

    aggregator->_streaming_preaggregation_mode = TStreamingPreaggregationMode::AUTO;
    aggregator->_hash_map_variant.init(&_state, AggHashMapVariant::Type::phase1_int32, &_agg_stat);

    op->set_execute_mode(1);

    // AUTO is downgraded to LIMITED_MEM and the latched budget is positive and config-capped.
    EXPECT_EQ(TStreamingPreaggregationMode::LIMITED_MEM, aggregator->streaming_preaggregation_mode());
    auto* sink = static_cast<AggregateStreamingSinkOperator*>(op.get());
    EXPECT_GT(sink->_limited_mem_state.limited_memory_size, 0u);
    EXPECT_LE(sink->_limited_mem_state.limited_memory_size,
              static_cast<size_t>(config::streaming_agg_limited_memory_size));
    EXPECT_EQ(LimitedMemAggState::clamp_budget(aggregator->hash_map_memory_usage(),
                                               config::streaming_agg_limited_memory_size),
              sink->_limited_mem_state.limited_memory_size);
}

TEST_F(StreamingAggSinkExecuteModeTest, streaming_sink_keeps_non_auto_mode) {
    auto aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);
    AggregateStreamingSinkOperatorFactory factory(1, 1, aggregator_factory, _no_runtime_filters);
    auto op = factory.create(1, 0);
    auto aggregator = aggregator_factory->get_or_create(0);

    aggregator->_streaming_preaggregation_mode = TStreamingPreaggregationMode::FORCE_STREAMING;
    aggregator->_hash_map_variant.init(&_state, AggHashMapVariant::Type::phase1_int32, &_agg_stat);

    op->set_execute_mode(1);

    // Only AUTO is downgraded; an explicit mode is preserved but the budget is still latched.
    EXPECT_EQ(TStreamingPreaggregationMode::FORCE_STREAMING, aggregator->streaming_preaggregation_mode());
    auto* sink = static_cast<AggregateStreamingSinkOperator*>(op.get());
    EXPECT_GT(sink->_limited_mem_state.limited_memory_size, 0u);
}

TEST_F(StreamingAggSinkExecuteModeTest, distinct_sink_uses_hash_set_metric_and_caps_budget) {
    auto aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);
    AggregateDistinctStreamingSinkOperatorFactory factory(1, 1, aggregator_factory);
    auto op = factory.create(1, 0);
    auto aggregator = aggregator_factory->get_or_create(0);

    // Distinct aggregation: only the hash SET is initialized, _hash_map_variant never is.
    aggregator->_streaming_preaggregation_mode = TStreamingPreaggregationMode::AUTO;
    aggregator->_is_only_group_by_columns = true;
    aggregator->_hash_set_variant.init(&_state, AggHashSetVariant::Type::phase1_int32, &_agg_stat);

    op->set_execute_mode(1);

    EXPECT_EQ(TStreamingPreaggregationMode::LIMITED_MEM, aggregator->streaming_preaggregation_mode());
    auto* sink = static_cast<AggregateDistinctStreamingSinkOperator*>(op.get());
    // The budget comes from the hash-set-based memory_usage(), stays positive, and is
    // config-capped — the same scale has_limited() compares against.
    EXPECT_GT(sink->_limited_mem_state.limited_memory_size, 0u);
    EXPECT_LE(sink->_limited_mem_state.limited_memory_size,
              static_cast<size_t>(config::streaming_agg_limited_memory_size));
    EXPECT_EQ(LimitedMemAggState::clamp_budget(aggregator->memory_usage(), config::streaming_agg_limited_memory_size),
              sink->_limited_mem_state.limited_memory_size);
}

TEST_F(StreamingAggSinkExecuteModeTest, distinct_sink_floors_zero_usage_budget_to_one) {
    auto aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);
    AggregateDistinctStreamingSinkOperatorFactory factory(1, 1, aggregator_factory);
    auto op = factory.create(1, 0);
    auto aggregator = aggregator_factory->get_or_create(0);

    // With no group-by state at all, memory_usage() reports 0. The latched budget must be
    // floored to 1: a 0 budget would make has_limited() permanently false (its predicate is
    // `limited_memory_size > 0 && ...`), silently degrading LIMITED_MEM back to AUTO.
    aggregator->_streaming_preaggregation_mode = TStreamingPreaggregationMode::AUTO;
    ASSERT_EQ(0, aggregator->memory_usage());

    op->set_execute_mode(1);

    auto* sink = static_cast<AggregateDistinctStreamingSinkOperator*>(op.get());
    EXPECT_EQ(1u, sink->_limited_mem_state.limited_memory_size);
}

} // namespace starrocks::pipeline
