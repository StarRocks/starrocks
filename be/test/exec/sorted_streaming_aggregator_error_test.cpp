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

#include <memory>
#include <stdexcept>
#include <unordered_set>

#include "column/fixed_length_column.h"
#include "exec/sorted_streaming_aggregator.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/ai_collect_text.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {
struct TrackedAggregateState {
    std::unique_ptr<int64_t> value = std::make_unique<int64_t>(0);
};

class TrackedAggregate final : public AggregateFunctionBatchHelper<TrackedAggregateState, TrackedAggregate> {
public:
    enum Failure { NONE, UPDATE, MERGE, SERIALIZE, FINALIZE, THROW_UPDATE, THROW_FINALIZE };
    mutable size_t created = 0;
    mutable size_t destroyed = 0;
    mutable std::unordered_set<AggDataPtr> live;
    size_t throw_on_create = 0;
    Failure failure = NONE;

    void create(FunctionContext* ctx, AggDataPtr state) const override {
        if (throw_on_create && created + 1 == throw_on_create) throw std::bad_alloc();
        new (state) TrackedAggregateState;
        EXPECT_TRUE(live.insert(state).second);
        ++created;
    }
    void destroy(FunctionContext* ctx, AggDataPtr state) const override {
        ASSERT_EQ(1, live.erase(state));
        data(state).~TrackedAggregateState();
        ++destroyed;
    }
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row) const override {
        ++*data(state).value;
        if (failure == UPDATE) ctx->set_error("injected update failure");
        if (failure == THROW_UPDATE) throw std::bad_alloc();
    }
    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row) const override {
        ++*data(state).value;
        if (failure == MERGE) ctx->set_error("injected merge failure");
    }
    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        down_cast<Int64Column*>(to)->append(*data(state).value);
        if (failure == SERIALIZE) ctx->set_error("injected serialize failure");
    }
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        down_cast<Int64Column*>(to)->append(*data(state).value);
        if (failure == FINALIZE) ctx->set_error("injected finalize failure");
        if (failure == THROW_FINALIZE) throw std::bad_alloc();
    }
    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t size,
                                     MutableColumnPtr& dst) const override {}
    std::string get_name() const override { return "tracked_test_aggregate"; }
};
} // namespace

class SortedStreamingAggregatorErrorTest : public testing::Test {
protected:
    void SetUp() override { runtime.set_chunk_size(16); }

    std::unique_ptr<SortedStreamingAggregator> make_aggregator(const std::vector<const AggregateFunction*>& functions,
                                                               bool finalize = true,
                                                               LogicalType result_type = TYPE_BIGINT) {
        auto aggregate = std::make_unique<SortedStreamingAggregator>(std::make_shared<AggregatorParams>());
        aggregate->_state = &runtime;
        aggregate->_pool = std::make_unique<ObjectPool>();
        aggregate->_mem_pool = std::make_unique<MemPool>();
        aggregate->_agg_stat = &statistics;
        aggregate->_needs_finalize = finalize;
        aggregate->_is_opened = true; // Inject prepared functions; open only initializes sorted state storage.
        aggregate->_group_by_types = {{TypeDescriptor(TYPE_BIGINT), false}};
        aggregate->_agg_functions = functions;
        size_t offset = 0;
        TTupleDescriptor tuple;
        tuple.id = 0;
        auto* output = aggregate->_pool->add(new TupleDescriptor(tuple));
        output->add_slot(aggregate->_pool->add(new SlotDescriptor(0, "key", TypeDescriptor(TYPE_BIGINT))));
        for (size_t i = 0; i < functions.size(); ++i) {
            auto* ctx =
                    FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, TypeDescriptor(result_type));
            contexts.emplace_back(ctx);
            aggregate->_agg_fn_ctxs.push_back(ctx);
            aggregate->_agg_states_offsets.push_back(offset);
            offset += (functions[i]->size() + 15) / 16 * 16;
            aggregate->_agg_fn_types.push_back(
                    {TypeDescriptor(result_type), TypeDescriptor(result_type), {}, false, false});
            aggregate->_fns.emplace_back();
            aggregate->_fns.back().binary_type = TFunctionBinaryType::BUILTIN;
            aggregate->_is_merge_funcs.push_back(false);
            output->add_slot(aggregate->_pool->add(new SlotDescriptor(i + 1, "value", TypeDescriptor(result_type))));
        }
        aggregate->_agg_states_total_size = offset;
        aggregate->_output_tuple_desc = output;
        aggregate->_intermediate_tuple_desc = output;
        EXPECT_TRUE(aggregate->open(&runtime).ok());
        return aggregate;
    }

    void input(SortedStreamingAggregator* aggregate, std::initializer_list<int64_t> keys, ColumnPtr values = nullptr) {
        auto key_column = Int64Column::create();
        for (int64_t key : keys) key_column->append(key);
        aggregate->_group_by_columns = {key_column};
        if (!values) values = key_column;
        aggregate->_agg_input_columns.assign(aggregate->_agg_functions.size(), Columns{values});
        aggregate->_agg_input_raw_columns.assign(aggregate->_agg_functions.size(), {values.get()});
    }

    RuntimeState runtime;
    RuntimeProfile profile{"sorted failure test"};
    AggStatistics statistics{&profile};
    std::vector<std::unique_ptr<FunctionContext>> contexts;
};

TEST_F(SortedStreamingAggregatorErrorTest, NoRowsEscapeAnyFunctionError) {
    for (auto failure :
         {TrackedAggregate::UPDATE, TrackedAggregate::MERGE, TrackedAggregate::SERIALIZE, TrackedAggregate::FINALIZE,
          TrackedAggregate::THROW_UPDATE, TrackedAggregate::THROW_FINALIZE}) {
        TrackedAggregate function;
        function.failure = failure;
        auto aggregate = make_aggregator({&function}, failure != TrackedAggregate::SERIALIZE);
        input(aggregate.get(), {1, 1, 2, 2, 3});
        auto result = aggregate->streaming_compute_agg_state(5, failure != TrackedAggregate::MERGE);
        EXPECT_FALSE(result.ok()) << failure;
        EXPECT_EQ(3, function.created);
        EXPECT_EQ(function.created, function.destroyed);
        EXPECT_TRUE(function.live.empty());
        EXPECT_EQ(nullptr, aggregate->_last_state);
        EXPECT_TRUE(aggregate->_last_columns.empty());
        aggregate->close(&runtime);
        EXPECT_EQ(function.created, function.destroyed);
    }
}

TEST_F(SortedStreamingAggregatorErrorTest, ErrorReclaimsRepeatedAndSeparateOldTailExactlyOnce) {
    for (bool continues_old_tail : {false, true}) {
        TrackedAggregate function;
        auto aggregate = make_aggregator({&function});
        input(aggregate.get(), {1, 1});
        auto first = aggregate->streaming_compute_agg_state(2);
        ASSERT_TRUE(first.ok());
        EXPECT_EQ(0, (*first)->num_rows());
        ASSERT_NE(nullptr, aggregate->_last_state);
        function.failure = TrackedAggregate::UPDATE;
        if (continues_old_tail)
            input(aggregate.get(), {1, 1, 2, 2});
        else
            input(aggregate.get(), {2, 2, 3, 3});
        auto failed = aggregate->streaming_compute_agg_state(4);
        EXPECT_FALSE(failed.ok());
        EXPECT_EQ(function.created, function.destroyed);
        EXPECT_TRUE(function.live.empty());
        EXPECT_EQ(nullptr, aggregate->_last_state);
    }
}

TEST_F(SortedStreamingAggregatorErrorTest, PartialCreateRollsBackOnlyConstructedStates) {
    TrackedAggregate first, second;
    auto aggregate = make_aggregator({&first, &second});
    input(aggregate.get(), {1, 1});
    ASSERT_TRUE(aggregate->streaming_compute_agg_state(2).ok());
    second.throw_on_create = 3; // Old tail + first new group succeed, next new group throws.
    input(aggregate.get(), {1, 2, 2, 3});
    auto result = aggregate->streaming_compute_agg_state(4);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(3, first.created);
    EXPECT_EQ(2, second.created);
    EXPECT_EQ(first.created, first.destroyed);
    EXPECT_EQ(second.created, second.destroyed);
    EXPECT_TRUE(first.live.empty());
    EXPECT_TRUE(second.live.empty());
    EXPECT_EQ(nullptr, aggregate->_last_state);
}

TEST_F(SortedStreamingAggregatorErrorTest, EosErrorsReleaseTailBeforeReturning) {
    for (auto failure : {TrackedAggregate::FINALIZE, TrackedAggregate::SERIALIZE, TrackedAggregate::THROW_FINALIZE}) {
        TrackedAggregate function;
        auto aggregate = make_aggregator({&function}, failure != TrackedAggregate::SERIALIZE);
        input(aggregate.get(), {1, 1});
        ASSERT_TRUE(aggregate->streaming_compute_agg_state(2).ok());
        function.failure = failure;
        EXPECT_FALSE(aggregate->pull_eos_chunk().ok());
        EXPECT_EQ(1, function.created);
        EXPECT_EQ(1, function.destroyed);
        EXPECT_TRUE(function.live.empty());
        EXPECT_EQ(nullptr, aggregate->_last_state);
        EXPECT_TRUE(aggregate->_last_columns.empty());
        aggregate->close(&runtime);
        EXPECT_EQ(1, function.destroyed);
    }
}

TEST_F(SortedStreamingAggregatorErrorTest, EarlyCloseAndRepeatedCloseDestroyRetainedTailOnce) {
    TrackedAggregate function;
    auto aggregate = make_aggregator({&function});
    input(aggregate.get(), {1, 1});
    ASSERT_TRUE(aggregate->streaming_compute_agg_state(2).ok());
    EXPECT_EQ(0, function.destroyed);
    aggregate->close(&runtime);
    aggregate->close(&runtime);
    EXPECT_EQ(1, function.destroyed);
    EXPECT_TRUE(function.live.empty());
    EXPECT_EQ(nullptr, aggregate->_last_state);
    EXPECT_TRUE(aggregate->_last_columns.empty());
}

TEST_F(SortedStreamingAggregatorErrorTest, OrdinarySumAndCountKeepCompletedGroupsAndTail) {
    auto* sum = get_aggregate_function("sum", TYPE_BIGINT, TYPE_BIGINT, false);
    auto* count = get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, false);
    ASSERT_NE(nullptr, sum);
    ASSERT_NE(nullptr, count);
    auto aggregate = make_aggregator({sum, count});
    auto values = Int64Column::create();
    for (int64_t value : {10, 20, 30}) values->append(value);
    input(aggregate.get(), {1, 1, 2}, values);
    auto result = aggregate->streaming_compute_agg_state(3);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(1, (*result)->num_rows());
    EXPECT_EQ(1, (*result)->get_column_by_index(0)->get(0).get_int64());
    EXPECT_EQ(30, (*result)->get_column_by_index(1)->get(0).get_int64());
    EXPECT_EQ(2, (*result)->get_column_by_index(2)->get(0).get_int64());
    auto tail = aggregate->pull_eos_chunk();
    ASSERT_TRUE(tail.ok()) << tail.status();
    ASSERT_EQ(1, (*tail)->num_rows());
    EXPECT_EQ(2, (*tail)->get_column_by_index(0)->get(0).get_int64());
    EXPECT_EQ(30, (*tail)->get_column_by_index(1)->get(0).get_int64());
    EXPECT_EQ(1, (*tail)->get_column_by_index(2)->get(0).get_int64());
    auto exhausted = aggregate->pull_eos_chunk();
    ASSERT_TRUE(exhausted.ok());
    EXPECT_EQ(nullptr, *exhausted);
}

TEST_F(SortedStreamingAggregatorErrorTest, MergeOverCollectorLimitCannotPublishPartialGroup) {
    AICollectTextAggregateFunction collector(kAiAggregateStateHeaderBytes + kAiAggregateRecordLengthBytes + 1);
    auto aggregate = make_aggregator({&collector}, true, TYPE_VARBINARY);
    auto text = BinaryColumn::create();
    text->append(Slice("x"));
    text->append(Slice("y"));
    MutableColumnPtr partials = BinaryColumn::create();
    collector.convert_to_serialize_format(aggregate->_agg_fn_ctxs[0], {text}, 2, partials);
    input(aggregate.get(), {1, 1}, partials);
    auto result = aggregate->streaming_compute_agg_state(2, false);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(nullptr, aggregate->_last_state);
    EXPECT_EQ(0, aggregate->_allocator->memory_usage());
}
} // namespace starrocks
