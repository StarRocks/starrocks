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

#include <array>
#include <memory>
#include <unordered_set>

#include "exprs/agg/aggregate.h"

namespace starrocks {
namespace {
struct OwnedBatchState {
    std::unique_ptr<int> value = std::make_unique<int>(0);
};

class CreateFailureAggregate : public AggregateFunctionBatchHelper<OwnedBatchState, CreateFailureAggregate> {
public:
    mutable size_t created = 0;
    mutable size_t destroyed = 0;
    mutable std::unordered_set<AggDataPtr> live;
    void create(FunctionContext*, AggDataPtr state) const override {
        if (created == 2) throw std::bad_alloc();
        new (state) OwnedBatchState;
        live.insert(state);
        ++created;
    }
    void destroy(FunctionContext*, AggDataPtr state) const override {
        ASSERT_EQ(1, live.erase(state));
        data(state).~OwnedBatchState();
        ++destroyed;
    }
    void update(FunctionContext*, const Column**, AggDataPtr, size_t) const override {}
    void merge(FunctionContext*, const Column*, AggDataPtr, size_t) const override {}
    void serialize_to_column(FunctionContext*, ConstAggDataPtr, Column*) const override {}
    void finalize_to_column(FunctionContext*, ConstAggDataPtr, Column*) const override {}
    void convert_to_serialize_format(FunctionContext*, const Columns&, size_t, MutableColumnPtr&) const override {}
    std::string get_name() const override { return "create_failure_test"; }
};

class DefaultCreateFailureAggregate final : public CreateFailureAggregate {
public:
    void batch_create_with_selection(FunctionContext* ctx, size_t size, Buffer<AggDataPtr>& states, size_t offset,
                                     const Filter& selection) const override {
        AggregateFunction::batch_create_with_selection(ctx, size, states, offset, selection);
    }
};

template <typename Function>
void check_partial_create_cleanup() {
    Function function;
    alignas(OwnedBatchState) std::array<uint8_t, 4 * sizeof(OwnedBatchState)> memory;
    Buffer<AggDataPtr> states(4);
    for (size_t i = 0; i < 4; ++i) states[i] = memory.data() + i * sizeof(OwnedBatchState);
    Filter selected(4, 0);
    selected[1] = 1;
    EXPECT_THROW(function.batch_create_with_selection(nullptr, 4, states, 0, selected), std::bad_alloc);
    EXPECT_EQ(2, function.created);
    EXPECT_EQ(2, function.destroyed);
    EXPECT_TRUE(function.live.empty());
}
} // namespace

TEST(AggregateBatchCreateTest, DefaultImplementationRollsBackOnlyCompletedSelectedPrefix) {
    check_partial_create_cleanup<DefaultCreateFailureAggregate>();
}

TEST(AggregateBatchCreateTest, CrtpImplementationRollsBackOnlyCompletedSelectedPrefix) {
    check_partial_create_cleanup<CreateFailureAggregate>();
}
} // namespace starrocks
