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

#include "exprs/agg/ai_collect_text.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "base/coding.h"
#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "common/memory/mem_hook_allocator.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/ai/ai_aggregate_state.h"

namespace starrocks {
namespace {
// AggregateStateAllocator uses checked_alloc/free. Count that pair directly because
// the malloc hook used by CountingAllocatorWithHook is not enabled in expr_test.
class TrackingAggregateStateAllocator final : public MemHookAllocator {
public:
    void* checked_alloc(size_t size) override {
        void* ptr = MemHookAllocator::checked_alloc(size);
        ++_outstanding_allocations;
        return ptr;
    }

    void free(void* ptr) override {
        if (ptr != nullptr) --_outstanding_allocations;
        MemHookAllocator::free(ptr);
    }

    int64_t outstanding_allocations() const { return _outstanding_allocations; }

private:
    int64_t _outstanding_allocations = 0;
};

class ScopedCollectState {
public:
    ScopedCollectState(const AggregateFunction* function, FunctionContext* context)
            : function(function), context(context), data(static_cast<AggDataPtr>(::operator new(function->size()))) {
        function->create(context, data);
    }
    ~ScopedCollectState() {
        function->destroy(context, data);
        ::operator delete(data);
    }
    const AggregateFunction* function;
    FunctionContext* context;
    AggDataPtr data;
};

std::vector<std::string> read_records(Slice encoded) {
    auto parsed = parse_ai_aggregate_state(encoded);
    EXPECT_TRUE(parsed.ok()) << parsed.status();
    if (!parsed.ok()) return {};
    std::vector<std::string> result;
    auto cursor = parsed->cursor();
    Slice record;
    while (cursor.next(&record)) result.push_back(record.to_string());
    return result;
}
} // namespace

class AiCollectTextTest : public testing::Test {
public:
    void SetUp() override {
        ctx.reset(FunctionContext::create_test_context({TypeDescriptor::from_logical_type(TYPE_VARCHAR)},
                                                       TypeDescriptor::from_logical_type(TYPE_VARBINARY)));
        setter = std::make_unique<ThreadLocalAggregateStateAllocatorSetter>(&allocator);
    }
    void TearDown() override {
        EXPECT_EQ(0, allocator.outstanding_allocations());
        setter.reset();
    }
    std::unique_ptr<FunctionContext> ctx;
    TrackingAggregateStateAllocator allocator;
    std::unique_ptr<ThreadLocalAggregateStateAllocatorSetter> setter;
};

TEST_F(AiCollectTextTest, FactoryRegistersOnlyNativeSignature) {
    for (bool nullable : {false, true}) {
        EXPECT_NE(nullptr, get_aggregate_function("__ai_collect_text", TYPE_VARCHAR, TYPE_VARBINARY, nullable));
        EXPECT_EQ(nullptr, get_window_function("__ai_collect_text", TYPE_VARCHAR, TYPE_VARBINARY, nullable));
    }
    EXPECT_EQ(nullptr, get_aggregate_function("__ai_collect_text", TYPE_VARCHAR, TYPE_VARCHAR, false));
    EXPECT_EQ(nullptr, get_aggregate_function("__ai_collect_text", TYPE_VARCHAR, TYPE_VARBINARY, false,
                                              TFunctionBinaryType::AI));
}

TEST_F(AiCollectTextTest, OwnsRawRecordsAndFinalizedBytes) {
    AICollectTextAggregateFunction function;
    auto result = BinaryColumn::create();
    const std::vector<std::string> expected = {"", "same", "same", std::string("a\0b", 3)};
    {
        ScopedCollectState state(&function, ctx.get());
        auto input = BinaryColumn::create();
        for (const auto& value : expected) input->append(Slice(value));
        const Column* columns[] = {input.get()};
        function.update_batch_single_state(ctx.get(), input->size(), columns, state.data);
        input->reset_column();
        EXPECT_GT(allocator.outstanding_allocations(), 0);
        EXPECT_EQ(0, ctx->mem_usage());
        function.finalize_to_column(ctx.get(), state.data, result.get());
        EXPECT_EQ(expected, read_records(result->get_slice(0)));
        function.reset(ctx.get(), {}, state.data);
        EXPECT_EQ(0, allocator.outstanding_allocations());
        function.serialize_to_column(ctx.get(), state.data, result.get());
        EXPECT_TRUE(read_records(result->get_slice(1)).empty());
        // Refill the reset state so destruction also releases a live allocation.
        input->append(Slice("destroyed"));
        function.update(ctx.get(), columns, state.data, 0);
        EXPECT_GT(allocator.outstanding_allocations(), 0);
    }
    EXPECT_EQ(0, allocator.outstanding_allocations());
    EXPECT_EQ(expected, read_records(result->get_slice(0)));
    EXPECT_FALSE(ctx->has_error());
}

TEST_F(AiCollectTextTest, NullableWrapperSkipsNullAndPreservesEmpty) {
    const auto* function = get_aggregate_function("__ai_collect_text", TYPE_VARCHAR, TYPE_VARBINARY, true);
    ASSERT_NE(nullptr, function);
    ScopedCollectState state(function, ctx.get());
    auto input = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    input->append_nulls(1);
    input->append_datum(Datum(Slice("")));
    input->append_datum(Datum(Slice("x")));
    const Column* columns[] = {input.get()};
    function->update_batch_single_state(ctx.get(), input->size(), columns, state.data);
    auto output = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    function->finalize_to_column(ctx.get(), state.data, output.get());
    ASSERT_FALSE(output->is_null(0));
    EXPECT_EQ((std::vector<std::string>{"", "x"}), read_records(output->get(0).get_slice()));
    function->reset(ctx.get(), {}, state.data);
    function->serialize_to_column(ctx.get(), state.data, output.get());
    EXPECT_TRUE(output->is_null(1));
}

TEST_F(AiCollectTextTest, SingletonConversionSupportsConstAndNullable) {
    AICollectTextAggregateFunction function;
    auto input = BinaryColumn::create();
    input->append(Slice("same"));
    Columns columns{ConstColumn::create(input, 3)};
    MutableColumnPtr output = BinaryColumn::create();
    function.convert_to_serialize_format(ctx.get(), columns, 3, output);
    ASSERT_EQ(3, output->size());
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ((std::vector<std::string>{"same"}), read_records(output->get(i).get_slice()));
    }
    auto nullable = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    nullable->append_nulls(1);
    nullable->append_datum(Datum(Slice("")));
    output = BinaryColumn::create();
    function.convert_to_serialize_format(ctx.get(), {nullable}, 2, output);
    EXPECT_TRUE(read_records(output->get(0).get_slice()).empty());
    EXPECT_EQ((std::vector<std::string>{""}), read_records(output->get(1).get_slice()));
    output = BinaryColumn::create();
    auto only_null = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    only_null->append_nulls(1);
    function.convert_to_serialize_format(ctx.get(), {ConstColumn::create(only_null, 2)}, 2, output);
    ASSERT_EQ(2, output->size());
    EXPECT_TRUE(read_records(output->get(0).get_slice()).empty());
    EXPECT_TRUE(read_records(output->get(1).get_slice()).empty());
    EXPECT_FALSE(ctx->has_error());
}

TEST_F(AiCollectTextTest, NullableSingletonConversionPreservesNullMask) {
    const auto* function = get_aggregate_function("__ai_collect_text", TYPE_VARCHAR, TYPE_VARBINARY, true);
    ASSERT_NE(nullptr, function);
    auto input = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    input->append_nulls(1);
    input->append_datum(Datum(Slice("")));
    MutableColumnPtr output = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    function->convert_to_serialize_format(ctx.get(), {input}, 2, output);
    ASSERT_EQ(2, output->size());
    EXPECT_TRUE(output->is_null(0));
    EXPECT_FALSE(output->is_null(1));
    EXPECT_EQ((std::vector<std::string>{""}), read_records(output->get(1).get_slice()));
}

TEST_F(AiCollectTextTest, NullableBypassDoesNotReadMaskedOversizedText) {
    const auto* function = get_aggregate_function("__ai_collect_text", TYPE_VARCHAR, TYPE_VARBINARY, true);
    ASSERT_NE(nullptr, function);
    auto data = BinaryColumn::create();
    data->append(Slice(std::string(kAiAggregateStateMaxBytes + 1, 'x')));
    data->append(Slice("kept"));
    auto nulls = NullColumn::create();
    nulls->append(1);
    nulls->append(0);
    auto input = NullableColumn::create(data, nulls);
    MutableColumnPtr partials = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    function->convert_to_serialize_format(ctx.get(), {input}, 2, partials);
    ASSERT_FALSE(ctx->has_error());
    ASSERT_EQ(2, partials->size());
    EXPECT_TRUE(partials->is_null(0));
    EXPECT_EQ((std::vector<std::string>{"kept"}), read_records(partials->get(1).get_slice()));

    ScopedCollectState merged(function, ctx.get()), updated(function, ctx.get());
    function->merge_batch_single_state(ctx.get(), merged.data, partials.get(), 0, 2);
    const Column* columns[] = {input.get()};
    function->update_batch_single_state(ctx.get(), 2, columns, updated.data);
    auto output = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    function->finalize_to_column(ctx.get(), merged.data, output.get());
    function->finalize_to_column(ctx.get(), updated.data, output.get());
    ASSERT_FALSE(ctx->has_error());
    EXPECT_EQ((std::vector<std::string>{"kept"}), read_records(output->get(0).get_slice()));
    EXPECT_EQ(output->get(0).get_slice().to_string(), output->get(1).get_slice().to_string());
}

TEST_F(AiCollectTextTest, EmptyPartialAddsNoRecord) {
    AICollectTextAggregateFunction function;
    ScopedCollectState empty(&function, ctx.get()), result(&function, ctx.get());
    auto partial = BinaryColumn::create();
    function.serialize_to_column(ctx.get(), empty.data, partial.get());
    function.merge(ctx.get(), partial.get(), result.data, 0);
    auto output = BinaryColumn::create();
    function.finalize_to_column(ctx.get(), result.data, output.get());
    EXPECT_TRUE(read_records(output->get_slice(0)).empty());
    EXPECT_EQ(0, allocator.outstanding_allocations());
    EXPECT_FALSE(ctx->has_error());
}

TEST_F(AiCollectTextTest, DifferentMergeTreesPreserveAllRecords) {
    AICollectTextAggregateFunction function;
    auto input = BinaryColumn::create();
    const std::vector<std::string> expected = {"", "a", "a", std::string("b\0c", 3)};
    for (const auto& value : expected) input->append(Slice(value));
    MutableColumnPtr singleton = BinaryColumn::create();
    function.convert_to_serialize_format(ctx.get(), {input}, input->size(), singleton);
    ScopedCollectState left(&function, ctx.get()), right(&function, ctx.get()), combined(&function, ctx.get());
    function.merge_batch_single_state(ctx.get(), left.data, singleton.get(), 0, 2);
    function.merge_batch_single_state(ctx.get(), right.data, singleton.get(), 2, 2);
    auto partials = BinaryColumn::create();
    function.serialize_to_column(ctx.get(), left.data, partials.get());
    function.serialize_to_column(ctx.get(), right.data, partials.get());
    function.merge_batch_single_state(ctx.get(), combined.data, partials.get(), 0, 2);
    auto output = BinaryColumn::create();
    function.finalize_to_column(ctx.get(), combined.data, output.get());
    EXPECT_EQ(expected, read_records(output->get_slice(0)));
    EXPECT_FALSE(ctx->has_error());
}

TEST_F(AiCollectTextTest, MalformedMergeDoesNotMutateDestination) {
    AICollectTextAggregateFunction function;
    ScopedCollectState state(&function, ctx.get());
    auto input = BinaryColumn::create();
    input->append(Slice("kept"));
    const Column* columns[] = {input.get()};
    function.update(ctx.get(), columns, state.data, 0);
    auto partial = BinaryColumn::create();
    function.serialize_to_column(ctx.get(), state.data, partial.get());
    auto bytes = partial->get_slice(0).to_string();
    put_fixed64_le(&bytes, std::numeric_limits<uint64_t>::max());
    encode_ai_aggregate_state_header(reinterpret_cast<uint8_t*>(bytes.data()), 2,
                                     bytes.size() - kAiAggregateStateHeaderBytes);
    auto bad = BinaryColumn::create();
    bad->append(Slice(bytes));
    function.merge(ctx.get(), bad.get(), state.data, 0);
    EXPECT_TRUE(ctx->has_error());
    function.serialize_to_column(ctx.get(), state.data, partial.get());
    EXPECT_EQ(partial->get_slice(0).to_string(), partial->get_slice(1).to_string());
}

TEST_F(AiCollectTextTest, UpdateLimitIncludesLengthPrefixesAndIsAtomic) {
    AICollectTextAggregateFunction function(kAiAggregateStateHeaderBytes + 2 * kAiAggregateRecordLengthBytes);
    ScopedCollectState state(&function, ctx.get());
    auto input = BinaryColumn::create();
    input->append(Slice(""));
    const Column* columns[] = {input.get()};
    function.update(ctx.get(), columns, state.data, 0);
    function.update(ctx.get(), columns, state.data, 0);
    EXPECT_FALSE(ctx->has_error());
    function.update(ctx.get(), columns, state.data, 0);
    EXPECT_TRUE(ctx->has_error());
    auto output = BinaryColumn::create();
    function.serialize_to_column(ctx.get(), state.data, output.get());
    EXPECT_EQ((std::vector<std::string>{"", ""}), read_records(output->get_slice(0)));
}

TEST_F(AiCollectTextTest, ValidPartialMergeCannotExceedGroupLimit) {
    AICollectTextAggregateFunction function(kAiAggregateStateHeaderBytes + kAiAggregateRecordLengthBytes + 1);
    auto input = BinaryColumn::create();
    input->append(Slice("x"));
    MutableColumnPtr partial = BinaryColumn::create();
    function.convert_to_serialize_format(ctx.get(), {input}, 1, partial);
    ScopedCollectState state(&function, ctx.get());
    function.merge(ctx.get(), partial.get(), state.data, 0);
    EXPECT_FALSE(ctx->has_error());
    function.merge(ctx.get(), partial.get(), state.data, 0);
    EXPECT_TRUE(ctx->has_error());
    auto output = BinaryColumn::create();
    function.serialize_to_column(ctx.get(), state.data, output.get());
    EXPECT_EQ((std::vector<std::string>{"x"}), read_records(output->get_slice(0)));
}

TEST_F(AiCollectTextTest, OversizedSingletonFailsInsteadOfPassingRawText) {
    AICollectTextAggregateFunction function(kAiAggregateStateHeaderBytes + kAiAggregateRecordLengthBytes);
    auto input = BinaryColumn::create();
    input->append(Slice("x"));
    MutableColumnPtr output = BinaryColumn::create();
    function.convert_to_serialize_format(ctx.get(), {input}, 1, output);
    EXPECT_TRUE(ctx->has_error());
    EXPECT_EQ(0, output->size());
}
} // namespace starrocks
