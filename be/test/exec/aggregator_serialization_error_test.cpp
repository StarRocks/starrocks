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
#include <string>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "exec/aggregator.h"
#include "exprs/agg/ai_collect_text.h"
#include "exprs/agg/array_agg.h"
#include "exprs/agg/nullable_aggregate.h"

namespace starrocks {

class AggregatorSerializationErrorTest : public testing::Test {
protected:
    enum class Path { STREAMING, SELECTED_STREAMING, SPILL };

    std::unique_ptr<Aggregator> make_aggregator(const AggregateFunction* function, ColumnPtr values, bool nullable) {
        auto aggregate = std::make_unique<Aggregator>(std::make_shared<AggregatorParams>());
        aggregate->_pool = std::make_unique<ObjectPool>();
        aggregate->_agg_stat = &statistics;
        aggregate->_group_by_types = {{TypeDescriptor(TYPE_BIGINT), false}};
        aggregate->_agg_functions = {function};
        context.reset(
                FunctionContext::create_test_context({TypeDescriptor(TYPE_VARCHAR)}, TypeDescriptor(TYPE_VARBINARY)));
        aggregate->_agg_fn_ctxs = {context.get()};
        aggregate->_agg_fn_types = {
                {TypeDescriptor(TYPE_VARBINARY), TypeDescriptor(TYPE_VARBINARY), {}, nullable, nullable}};
        aggregate->_is_merge_funcs = {false};
        // Inputs are already evaluated, as in the sorted aggregator error fixture.
        aggregate->_agg_input_columns = {{std::move(values)}};
        auto keys = Int64Column::create();
        for (size_t i = 0; i < aggregate->_agg_input_columns[0][0]->size(); ++i) keys->append(i);
        aggregate->_group_by_columns = {keys};
        TTupleDescriptor tuple;
        tuple.id = 0;
        auto* descriptor = aggregate->_pool->add(new TupleDescriptor(tuple));
        descriptor->add_slot(aggregate->_pool->add(new SlotDescriptor(0, "key", TypeDescriptor(TYPE_BIGINT))));
        descriptor->add_slot(aggregate->_pool->add(new SlotDescriptor(1, "state", TypeDescriptor(TYPE_VARBINARY))));
        aggregate->_intermediate_tuple_desc = descriptor;
        return aggregate;
    }

    Status convert(Aggregator* aggregate, Path path, ChunkPtr* output) {
        Chunk input;
        input.append_column(aggregate->_group_by_columns[0], SlotId{0});
        if (path == Path::SPILL) return aggregate->convert_to_spill_format(&input, output);
        if (path == Path::SELECTED_STREAMING) {
            aggregate->_streaming_selection.assign(input.num_rows(), 1);
            return aggregate->output_chunk_by_streaming_with_selection(&input, output);
        }
        return aggregate->output_chunk_by_streaming(&input, output);
    }

    void check_oversized_input(Path path, bool nullable) {
        AICollectTextAggregateFunction collector;
        using NullableState = NullableAggregateFunctionState<AICollectTextState, false>;
        NullableAggregateFunctionUnary<AggregateFunctionPtr, NullableState, false, false> wrapper(&collector);
        auto data = BinaryColumn::create();
        data->append(Slice("valid prefix"));
        data->append(Slice(std::string(kAiAggregateStateMaxBytes + 1, 'x')));
        ColumnPtr values = data;
        if (nullable) {
            auto nulls = NullColumn::create();
            nulls->append(0);
            nulls->append(0);
            values = NullableColumn::create(std::move(data), std::move(nulls));
        }
        auto aggregate = make_aggregator(nullable ? static_cast<const AggregateFunction*>(&wrapper) : &collector,
                                         std::move(values), nullable);
        ChunkPtr output;
        auto result = convert(aggregate.get(), path, &output);
        EXPECT_FALSE(result.ok());
        EXPECT_NE(std::string::npos, result.message().find("encoded byte limit"));
        EXPECT_EQ(nullptr, output);
        EXPECT_EQ(0, aggregate->_num_rows_processed);
        EXPECT_EQ(0, aggregate->_num_rows_returned);
    }

    RuntimeProfile profile{"aggregate serialization errors"};
    AggStatistics statistics{&profile};
    std::unique_ptr<FunctionContext> context;
};

TEST_F(AggregatorSerializationErrorTest, StreamingRejectsPartialSerializedColumnsBeforeAssembly) {
    for (bool nullable : {false, true}) check_oversized_input(Path::STREAMING, nullable);
}

TEST_F(AggregatorSerializationErrorTest, SelectedStreamingRejectsPartialSerializedColumnsBeforeAssembly) {
    for (bool nullable : {false, true}) check_oversized_input(Path::SELECTED_STREAMING, nullable);
}

TEST_F(AggregatorSerializationErrorTest, SpillRejectsPartialSerializedColumnsBeforeAssembly) {
    for (bool nullable : {false, true}) check_oversized_input(Path::SPILL, nullable);
}

TEST_F(AggregatorSerializationErrorTest, ValidNullableInputRetainsRowsOnEveryConversionPath) {
    AICollectTextAggregateFunction collector;
    using NullableState = NullableAggregateFunctionState<AICollectTextState, false>;
    NullableAggregateFunctionUnary<AggregateFunctionPtr, NullableState, false, false> wrapper(&collector);
    for (Path path : {Path::STREAMING, Path::SELECTED_STREAMING, Path::SPILL}) {
        auto data = BinaryColumn::create();
        data->append(Slice(std::string(kAiAggregateStateMaxBytes + 1, 'x')));
        data->append(Slice("kept"));
        auto nulls = NullColumn::create();
        nulls->append(1);
        nulls->append(0);
        auto aggregate = make_aggregator(&wrapper, NullableColumn::create(std::move(data), std::move(nulls)), true);
        ChunkPtr output;
        ASSERT_TRUE(convert(aggregate.get(), path, &output).ok());
        ASSERT_NE(nullptr, output);
        ASSERT_EQ(2, output->num_rows());
        const auto& states = output->get_column_by_slot_id(1);
        EXPECT_TRUE(states->is_null(0));
        auto parsed = parse_ai_aggregate_state(states->get(1).get_slice());
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto cursor = parsed->cursor();
        Slice record;
        ASSERT_TRUE(cursor.next(&record));
        EXPECT_EQ("kept", record.to_string());
        EXPECT_FALSE(cursor.next(&record));
    }
}

} // namespace starrocks
