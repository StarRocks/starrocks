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

#include "storage/chunk_helper.h"

#include <string>
#include <string_view>
#include <vector>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "gtest/gtest.h"
#include "storage/primitive/schema_helper.h"

namespace starrocks {

namespace {

TabletSchemaCSPtr create_char_tablet_schema(size_t length) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_num_rows_per_row_block(1024);
    schema_pb.set_next_column_unique_id(1);

    ColumnPB* column = schema_pb.add_column();
    column->set_unique_id(0);
    column->set_name("c0");
    column->set_type("CHAR");
    column->set_is_key(true);
    column->set_is_nullable(true);
    column->set_length(length);
    column->set_index_length(length);

    return TabletSchema::create(schema_pb);
}

NullableColumn::MutablePtr make_nullable_binary_column(const std::vector<std::string_view>& values,
                                                       const std::vector<uint8_t>& nulls) {
    CHECK_EQ(values.size(), nulls.size());

    auto data_column = BinaryColumn::create();
    for (std::string_view value : values) {
        data_column->append(Slice(value));
    }

    auto null_column = NullColumn::create();
    null_column->get_data().insert(null_column->get_data().end(), nulls.begin(), nulls.end());
    auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
    nullable_column->update_has_null();
    return nullable_column;
}

std::string padded_char(std::string_view value, size_t length) {
    std::string result(value.substr(0, length));
    result.resize(length, char(0));
    return result;
}

void expect_padded_char_column(const BinaryColumn& column, const std::vector<std::string>& expected) {
    const auto& offsets = column.get_offset();
    ASSERT_EQ(expected.size() + 1, offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(i * 4, offsets[i]);
    }

    for (size_t i = 0; i < expected.size(); ++i) {
        Slice value = column.get_slice(i);
        EXPECT_EQ(expected[i], std::string(value.data, value.size));
    }
}

} // namespace

class ChunkHelperTest : public ::testing::Test {};

TEST_F(ChunkHelperTest, PaddingNullableCharColumnSkipsNullRowsWhenNullsAreDense) {
    auto tablet_schema = create_char_tablet_schema(4);
    Field field = StorageSchemaHelper::convert_field(0, tablet_schema->column(0));
    auto column = make_nullable_binary_column({"ab", "null", "wxyzq", "skip", "defg", "hi", "null", ""},
                                              {0, 1, 0, 1, 0, 0, 1, 0});

    ChunkHelper::padding_char_column(tablet_schema, field, column.get());

    auto* result = down_cast<NullableColumn*>(column.get());
    ASSERT_TRUE(result->has_null());
    EXPECT_FALSE(result->is_null(0));
    EXPECT_TRUE(result->is_null(1));
    EXPECT_FALSE(result->is_null(2));
    EXPECT_TRUE(result->is_null(3));
    EXPECT_FALSE(result->is_null(4));
    EXPECT_FALSE(result->is_null(5));
    EXPECT_TRUE(result->is_null(6));
    EXPECT_FALSE(result->is_null(7));

    auto* data_column = down_cast<BinaryColumn*>(result->data_column_raw_ptr());
    expect_padded_char_column(*data_column,
                              {padded_char("ab", 4), padded_char("", 4), padded_char("wxyzq", 4), padded_char("", 4),
                               padded_char("defg", 4), padded_char("hi", 4), padded_char("", 4), padded_char("", 4)});
}

TEST_F(ChunkHelperTest, PaddingNullableCharColumnCopiesAllRowsWhenNullsAreSparse) {
    auto tablet_schema = create_char_tablet_schema(4);
    Field field = StorageSchemaHelper::convert_field(0, tablet_schema->column(0));
    auto column = make_nullable_binary_column({"a", "bc", "def", "nullv", "wxyzq", "m", "no", "pqrs"},
                                              {0, 0, 0, 1, 0, 0, 0, 0});

    ChunkHelper::padding_char_column(tablet_schema, field, column.get());

    auto* result = down_cast<NullableColumn*>(column.get());
    ASSERT_TRUE(result->has_null());
    EXPECT_FALSE(result->is_null(0));
    EXPECT_FALSE(result->is_null(1));
    EXPECT_FALSE(result->is_null(2));
    EXPECT_TRUE(result->is_null(3));
    EXPECT_FALSE(result->is_null(4));
    EXPECT_FALSE(result->is_null(5));
    EXPECT_FALSE(result->is_null(6));
    EXPECT_FALSE(result->is_null(7));

    auto* data_column = down_cast<BinaryColumn*>(result->data_column_raw_ptr());
    expect_padded_char_column(
            *data_column, {padded_char("a", 4), padded_char("bc", 4), padded_char("def", 4), padded_char("nullv", 4),
                           padded_char("wxyzq", 4), padded_char("m", 4), padded_char("no", 4), padded_char("pqrs", 4)});
}

class ChunkPipelineAccumulatorTest : public ::testing::Test {
protected:
    ChunkPtr _generate_chunk(size_t rows, size_t cols, size_t reserve_size = 0);
};

ChunkPtr ChunkPipelineAccumulatorTest::_generate_chunk(size_t rows, size_t cols, size_t reserve_size) {
    auto chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < cols; i++) {
        auto col = Int8Column::create(rows, reserve_size);
        chunk->append_column(std::move(col), i);
    }
    return chunk;
}

TEST_F(ChunkPipelineAccumulatorTest, test_push) {
    ChunkPipelineAccumulator accumulator;

    // rows reach limit
    accumulator.push(_generate_chunk(4093, 1));
    ASSERT_TRUE(accumulator.has_output());
    auto result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 4093);
    accumulator.finalize();
    ASSERT_FALSE(accumulator.has_output());

    // mem reach limit
    accumulator.reset_state();
    accumulator.push(_generate_chunk(2048, 64));
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 2048);
    accumulator.finalize();
    ASSERT_FALSE(accumulator.has_output());

    // merge chunk and reach rows limit
    accumulator.reset_state();
    for (size_t i = 0; i < 3; i++) {
        accumulator.push(_generate_chunk(1000, 1));
        ASSERT_FALSE(accumulator.has_output());
    }
    accumulator.push(_generate_chunk(1000, 1));
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 4000);
    accumulator.finalize();
    ASSERT_FALSE(accumulator.has_output());

    // merge chunk and read mem limit
    accumulator.reset_state();
    for (size_t i = 0; i < 2; i++) {
        accumulator.push(_generate_chunk(1000, 30));
        ASSERT_FALSE(accumulator.has_output());
    }
    accumulator.push(_generate_chunk(1000, 30));
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 3000);
    accumulator.finalize();
    ASSERT_FALSE(accumulator.has_output());

    // merge chunk and rows overflow
    accumulator.reset_state();
    accumulator.push(_generate_chunk(3000, 1));
    ASSERT_FALSE(accumulator.has_output());
    accumulator.push(_generate_chunk(3000, 1));
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 3000);
    accumulator.finalize();
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 3000);
    ASSERT_FALSE(accumulator.has_output());

    // reserve large and use less
    accumulator.reset_state();
    accumulator.push(_generate_chunk(1000, 25, 4000));
    ASSERT_FALSE(accumulator.has_output());
    accumulator.push(_generate_chunk(1000, 25, 4000));
    ASSERT_FALSE(accumulator.has_output());
    accumulator.push(_generate_chunk(1000, 25, 4000));
    ASSERT_TRUE(accumulator.has_output());
    result_chunk = std::move(accumulator.pull());
    ASSERT_EQ(result_chunk->num_rows(), 3000);
    ASSERT_FALSE(accumulator.has_output());
}

TEST_F(ChunkPipelineAccumulatorTest, test_owner_info) {
    constexpr size_t kDesiredSize = 4096;

    {
        ChunkPipelineAccumulator accumulator;
        accumulator.set_max_size(kDesiredSize);
        DCHECK(accumulator.need_input());
        // same owner info
        {
            auto chunk = _generate_chunk(1025, 2);
            chunk->owner_info().set_owner_id(1, false);
            accumulator.push(std::move(chunk));
        }
        DCHECK(accumulator.need_input());
        {
            // new empty chunk
            auto chunk = std::make_unique<Chunk>();
            chunk->owner_info().set_owner_id(1, true);
            accumulator.push(std::move(chunk));
        }

        DCHECK(!accumulator.need_input());
        DCHECK(accumulator.has_output());
        auto chunk = std::move(accumulator.pull());
        DCHECK(!chunk->owner_info().is_last_chunk());
        accumulator.finalize();
        DCHECK(accumulator.has_output());
        chunk = std::move(accumulator.pull());
        DCHECK(chunk->owner_info().is_last_chunk());
    }

    {
        ChunkPipelineAccumulator accumulator;
        accumulator.set_max_size(kDesiredSize);
        DCHECK(accumulator.need_input());
        // same owner info
        {
            auto chunk = _generate_chunk(1025, 2);
            chunk->owner_info().set_owner_id(2, false);
            accumulator.push(std::move(chunk));
            chunk = _generate_chunk(1025, 2);
            chunk->owner_info().set_owner_id(2, true);
            accumulator.push(std::move(chunk));
        }
        DCHECK(accumulator.has_output());
        auto chunk = std::move(accumulator.pull());
        DCHECK(!chunk->owner_info().is_last_chunk());
    }

    {
        ChunkPipelineAccumulator accumulator;
        accumulator.set_max_size(kDesiredSize);
        DCHECK(accumulator.need_input());
        // not the same owner info
        {
            auto chunk = _generate_chunk(1025, 2);
            chunk->owner_info().set_owner_id(3, false);
            accumulator.push(std::move(chunk));
            chunk = _generate_chunk(1025, 2);
            chunk->owner_info().set_owner_id(4, false);
            accumulator.push(std::move(chunk));
        }
        auto chunk = std::move(accumulator.pull());
        DCHECK_EQ(chunk->owner_info().owner_id(), 3);
    }

    {
        ChunkPipelineAccumulator accumulator;
        accumulator.set_max_size(kDesiredSize);
        DCHECK(accumulator.need_input());
        // not the same owner info
        {
            auto chunk = _generate_chunk(1025, 2);
            chunk->owner_info().set_owner_id(1, true);
            accumulator.push(std::move(chunk));
            DCHECK(!accumulator.need_input());
        }
        auto chunk = std::move(accumulator.pull());
        DCHECK(chunk->owner_info().is_last_chunk());
    }
}

} // namespace starrocks
