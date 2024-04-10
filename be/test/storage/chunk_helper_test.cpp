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

#include "column/chunk.h"
#include "column/column.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "gtest/gtest.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

class ChunkHelperTest : public ::testing::Test {
protected:
    LogicalType _primitive_type[9] = {LogicalType::TYPE_TINYINT, LogicalType::TYPE_SMALLINT, LogicalType::TYPE_INT,
                                      LogicalType::TYPE_BIGINT,  LogicalType::TYPE_LARGEINT, LogicalType::TYPE_FLOAT,
                                      LogicalType::TYPE_DOUBLE,  LogicalType::TYPE_VARCHAR,  LogicalType::TYPE_CHAR};

    TSlotDescriptor _create_slot_desc(LogicalType type, const std::string& col_name, int col_pos);
    TupleDescriptor* _create_tuple_desc();

    // A tuple with one column
    TupleDescriptor* _create_simple_desc() {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(_create_slot_desc(LogicalType::TYPE_INT, "c0", 0));
        tuple_builder.build(&table_builder);

        std::vector<TTupleId> row_tuples{0};
        std::vector<bool> nullable_tuples{true};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                      .ok());

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

    RuntimeState _runtime_state;
    ObjectPool _pool;
};

TSlotDescriptor ChunkHelperTest::_create_slot_desc(LogicalType type, const std::string& col_name, int col_pos) {
    TSlotDescriptorBuilder builder;

    if (type == LogicalType::TYPE_VARCHAR || type == LogicalType::TYPE_CHAR) {
        return builder.string_type(1024).column_name(col_name).column_pos(col_pos).nullable(false).build();
    } else {
        return builder.type(type).column_name(col_name).column_pos(col_pos).nullable(false).build();
    }
}

TupleDescriptor* ChunkHelperTest::_create_tuple_desc() {
    TDescriptorTableBuilder table_builder;
    TTupleDescriptorBuilder tuple_builder;

    for (size_t i = 0; i < 9; i++) {
        tuple_builder.add_slot(_create_slot_desc(_primitive_type[i], "c" + std::to_string(i), 0));
    }

    tuple_builder.build(&table_builder);

    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    std::vector<bool> nullable_tuples = std::vector<bool>{true};
    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                  .ok());

    auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
    auto* tuple_desc = row_desc->tuple_descriptors()[0];

    return tuple_desc;
}

TEST_F(ChunkHelperTest, new_chunk_with_tuple) {
    auto* tuple_desc = _create_tuple_desc();

    auto chunk = ChunkHelper::new_chunk(*tuple_desc, 1024);

    // check
    ASSERT_EQ(chunk->num_columns(), 9);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->get_name(), "integral-1");
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->get_name(), "integral-2");
    ASSERT_EQ(chunk->get_column_by_slot_id(2)->get_name(), "integral-4");
    ASSERT_EQ(chunk->get_column_by_slot_id(3)->get_name(), "integral-8");
    ASSERT_EQ(chunk->get_column_by_slot_id(4)->get_name(), "int128");
    ASSERT_EQ(chunk->get_column_by_slot_id(5)->get_name(), "float-4");
    ASSERT_EQ(chunk->get_column_by_slot_id(6)->get_name(), "float-8");
    ASSERT_EQ(chunk->get_column_by_slot_id(7)->get_name(), "binary");
    ASSERT_EQ(chunk->get_column_by_slot_id(8)->get_name(), "binary");
}

TEST_F(ChunkHelperTest, ReorderChunk) {
    auto* tuple_desc = _create_tuple_desc();

    auto reversed_slots = tuple_desc->slots();
    std::reverse(reversed_slots.begin(), reversed_slots.end());
    auto chunk = ChunkHelper::new_chunk(reversed_slots, 1024);

    // check
    ASSERT_EQ(chunk->num_columns(), 9);
    ASSERT_EQ(chunk->columns()[8]->get_name(), "integral-1");
    ASSERT_EQ(chunk->columns()[7]->get_name(), "integral-2");
    ASSERT_EQ(chunk->columns()[6]->get_name(), "integral-4");
    ASSERT_EQ(chunk->columns()[5]->get_name(), "integral-8");
    ASSERT_EQ(chunk->columns()[4]->get_name(), "int128");
    ASSERT_EQ(chunk->columns()[3]->get_name(), "float-4");
    ASSERT_EQ(chunk->columns()[2]->get_name(), "float-8");
    ASSERT_EQ(chunk->columns()[1]->get_name(), "binary");
    ASSERT_EQ(chunk->columns()[0]->get_name(), "binary");

    ChunkHelper::reorder_chunk(*tuple_desc, chunk.get());
    // check
    ASSERT_EQ(chunk->num_columns(), 9);
    ASSERT_EQ(chunk->columns()[0]->get_name(), "integral-1");
    ASSERT_EQ(chunk->columns()[1]->get_name(), "integral-2");
    ASSERT_EQ(chunk->columns()[2]->get_name(), "integral-4");
    ASSERT_EQ(chunk->columns()[3]->get_name(), "integral-8");
    ASSERT_EQ(chunk->columns()[4]->get_name(), "int128");
    ASSERT_EQ(chunk->columns()[5]->get_name(), "float-4");
    ASSERT_EQ(chunk->columns()[6]->get_name(), "float-8");
    ASSERT_EQ(chunk->columns()[7]->get_name(), "binary");
    ASSERT_EQ(chunk->columns()[8]->get_name(), "binary");
}

TEST_F(ChunkHelperTest, Accumulator) {
    constexpr size_t kDesiredSize = 4096;
    auto* tuple_desc = _create_simple_desc();
    ChunkAccumulator accumulator(kDesiredSize);
    size_t input_rows = 0;
    size_t output_rows = 0;
    // push small chunks
    for (int i = 0; i < 10; i++) {
        auto chunk = ChunkHelper::new_chunk(*tuple_desc, 1025);
        chunk->get_column_by_index(0)->append_default(1025);
        input_rows += 1025;

        static_cast<void>(accumulator.push(std::move(chunk)));
        if (ChunkPtr output = accumulator.pull()) {
            output_rows += output->num_rows();
            EXPECT_EQ(kDesiredSize, output->num_rows());
        }
    }
    // push large chunks
    for (int i = 0; i < 10; i++) {
        auto chunk = ChunkHelper::new_chunk(*tuple_desc, 8888);
        chunk->get_column_by_index(0)->append_default(8888);
        input_rows += 8888;
        static_cast<void>(accumulator.push(std::move(chunk)));
    }

    accumulator.finalize();
    while (ChunkPtr output = accumulator.pull()) {
        EXPECT_LE(output->num_rows(), kDesiredSize);
        output_rows += output->num_rows();
    }
    EXPECT_EQ(input_rows, output_rows);

    // push empty chunks
    for (int i = 0; i < ChunkAccumulator::kAccumulateLimit; i++) {
        auto chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
        static_cast<void>(accumulator.push(std::move(chunk)));
    }
    EXPECT_TRUE(accumulator.reach_limit());
    auto output = accumulator.pull();
    EXPECT_EQ(nullptr, output);
    EXPECT_TRUE(accumulator.reach_limit());
}

class ChunkPipelineAccumulatorTest : public ::testing::Test {
protected:
    ChunkPtr _generate_chunk(size_t rows, size_t cols, size_t reserve_size = 0);
};

ChunkPtr ChunkPipelineAccumulatorTest::_generate_chunk(size_t rows, size_t cols, size_t reserve_size) {
    auto chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < cols; i++) {
        auto col = Int8Column::create(rows, reserve_size);
        chunk->append_column(col, i);
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
