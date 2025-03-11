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
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
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

    SegmentedChunkPtr build_segmented_chunk();

    // A tuple with one column
    TupleDescriptor* _create_simple_desc() {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(_create_slot_desc(LogicalType::TYPE_INT, "c0", 0));
        tuple_builder.build(&table_builder);

        std::vector<TTupleId> row_tuples{0};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                      .ok());

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

    // A tuple with two column
    TupleDescriptor* _create_simple_desc2() {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(_create_slot_desc(LogicalType::TYPE_INT, "c0", 0));
        tuple_builder.add_slot(_create_slot_desc(LogicalType::TYPE_VARCHAR, "c1", 1));
        tuple_builder.build(&table_builder);

        std::vector<TTupleId> row_tuples{0};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                      .ok());

        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples));
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
    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                  .ok());

    auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples));
    auto* tuple_desc = row_desc->tuple_descriptors()[0];

    return tuple_desc;
}

SegmentedChunkPtr ChunkHelperTest::build_segmented_chunk() {
    auto* tuple_desc = _create_simple_desc2();
    auto segmented_chunk = SegmentedChunk::create(1 << 16);
    segmented_chunk->append_column(Int32Column::create(), 0);
    segmented_chunk->append_column(BinaryColumn::create(), 1);
    segmented_chunk->build_columns();

    // put 100 chunks into the segmented chunk
    int row_id = 0;
    for (int i = 0; i < 100; i++) {
        size_t chunk_rows = 4096;
        auto chunk = ChunkHelper::new_chunk(*tuple_desc, chunk_rows);
        for (int j = 0; j < chunk_rows; j++) {
            chunk->get_column_by_index(0)->append_datum(row_id++);
            std::string str = fmt::format("str{}", row_id);
            chunk->get_column_by_index(1)->append_datum(Slice(str));
        }

        segmented_chunk->append_chunk(std::move(chunk));
    }
    return segmented_chunk;
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

TEST_F(ChunkHelperTest, SegmentedChunk) {
    auto segmented_chunk = build_segmented_chunk();

    [[maybe_unused]] auto downgrade_result = segmented_chunk->downgrade();
    [[maybe_unused]] auto upgrade_result = segmented_chunk->upgrade_if_overflow();

    EXPECT_EQ(409600, segmented_chunk->num_rows());
    EXPECT_EQ(7, segmented_chunk->num_segments());
    EXPECT_EQ(7781406, segmented_chunk->memory_usage());
    EXPECT_EQ(2, segmented_chunk->columns().size());
    auto column0 = segmented_chunk->columns()[0];
    EXPECT_EQ(false, column0->is_nullable());
    EXPECT_EQ(false, column0->has_null());
    EXPECT_EQ(409600, column0->size());
    std::vector<uint32_t> indexes = {1, 2, 4, 10000, 20000};
    ColumnPtr cloned = column0->clone_selective(indexes.data(), 0, indexes.size());
    EXPECT_EQ("[1, 2, 4, 10000, 20000]", cloned->debug_string());

    // reset
    segmented_chunk->reset();
    EXPECT_EQ(0, segmented_chunk->num_rows());
    EXPECT_EQ(7, segmented_chunk->num_segments());
    EXPECT_EQ(7781406, segmented_chunk->memory_usage());

    // slicing
    segmented_chunk = build_segmented_chunk();
    SegmentedChunkSlice slice;
    slice.reset(segmented_chunk);
    size_t total_rows = 0;
    while (!slice.empty()) {
        auto chunk = slice.cutoff(1000);
        EXPECT_LE(chunk->num_rows(), 1000);
        auto& slices = ColumnHelper::as_column<BinaryColumn>(chunk->get_column_by_index(1))->get_data();
        for (int i = 0; i < chunk->num_rows(); i++) {
            EXPECT_EQ(total_rows + i, chunk->get_column_by_index(0)->get(i).get_int32());
            EXPECT_EQ(fmt::format("str{}", total_rows + i + 1), slices[i].to_string());
        }
        total_rows += chunk->num_rows();
    }
    EXPECT_EQ(409600, total_rows);
    EXPECT_EQ(0, segmented_chunk->num_rows());
    EXPECT_EQ(7, segmented_chunk->num_segments());
    segmented_chunk->check_or_die();

    // append
    auto seg1 = build_segmented_chunk();
    auto seg2 = build_segmented_chunk();
    seg1->append(seg2, 1);
    EXPECT_EQ(409600 * 2 - 1, seg1->num_rows());
    seg1->check_or_die();
    // clone_selective
    {
        std::vector<uint32_t> index{1, 2, 4, 10000, 20000};
        auto column1 = seg1->columns()[1];
        ColumnPtr str_column1 = column1->clone_selective(index.data(), 0, index.size());
        EXPECT_EQ("['str2', 'str3', 'str5', 'str10001', 'str20001']", str_column1->debug_string());
    }
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
