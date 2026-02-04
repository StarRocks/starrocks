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

#include "column/chunk.h"

#include <gtest/gtest.h>

#include "base/testutil/parallel_test.h"
#include "column/binary_column.h"
#include "column/chunk_extra_data.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/datum_tuple.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

class ChunkTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    std::string make_string(size_t i) { return std::string("c").append(std::to_string(static_cast<int32_t>(i))); }

    FieldPtr make_field(size_t i) { return std::make_shared<Field>(i, make_string(i), get_type_info(TYPE_INT), false); }

    Fields make_fields(size_t size) {
        Fields fields;
        for (size_t i = 0; i < size; i++) {
            fields.emplace_back(make_field(i));
        }
        return fields;
    }

    SchemaPtr make_schema(size_t i) {
        Fields fields = make_fields(i);
        return std::make_shared<Schema>(fields);
    }

    ColumnPtr make_column(size_t start, size_t size = 100) {
        auto column = FixedLengthColumn<int32_t>::create();
        for (int i = 0; i < size; i++) {
            column->append(start + i);
        }
        return column;
    }

    Columns make_columns(size_t num_cols, size_t size = 100, size_t start = -1) {
        Columns columns;
        for (size_t i = 0; i < num_cols; i++) {
            columns.emplace_back(make_column(start == -1 ? i : start, size));
        }
        return columns;
    }

    void check_column(const FixedLengthColumn<int32_t>* column, size_t idx) {
        for (size_t i = 0; i < 100; i++) {
            ASSERT_EQ(column->immutable_data()[i], static_cast<int32_t>(i + idx));
        }
    }

    void check_column(const FixedLengthColumn<int32_t>* column, std::vector<int32_t> expect_datas) {
        for (size_t i = 0; i < expect_datas.size(); i++) {
            ASSERT_EQ(column->immutable_data()[i], static_cast<int32_t>(expect_datas[i]));
        }
    }

    ChunkExtraColumnsDataPtr make_extra_data(size_t num_cols, size_t size = 100) {
        auto chunk = std::make_unique<Chunk>(make_columns(num_cols, size), make_schema(num_cols));
        std::vector<ChunkExtraColumnsMeta> extra_data_metas;
        for (size_t i = 0; i < num_cols; i++) {
            extra_data_metas.emplace_back(
                    ChunkExtraColumnsMeta{.type = TypeDescriptor(TYPE_INT), .is_null = false, .is_const = false});
        }
        auto extra_data_cols = make_columns(num_cols, size);
        return std::make_shared<ChunkExtraColumnsData>(std::move(extra_data_metas), std::move(extra_data_cols));
    }

    MutableColumnPtr make_mutable_column(size_t start, size_t size = 100) {
        auto column = FixedLengthColumn<int32_t>::create();
        for (int i = 0; i < size; i++) {
            column->append(start + i);
        }
        return column;
    }

    MutableColumns make_mutable_columns(size_t num_cols, size_t size = 100, size_t start = -1) {
        MutableColumns columns;
        for (size_t i = 0; i < num_cols; i++) {
            columns.emplace_back(make_mutable_column(start == -1 ? i : start, size));
        }
        return columns;
    }
};

// NOLINTNEXTLINE
GROUP_SLOW_TEST_F(ChunkTest, test_chunk_upgrade_if_overflow) {
    size_t row_count = 1 << 30;
    auto c1 = BinaryColumn::create();
    c1->resize(row_count);
    auto c2 = BinaryColumn::create();
    for (size_t i = 0; i < row_count; i++) {
        c2->append(std::to_string(i));
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(c1, 1);
    chunk->append_column(c2, 2);

    Status st = chunk->upgrade_if_overflow();
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(chunk->get_column_by_slot_id(1)->is_binary());
    ASSERT_TRUE(chunk->get_column_by_slot_id(2)->is_large_binary());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_remove_column_by_slot_id) {
    ColumnPtr c1 = ColumnTestHelper::build_column<int32_t>({1});
    ColumnPtr c2 = ColumnTestHelper::build_column<int32_t>({2});
    ColumnPtr c3 = ColumnTestHelper::build_column<int32_t>({3});
    ColumnPtr c4 = ColumnTestHelper::build_column<int32_t>({4});

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(c1), 1);
    chunk->append_column(std::move(c2), 2);
    chunk->append_column(std::move(c3), 3);
    chunk->append_column(std::move(c4), 4);

    chunk->remove_column_by_slot_id(2);
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->get(0).get_int32(), 1);
    ASSERT_FALSE(chunk->is_slot_exist(2));
    ASSERT_EQ(chunk->get_column_by_slot_id(3)->get(0).get_int32(), 3);
    ASSERT_EQ(chunk->get_column_by_slot_id(4)->get(0).get_int32(), 4);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_chunk_downgrade) {
    auto c1 = BinaryColumn::create();
    c1->append_string("1");
    auto c2 = BinaryColumn::create();
    c2->append_string("11");
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(c1), 1);
    chunk->append_column(std::move(c2), 2);
    ASSERT_FALSE(chunk->has_large_column());

    auto ret = chunk->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_FALSE(chunk->has_large_column());

    auto c3 = LargeBinaryColumn::create();
    c3->append_string("1");
    auto c4 = LargeBinaryColumn::create();
    c4->append_string("2");
    chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(c3), 1);
    chunk->append_column(std::move(c4), 2);
    ASSERT_TRUE(chunk->has_large_column());

    ret = chunk->downgrade();
    ASSERT_FALSE(chunk->has_large_column());
    ASSERT_TRUE(ret.ok());
    ASSERT_FALSE(chunk->has_large_column());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_is_column_nullable) {
    Chunk chunk;
    ColumnPtr c1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), false);
    ColumnPtr c2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
    chunk.append_column(std::move(c1), 1);
    chunk.append_column(std::move(c2), 2);

    ASSERT_FALSE(chunk.is_column_nullable(1));
    ASSERT_TRUE(chunk.is_column_nullable(2));
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_construct) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    Columns columns = chunk->columns();
    ASSERT_EQ(2, columns.size());
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(columns[0].get()), 0);
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(columns[1].get()), 1);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_basic) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    ASSERT_TRUE(chunk->has_rows());
    ASSERT_TRUE(chunk->has_columns());
    ASSERT_EQ(2, chunk->num_columns());
    ASSERT_EQ(100, chunk->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_append_column) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));
    chunk->append_column(make_column(2), make_field(2));

    Columns columns = chunk->columns();
    ASSERT_EQ(3, columns.size());
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(columns[0].get()), 0);
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(columns[1].get()), 1);
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(columns[2].get()), 2);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_remove_column_by_index) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    chunk->remove_column_by_index(1);
    Columns columns = chunk->columns();
    ASSERT_EQ(1, columns.size());
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(columns[0].get()), 0);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, get_column_by_name) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));
    ColumnPtr column = chunk->get_column_by_name("c1");
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(column.get()), 1);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, get_mutable_column_by_index) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));
    auto* column = chunk->get_column_raw_ptr_by_index(1);
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(column), 1);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_copy_one_row) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    ChunkUniquePtr new_chunk = chunk->clone_empty();
    for (size_t i = 0; i < chunk->num_rows(); ++i) {
        new_chunk->append(*chunk, i, 1);
    }

    ASSERT_EQ(new_chunk->num_rows(), chunk->num_rows());
    for (size_t i = 0; i < chunk->columns().size(); ++i) {
        ASSERT_EQ(chunk->mutable_columns()[i]->size(), new_chunk->mutable_columns()[i]->size());
        for (size_t j = 0; j < chunk->mutable_columns()[i]->size(); ++j) {
            ASSERT_EQ(chunk->mutable_columns()[i]->get(j).get_int32(),
                      new_chunk->mutable_columns()[i]->get(j).get_int32());
        }
    }
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_swap_chunk) {
    auto chk1 = std::make_unique<Chunk>(make_columns(2), make_schema(2));
    auto chk2 = std::make_unique<Chunk>(make_columns(3), make_schema(3));
    chk1->set_delete_state(DEL_PARTIAL_SATISFIED);
    chk1->set_slot_id_to_index(1001, 0);

    chk2->set_delete_state(DEL_NOT_SATISFIED);

    chk1->swap_chunk(*chk2);

    ASSERT_EQ(3, chk1->num_columns());
    ASSERT_EQ(DEL_NOT_SATISFIED, chk1->delete_state());
    ASSERT_EQ(0, chk1->get_slot_id_to_index_map().size());

    ASSERT_EQ(2, chk2->num_columns());
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, chk2->delete_state());
    ASSERT_EQ(1, chk2->get_slot_id_to_index_map().size());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_reset) {
    auto chk = std::make_unique<Chunk>(make_columns(1), make_schema(1));
    chk->set_delete_state(DEL_PARTIAL_SATISFIED);
    chk->set_slot_id_to_index(1, 0);
    chk->get_column_raw_ptr_by_index(0)->resize(10);
    ASSERT_EQ(10, chk->num_rows());

    chk->reset();
    ASSERT_EQ(1, chk->num_columns());
    ASSERT_EQ(1, chk->get_slot_id_to_index_map().size());
    ASSERT_EQ(0, chk->get_slot_id_to_index_map().find(1)->second);
    ASSERT_EQ(0, chk->num_rows());
    ASSERT_EQ(DEL_NOT_SATISFIED, chk->delete_state());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_append_chunk_safe) {
    auto chunk_1 = std::make_shared<Chunk>();
    auto chunk_2 = std::make_shared<Chunk>();

    auto c1 = make_column(0);

    chunk_1->append_column(c1->clone(), 0);
    chunk_1->append_column(c1->clone(), 1);
    chunk_2->append_column(c1->clone(), 0);
    chunk_2->append_column(c1->clone(), 1);

    chunk_1->append_safe(*chunk_2);

    for (size_t i = 0; i < chunk_1->num_columns(); i++) {
        auto column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(chunk_1->mutable_columns()[i].get());
        ASSERT_EQ(column->size(), 200);
        for (size_t j = 0; j < 100; j++) {
            ASSERT_EQ(column->get(j).get_int32(), j);
            ASSERT_EQ(column->get(j + 100).get_int32(), j);
        }
    }
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_clone_unique) {
    auto chunk = std::make_shared<Chunk>();

    auto c1 = make_column(0);
    auto c2 = make_column(20);
    chunk->append_column(std::move(c1), 0);
    chunk->append_column(std::move(c2), 1);

    auto copy = chunk->clone_unique();
    copy->check_or_die();
    ASSERT_EQ(copy->num_rows(), chunk->num_rows());
}

// Test Chunk with extra data
// NOLINTNEXTLINE
TEST_F(ChunkTest, test_append_chunk_with_extra_data) {
    auto extra_data1 = make_extra_data(2, 2);
    // col0: 0, 1
    // col1: 1, 2
    auto chunk1 = std::make_unique<Chunk>(make_columns(2, 2), make_schema(2), extra_data1);

    // col0: 0, 1
    // col1: 1, 2
    auto extra_data2 = make_extra_data(2, 2);
    auto chunk2 = std::make_unique<Chunk>(make_columns(2, 2), make_schema(2), extra_data2);

    chunk1->append(*chunk2);
    // col0: 0, 1, 0, 1
    // col1: 1, 2, 1, 2

    Columns columns = chunk1->columns();
    ASSERT_EQ(2, columns.size());
    ASSERT_EQ(4, chunk1->num_rows());

    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(columns[0].get()), {0, 1, 0, 1});
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(columns[1].get()), {1, 2, 1, 2});

    ASSERT_TRUE(chunk1->has_extra_data());
    ASSERT_TRUE(chunk2->has_extra_data());
    auto* chunk_extra_data1 = dynamic_cast<ChunkExtraColumnsData*>(chunk1->get_extra_data().get());
    auto* chunk_extra_data2 = dynamic_cast<ChunkExtraColumnsData*>(chunk2->get_extra_data().get());
    ASSERT_TRUE(chunk_extra_data1);
    ASSERT_TRUE(chunk_extra_data2);
    chunk_extra_data1->append(*chunk_extra_data2, 0, chunk_extra_data2->num_rows());
    auto extra_data_columns = chunk_extra_data1->columns();
    ASSERT_EQ(2, extra_data_columns.size());
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(extra_data_columns[0].get()), {0, 1, 0, 1});
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(extra_data_columns[1].get()), {1, 2, 1, 2});
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_filter_with_extra_data) {
    auto extra_data1 = make_extra_data(2, 4);
    // 0, 1, 2, 3
    // 1, 2, 3, 4
    auto chunk1 = std::make_unique<Chunk>(make_columns(2, 4), make_schema(2), extra_data1);
    ASSERT_EQ(4, chunk1->num_rows());

    Buffer<uint8_t> selection{0, 1, 0, 1};
    auto filtered = chunk1->filter(selection);
    ASSERT_EQ(2, filtered);
    chunk1->check_or_die();
    // 1, 3
    // 2, 4

    ASSERT_EQ(chunk1->num_rows(), 2);
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(chunk1->columns()[0].get()), {1, 3});
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(chunk1->columns()[1].get()), {2, 4});

    ASSERT_TRUE(chunk1->has_extra_data());
    auto* chunk_extra_data1 = dynamic_cast<ChunkExtraColumnsData*>(chunk1->get_extra_data().get());
    ASSERT_TRUE(chunk_extra_data1);
    chunk_extra_data1->filter(selection);
    ASSERT_EQ(chunk_extra_data1->columns().size(), 2);
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(chunk_extra_data1->columns()[0].get()),
                 {1, 3});
    check_column(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(chunk_extra_data1->columns()[1].get()),
                 {2, 4});
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_clone_empty_with_extra_data) {
    auto extra_data1 = make_extra_data(2);
    auto chunk1 = std::make_unique<Chunk>(make_columns(2), make_schema(2), extra_data1);
    auto* extra_data = dynamic_cast<ChunkExtraColumnsData*>(chunk1->get_extra_data().get());
    auto copy = chunk1->clone_empty();
    copy->check_or_die();
    ASSERT_EQ(copy->num_rows(), 0);
    ASSERT_TRUE(!copy->has_extra_data());

    copy->set_extra_data(extra_data->clone_empty(copy->num_rows()));
    auto* copy_extra_data = dynamic_cast<ChunkExtraColumnsData*>(copy->get_extra_data().get());
    ASSERT_TRUE(copy_extra_data);
    ASSERT_EQ(copy_extra_data->columns().size(), extra_data->columns().size());
    ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(copy_extra_data->columns()[0].get())->size(), 0);
    ASSERT_EQ(copy_extra_data->chunk_data_metas().size(), extra_data->chunk_data_metas().size());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_clone_unique_with_extra_data) {
    auto extra_data1 = make_extra_data(2);
    auto chunk1 = std::make_unique<Chunk>(make_columns(2), make_schema(2), extra_data1);

    auto copy = chunk1->clone_unique();
    copy->check_or_die();
    ASSERT_EQ(copy->num_rows(), chunk1->num_rows());

    ASSERT_TRUE(copy->has_extra_data());

    auto expect_extra_data = make_extra_data(2);
    auto* copy_extra_data = dynamic_cast<ChunkExtraColumnsData*>(copy->get_extra_data().get());
    ASSERT_EQ(copy_extra_data->columns().size(), expect_extra_data->columns().size());
    ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(copy_extra_data->columns()[0].get())->size(),
              expect_extra_data->columns()[0]->size());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_reset_with_extra_data) {
    auto extra_data1 = make_extra_data(2);
    auto chunk1 = std::make_unique<Chunk>(make_columns(2), make_schema(2), extra_data1);
    ASSERT_EQ(100, chunk1->num_rows());
    ASSERT_TRUE(chunk1->has_extra_data());

    chunk1->reset();
    ASSERT_EQ(2, chunk1->num_columns());
    ASSERT_EQ(0, chunk1->num_rows());
    ASSERT_TRUE(!chunk1->has_extra_data());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_empty_chunk) {
    // Test empty chunk with no schema
    auto chunk1 = std::make_unique<Chunk>();
    ASSERT_NO_FATAL_FAILURE(chunk1->check_or_die());

    // Test empty chunk with empty schema
    auto chunk2 = std::make_unique<Chunk>(Columns{}, std::make_shared<Schema>(Fields{}));
    ASSERT_NO_FATAL_FAILURE(chunk2->check_or_die());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_valid_chunk) {
    // Test chunk with valid columns and schema
    auto chunk = std::make_unique<Chunk>(make_columns(3, 100), make_schema(3));
    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_with_constant_columns) {
    // Test chunk with constant columns (different sizes allowed)
    auto chunk = std::make_unique<Chunk>();
    auto col1 = make_column(0, 100);
    auto col2 = ConstColumn::create(make_column(0, 1), 100);
    auto col3 = make_column(0, 100);

    chunk->append_column(std::move(col1), 0);
    chunk->append_column(std::move(col2), 1);
    chunk->append_column(std::move(col3), 2);

    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_shared_columns) {
    // Test chunk with shared columns (same pointer used twice)
    auto chunk = std::make_unique<Chunk>();
    auto shared_col = make_column(0, 100);

    // Add the same column twice by cloning the shared pointer
    chunk->append_column(shared_col, 0);
    // chunk->append_column(shared_col, 1);
    // This should trigger CHECK failure because column is shared
    // EXPECT_DEATH(chunk->check_or_die(), "Column is shared with others");
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_with_schema) {
    // Test chunk with schema and proper cid_to_index mapping
    auto schema = make_schema(3);
    auto columns = make_columns(3, 100);
    auto chunk = std::make_unique<Chunk>(std::move(columns), schema);

    // Verify the cid_to_index mapping is correct
    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_with_slot_id) {
    // Test chunk with slot_id_to_index mapping
    auto chunk = std::make_unique<Chunk>();
    auto col1 = make_column(0, 100);
    auto col2 = make_column(1, 100);

    chunk->append_column(std::move(col1), 10); // slot_id = 10
    chunk->append_column(std::move(col2), 20); // slot_id = 20

    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());
    ASSERT_TRUE(chunk->is_slot_exist(10));
    ASSERT_TRUE(chunk->is_slot_exist(20));
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_after_operations) {
    // Test check_or_die after various operations
    auto chunk = std::make_unique<Chunk>(make_columns(2, 100), make_schema(2));

    // After construction
    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());

    // After appending a column
    chunk->append_column(make_column(2, 100), make_field(2));
    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());

    // After removing a column
    chunk->remove_column_by_index(1);
    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());

    // After reset
    chunk->reset();
    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_after_clone) {
    // Test check_or_die on cloned chunks
    auto original = std::make_unique<Chunk>(make_columns(2, 100), make_schema(2));
    ASSERT_NO_FATAL_FAILURE(original->check_or_die());

    // Clone and verify
    auto cloned = original->clone_unique();
    ASSERT_NO_FATAL_FAILURE(cloned->check_or_die());

    // Clone empty and verify
    auto empty_clone = original->clone_empty();
    ASSERT_NO_FATAL_FAILURE(empty_clone->check_or_die());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_nullable_columns) {
    // Test chunk with nullable columns
    auto chunk = std::make_unique<Chunk>();
    auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
    col1->append_default(100);
    auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
    col2->append_default(100);

    chunk->append_column(std::move(col1), 0);
    chunk->append_column(std::move(col2), 1);

    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_after_filter) {
    // Test check_or_die after filtering
    auto chunk = std::make_unique<Chunk>(make_columns(2, 100), make_schema(2));

    Buffer<uint8_t> selection(100, 0);
    for (size_t i = 0; i < 50; i++) {
        selection[i] = 1;
    }

    chunk->filter(selection);
    ASSERT_EQ(50, chunk->num_rows());
    ASSERT_NO_FATAL_FAILURE(chunk->check_or_die());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_after_append_safe) {
    // Test check_or_die after append_safe
    auto chunk1 = std::make_unique<Chunk>(make_columns(2, 100), make_schema(2));
    auto chunk2 = std::make_unique<Chunk>(make_columns(2, 100), make_schema(2));

    chunk1->append_safe(*chunk2);
    ASSERT_EQ(200, chunk1->num_rows());
    ASSERT_NO_FATAL_FAILURE(chunk1->check_or_die());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_check_or_die_after_swap) {
    // Test check_or_die after swap
    auto chunk1 = std::make_unique<Chunk>(make_columns(2, 100), make_schema(2));
    auto chunk2 = std::make_unique<Chunk>(make_columns(3, 50), make_schema(3));

    chunk1->swap_chunk(*chunk2);

    ASSERT_NO_FATAL_FAILURE(chunk1->check_or_die());
    ASSERT_NO_FATAL_FAILURE(chunk2->check_or_die());
    ASSERT_EQ(3, chunk1->num_columns());
    ASSERT_EQ(2, chunk2->num_columns());
}

// ==================== MutableChunk Tests ====================

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_construct_default) {
    auto mutable_chunk = std::make_shared<MutableChunk>();
    ASSERT_FALSE(mutable_chunk->has_rows());
    ASSERT_FALSE(mutable_chunk->has_columns());
    ASSERT_EQ(0, mutable_chunk->num_columns());
    ASSERT_EQ(0, mutable_chunk->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_construct_with_schema) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    ASSERT_TRUE(mutable_chunk->has_rows());
    ASSERT_TRUE(mutable_chunk->has_columns());
    ASSERT_EQ(2, mutable_chunk->num_columns());
    ASSERT_EQ(100, mutable_chunk->num_rows());
    ASSERT_NE(nullptr, mutable_chunk->schema());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_construct_with_slot_map) {
    auto mutable_columns = make_mutable_columns(2);
    MutableChunk::SlotHashMap slot_map;
    slot_map[1] = 0;
    slot_map[2] = 1;
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), std::move(slot_map));

    ASSERT_TRUE(mutable_chunk->has_rows());
    ASSERT_EQ(2, mutable_chunk->num_columns());
    ASSERT_TRUE(mutable_chunk->is_slot_exist(1));
    ASSERT_TRUE(mutable_chunk->is_slot_exist(2));
    ASSERT_EQ(0, mutable_chunk->get_index_by_slot_id(1));
    ASSERT_EQ(1, mutable_chunk->get_index_by_slot_id(2));
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_construct_with_extra_data) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto extra_data = make_extra_data(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema, extra_data);

    ASSERT_TRUE(mutable_chunk->has_extra_data());
    ASSERT_EQ(2, mutable_chunk->num_columns());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_from_chunk) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(*chunk));

    ASSERT_EQ(2, mutable_chunk->num_columns());
    ASSERT_EQ(100, mutable_chunk->num_rows());
    ASSERT_NE(nullptr, mutable_chunk->schema());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_to_chunk) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto chunk = std::move(*mutable_chunk).to_chunk();
    ASSERT_EQ(2, chunk.num_columns());
    ASSERT_EQ(100, chunk.num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_basic_operations) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    ASSERT_TRUE(mutable_chunk->has_rows());
    ASSERT_FALSE(mutable_chunk->is_empty());
    ASSERT_TRUE(mutable_chunk->has_columns());
    ASSERT_EQ(2, mutable_chunk->num_columns());
    ASSERT_EQ(100, mutable_chunk->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_set_num_rows) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    mutable_chunk->set_num_rows(50);
    ASSERT_EQ(50, mutable_chunk->num_rows());

    mutable_chunk->set_num_rows(150);
    ASSERT_EQ(150, mutable_chunk->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_get_column_by_index) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    const auto& col0 = mutable_chunk->get_column_by_index(0);
    auto* fixed_col = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col0.get());
    ASSERT_EQ(100, fixed_col->size());
    ASSERT_EQ(0, fixed_col->get(0).get_int32());

    auto& col1 = mutable_chunk->get_column_by_index(1);
    auto* fixed_col1 = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col1.get());
    ASSERT_EQ(1, fixed_col1->get(0).get_int32());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_get_column_by_slot_id) {
    auto mutable_columns = make_mutable_columns(2);
    MutableChunk::SlotHashMap slot_map;
    slot_map[1] = 0;
    slot_map[2] = 1;
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), std::move(slot_map));

    const auto& col1 = mutable_chunk->get_column_by_slot_id(1);
    auto* fixed_col = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col1.get());
    ASSERT_EQ(0, fixed_col->get(0).get_int32());

    auto& col2 = mutable_chunk->get_column_by_slot_id(2);
    auto* fixed_col2 = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col2.get());
    ASSERT_EQ(1, fixed_col2->get(0).get_int32());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_get_column_by_name) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    const auto& col = mutable_chunk->get_column_by_name("c1");
    auto* fixed_col = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col.get());
    check_column(fixed_col, 1);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_get_column_by_id) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    const auto& col = mutable_chunk->get_column_by_id(1);
    auto* fixed_col = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col.get());
    check_column(fixed_col, 1);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_append_column_with_field) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto new_col = make_mutable_column(2);
    mutable_chunk->append_column(std::move(new_col), make_field(2));

    ASSERT_EQ(3, mutable_chunk->num_columns());
    const auto& col = mutable_chunk->get_column_by_index(2);
    auto* fixed_col = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col.get());
    check_column(fixed_col, 2);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_append_column_with_slot_id) {
    auto mutable_chunk = std::make_shared<MutableChunk>();
    auto col1 = make_mutable_column(0);
    auto col2 = make_mutable_column(1);

    mutable_chunk->append_column(std::move(col1), 1);
    mutable_chunk->append_column(std::move(col2), 2);

    ASSERT_EQ(2, mutable_chunk->num_columns());
    ASSERT_TRUE(mutable_chunk->is_slot_exist(1));
    ASSERT_TRUE(mutable_chunk->is_slot_exist(2));
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_insert_column) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto new_col = make_mutable_column(2);
    mutable_chunk->insert_column(1, std::move(new_col), make_field(2));

    ASSERT_EQ(3, mutable_chunk->num_columns());
    const auto& col0 = mutable_chunk->get_column_by_index(0);
    const auto& col1 = mutable_chunk->get_column_by_index(1);
    const auto& col2 = mutable_chunk->get_column_by_index(2);
    auto* fixed_col0 = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col0.get());
    auto* fixed_col1 = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col1.get());
    auto* fixed_col2 = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col2.get());
    check_column(fixed_col0, 0);
    check_column(fixed_col1, 2);
    check_column(fixed_col2, 1);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_update_column) {
    auto mutable_columns = make_mutable_columns(2);
    MutableChunk::SlotHashMap slot_map;
    slot_map[1] = 0;
    slot_map[2] = 1;
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), std::move(slot_map));

    auto new_col = make_mutable_column(100);
    mutable_chunk->update_column(std::move(new_col), 1);

    const auto& col = mutable_chunk->get_column_by_slot_id(1);
    auto* fixed_col = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col.get());
    check_column(fixed_col, 100);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_update_column_by_index) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto new_col = make_mutable_column(100);
    mutable_chunk->update_column_by_index(std::move(new_col), 0);

    const auto& col = mutable_chunk->get_column_by_index(0);
    auto* fixed_col = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col.get());
    check_column(fixed_col, 100);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_append_or_update_column) {
    auto mutable_chunk = std::make_shared<MutableChunk>();
    auto col1 = make_mutable_column(0);
    mutable_chunk->append_or_update_column(std::move(col1), 1);

    ASSERT_EQ(1, mutable_chunk->num_columns());
    ASSERT_TRUE(mutable_chunk->is_slot_exist(1));

    auto col2 = make_mutable_column(100);
    mutable_chunk->append_or_update_column(std::move(col2), 1);

    ASSERT_EQ(1, mutable_chunk->num_columns());
    const auto& col = mutable_chunk->get_column_by_slot_id(1);
    auto* fixed_col = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col.get());
    check_column(fixed_col, 100);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_remove_column_by_index) {
    auto mutable_columns = make_mutable_columns(3);
    auto schema = make_schema(3);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    mutable_chunk->remove_column_by_index(1);
    ASSERT_EQ(2, mutable_chunk->num_columns());

    const auto& col0 = mutable_chunk->get_column_by_index(0);
    const auto& col1 = mutable_chunk->get_column_by_index(1);
    auto* fixed_col0 = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col0.get());
    auto* fixed_col1 = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col1.get());
    check_column(fixed_col0, 0);
    check_column(fixed_col1, 2);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_remove_column_by_slot_id) {
    auto mutable_columns = make_mutable_columns(3);
    MutableChunk::SlotHashMap slot_map;
    slot_map[1] = 0;
    slot_map[2] = 1;
    slot_map[3] = 2;
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), std::move(slot_map));

    mutable_chunk->remove_column_by_slot_id(2);
    ASSERT_EQ(2, mutable_chunk->num_columns());
    ASSERT_FALSE(mutable_chunk->is_slot_exist(2));
    ASSERT_TRUE(mutable_chunk->is_slot_exist(1));
    ASSERT_TRUE(mutable_chunk->is_slot_exist(3));
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_remove_columns_by_index) {
    auto mutable_columns = make_mutable_columns(4);
    auto schema = make_schema(4);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    std::vector<size_t> indexes{1, 3};
    mutable_chunk->remove_columns_by_index(indexes);
    ASSERT_EQ(2, mutable_chunk->num_columns());

    const auto& col0 = mutable_chunk->get_column_by_index(0);
    const auto& col1 = mutable_chunk->get_column_by_index(1);
    auto* fixed_col0 = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col0.get());
    auto* fixed_col1 = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(col1.get());
    check_column(fixed_col0, 0);
    check_column(fixed_col1, 2);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_append_default) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    size_t old_size = mutable_chunk->num_rows();
    mutable_chunk->append_default();
    ASSERT_EQ(old_size + 1, mutable_chunk->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_clone_empty) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto cloned = mutable_chunk->clone_empty();
    ASSERT_EQ(2, cloned->num_columns());
    ASSERT_EQ(0, cloned->num_rows());
    ASSERT_NE(nullptr, cloned->schema());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_clone_empty_with_size) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto cloned = mutable_chunk->clone_empty(200);
    ASSERT_EQ(2, cloned->num_columns());
    ASSERT_EQ(0, cloned->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_clone_empty_with_slot) {
    auto mutable_columns = make_mutable_columns(2);
    MutableChunk::SlotHashMap slot_map;
    slot_map[1] = 0;
    slot_map[2] = 1;
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), std::move(slot_map));

    auto cloned = mutable_chunk->clone_empty_with_slot();
    ASSERT_EQ(2, cloned->num_columns());
    ASSERT_EQ(0, cloned->num_rows());
    ASSERT_TRUE(cloned->is_slot_exist(1));
    ASSERT_TRUE(cloned->is_slot_exist(2));
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_clone_empty_with_schema) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto cloned = mutable_chunk->clone_empty_with_schema();
    ASSERT_EQ(2, cloned->num_columns());
    ASSERT_EQ(0, cloned->num_rows());
    ASSERT_NE(nullptr, cloned->schema());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_clone_unique) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto cloned = mutable_chunk->clone_unique();
    ASSERT_EQ(2, cloned->num_columns());
    ASSERT_EQ(100, cloned->num_rows());
    cloned->check_or_die();
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_append) {
    auto mutable_columns1 = make_mutable_columns(2, 50);
    auto schema = make_schema(2);
    auto mutable_chunk1 = std::make_shared<MutableChunk>(std::move(mutable_columns1), schema);

    auto chunk2 = std::make_unique<Chunk>(make_columns(2, 30), make_schema(2));
    mutable_chunk1->append(*chunk2);

    ASSERT_EQ(80, mutable_chunk1->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_append_with_offset) {
    auto mutable_columns1 = make_mutable_columns(2, 50);
    auto schema = make_schema(2);
    auto mutable_chunk1 = std::make_shared<MutableChunk>(std::move(mutable_columns1), schema);

    auto chunk2 = std::make_unique<Chunk>(make_columns(2, 30), make_schema(2));
    mutable_chunk1->append(*chunk2, 10, 20);

    ASSERT_EQ(70, mutable_chunk1->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_append_safe) {
    auto mutable_columns1 = make_mutable_columns(2, 50);
    auto schema = make_schema(2);
    auto mutable_chunk1 = std::make_shared<MutableChunk>(std::move(mutable_columns1), schema);

    auto chunk2 = std::make_unique<Chunk>(make_columns(2, 30), make_schema(2));
    mutable_chunk1->append_safe(*chunk2);

    ASSERT_EQ(80, mutable_chunk1->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_append_selective) {
    auto mutable_columns1 = make_mutable_columns(2, 10);
    auto schema = make_schema(2);
    auto mutable_chunk1 = std::make_shared<MutableChunk>(std::move(mutable_columns1), schema);

    auto chunk2 = std::make_unique<Chunk>(make_columns(2, 10), make_schema(2));
    uint32_t indexes[] = {0, 2, 4, 6, 8};
    mutable_chunk1->append_selective(*chunk2, indexes, 0, 5);

    ASSERT_EQ(15, mutable_chunk1->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_filter) {
    auto mutable_columns = make_mutable_columns(2, 4);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    Buffer<uint8_t> selection{0, 1, 0, 1};
    size_t filtered = mutable_chunk->filter(selection);
    ASSERT_EQ(2, filtered);
    ASSERT_EQ(2, mutable_chunk->num_rows());
    mutable_chunk->check_or_die();
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_filter_range) {
    auto mutable_columns = make_mutable_columns(2, 6);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    Buffer<uint8_t> selection{0, 1, 0, 1, 0, 1};
    size_t filtered = mutable_chunk->filter_range(selection, 1, 4);
    ASSERT_EQ(3, filtered);
    ASSERT_EQ(3, mutable_chunk->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_get) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto tuple = mutable_chunk->get(0);
    ASSERT_EQ(2, tuple.size());
    ASSERT_EQ(0, tuple[0].get_int32());
    ASSERT_EQ(1, tuple[1].get_int32());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_swap_chunk) {
    auto mutable_columns1 = make_mutable_columns(2);
    auto schema1 = make_schema(2);
    auto mutable_chunk1 = std::make_shared<MutableChunk>(std::move(mutable_columns1), schema1);
    mutable_chunk1->set_delete_state(DEL_PARTIAL_SATISFIED);
    mutable_chunk1->set_slot_id_to_index(1001, 0);

    auto mutable_columns2 = make_mutable_columns(3);
    auto schema2 = make_schema(3);
    auto mutable_chunk2 = std::make_shared<MutableChunk>(std::move(mutable_columns2), schema2);
    mutable_chunk2->set_delete_state(DEL_NOT_SATISFIED);

    mutable_chunk1->swap_chunk(*mutable_chunk2);

    ASSERT_EQ(3, mutable_chunk1->num_columns());
    ASSERT_EQ(DEL_NOT_SATISFIED, mutable_chunk1->delete_state());
    ASSERT_EQ(0, mutable_chunk1->get_slot_id_to_index_map().size());

    ASSERT_EQ(2, mutable_chunk2->num_columns());
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, mutable_chunk2->delete_state());
    ASSERT_EQ(1, mutable_chunk2->get_slot_id_to_index_map().size());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_reset) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);
    mutable_chunk->set_delete_state(DEL_PARTIAL_SATISFIED);
    mutable_chunk->set_slot_id_to_index(1, 0);

    mutable_chunk->reset();
    ASSERT_EQ(2, mutable_chunk->num_columns());
    ASSERT_EQ(0, mutable_chunk->num_rows());
    ASSERT_EQ(DEL_NOT_SATISFIED, mutable_chunk->delete_state());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_reserve) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    mutable_chunk->reserve(200);
    // Reserve doesn't change the number of rows, just capacity
    ASSERT_EQ(100, mutable_chunk->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_memory_usage) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    size_t mem_usage = mutable_chunk->memory_usage();
    ASSERT_GT(mem_usage, 0);

    size_t container_mem = mutable_chunk->container_memory_usage();
    ASSERT_GT(container_mem, 0);

    size_t ref_mem = mutable_chunk->reference_memory_usage();
    ASSERT_GE(ref_mem, 0);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_bytes_usage) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    size_t bytes = mutable_chunk->bytes_usage();
    ASSERT_GT(bytes, 0);

    size_t bytes_range = mutable_chunk->bytes_usage(0, 50);
    ASSERT_GT(bytes_range, 0);
    ASSERT_LE(bytes_range, bytes);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_has_const_column) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    ASSERT_FALSE(mutable_chunk->has_const_column());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_materialized_nullable) {
    auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
    col1->append_default();
    auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
    col2->append_default();

    MutableColumns mutable_columns;
    mutable_columns.push_back(Column::mutate(std::move(col1)));
    mutable_columns.push_back(Column::mutate(std::move(col2)));

    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    mutable_chunk->materialized_nullable();
    mutable_chunk->check_or_die();
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_upgrade_if_overflow) {
    size_t row_count = 1 << 20;
    auto c1 = BinaryColumn::create();
    c1->resize(row_count);
    auto c2 = BinaryColumn::create();
    for (size_t i = 0; i < row_count; i++) {
        c2->append(std::to_string(i));
    }

    MutableColumns mutable_columns;
    mutable_columns.push_back(Column::mutate(std::move(c1)));
    mutable_columns.push_back(Column::mutate(std::move(c2)));

    MutableChunk::SlotHashMap slot_map;
    slot_map[1] = 0;
    slot_map[2] = 1;
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), std::move(slot_map));

    Status st = mutable_chunk->upgrade_if_overflow();
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(mutable_chunk->get_column_by_slot_id(1)->is_binary());
    ASSERT_FALSE(mutable_chunk->get_column_by_slot_id(2)->is_large_binary());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_downgrade) {
    auto c1 = LargeBinaryColumn::create();
    c1->append_string("1");
    auto c2 = LargeBinaryColumn::create();
    c2->append_string("2");

    MutableColumns mutable_columns;
    mutable_columns.push_back(Column::mutate(std::move(c1)));
    mutable_columns.push_back(Column::mutate(std::move(c2)));

    MutableChunk::SlotHashMap slot_map;
    slot_map[1] = 0;
    slot_map[2] = 1;
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), std::move(slot_map));

    ASSERT_TRUE(mutable_chunk->has_large_column());
    auto ret = mutable_chunk->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_FALSE(mutable_chunk->has_large_column());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_has_large_column) {
    auto c1 = BinaryColumn::create();
    c1->append_string("1");
    auto c2 = LargeBinaryColumn::create();
    c2->append_string("2");

    MutableColumns mutable_columns;
    mutable_columns.push_back(Column::mutate(std::move(c1)));
    mutable_columns.push_back(Column::mutate(std::move(c2)));

    MutableChunk::SlotHashMap slot_map;
    slot_map[1] = 0;
    slot_map[2] = 1;
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), std::move(slot_map));

    ASSERT_TRUE(mutable_chunk->has_large_column());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_is_column_nullable) {
    auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), false);
    auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);

    MutableColumns mutable_columns;
    mutable_columns.push_back(Column::mutate(std::move(col1)));
    mutable_columns.push_back(Column::mutate(std::move(col2)));

    MutableChunk::SlotHashMap slot_map;
    slot_map[1] = 0;
    slot_map[2] = 1;
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), std::move(slot_map));

    ASSERT_FALSE(mutable_chunk->is_column_nullable(1));
    ASSERT_TRUE(mutable_chunk->is_column_nullable(2));
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_with_extra_data) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto extra_data = make_extra_data(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema, extra_data);

    ASSERT_TRUE(mutable_chunk->has_extra_data());
    ASSERT_NE(nullptr, mutable_chunk->get_extra_data());

    auto new_extra_data = make_extra_data(2);
    mutable_chunk->set_extra_data(new_extra_data);
    ASSERT_TRUE(mutable_chunk->has_extra_data());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_get_column_name) {
    auto mutable_columns = make_mutable_columns(2);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    std::string_view name0 = mutable_chunk->get_column_name(0);
    ASSERT_EQ("c0", name0);

    std::string_view name1 = mutable_chunk->get_column_name(1);
    ASSERT_EQ("c1", name1);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_merge) {
    auto mutable_columns1 = make_mutable_columns(2, 40, 0);
    auto schema = make_schema(2);
    auto mutable_chunk1 = std::make_shared<MutableChunk>(std::move(mutable_columns1), schema);

    auto mutable_columns2 = make_mutable_columns(2, 40, 0);
    auto mutable_chunk2 = std::make_shared<MutableChunk>(std::move(mutable_columns2), schema);

    mutable_chunk1->merge(std::move(*mutable_chunk2));
    ASSERT_EQ(40, mutable_chunk1->num_rows());
    ASSERT_EQ(2, mutable_chunk1->num_columns());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_rolling_append_selective) {
    auto mutable_columns1 = make_mutable_columns(2, 10);
    auto schema = make_schema(2);
    auto mutable_chunk1 = std::make_shared<MutableChunk>(std::move(mutable_columns1), schema);

    auto chunk2 = std::make_unique<Chunk>(make_columns(2, 10), make_schema(2));
    uint32_t indexes[] = {0, 2, 4, 6, 8};
    mutable_chunk1->rolling_append_selective(*chunk2, indexes, 0, 5);

    ASSERT_EQ(15, mutable_chunk1->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_update_rows) {
    auto mutable_columns = make_mutable_columns(2, 5, 0);
    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    auto src_chunk = std::make_unique<Chunk>(make_columns(2, 3, 0), make_schema(2));
    uint32_t indexes[] = {0, 1, 2};
    mutable_chunk->update_rows(*src_chunk, indexes);

    mutable_chunk->check_or_die();
    ASSERT_EQ(5, mutable_chunk->num_rows());
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_mutable_chunk_unpack_and_duplicate_const_columns) {
    auto const_col = ConstColumn::create(make_column(0, 1), 100);
    auto normal_col = make_mutable_column(1);

    MutableColumns mutable_columns;
    mutable_columns.push_back(Column::mutate(std::move(const_col)));
    mutable_columns.push_back(std::move(normal_col));

    auto schema = make_schema(2);
    auto mutable_chunk = std::make_shared<MutableChunk>(std::move(mutable_columns), schema);

    mutable_chunk->unpack_and_duplicate_const_columns();
    mutable_chunk->check_or_die();
    ASSERT_FALSE(mutable_chunk->has_const_column());
}

} // namespace starrocks
