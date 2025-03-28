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

#include "column/binary_column.h"
#include "column/chunk_extra_data.h"
#include "column/column_helper.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "testutil/column_test_helper.h"
#include "testutil/parallel_test.h"

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

    Columns make_columns(size_t num_cols, size_t size = 100) {
        Columns columns;
        for (size_t i = 0; i < num_cols; i++) {
            columns.emplace_back(make_column(i, size));
        }
        return columns;
    }

    void check_column(const FixedLengthColumn<int32_t>* column, size_t idx) {
        for (size_t i = 0; i < 100; i++) {
            ASSERT_EQ(column->get_data()[i], static_cast<int32_t>(i + idx));
        }
    }

    void check_column(const FixedLengthColumn<int32_t>* column, std::vector<int32_t> expect_datas) {
        for (size_t i = 0; i < expect_datas.size(); i++) {
            ASSERT_EQ(column->get_data()[i], static_cast<int32_t>(expect_datas[i]));
        }
    }

    ChunkExtraColumnsDataPtr make_extra_data(size_t num_cols, size_t size = 100) {
        auto chunk = std::make_unique<Chunk>(make_columns(num_cols, size), make_schema(num_cols));
        std::vector<ChunkExtraColumnsMeta> extra_data_metas;
        for (size_t i = 0; i < num_cols; i++) {
            extra_data_metas.push_back(
                    ChunkExtraColumnsMeta{.type = TypeDescriptor(TYPE_INT), .is_null = false, .is_const = false});
        }
        auto extra_data_cols = make_columns(num_cols, size);
        return std::make_shared<ChunkExtraColumnsData>(std::move(extra_data_metas), std::move(extra_data_cols));
    }
};

// NOLINTNEXTLINE
GROUP_SLOW_TEST_F(ChunkTest, test_chunk_upgrade_if_overflow) {
    size_t row_count = 1 << 30;
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->resize(row_count);
    BinaryColumn::Ptr c2 = BinaryColumn::create();
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
    chunk->append_column(c1, 1);
    chunk->append_column(c2, 2);
    chunk->append_column(c3, 3);
    chunk->append_column(c4, 4);

    chunk->remove_column_by_slot_id(2);
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->get(0).get_int32(), 1);
    ASSERT_FALSE(chunk->is_slot_exist(2));
    ASSERT_EQ(chunk->get_column_by_slot_id(3)->get(0).get_int32(), 3);
    ASSERT_EQ(chunk->get_column_by_slot_id(4)->get(0).get_int32(), 4);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_chunk_downgrade) {
    BinaryColumn::Ptr c1 = BinaryColumn::create();
    c1->append_string("1");
    BinaryColumn::Ptr c2 = BinaryColumn::create();
    c2->append_string("11");
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(c1, 1);
    chunk->append_column(c2, 2);
    ASSERT_FALSE(chunk->has_large_column());

    auto ret = chunk->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_FALSE(chunk->has_large_column());

    LargeBinaryColumn::Ptr c3 = LargeBinaryColumn::create();
    c3->append_string("1");
    LargeBinaryColumn::Ptr c4 = LargeBinaryColumn::create();
    c4->append_string("2");
    chunk = std::make_shared<Chunk>();
    chunk->append_column(c3, 1);
    chunk->append_column(c4, 2);
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
    chunk.append_column(c1, 1);
    chunk.append_column(c2, 2);

    ASSERT_FALSE(chunk.is_column_nullable(1));
    ASSERT_TRUE(chunk.is_column_nullable(2));
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_construct) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    Columns columns = chunk->columns();
    ASSERT_EQ(2, columns.size());
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[0].get()), 0);
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[1].get()), 1);
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
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[0].get()), 0);
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[1].get()), 1);
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[2].get()), 2);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_insert_column) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));
    chunk->insert_column(1, make_column(2), make_field(2));

    Columns columns = chunk->columns();
    ASSERT_EQ(3, columns.size());
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[0].get()), 0);
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[2].get()), 1);
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[1].get()), 2);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_remove_column_by_index) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    chunk->remove_column_by_index(1);
    Columns columns = chunk->columns();
    ASSERT_EQ(1, columns.size());
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[0].get()), 0);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, get_column_by_name) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));
    ColumnPtr column = chunk->get_column_by_name("c1");
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(column.get()), 1);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, get_column_by_index) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));
    ColumnPtr column = chunk->get_column_by_index(1);
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(column.get()), 1);
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_copy_one_row) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    std::unique_ptr<Chunk> new_chunk = chunk->clone_empty();
    for (size_t i = 0; i < chunk->num_rows(); ++i) {
        new_chunk->append(*chunk, i, 1);
    }

    ASSERT_EQ(new_chunk->num_rows(), chunk->num_rows());
    for (size_t i = 0; i < chunk->columns().size(); ++i) {
        ASSERT_EQ(chunk->columns()[i]->size(), new_chunk->columns()[i]->size());
        for (size_t j = 0; j < chunk->columns()[i]->size(); ++j) {
            ASSERT_EQ(chunk->columns()[i]->get(j).get_int32(), new_chunk->columns()[i]->get(j).get_int32());
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
    chk->get_column_by_index(0)->resize(10);
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

    chunk_1->append_column(c1, 0);
    chunk_1->append_column(c1, 1);
    chunk_2->append_column(c1, 0);
    chunk_2->append_column(c1, 1);

    chunk_1->append_safe(*chunk_2);

    for (size_t i = 0; i < chunk_1->num_columns(); i++) {
        auto column = chunk_1->columns()[i];
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
    chunk->append_column(c1, 0);
    chunk->append_column(c2, 1);

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

    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[0].get()), {0, 1, 0, 1});
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(columns[1].get()), {1, 2, 1, 2});

    ASSERT_TRUE(chunk1->has_extra_data());
    ASSERT_TRUE(chunk2->has_extra_data());
    auto* chunk_extra_data1 = dynamic_cast<ChunkExtraColumnsData*>(chunk1->get_extra_data().get());
    auto* chunk_extra_data2 = dynamic_cast<ChunkExtraColumnsData*>(chunk2->get_extra_data().get());
    ASSERT_TRUE(chunk_extra_data1);
    ASSERT_TRUE(chunk_extra_data2);
    chunk_extra_data1->append(*chunk_extra_data2, 0, chunk_extra_data2->num_rows());
    auto extra_data_columns = chunk_extra_data1->columns();
    ASSERT_EQ(2, extra_data_columns.size());
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(extra_data_columns[0].get()), {0, 1, 0, 1});
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(extra_data_columns[1].get()), {1, 2, 1, 2});
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
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk1->columns()[0].get()), {1, 3});
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk1->columns()[1].get()), {2, 4});

    ASSERT_TRUE(chunk1->has_extra_data());
    auto* chunk_extra_data1 = dynamic_cast<ChunkExtraColumnsData*>(chunk1->get_extra_data().get());
    ASSERT_TRUE(chunk_extra_data1);
    chunk_extra_data1->filter(selection);
    ASSERT_EQ(chunk_extra_data1->columns().size(), 2);
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk_extra_data1->columns()[0].get()), {1, 3});
    check_column(reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk_extra_data1->columns()[1].get()), {2, 4});
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
    ASSERT_EQ(copy_extra_data->columns()[0]->size(), 0);
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
    ASSERT_EQ(copy_extra_data->columns()[0]->size(), expect_extra_data->columns()[0]->size());
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

} // namespace starrocks
