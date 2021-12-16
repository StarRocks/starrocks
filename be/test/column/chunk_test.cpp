// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/column/chunk_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "column/chunk.h"

#include <gtest/gtest.h>

#include "column/field.h"
#include "column/fixed_length_column.h"

namespace starrocks::vectorized {

class ChunkTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    std::string make_string(size_t i) { return std::string("c").append(std::to_string(static_cast<int32_t>(i))); }

    FieldPtr make_field(size_t i) {
        return std::make_shared<Field>(i, make_string(i), get_type_info(OLAP_FIELD_TYPE_INT), false);
    }

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

    ColumnPtr make_column(size_t start) {
        auto column = FixedLengthColumn<int32_t>::create();
        for (int i = 0; i < 100; i++) {
            column->append(start + i);
        }
        return column;
    }

    Columns make_columns(size_t size) {
        Columns columns;
        for (size_t i = 0; i < size; i++) {
            columns.emplace_back(make_column(i));
        }
        return columns;
    }

    void check_column(const FixedLengthColumn<int32_t>* column, size_t idx) {
        for (size_t i = 0; i < 100; i++) {
            ASSERT_EQ(column->get_data()[i], static_cast<int32_t>(i + idx));
        }
    }
};

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
TEST_F(ChunkTest, test_serde) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    std::string buffer;
    buffer.resize(chunk->serialize_size());
    size_t written_size = chunk->serialize((uint8_t*)buffer.data());

    RuntimeChunkMeta meta;
    meta.slot_id_to_index.init(2);
    meta.slot_id_to_index.insert(0, 0);
    meta.slot_id_to_index.insert(1, 1);
    meta.is_nulls.resize(2, false);
    meta.is_consts.resize(2, false);
    meta.types.resize(2);
    meta.types[0] = TypeDescriptor(PrimitiveType::TYPE_INT);
    meta.types[1] = TypeDescriptor(PrimitiveType::TYPE_INT);

    std::unique_ptr<Chunk> new_chunk = chunk->clone_empty_with_schema();
    new_chunk->deserialize((uint8_t*)buffer.data(), buffer.size(), meta, written_size);

    ASSERT_EQ(new_chunk->num_rows(), chunk->num_rows());
    for (size_t i = 0; i < chunk->columns().size(); ++i) {
        ASSERT_EQ(chunk->columns()[i]->size(), new_chunk->columns()[i]->size());
        for (size_t j = 0; j < chunk->columns()[i]->size(); ++j) {
            ASSERT_EQ(chunk->columns()[i]->get(j).get_int32(), new_chunk->columns()[i]->get(j).get_int32());
        }
    }
}

// NOLINTNEXTLINE
TEST_F(ChunkTest, test_copy_one_row) {
    auto chunk = std::make_unique<Chunk>(make_columns(2), make_schema(2));

    std::unique_ptr<Chunk> new_chunk = chunk->clone_empty_with_tuple();
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
    ASSERT_EQ(0, *(chk->get_slot_id_to_index_map().seek(1)));
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

} // namespace starrocks::vectorized
