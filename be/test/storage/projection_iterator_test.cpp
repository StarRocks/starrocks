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

#include "storage/projection_iterator.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column.h"
#include "column/column_pool.h"
#include "column/datum.h"
#include "storage/chunk_helper.h"

namespace starrocks {

// Schema with 3 columns: INT, VARCHAR, INT.
class VectorIterator final : public ChunkIterator {
public:
    explicit VectorIterator(std::vector<int32_t> c1, std::vector<std::string> c2, std::vector<int32_t> c3)
            : ChunkIterator(schema()), _c1(std::move(c1)), _c2(std::move(c2)), _c3(std::move(c3)) {}

    // 10 elements at most every time.
    Status do_get_next(Chunk* chunk) override {
        size_t n = std::min(10LU, _c1.size() - _idx);
        for (size_t i = 0; i < n; i++) {
            chunk->get_column_by_index(0)->append_datum(Datum(_c1[_idx]));
            chunk->get_column_by_index(1)->append_datum(Datum(Slice(_c2[_idx])));
            chunk->get_column_by_index(2)->append_datum(Datum(_c3[_idx]));
            _idx++;
        }
        return n > 0 ? Status::OK() : Status::EndOfFile("eof");
    }

    void reset() {
        _idx = 0;
        _encoded_schema.clear();
    }

    static Schema schema() {
        FieldPtr f1 = std::make_shared<Field>(0, "c1", get_type_info(TYPE_INT), false);
        FieldPtr f2 = std::make_shared<Field>(1, "c2", get_type_info(TYPE_VARCHAR), false);
        FieldPtr f3 = std::make_shared<Field>(2, "c3", get_type_info(TYPE_INT), false);
        f1->set_is_key(true);
        return Schema(std::vector<FieldPtr>{f1, f2, f3});
    }

    void close() override {}

private:
    size_t _idx = 0;
    std::vector<int32_t> _c1;
    std::vector<std::string> _c2;
    std::vector<int32_t> _c3;
};

class ProjectionIteratorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override { TEST_clear_all_columns_this_thread(); }
};

// NOLINTNEXTLINE
TEST_F(ProjectionIteratorTest, all) {
    std::vector<int32_t> c1{1, 2, 3, 4, 5};
    std::vector<std::string> c2{"a", "b", "c", "d", "e"};
    std::vector<int32_t> c3{10, 11, 12, 13, 14};

    auto child = std::make_shared<VectorIterator>(c1, c2, c3);
    // select c1
    {
        Schema schema = child->schema();
        schema.remove(2);
        schema.remove(1);
        auto iter = new_projection_iterator(schema, child);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(iter->encoded_schema(), config::vector_chunk_size);
        auto st = iter->get_next(chunk.get());
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(5u, chunk->num_rows());
        ASSERT_EQ(1u, chunk->num_columns());
        for (int i = 0; i < 5; i++) {
            ASSERT_EQ(c1[i], chunk->get_column_by_index(0)->get(i).get_int32());
        }
    }
    // select c2, c3
    {
        child->reset();
        Schema schema = child->schema();
        schema.remove(0);
        auto iter = new_projection_iterator(schema, child);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(iter->encoded_schema(), config::vector_chunk_size);
        auto st = iter->get_next(chunk.get());
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(5u, chunk->num_rows());
        ASSERT_EQ(2u, chunk->num_columns());
        for (int i = 0; i < 5; i++) {
            ASSERT_EQ(c2[i], chunk->get_column_by_index(0)->get(i).get_slice());
            ASSERT_EQ(c3[i], chunk->get_column_by_index(1)->get(i).get_int32());
        }
    }
    // select c3, c1
    {
        child->reset();
        FieldPtr f1 = child->schema().field(0);
        FieldPtr f3 = child->schema().field(2);
        Schema schema({f3, f1});
        auto iter = new_projection_iterator(schema, child);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(iter->encoded_schema(), config::vector_chunk_size);
        auto st = iter->get_next(chunk.get());
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(5u, chunk->num_rows());
        ASSERT_EQ(2u, chunk->num_columns());
        for (int i = 0; i < 5; i++) {
            ASSERT_EQ(c3[i], chunk->get_column_by_index(0)->get(i).get_int32());
            ASSERT_EQ(c1[i], chunk->get_column_by_index(1)->get(i).get_int32());
        }
    }
}

} // namespace starrocks
