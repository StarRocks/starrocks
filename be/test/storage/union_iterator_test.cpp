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

#include "storage/union_iterator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/column_pool.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "storage/chunk_helper.h"

namespace starrocks {

class UnionIteratorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override { TEST_clear_all_columns_this_thread(); }

    // return chunk with single column of type int32_t.
    class IntIterator final : public ChunkIterator {
    public:
        explicit IntIterator(std::vector<int32_t> numbers) : ChunkIterator(schema()), _numbers(std::move(numbers)) {}

        // 10 elements at most every time.
        Status do_get_next(Chunk* chunk) override {
            if (_idx >= _numbers.size()) {
                return Status::EndOfFile("eof");
            }
            size_t n = std::min(10LU, _numbers.size() - _idx);
            ColumnPtr c = chunk->get_column_by_index(0);
            (void)c->append_numbers(_numbers.data() + _idx, n * sizeof(int32_t));
            _idx += n;
            return Status::OK();
        }

        void close() override {}

        static Schema schema() {
            FieldPtr f = std::make_shared<Field>(0, "c1", get_type_info(TYPE_INT), false);
            return Schema(std::vector<FieldPtr>{f});
        }

    private:
        size_t _idx = 0;
        std::vector<int32_t> _numbers;
    };
};

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, union_two) {
    config::vector_chunk_size = 1024;
    std::vector<int32_t> n1{6, 7, 8};
    std::vector<int32_t> n2{1, 2, 3, 4, 5};
    auto sub1 = std::make_shared<IntIterator>(n1);
    auto sub2 = std::make_shared<IntIterator>(n2);

    auto iter = new_union_iterator({sub1, sub2});

    auto get_row = [](const ChunkPtr& chunk, size_t row) -> int32_t {
        auto c = std::dynamic_pointer_cast<FixedLengthColumn<int32_t>>(chunk->get_column_by_index(0));
        return c->get_data()[row];
    };

    ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    Status st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(3U, chunk->num_rows());
    ASSERT_EQ(6, get_row(chunk, 0));
    ASSERT_EQ(7, get_row(chunk, 1));
    ASSERT_EQ(8, get_row(chunk, 2));

    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(5U, chunk->num_rows());
    ASSERT_EQ(1, get_row(chunk, 0));
    ASSERT_EQ(2, get_row(chunk, 1));
    ASSERT_EQ(3, get_row(chunk, 2));
    ASSERT_EQ(4, get_row(chunk, 3));
    ASSERT_EQ(5, get_row(chunk, 4));

    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, union_one) {
    config::vector_chunk_size = 1024;
    std::vector<int32_t> n1{1, 2, 3, 4, 5};
    auto sub1 = std::make_shared<IntIterator>(n1);

    auto iter = new_union_iterator({sub1});
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    auto get_row = [](const ChunkPtr& chunk, size_t row) -> int32_t {
        auto c = std::dynamic_pointer_cast<FixedLengthColumn<int32_t>>(chunk->get_column_by_index(0));
        return c->get_data()[row];
    };

    ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
    Status st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(5U, chunk->num_rows());
    ASSERT_EQ(1, get_row(chunk, 0));
    ASSERT_EQ(2, get_row(chunk, 1));
    ASSERT_EQ(3, get_row(chunk, 2));
    ASSERT_EQ(4, get_row(chunk, 3));
    ASSERT_EQ(5, get_row(chunk, 4));

    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

} // namespace starrocks
