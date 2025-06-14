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

#include "storage/unique_iterator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "storage/chunk_helper.h"

namespace starrocks {

class UniqueIteratorTest : public testing::Test {
protected:
    // return chunk with single column of type int32_t.
    class IntIterator final : public ChunkIterator {
    public:
        explicit IntIterator(std::vector<int32_t> numbers)
                : ChunkIterator(new_schema()), _numbers(std::move(numbers)) {}

        // 10 elements at most every time.
        Status do_get_next(Chunk* chunk) override {
            if (_idx >= _numbers.size()) {
                return Status::EndOfFile("eof");
            }
            size_t n = std::min<size_t>(4, _numbers.size() - _idx);
            ColumnPtr c = chunk->get_column_by_index(0);
            (void)c->append_numbers(_numbers.data() + _idx, n * sizeof(int32_t));
            _idx += n;
            return Status::OK();
        }

        // 10 elements at most every time.
        Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override {
            return do_get_next(chunk);
        }

        void close() override {}

        static Schema new_schema() {
            FieldPtr f = std::make_shared<Field>(0, "c1", get_type_info(TYPE_INT), false);
            f->set_is_key(true);
            return Schema(std::vector<FieldPtr>{f});
        }

    private:
        size_t _idx = 0;
        std::vector<int32_t> _numbers;
    };
};

// NOLINTNEXTLINE
TEST_F(UniqueIteratorTest, single_int) {
    {
        std::vector<int32_t> numbers{};
        auto sub = std::make_shared<IntIterator>(numbers);
        ChunkIteratorPtr iter = new_unique_iterator(sub);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
        Status st = iter->get_next(chunk.get());
        ASSERT_TRUE(st.is_end_of_file());
    }
    {
        std::vector<int32_t> numbers{1, 2, 3, 4};
        auto sub = std::make_shared<IntIterator>(numbers);
        ChunkIteratorPtr iter = new_unique_iterator(sub);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
        std::vector<int32_t> fetched;
        while (iter->get_next(chunk.get()).ok()) {
            auto c = reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk->get_column_by_index(0).get());
            for (size_t i = 0; i < c->size(); i++) {
                fetched.push_back(c->get(i).get_int32());
            }
            chunk->reset();
        }
        EXPECT_EQ(4, fetched.size());
        for (size_t i = 0; i < fetched.size(); i++) {
            EXPECT_EQ(i + 1, fetched[i]);
        }
    }
    {
        std::vector<int32_t> numbers{1, 2, 3, 4, 5, 6, 7, 8, 9};
        auto sub = std::make_shared<IntIterator>(numbers);
        ChunkIteratorPtr iter = new_unique_iterator(sub);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
        std::vector<int32_t> fetched;
        while (iter->get_next(chunk.get()).ok()) {
            auto c = reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk->get_column_by_index(0).get());
            for (size_t i = 0; i < c->size(); i++) {
                fetched.push_back(c->get(i).get_int32());
            }
            chunk->reset();
        }
        EXPECT_EQ(9, fetched.size());
        for (size_t i = 0; i < fetched.size(); i++) {
            EXPECT_EQ(i + 1, fetched[i]);
        }
    }
    {
        std::vector<int32_t> numbers{1, 1, 1};
        auto sub = std::make_shared<IntIterator>(numbers);
        ChunkIteratorPtr iter = new_unique_iterator(sub);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
        std::vector<int32_t> fetched;
        while (iter->get_next(chunk.get()).ok()) {
            auto c = reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk->get_column_by_index(0).get());
            for (size_t i = 0; i < c->size(); i++) {
                fetched.push_back(c->get(i).get_int32());
            }
            chunk->reset();
        }
        EXPECT_EQ(1, fetched.size());
        for (size_t i = 0; i < fetched.size(); i++) {
            EXPECT_EQ(i + 1, fetched[i]);
        }
    }
    {
        std::vector<int32_t> numbers{1, 1, 1, 1, 1, 1};
        auto sub = std::make_shared<IntIterator>(numbers);
        ChunkIteratorPtr iter = new_unique_iterator(sub);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
        std::vector<int32_t> fetched;
        while (iter->get_next(chunk.get()).ok()) {
            auto c = reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk->get_column_by_index(0).get());
            for (size_t i = 0; i < c->size(); i++) {
                fetched.push_back(c->get(i).get_int32());
            }
            chunk->reset();
        }
        EXPECT_EQ(1, fetched.size());
        for (size_t i = 0; i < fetched.size(); i++) {
            EXPECT_EQ(i + 1, fetched[i]);
        }
    }
    {
        std::vector<int32_t> numbers{1, 2, 3, 4, 5, 5, 6, 6, 6, 6, 7, 8, 8, 8, 9, 9, 10, 11, 12};
        auto sub = std::make_shared<IntIterator>(numbers);
        ChunkIteratorPtr iter = new_unique_iterator(sub);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
        std::vector<int32_t> fetched;
        while (iter->get_next(chunk.get()).ok()) {
            auto c = reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk->get_column_by_index(0).get());
            for (size_t i = 0; i < c->size(); i++) {
                fetched.push_back(c->get(i).get_int32());
            }
            chunk->reset();
        }
        EXPECT_EQ(12, fetched.size());
        for (size_t i = 0; i < fetched.size(); i++) {
            EXPECT_EQ(i + 1, fetched[i]);
        }
    }
    {
        std::vector<int32_t> numbers{1, 2, 3, 3, 4, 4, 4, 4, 4, 5, 6, 6, 7, 8, 9, 9, 10, 11, 12};
        auto sub = std::make_shared<IntIterator>(numbers);
        ChunkIteratorPtr iter = new_unique_iterator(sub);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

        ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
        std::vector<int32_t> fetched;
        while (iter->get_next(chunk.get()).ok()) {
            auto c = reinterpret_cast<FixedLengthColumn<int32_t>*>(chunk->get_column_by_index(0).get());
            for (size_t i = 0; i < c->size(); i++) {
                fetched.push_back(c->get(i).get_int32());
            }
            chunk->reset();
        }
        EXPECT_EQ(12, fetched.size());
        for (size_t i = 0; i < fetched.size(); i++) {
            EXPECT_EQ(i + 1, fetched[i]);
        }
    }
}

} // namespace starrocks
