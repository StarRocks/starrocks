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

#include "storage/merge_iterator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <vector>

#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config.h"
#include "storage/vector_chunk_iterator.h"

namespace starrocks {

template <typename T>
static inline std::string to_string(const std::vector<T>& v) {
    std::stringstream ss;
    for (T n : v) {
        ss << n << ",";
    }
    std::string s = ss.str();
    s.pop_back();
    return s;
}

class MergeIteratorTest : public testing::Test {
protected:
    void SetUp() override {
        auto f = std::make_shared<Field>(0, "c1", get_type_info(TYPE_INT), false);
        f->set_is_key(true);
        _schema = Schema(std::vector<FieldPtr>{f});
    }

    void TearDown() override {}

    Schema _schema;
};

// NOLINTNEXTLINE
TEST_F(MergeIteratorTest, heap_merge_overlapping) {
    std::vector<int32_t> v1{1, 1, 2, 3, 4, 5};
    std::vector<int32_t> v2{10, 11, 13, 15, 15, 16, 17};
    std::vector<int32_t> v3{12, 13, 14, 18, 19};
    auto sub1 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v1));
    auto sub2 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v2));
    auto sub3 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v3));

    std::vector<RowSourceMask> source_masks;

    auto iter = new_heap_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2, sub3});
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    std::vector<int32_t> expected;
    expected.insert(expected.end(), v1.begin(), v1.end());
    expected.insert(expected.end(), v2.begin(), v2.end());
    expected.insert(expected.end(), v3.begin(), v3.end());
    std::sort(expected.begin(), expected.end());

    std::vector<int32_t> real;
    ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
    while (iter->get_next(chunk.get(), &source_masks).ok()) {
        ColumnPtr& c = chunk->get_column_by_index(0);
        for (size_t i = 0; i < c->size(); i++) {
            real.push_back(c->get(i).get_int32());
        }
        chunk->reset();
    }
    ASSERT_EQ(expected.size(), real.size());
    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(expected[i], real[i]);
    }
    chunk->reset();
    ASSERT_TRUE(iter->get_next(chunk.get(), &source_masks).is_end_of_file());

    // check source masks
    std::vector<uint16_t> expected_sources{0, 0, 0, 0, 0, 0, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 2, 2};
    for (size_t i = 0; i < expected_sources.size(); i++) {
        EXPECT_EQ(expected_sources[i], source_masks.at(i).get_source_num());
    }
}

// NOLINTNEXTLINE
TEST_F(MergeIteratorTest, heap_merge_no_overlapping) {
    std::vector<int32_t> v1{1, 1, 2, 3, 4, 5};
    std::vector<int32_t> v2{6, 7, 8, 9, 10, 11, 12};
    std::vector<int32_t> v3{13, 14, 15, 16, 17};
    auto sub1 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v1));
    auto sub2 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v2));
    auto sub3 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v3));

    auto iter = new_heap_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2, sub3});
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    std::vector<int32_t> expected;
    expected.insert(expected.end(), v1.begin(), v1.end());
    expected.insert(expected.end(), v2.begin(), v2.end());
    expected.insert(expected.end(), v3.begin(), v3.end());
    std::sort(expected.begin(), expected.end());

    std::vector<int32_t> real;
    ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
    while (iter->get_next(chunk.get()).ok()) {
        ColumnPtr& c = chunk->get_column_by_index(0);
        for (size_t i = 0; i < c->size(); i++) {
            real.push_back(c->get(i).get_int32());
        }
        chunk->reset();
    }
    ASSERT_EQ(expected.size(), real.size());
    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(expected[i], real[i]);
    }
    ASSERT_TRUE(iter->get_next(chunk.get()).is_end_of_file());
}

// NOLINTNEXTLINE
TEST_F(MergeIteratorTest, merge_one) {
    auto sub1 = std::make_shared<VectorChunkIterator>(_schema, COL_INT({1, 1, 2, 3, 4, 5}));
    auto iter = new_heap_merge_iterator(std::vector<ChunkIteratorPtr>{sub1});
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    auto get_row = [](const ChunkPtr& chunk, size_t row) -> int32_t {
        auto c = std::dynamic_pointer_cast<FixedLengthColumn<int32_t>>(chunk->get_column_by_index(0));
        return c->get_data()[row];
    };

    ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
    Status st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(6U, chunk->num_rows());
    EXPECT_EQ(1, get_row(chunk, 0));
    EXPECT_EQ(1, get_row(chunk, 1));
    EXPECT_EQ(2, get_row(chunk, 2));
    EXPECT_EQ(3, get_row(chunk, 3));
    EXPECT_EQ(4, get_row(chunk, 4));
    EXPECT_EQ(5, get_row(chunk, 5));

    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

// NOLINTNEXTLINE
TEST_F(MergeIteratorTest, test_issue_DSDB_2715) {
    auto sub1 = std::make_shared<VectorChunkIterator>(_schema, COL_INT({1, 1, 2, 3, 4, 5}));
    auto sub2 = std::make_shared<VectorChunkIterator>(_schema, COL_INT({1, 1, 2, 3, 4, 5}));
    auto iter = new_heap_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2});
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    iter->close();
}

// NOLINTNEXTLINE
TEST_F(MergeIteratorTest, test_issue_6167) {
    std::vector<ChunkIteratorPtr> subs;
    int chunk_size = 4096;
    for (int i = 0; i < chunk_size; i++) {
        subs.push_back(std::make_shared<VectorChunkIterator>(_schema, COL_INT({1, 1, 1, 3, 4, 5})));
    }
    auto iter = new_heap_merge_iterator(subs);

    auto get_row = [](const ChunkPtr& chunk, size_t row) -> int32_t {
        auto c = std::dynamic_pointer_cast<FixedLengthColumn<int32_t>>(chunk->get_column_by_index(0));
        return c->get_data()[row];
    };

    ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), chunk_size);
    Status st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(chunk_size, chunk->num_rows());
    for (int i = 0; i < chunk_size; i++) {
        EXPECT_EQ(1, get_row(chunk, 1));
    }
    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(chunk_size, chunk->num_rows());
    for (int i = 0; i < chunk_size; i++) {
        EXPECT_EQ(1, get_row(chunk, 1));
    }
    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(chunk_size, chunk->num_rows());
    for (int i = 0; i < chunk_size; i++) {
        EXPECT_EQ(1, get_row(chunk, 1));
    }
    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(chunk_size, chunk->num_rows());
    for (int i = 0; i < chunk_size; i++) {
        EXPECT_EQ(3, get_row(chunk, 1));
    }
    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(chunk_size, chunk->num_rows());
    for (int i = 0; i < chunk_size; i++) {
        EXPECT_EQ(4, get_row(chunk, 1));
    }
    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(chunk_size, chunk->num_rows());
    for (int i = 0; i < chunk_size; i++) {
        EXPECT_EQ(5, get_row(chunk, 1));
    }
    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
}

// NOLINTNEXTLINE
TEST_F(MergeIteratorTest, mask_merge) {
    std::vector<int32_t> v1{1, 1, 2, 3, 4, 5};
    std::vector<int32_t> v2{10, 11, 13, 15, 15, 16, 17};
    std::vector<int32_t> v3{12, 13, 14, 18, 19};
    auto sub1 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v1));
    auto sub2 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v2));
    auto sub3 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v3));

    std::vector<RowSourceMask> source_masks;
    std::vector<uint16_t> expected_sources{0, 0, 0, 0, 0, 0, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 2, 2};
    for (unsigned short expected_source : expected_sources) {
        source_masks.emplace_back(RowSourceMask(expected_source, false));
    }
    RowSourceMaskBuffer mask_buffer(0, config::storage_root_path);
    mask_buffer.write(source_masks);
    mask_buffer.flush();
    mask_buffer.flip_to_read();
    source_masks.clear();

    auto iter = new_mask_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2, sub3}, &mask_buffer);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    std::vector<int32_t> expected;
    expected.insert(expected.end(), v1.begin(), v1.end());
    expected.insert(expected.end(), v2.begin(), v2.end());
    expected.insert(expected.end(), v3.begin(), v3.end());
    std::sort(expected.begin(), expected.end());

    std::vector<int32_t> real;
    ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
    while (iter->get_next(chunk.get(), &source_masks).ok()) {
        ColumnPtr& c = chunk->get_column_by_index(0);
        for (size_t i = 0; i < c->size(); i++) {
            real.push_back(c->get(i).get_int32());
        }
        chunk->reset();
    }
    ASSERT_EQ(expected.size(), real.size());
    for (size_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], real[i]);
    }
    chunk->reset();
    ASSERT_TRUE(iter->get_next(chunk.get(), &source_masks).is_end_of_file());

    // check source masks
    for (size_t i = 0; i < expected_sources.size(); i++) {
        ASSERT_EQ(expected_sources[i], source_masks.at(i).get_source_num());
    }
}

TEST_F(MergeIteratorTest, mask_merge_boundary_test) {
    std::vector<int32_t> v1;
    std::vector<int32_t> v2;
    std::vector<int32_t> v3;
    std::vector<int32_t> v4;
    std::vector<int32_t> expected;
    std::vector<RowSourceMask> source_masks;
    std::vector<uint16_t> expected_sources;

    for (int i = 0; i < 2048; i++) {
        v1.push_back(0);
        expected.push_back(0);
        expected_sources.push_back(0);
    }

    for (int i = 0; i < 4096; i++) {
        v2.push_back(1);
        expected.push_back(1);
        expected_sources.push_back(1);
    }

    for (int i = 0; i < 1024; i++) {
        v1.push_back(2);
        expected.push_back(2);
        expected_sources.push_back(0);
    }

    for (int i = 0; i < 1000; i++) {
        v3.push_back(3);
        expected.push_back(3);
        expected_sources.push_back(2);
    }

    for (int i = 0; i < 1024; i++) {
        v1.push_back(4);
        expected.push_back(4);
        expected_sources.push_back(0);
    }

    for (int i = 0; i < 2000; i++) {
        v3.push_back(5);
        expected.push_back(5);
        expected_sources.push_back(2);
    }

    for (int i = 0; i < 4096; i++) {
        v4.push_back(6);
        expected.push_back(6);
        expected_sources.push_back(3);
    }

    for (unsigned short expected_source : expected_sources) {
        source_masks.emplace_back(RowSourceMask(expected_source, false));
    }

    auto sub1 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v1));
    auto sub2 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v2));
    auto sub3 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v3));
    auto sub4 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v4));
    RowSourceMaskBuffer mask_buffer(0, config::storage_root_path);
    mask_buffer.write(source_masks);
    mask_buffer.flush();
    mask_buffer.flip_to_read();
    source_masks.clear();

    auto iter = new_mask_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2, sub3, sub4}, &mask_buffer);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    std::vector<int32_t> real;
    ChunkPtr chunk = ChunkHelper::new_chunk(iter->schema(), config::vector_chunk_size);
    while (iter->get_next(chunk.get(), &source_masks).ok()) {
        ColumnPtr& c = chunk->get_column_by_index(0);
        for (size_t i = 0; i < c->size(); i++) {
            real.push_back(c->get(i).get_int32());
        }
        chunk->reset();
    }
    ASSERT_EQ(expected.size(), real.size());
    for (size_t i = 0; i < expected.size(); i++) {
        ASSERT_EQ(expected[i], real[i]);
    }
    chunk->reset();
    ASSERT_TRUE(iter->get_next(chunk.get(), &source_masks).is_end_of_file());

    // check source masks
    for (size_t i = 0; i < expected_sources.size(); i++) {
        ASSERT_EQ(expected_sources[i], source_masks.at(i).get_source_num());
    }
}

} // namespace starrocks
