// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/merge_iterator.h"

#include <algorithm>
#include <memory>
#include <vector>

#include "column/column_pool.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "gtest/gtest.h"
#include "storage/vectorized/vector_chunk_iterator.h"

namespace starrocks::vectorized {

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
        auto f = std::make_shared<Field>(0, "c1", get_type_info(OLAP_FIELD_TYPE_INT), false);
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

    auto iter = new_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2, sub3});
    iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS);

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
    chunk->reset();
    ASSERT_TRUE(iter->get_next(chunk.get()).is_end_of_file());
}

// NOLINTNEXTLINE
TEST_F(MergeIteratorTest, heap_merge_no_overlapping) {
    std::vector<int32_t> v1{1, 1, 2, 3, 4, 5};
    std::vector<int32_t> v2{6, 7, 8, 9, 10, 11, 12};
    std::vector<int32_t> v3{13, 14, 15, 16, 17};
    auto sub1 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v1));
    auto sub2 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v2));
    auto sub3 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v3));

    auto iter = new_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2, sub3});
    iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS);

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
    auto iter = new_merge_iterator(std::vector<ChunkIteratorPtr>{sub1});
    iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS);

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
    auto iter = new_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2});
    iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS);
    iter->close();
}

} // namespace starrocks::vectorized
