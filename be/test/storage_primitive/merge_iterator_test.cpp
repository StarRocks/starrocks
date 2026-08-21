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

#include "storage_primitive/merge_iterator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <vector>

#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_exec_fwd.h"
#include "common/config_storage_fwd.h"
#include "storage_primitive/vector_chunk_iterator.h"

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
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
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
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
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
        auto c = FixedLengthColumn<int32_t>::dynamic_pointer_cast(chunk->get_column_by_index(0));
        return c->get_data()[row];
    };

    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
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
        auto c = FixedLengthColumn<int32_t>::dynamic_pointer_cast(chunk->get_column_by_index(0));
        return c->get_data()[row];
    };

    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), chunk_size);
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
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
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
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
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

TEST_F(MergeIteratorTest, mask_merge_with_selection) {
    auto sub1 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(std::vector<int32_t>{1, 3, 5}));
    auto sub2 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(std::vector<int32_t>{2, 4, 6}));

    std::vector<RowSourceMask> source_masks;
    for (uint16_t source : std::vector<uint16_t>{0, 1, 0, 1, 0, 1}) {
        source_masks.emplace_back(source, false);
    }
    std::vector<RowSourceMask> selections;
    for (uint16_t selected : std::vector<uint16_t>{0, 1, 1, 0, 0, 1}) {
        selections.emplace_back(selected, false);
    }

    RowSourceMaskBuffer mask_buffer(0, config::storage_root_path);
    ASSERT_TRUE(mask_buffer.write(source_masks).ok());
    ASSERT_TRUE(mask_buffer.flush().ok());
    ASSERT_TRUE(mask_buffer.flip_to_read().ok());
    RowSourceMaskBuffer selection_buffer(0, config::storage_root_path);
    ASSERT_TRUE(selection_buffer.write(selections).ok());
    ASSERT_TRUE(selection_buffer.flush().ok());
    ASSERT_TRUE(selection_buffer.flip_to_read().ok());

    auto iter = new_mask_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2}, &mask_buffer, &selection_buffer);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    std::vector<int32_t> actual;
    auto chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
    while (iter->get_next(chunk.get()).ok()) {
        const auto& column = chunk->get_column_by_index(0);
        for (size_t i = 0; i < column->size(); ++i) {
            actual.push_back(column->get(i).get_int32());
        }
        chunk->reset();
    }
    EXPECT_EQ((std::vector<int32_t>{2, 3, 6}), actual);
}

// Regression for the branch the mixed-selection case above cannot reach. With every row selected,
// max_same_source_count equals the whole chunk, so do_get_next takes the `swap_chunk` shortcut and
// returns `fill(child)` -- the call that also observes the child's EOF. MaskMergeIterator::fill
// swallows that EOF and returns OK, so the swapped rows must still be delivered on this call and the
// EOF must surface only on the next one. Getting that wrong silently truncates the last chunk of an
// UNSHARE rewrite.
TEST_F(MergeIteratorTest, mask_merge_single_child_all_selected_defers_eof) {
    auto sub = std::make_shared<VectorChunkIterator>(_schema, COL_INT(std::vector<int32_t>{1, 2, 3, 4}));
    std::vector<RowSourceMask> source_masks(4, RowSourceMask{0, false});
    std::vector<RowSourceMask> selections(4, RowSourceMask{1, false});

    RowSourceMaskBuffer mask_buffer(0, config::storage_root_path);
    ASSERT_TRUE(mask_buffer.write(source_masks).ok());
    ASSERT_TRUE(mask_buffer.flush().ok());
    ASSERT_TRUE(mask_buffer.flip_to_read().ok());
    RowSourceMaskBuffer selection_buffer(0, config::storage_root_path);
    ASSERT_TRUE(selection_buffer.write(selections).ok());
    ASSERT_TRUE(selection_buffer.flush().ok());
    ASSERT_TRUE(selection_buffer.flip_to_read().ok());

    auto iter = new_mask_merge_iterator(std::vector<ChunkIteratorPtr>{sub}, &mask_buffer, &selection_buffer);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    auto chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
    auto st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(4, chunk->num_rows()) << "the whole-chunk swap must deliver its rows, not drop them with the EOF";
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(i + 1, chunk->get_column_by_index(0)->get(i).get_int32());
    }

    chunk->reset();
    ASSERT_TRUE(iter->get_next(chunk.get()).is_end_of_file()) << "EOF belongs to the call after the last rows";
    EXPECT_EQ(0, chunk->num_rows());
}

// The two buffers are read in lockstep, one entry per row. If they disagree on how many rows remain,
// the merge would silently pair a row with another row's verdict, so it must fail instead. This is
// the check on entry; the one below covers the same disagreement discovered mid-skip.
TEST_F(MergeIteratorTest, mask_merge_rejects_selection_shorter_than_mask) {
    auto sub = std::make_shared<VectorChunkIterator>(_schema, COL_INT(std::vector<int32_t>{1, 2, 3, 4}));
    std::vector<RowSourceMask> source_masks(4, RowSourceMask{0, false});
    std::vector<RowSourceMask> selections(2, RowSourceMask{1, false}); // two rows short

    RowSourceMaskBuffer mask_buffer(0, config::storage_root_path);
    ASSERT_TRUE(mask_buffer.write(source_masks).ok());
    ASSERT_TRUE(mask_buffer.flush().ok());
    ASSERT_TRUE(mask_buffer.flip_to_read().ok());
    RowSourceMaskBuffer selection_buffer(0, config::storage_root_path);
    ASSERT_TRUE(selection_buffer.write(selections).ok());
    ASSERT_TRUE(selection_buffer.flush().ok());
    ASSERT_TRUE(selection_buffer.flip_to_read().ok());

    auto iter = new_mask_merge_iterator(std::vector<ChunkIteratorPtr>{sub}, &mask_buffer, &selection_buffer);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    auto chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);

    Status st = Status::OK();
    for (int i = 0; i < 4 && st.ok(); ++i) {
        chunk->reset();
        st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) break;
    }
    ASSERT_FALSE(st.ok()) << "a truncated selection stream must not merge silently";
    EXPECT_TRUE(st.message().find("different lengths") != std::string::npos) << st.to_string();
}

// Every row unselected: the skip path runs to the end of the chunk, and the iterator must report EOF
// rather than emit an empty chunk as if it were data.
TEST_F(MergeIteratorTest, mask_merge_all_rows_unselected_yields_eof) {
    auto sub = std::make_shared<VectorChunkIterator>(_schema, COL_INT(std::vector<int32_t>{1, 2, 3, 4}));
    std::vector<RowSourceMask> source_masks(4, RowSourceMask{0, false});
    std::vector<RowSourceMask> selections(4, RowSourceMask{0, false});

    RowSourceMaskBuffer mask_buffer(0, config::storage_root_path);
    ASSERT_TRUE(mask_buffer.write(source_masks).ok());
    ASSERT_TRUE(mask_buffer.flush().ok());
    ASSERT_TRUE(mask_buffer.flip_to_read().ok());
    RowSourceMaskBuffer selection_buffer(0, config::storage_root_path);
    ASSERT_TRUE(selection_buffer.write(selections).ok());
    ASSERT_TRUE(selection_buffer.flush().ok());
    ASSERT_TRUE(selection_buffer.flip_to_read().ok());

    auto iter = new_mask_merge_iterator(std::vector<ChunkIteratorPtr>{sub}, &mask_buffer, &selection_buffer);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    auto chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
    auto st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file()) << st.to_string();
    EXPECT_EQ(0, chunk->num_rows());
}

// The unselected rows sit at the END of the chunk, so the skip loop is what exhausts it. The rows
// before them must still come out, and the exhaustion must not be mistaken for a failure.
TEST_F(MergeIteratorTest, mask_merge_trailing_unselected_rows_still_emit_the_prefix) {
    auto sub = std::make_shared<VectorChunkIterator>(_schema, COL_INT(std::vector<int32_t>{1, 2, 3, 4}));
    std::vector<RowSourceMask> source_masks(4, RowSourceMask{0, false});
    std::vector<RowSourceMask> selections{RowSourceMask{1, false}, RowSourceMask{1, false}, RowSourceMask{0, false},
                                          RowSourceMask{0, false}};

    RowSourceMaskBuffer mask_buffer(0, config::storage_root_path);
    ASSERT_TRUE(mask_buffer.write(source_masks).ok());
    ASSERT_TRUE(mask_buffer.flush().ok());
    ASSERT_TRUE(mask_buffer.flip_to_read().ok());
    RowSourceMaskBuffer selection_buffer(0, config::storage_root_path);
    ASSERT_TRUE(selection_buffer.write(selections).ok());
    ASSERT_TRUE(selection_buffer.flush().ok());
    ASSERT_TRUE(selection_buffer.flip_to_read().ok());

    auto iter = new_mask_merge_iterator(std::vector<ChunkIteratorPtr>{sub}, &mask_buffer, &selection_buffer);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    auto chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
    ASSERT_TRUE(iter->get_next(chunk.get()).ok());
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ(1, chunk->get_column_by_index(0)->get(0).get_int32());
    EXPECT_EQ(2, chunk->get_column_by_index(0)->get(1).get_int32());

    chunk->reset();
    EXPECT_TRUE(iter->get_next(chunk.get()).is_end_of_file());
}

TEST_F(MergeIteratorTest, mask_merge_single_child_with_selection) {
    auto sub = std::make_shared<VectorChunkIterator>(_schema, COL_INT(std::vector<int32_t>{1, 2, 3, 4}));
    std::vector<RowSourceMask> source_masks(4, RowSourceMask{0, false});
    std::vector<RowSourceMask> selections{RowSourceMask{0, false}, RowSourceMask{1, false}, RowSourceMask{0, false},
                                          RowSourceMask{1, false}};

    RowSourceMaskBuffer mask_buffer(0, config::storage_root_path);
    ASSERT_TRUE(mask_buffer.write(source_masks).ok());
    ASSERT_TRUE(mask_buffer.flush().ok());
    ASSERT_TRUE(mask_buffer.flip_to_read().ok());
    RowSourceMaskBuffer selection_buffer(0, config::storage_root_path);
    ASSERT_TRUE(selection_buffer.write(selections).ok());
    ASSERT_TRUE(selection_buffer.flush().ok());
    ASSERT_TRUE(selection_buffer.flip_to_read().ok());

    auto iter = new_mask_merge_iterator(std::vector<ChunkIteratorPtr>{sub}, &mask_buffer, &selection_buffer);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    auto chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
    ASSERT_TRUE(iter->get_next(chunk.get()).ok());
    ASSERT_EQ(2, chunk->num_rows());
    EXPECT_EQ(2, chunk->get_column_by_index(0)->get(0).get_int32());
    EXPECT_EQ(4, chunk->get_column_by_index(0)->get(1).get_int32());
}

TEST_F(MergeIteratorTest, mask_merge_exhausted_iterator) {
    std::vector<int32_t> v1{1, 2}; // Only 2 elements
    std::vector<int32_t> v2{10, 11};
    auto sub1 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v1));
    auto sub2 = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v2));

    std::vector<RowSourceMask> source_masks;
    // Request 3 elements from source 0, but it only has 2 elements.
    // This matches the exhausted iterator scenario that caused nullptr dereference.
    std::vector<uint16_t> expected_sources{0, 0, 0, 1, 1};
    for (unsigned short expected_source : expected_sources) {
        source_masks.emplace_back(RowSourceMask(expected_source, false));
    }
    RowSourceMaskBuffer mask_buffer(0, config::storage_root_path);
    mask_buffer.write(source_masks);
    mask_buffer.flush();
    mask_buffer.flip_to_read();
    source_masks.clear();

    auto iter = new_mask_merge_iterator(std::vector<ChunkIteratorPtr>{sub1, sub2}, &mask_buffer);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
    Status st;
    while (true) {
        chunk->reset();
        st = iter->get_next(chunk.get(), &source_masks);
        if (!st.ok()) {
            break;
        }
    }
    // Should return InternalError instead of crashing
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
    ASSERT_TRUE(st.message().find("child iterator is exhausted") != std::string::npos);
}

} // namespace starrocks
