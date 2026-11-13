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

#include "storage_primitive/union_iterator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_exec_fwd.h"
#include "storage_primitive/projection_iterator.h"
#include "types/type_info.h"

namespace starrocks {

class UnionIteratorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

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
            auto* c = chunk->get_column_raw_ptr_by_index(0);
            (void)c->append_numbers(_numbers.data() + _idx, n * sizeof(int32_t));
            _idx += n;
            return Status::OK();
        }

        // 10 elements at most every time. And also return rssid rowids
        Status do_get_next(Chunk* chunk, std::vector<uint64_t>* rssid_rowids) override {
            if (_idx >= _numbers.size()) {
                return Status::EndOfFile("eof");
            }
            size_t n = std::min(10LU, _numbers.size() - _idx);
            auto* c = chunk->get_column_raw_ptr_by_index(0);
            (void)c->append_numbers(_numbers.data() + _idx, n * sizeof(int32_t));
            _idx += n;
            for (size_t i = 0; i < n; i++) {
                rssid_rowids->push_back(i);
            }
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
        auto c = FixedLengthColumn<int32_t>::dynamic_pointer_cast(chunk->get_column_by_index(0));
        return c->immutable_data()[row];
    };

    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    Status st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(3U, chunk->num_rows());
    ASSERT_EQ(6, get_row(chunk, 0));
    ASSERT_EQ(7, get_row(chunk, 1));
    ASSERT_EQ(8, get_row(chunk, 2));

    chunk->reset();
    std::vector<uint64_t> rssid_rowids;
    st = iter->get_next(chunk.get(), nullptr, &rssid_rowids);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(5U, chunk->num_rows());
    ASSERT_EQ(5U, rssid_rowids.size());
    ASSERT_EQ(1, get_row(chunk, 0));
    ASSERT_EQ(2, get_row(chunk, 1));
    ASSERT_EQ(3, get_row(chunk, 2));
    ASSERT_EQ(4, get_row(chunk, 3));
    ASSERT_EQ(5, get_row(chunk, 4));
    ASSERT_EQ(0, rssid_rowids[0]);
    ASSERT_EQ(1, rssid_rowids[1]);
    ASSERT_EQ(2, rssid_rowids[2]);
    ASSERT_EQ(3, rssid_rowids[3]);
    ASSERT_EQ(4, rssid_rowids[4]);

    chunk->reset();
    st = iter->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());

    std::vector<RowSourceMask> source_masks;
    st = iter->get_next(chunk.get(), &source_masks, &rssid_rowids);
    ASSERT_TRUE(st.is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, union_one) {
    config::vector_chunk_size = 1024;
    std::vector<int32_t> n1{1, 2, 3, 4, 5};
    auto sub1 = std::make_shared<IntIterator>(n1);

    auto iter = new_union_iterator({sub1});
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    auto get_row = [](const ChunkPtr& chunk, size_t row) -> int32_t {
        auto c = FixedLengthColumn<int32_t>::dynamic_pointer_cast(chunk->get_column_by_index(0));
        return c->immutable_data()[row];
    };

    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
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

// A child that reads its numbers like IntIterator and reports a read-ahead of `blocks` blocks once started.
class ReadAheadIntIterator final : public ChunkIterator {
public:
    ReadAheadIntIterator(std::vector<int32_t> numbers, size_t blocks, bool has_read_ahead)
            : ChunkIterator(schema()), _numbers(std::move(numbers)), _blocks(blocks), _has_read_ahead(has_read_ahead) {}

    Status do_get_next(Chunk* chunk) override { return _fill(chunk, nullptr, nullptr); }
    Status do_get_next(Chunk* chunk, std::vector<uint32_t>* rowid) override { return _fill(chunk, rowid, nullptr); }
    Status do_get_next(Chunk* chunk, std::vector<uint64_t>* rssid_rowids) override {
        return _fill(chunk, nullptr, rssid_rowids);
    }

    Status start_read_ahead(bool* started) override {
        _start_calls++;
        *started = _has_read_ahead;
        return _start_status;
    }
    size_t read_ahead_blocks() const override { return (_start_calls > 0 || _read) ? _blocks : 0; }
    bool read_ahead_done() const override { return _done; }

    void close() override { _close_calls++; }

    int start_calls() const { return _start_calls; }
    int close_calls() const { return _close_calls; }
    void set_read_ahead_done(bool done) { _done = done; }
    // start_read_ahead() fails with |st|, and so does the first read.
    void fail_with(Status st) {
        _start_status = st;
        _read_status = std::move(st);
    }

    static Schema schema() {
        FieldPtr f = std::make_shared<Field>(0, "c1", get_type_info(TYPE_INT), false);
        return Schema(std::vector<FieldPtr>{f});
    }

private:
    Status _fill(Chunk* chunk, std::vector<uint32_t>* rowid, std::vector<uint64_t>* rssid_rowids) {
        _read = true;
        RETURN_IF_ERROR(_read_status);
        if (_idx >= _numbers.size()) {
            return Status::EndOfFile("eof");
        }
        size_t n = std::min(10LU, _numbers.size() - _idx);
        (void)chunk->get_column_raw_ptr_by_index(0)->append_numbers(_numbers.data() + _idx, n * sizeof(int32_t));
        for (size_t i = 0; i < n; i++) {
            if (rowid != nullptr) {
                rowid->push_back(_idx + i);
            }
            if (rssid_rowids != nullptr) {
                rssid_rowids->push_back(_idx + i);
            }
        }
        _idx += n;
        return Status::OK();
    }

    std::vector<int32_t> _numbers;
    size_t _idx = 0;
    const size_t _blocks;
    const bool _has_read_ahead;
    bool _done = true;
    Status _start_status;
    Status _read_status;
    int _start_calls = 0;
    int _close_calls = 0;
    bool _read = false;
};

static std::vector<int32_t> read_all(const ChunkIteratorPtr& iter) {
    std::vector<int32_t> out;
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), 1024);
    while (true) {
        chunk->reset();
        Status st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        EXPECT_TRUE(st.ok());
        auto c = FixedLengthColumn<int32_t>::dynamic_pointer_cast(chunk->get_column_by_index(0));
        out.insert(out.end(), c->immutable_data().begin(), c->immutable_data().end());
    }
    return out;
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, read_ahead_within_budget) {
    std::vector<std::shared_ptr<ReadAheadIntIterator>> children;
    std::vector<ChunkIteratorPtr> iters;
    for (int i = 0; i < 6; i++) {
        children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{i}, 3, true));
        iters.push_back(children.back());
    }
    // At most 4 children ahead, fewer than 8 blocks held ahead in total.
    auto iter = new_union_iterator(std::move(iters), 4, 8);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), 1024);
    ASSERT_TRUE(iter->get_next(chunk.get()).ok());
    // Child 0 holds 3, child 1 brings 6, child 2 brings 9: the third one is the last that starts.
    EXPECT_EQ(0, children[0]->start_calls());
    EXPECT_EQ(1, children[1]->start_calls());
    EXPECT_EQ(1, children[2]->start_calls());
    EXPECT_EQ(0, children[3]->start_calls());
    // Reading on never starts a child twice and keeps the order of the rows.
    auto rest = read_all(iter);
    EXPECT_EQ((std::vector<int32_t>{1, 2, 3, 4, 5}), rest);
    for (int i = 1; i < 6; i++) {
        EXPECT_EQ(1, children[i]->start_calls()) << i;
    }
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, read_ahead_stops_without_child_support) {
    std::vector<std::shared_ptr<ReadAheadIntIterator>> children;
    std::vector<ChunkIteratorPtr> iters;
    for (int i = 0; i < 4; i++) {
        children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{i}, 1, i != 1));
        iters.push_back(children.back());
    }
    auto iter = new_union_iterator(std::move(iters), 8, 100);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    EXPECT_EQ((std::vector<int32_t>{0, 1, 2, 3}), read_all(iter));
    EXPECT_EQ(1, children[1]->start_calls());
    EXPECT_EQ(0, children[2]->start_calls());
    EXPECT_EQ(0, children[3]->start_calls());
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, no_read_ahead_by_default) {
    std::vector<std::shared_ptr<ReadAheadIntIterator>> children;
    std::vector<ChunkIteratorPtr> iters;
    for (int i = 0; i < 3; i++) {
        children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{i}, 1, true));
        iters.push_back(children.back());
    }
    auto iter = new_union_iterator(std::move(iters));
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    EXPECT_EQ((std::vector<int32_t>{0, 1, 2}), read_all(iter));
    for (auto& c : children) {
        EXPECT_EQ(0, c->start_calls());
    }
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, read_ahead_one_child) {
    auto child = std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{0, 1}, 1, true);
    // A single child is read as it is: nothing to read ahead of.
    auto iter = new_union_iterator(std::vector<ChunkIteratorPtr>{child}, 4, 8);
    EXPECT_EQ(child.get(), iter.get());
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    EXPECT_EQ((std::vector<int32_t>{0, 1}), read_all(iter));
    EXPECT_EQ(0, child->start_calls());
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, read_ahead_zero_children) {
    std::vector<std::shared_ptr<ReadAheadIntIterator>> children;
    std::vector<ChunkIteratorPtr> iters;
    for (int i = 0; i < 3; i++) {
        children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{i}, 1, true));
        iters.push_back(children.back());
    }
    auto iter = new_union_iterator(std::move(iters), 0, 100);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    EXPECT_EQ((std::vector<int32_t>{0, 1, 2}), read_all(iter));
    for (auto& c : children) {
        EXPECT_EQ(0, c->start_calls());
    }
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, read_ahead_waits_for_current_child) {
    std::vector<std::shared_ptr<ReadAheadIntIterator>> children;
    std::vector<ChunkIteratorPtr> iters;
    // Child 0 reads two chunks.
    children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>(15, 7), 1, true));
    iters.push_back(children.back());
    for (int i = 1; i < 4; i++) {
        children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{i}, 1, true));
        iters.push_back(children.back());
    }
    children[0]->set_read_ahead_done(false);
    // Child 1 has not asked for everything it reads once started: the ones after it wait.
    children[1]->set_read_ahead_done(false);
    auto iter = new_union_iterator(std::move(iters), 8, 100);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), 1024);
    ASSERT_TRUE(iter->get_next(chunk.get()).ok());
    EXPECT_EQ(0, children[1]->start_calls());

    children[0]->set_read_ahead_done(true);
    chunk->reset();
    ASSERT_TRUE(iter->get_next(chunk.get()).ok());
    EXPECT_EQ(1, children[1]->start_calls());
    EXPECT_EQ(0, children[2]->start_calls());

    // Once it has, the next ones start while child 1 is read, each once.
    children[1]->set_read_ahead_done(true);
    chunk->reset();
    ASSERT_TRUE(iter->get_next(chunk.get()).ok());
    EXPECT_EQ(1, children[2]->start_calls());
    EXPECT_EQ(1, children[3]->start_calls());
    EXPECT_EQ((std::vector<int32_t>{2, 3}), read_all(iter));
    for (int i = 1; i < 4; i++) {
        EXPECT_EQ(1, children[i]->start_calls()) << i;
    }
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, read_ahead_error_returned_on_its_turn) {
    std::vector<std::shared_ptr<ReadAheadIntIterator>> children;
    std::vector<ChunkIteratorPtr> iters;
    for (int i = 0; i < 4; i++) {
        children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{i}, 1, true));
        iters.push_back(children.back());
    }
    children[1]->fail_with(Status::IOError("injected"));
    auto iter = new_union_iterator(std::move(iters), 8, 100);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), 1024);
    // The failed read-ahead stops reading ahead without failing the read of child 0.
    ASSERT_TRUE(iter->get_next(chunk.get()).ok());
    EXPECT_EQ(1, children[1]->start_calls());
    EXPECT_EQ(0, children[2]->start_calls());
    // Child 1 reports its error when its turn comes.
    chunk->reset();
    Status st = iter->get_next(chunk.get());
    EXPECT_TRUE(st.is_io_error()) << st;
    EXPECT_EQ(std::string("injected"), std::string(st.message()));
    EXPECT_EQ(0, children[2]->start_calls());
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, read_ahead_with_rowids) {
    for (bool rssid : {false, true}) {
        std::vector<std::shared_ptr<ReadAheadIntIterator>> children;
        std::vector<ChunkIteratorPtr> iters;
        for (int i = 0; i < 3; i++) {
            children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{i, i}, 1, true));
            iters.push_back(children.back());
        }
        auto iter = new_union_iterator(std::move(iters), 8, 100);
        ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), 1024);
        std::vector<uint32_t> rowids;
        std::vector<uint64_t> rssid_rowids;
        auto next = [&]() {
            return rssid ? iter->get_next(chunk.get(), &rssid_rowids) : iter->get_next(chunk.get(), &rowids);
        };
        ASSERT_TRUE(next().ok());
        EXPECT_EQ(1, children[1]->start_calls()) << rssid;
        EXPECT_EQ(1, children[2]->start_calls()) << rssid;
        while (true) {
            Status st = next();
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_TRUE(st.ok()) << st;
        }
        EXPECT_EQ(6, chunk->num_rows()) << rssid;
        EXPECT_EQ(rssid ? 0 : 6, rowids.size());
        EXPECT_EQ(rssid ? 6 : 0, rssid_rowids.size());
    }
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, read_ahead_through_wrappers) {
    RuntimeProfile::Counter timer(TUnit::TIME_NS);
    std::vector<std::shared_ptr<ReadAheadIntIterator>> children;
    std::vector<ChunkIteratorPtr> iters;
    for (int i = 0; i < 3; i++) {
        children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{i}, 2, true));
        // As a lake scan wraps its segment iterators.
        iters.push_back(
                timed_chunk_iterator(new_projection_iterator(ReadAheadIntIterator::schema(), children.back()), &timer));
    }
    auto iter = new_union_iterator(std::move(iters), 8, 3);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), 1024);
    ASSERT_TRUE(iter->get_next(chunk.get()).ok());
    // Child 0 holds 2 blocks, child 1 brings 4: the budget of 3 stops before child 2.
    EXPECT_EQ(1, children[1]->start_calls());
    EXPECT_EQ(0, children[2]->start_calls());
    EXPECT_EQ((std::vector<int32_t>{1, 2}), read_all(iter));
}

// NOLINTNEXTLINE
TEST_F(UnionIteratorTest, close_after_read_ahead) {
    std::vector<std::shared_ptr<ReadAheadIntIterator>> children;
    std::vector<ChunkIteratorPtr> iters;
    for (int i = 0; i < 4; i++) {
        children.push_back(std::make_shared<ReadAheadIntIterator>(std::vector<int32_t>{i}, 1, true));
        iters.push_back(children.back());
    }
    auto iter = new_union_iterator(std::move(iters), 2, 100);
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), 1024);
    ASSERT_TRUE(iter->get_next(chunk.get()).ok());
    EXPECT_EQ(1, children[1]->start_calls());
    EXPECT_EQ(1, children[2]->start_calls());
    EXPECT_EQ(0, children[3]->start_calls());
    // Closing before the end, as a cancelled scan does, closes every child once: started, current or not.
    iter->close();
    for (int i = 0; i < 4; i++) {
        EXPECT_EQ(1, children[i]->close_calls()) << i;
    }
}

} // namespace starrocks
