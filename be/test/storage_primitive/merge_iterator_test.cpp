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
#include "common/config_compaction_fwd.h"
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
// The read-ahead path must be indistinguishable from the serial one: the same rows, in the same
// order, with the same source masks, whatever the buffer depth. Children are given a small chunk
// size so the merge runs dry on them repeatedly and exercises the refill path, not just the
// initial prefill.
class MergePipelineEquivalenceTest : public MergeIteratorTest {
protected:
    void SetUp() override {
        MergeIteratorTest::SetUp();
        _saved_parallel = config::enable_compaction_parallel_merge_init;
        _saved_buffers = config::compaction_merge_child_buffers;
    }
    void TearDown() override {
        config::enable_compaction_parallel_merge_init = _saved_parallel;
        config::compaction_merge_child_buffers = _saved_buffers;
        MergeIteratorTest::TearDown();
    }

    // Five inputs whose key ranges overlap in different ways: fully disjoint, interleaved, and
    // duplicated across inputs, so the merge switches between them instead of draining one.
    std::vector<std::vector<int32_t>> inputs() const {
        std::vector<std::vector<int32_t>> vs(5);
        for (int i = 0; i < 200; i++) {
            vs[0].push_back(i * 4);
            vs[1].push_back(i * 4 + 1);
            vs[2].push_back(i * 2);
            vs[3].push_back(i);
            vs[4].push_back(500 + i);
        }
        return vs;
    }

    struct Output {
        std::vector<int32_t> rows;
        std::vector<uint16_t> sources;
    };

    Output run_heap(bool parallel, int buffers) {
        config::enable_compaction_parallel_merge_init = parallel;
        config::compaction_merge_child_buffers = buffers;

        std::vector<ChunkIteratorPtr> subs;
        for (const auto& v : inputs()) {
            auto sub = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v));
            sub->chunk_size(7); // force many refills
            subs.push_back(sub);
        }
        auto iter = new_heap_merge_iterator(subs);
        EXPECT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        return drain(iter);
    }

    Output run_mask(bool parallel, int buffers, const std::vector<uint16_t>& sources) {
        config::enable_compaction_parallel_merge_init = parallel;
        config::compaction_merge_child_buffers = buffers;

        std::vector<RowSourceMask> masks;
        masks.reserve(sources.size());
        for (uint16_t s : sources) {
            masks.emplace_back(RowSourceMask(s, false));
        }
        // A fresh id per call: the buffer is backed by a file named after it, and this runs many
        // times in one test.
        RowSourceMaskBuffer mask_buffer(_next_mask_id++, config::storage_root_path);
        EXPECT_TRUE(mask_buffer.write(masks).ok());
        EXPECT_TRUE(mask_buffer.flush().ok());
        EXPECT_TRUE(mask_buffer.flip_to_read().ok());

        std::vector<ChunkIteratorPtr> subs;
        for (const auto& v : inputs()) {
            auto sub = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v));
            sub->chunk_size(7);
            subs.push_back(sub);
        }
        auto iter = new_mask_merge_iterator(subs, &mask_buffer);
        EXPECT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        return drain(iter);
    }

    static Output drain(const ChunkIteratorPtr& iter) {
        Output out;
        std::vector<RowSourceMask> masks;
        ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
        while (iter->get_next(chunk.get(), &masks).ok()) {
            ColumnPtr& c = chunk->get_column_by_index(0);
            for (size_t i = 0; i < c->size(); i++) {
                out.rows.push_back(c->get(i).get_int32());
            }
            chunk->reset();
        }
        for (auto& m : masks) {
            out.sources.push_back(m.get_source_num());
        }
        return out;
    }

    bool _saved_parallel = false;
    int32_t _saved_buffers = 1;
    int64_t _next_mask_id = 1000;
};

// NOLINTNEXTLINE
TEST_F(MergePipelineEquivalenceTest, heap_merge_read_ahead_matches_serial) {
    const Output baseline = run_heap(false, 1);
    ASSERT_FALSE(baseline.rows.empty());
    ASSERT_TRUE(std::is_sorted(baseline.rows.begin(), baseline.rows.end()));

    for (int buffers : {1, 2, 3, 8}) {
        for (bool parallel : {false, true}) {
            const Output got = run_heap(parallel, buffers);
            EXPECT_EQ(baseline.rows, got.rows) << "parallel=" << parallel << " buffers=" << buffers;
            EXPECT_EQ(baseline.sources, got.sources) << "parallel=" << parallel << " buffers=" << buffers;
        }
    }
}

// NOLINTNEXTLINE
TEST_F(MergePipelineEquivalenceTest, mask_merge_read_ahead_matches_serial) {
    // Vertical compaction feeds the mask merge the sources the key group produced, so build them
    // the same way.
    const std::vector<uint16_t> sources = run_heap(false, 1).sources;
    ASSERT_FALSE(sources.empty());

    const Output baseline = run_mask(false, 1, sources);
    ASSERT_FALSE(baseline.rows.empty());

    for (int buffers : {1, 2, 3, 8}) {
        for (bool parallel : {false, true}) {
            const Output got = run_mask(parallel, buffers, sources);
            EXPECT_EQ(baseline.rows, got.rows) << "parallel=" << parallel << " buffers=" << buffers;
        }
    }
}

// A child whose prefetch() behavior is scripted per instance. The rows come from a wrapped
// VectorChunkIterator, so the data path is identical to the plain children the baselines use;
// only the IO-half contract differs:
//   RESIDENT   -- reserve the declared bytes from the budget (refund on overdraw, like
//                 SharedBufferedInputStream::prefetch_registered) and report the scan covered,
//                 so the merge runs this child's reads as decode-only on its own thread;
//   UNCOVERED  -- report not covered, keeping the pre-split behavior (full read on the pool);
//   IO_ERROR   -- fail the prefetch with a distinctive status.
class PrefetchModeIterator final : public ChunkIterator {
public:
    enum class Mode { RESIDENT, UNCOVERED, IO_ERROR };

    constexpr static const char* kInjectedErrorMessage = "injected prefetch failure";

    PrefetchModeIterator(Schema schema, Datums rows, Mode mode, int64_t declared_bytes = 0)
            : ChunkIterator(schema),
              _inner(std::make_shared<VectorChunkIterator>(std::move(schema), std::move(rows))),
              _mode(mode),
              _declared_bytes(declared_bytes) {}

    void read_chunk_size(size_t n) { _inner->chunk_size(n); }

    StatusOr<bool> prefetch(std::atomic<int64_t>* budget) override {
        _prefetch_calls.fetch_add(1);
        switch (_mode) {
        case Mode::RESIDENT:
            if (_declared_bytes > 0) {
                // Reserve before "loading", refund on overdraw: the protocol
                // SharedBufferedInputStream::prefetch_registered follows.
                if (budget->fetch_sub(_declared_bytes) < _declared_bytes) {
                    budget->fetch_add(_declared_bytes);
                    return false;
                }
            }
            _covered.store(true);
            return true;
        case Mode::UNCOVERED:
            return false;
        case Mode::IO_ERROR:
            return Status::IOError(kInjectedErrorMessage);
        }
        return false;
    }

    int prefetch_calls() const { return _prefetch_calls.load(); }
    bool covered() const { return _covered.load(); }
    // How many rows get_next has handed out; zero means this child was never decoded.
    size_t rows_delivered() const { return _inner->next_row(); }

    void close() override { _inner->close(); }

protected:
    Status do_get_next(Chunk* chunk) override { return _inner->get_next(chunk); }
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override { return do_get_next(chunk); }

private:
    std::shared_ptr<VectorChunkIterator> _inner;
    Mode _mode;
    int64_t _declared_bytes;
    // Written on prefill pool threads, read on the test thread after the merge drains.
    std::atomic<int> _prefetch_calls{0};
    std::atomic<bool> _covered{false};
};

// NOLINTNEXTLINE
// The prefetch split must be invisible from the outside: whatever mix of covered, uncovered,
// budget-refused, and empty children the prefill sees, the merge must emit the same rows and
// source masks as the serial path with plain children.
class MergePrefetchEquivalenceTest : public MergePipelineEquivalenceTest {
protected:
    using Mode = PrefetchModeIterator::Mode;

    void SetUp() override {
        MergePipelineEquivalenceTest::SetUp();
        _saved_prefetch_bytes = config::compaction_parallel_merge_prefetch_bytes;
    }
    void TearDown() override {
        config::compaction_parallel_merge_prefetch_bytes = _saved_prefetch_bytes;
        MergePipelineEquivalenceTest::TearDown();
    }

    std::vector<ChunkIteratorPtr> make_prefetch_children(const std::vector<std::vector<int32_t>>& vs,
                                                         const std::vector<Mode>& modes, int64_t declared_bytes,
                                                         std::vector<std::shared_ptr<PrefetchModeIterator>>* typed) {
        EXPECT_EQ(vs.size(), modes.size());
        std::vector<ChunkIteratorPtr> subs;
        for (size_t i = 0; i < vs.size(); i++) {
            auto sub = std::make_shared<PrefetchModeIterator>(_schema, COL_INT(vs[i]), modes[i], declared_bytes);
            sub->read_chunk_size(7); // force many refills, as the plain fixtures do
            if (typed != nullptr) {
                typed->push_back(sub);
            }
            subs.push_back(sub);
        }
        return subs;
    }

    Output run_heap_prefetch(bool parallel, int buffers, const std::vector<Mode>& modes, int64_t declared_bytes,
                             std::vector<std::shared_ptr<PrefetchModeIterator>>* typed = nullptr) {
        config::enable_compaction_parallel_merge_init = parallel;
        config::compaction_merge_child_buffers = buffers;

        auto iter = new_heap_merge_iterator(make_prefetch_children(inputs(), modes, declared_bytes, typed));
        EXPECT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        return drain(iter);
    }

    Output run_mask_prefetch(bool parallel, int buffers, const std::vector<uint16_t>& sources,
                             const std::vector<Mode>& modes, int64_t declared_bytes) {
        config::enable_compaction_parallel_merge_init = parallel;
        config::compaction_merge_child_buffers = buffers;

        std::vector<RowSourceMask> masks;
        masks.reserve(sources.size());
        for (uint16_t s : sources) {
            masks.emplace_back(RowSourceMask(s, false));
        }
        RowSourceMaskBuffer mask_buffer(_next_mask_id++, config::storage_root_path);
        EXPECT_TRUE(mask_buffer.write(masks).ok());
        EXPECT_TRUE(mask_buffer.flush().ok());
        EXPECT_TRUE(mask_buffer.flip_to_read().ok());

        auto iter =
                new_mask_merge_iterator(make_prefetch_children(inputs(), modes, declared_bytes, nullptr), &mask_buffer);
        EXPECT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        return drain(iter);
    }

    // Serial baseline over an arbitrary input set, with plain children and the read-ahead off.
    Output run_heap_rows(const std::vector<std::vector<int32_t>>& vs) {
        config::enable_compaction_parallel_merge_init = false;
        config::compaction_merge_child_buffers = 1;

        std::vector<ChunkIteratorPtr> subs;
        for (const auto& v : vs) {
            auto sub = std::make_shared<VectorChunkIterator>(_schema, COL_INT(v));
            sub->chunk_size(7);
            subs.push_back(sub);
        }
        auto iter = new_heap_merge_iterator(subs);
        EXPECT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        return drain(iter);
    }

    std::vector<Mode> all_resident() const { return std::vector<Mode>(inputs().size(), Mode::RESIDENT); }

    std::vector<Mode> alternating() const {
        std::vector<Mode> modes(inputs().size(), Mode::UNCOVERED);
        for (size_t i = 0; i < modes.size(); i += 2) {
            modes[i] = Mode::RESIDENT;
        }
        return modes;
    }

    int64_t _saved_prefetch_bytes = 0;
};

// NOLINTNEXTLINE
TEST_F(MergePrefetchEquivalenceTest, heap_merge_prefetch_matches_serial) {
    const Output baseline = run_heap(false, 1);
    ASSERT_FALSE(baseline.rows.empty());
    ASSERT_TRUE(std::is_sorted(baseline.rows.begin(), baseline.rows.end()));

    for (int buffers : {1, 3}) {
        for (const auto& modes : {all_resident(), alternating()}) {
            const Output got = run_heap_prefetch(true, buffers, modes, 0);
            EXPECT_EQ(baseline.rows, got.rows) << "buffers=" << buffers;
            EXPECT_EQ(baseline.sources, got.sources) << "buffers=" << buffers;
        }
    }
}

// NOLINTNEXTLINE
TEST_F(MergePrefetchEquivalenceTest, mask_merge_prefetch_matches_serial) {
    // Vertical compaction feeds the mask merge the sources the key group produced, so build them
    // the same way.
    const std::vector<uint16_t> sources = run_heap(false, 1).sources;
    ASSERT_FALSE(sources.empty());

    const Output baseline = run_mask(false, 1, sources);
    ASSERT_FALSE(baseline.rows.empty());

    for (int buffers : {1, 3}) {
        for (const auto& modes : {all_resident(), alternating()}) {
            const Output got = run_mask_prefetch(true, buffers, sources, modes, 0);
            EXPECT_EQ(baseline.rows, got.rows) << "buffers=" << buffers;
        }
    }
}

// NOLINTNEXTLINE
// Every child asks to be resident but the budget only fits two of them (5 x 64 bytes against a
// 160-byte allowance); which two win the race is scheduling-dependent, the output must not be.
TEST_F(MergePrefetchEquivalenceTest, heap_merge_prefetch_budget_exhaustion_matches_serial) {
    const Output baseline = run_heap(false, 1);
    ASSERT_FALSE(baseline.rows.empty());

    config::compaction_parallel_merge_prefetch_bytes = 160;
    for (int buffers : {1, 3}) {
        std::vector<std::shared_ptr<PrefetchModeIterator>> children;
        const Output got = run_heap_prefetch(true, buffers, all_resident(), 64, &children);
        EXPECT_EQ(baseline.rows, got.rows) << "buffers=" << buffers;
        EXPECT_EQ(baseline.sources, got.sources) << "buffers=" << buffers;

        // The reserve-then-refund protocol admits exactly floor(160 / 64) = 2 children no matter
        // how the pool threads interleave; the rest fall back to the pre-split read.
        int covered = 0;
        for (const auto& child : children) {
            EXPECT_EQ(1, child->prefetch_calls());
            covered += child->covered() ? 1 : 0;
        }
        EXPECT_EQ(2, covered) << "buffers=" << buffers;
    }
}

// NOLINTNEXTLINE
// A failing prefetch must surface through get_next with the child's own error, at the same point
// in child order the serial path would surface a read error: children before it are committed,
// children after it are never decoded, and nothing crashes or hangs.
TEST_F(MergePrefetchEquivalenceTest, heap_merge_prefetch_error_surfaces_in_child_order) {
    config::enable_compaction_parallel_merge_init = true;
    config::compaction_merge_child_buffers = 3;

    auto modes = all_resident();
    modes[2] = Mode::IO_ERROR;
    std::vector<std::shared_ptr<PrefetchModeIterator>> children;
    auto iter = new_heap_merge_iterator(make_prefetch_children(inputs(), modes, 0, &children));
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    ChunkPtr chunk = ChunkFactory::new_chunk(iter->schema(), config::vector_chunk_size);
    Status st = iter->get_next(chunk.get());
    ASSERT_FALSE(st.ok());
    ASSERT_FALSE(st.is_end_of_file());
    ASSERT_TRUE(st.is_io_error()) << st;
    ASSERT_TRUE(st.message().find(PrefetchModeIterator::kInjectedErrorMessage) != std::string::npos) << st;

    // Every child's prefetch ran on the pool before the commits started.
    for (const auto& child : children) {
        EXPECT_EQ(1, child->prefetch_calls());
    }
    // Children before the failure were decoded and committed in child order...
    EXPECT_GT(children[0]->rows_delivered(), 0u);
    EXPECT_GT(children[1]->rows_delivered(), 0u);
    // ...the failing child never produced rows, and the commit loop stopped at it, so the
    // resident children behind it were never decoded either.
    EXPECT_EQ(0u, children[2]->rows_delivered());
    EXPECT_EQ(0u, children[3]->rows_delivered());
    EXPECT_EQ(0u, children[4]->rows_delivered());

    iter->close();
}

// NOLINTNEXTLINE
// A resident child can turn out empty: its decode-only slot-0 read hits end-of-file on the merge
// thread, and the commit must fold that into a clean child close exactly like the serial path.
TEST_F(MergePrefetchEquivalenceTest, heap_merge_prefetch_empty_child_matches_serial) {
    auto vs = inputs();
    vs.insert(vs.begin() + 2, std::vector<int32_t>{});

    const Output baseline = run_heap_rows(vs);
    ASSERT_FALSE(baseline.rows.empty());

    config::enable_compaction_parallel_merge_init = true;
    config::compaction_merge_child_buffers = 3;
    std::vector<Mode> modes(vs.size(), Mode::RESIDENT);
    auto iter = new_heap_merge_iterator(make_prefetch_children(vs, modes, 0, nullptr));
    ASSERT_TRUE(iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    const Output got = drain(iter);

    EXPECT_EQ(baseline.rows, got.rows);
    EXPECT_EQ(baseline.sources, got.sources);
}

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
