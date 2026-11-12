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

#include "storage/rowset/segment_prefetch_window.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk_factory.h"
#include "fs/fs_memory.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/storage_stats.h"
#include "types/datum.h"

namespace starrocks {
namespace {

// Records every warm-up asked for; each handle reports whether it is still held.
class RecordingStream final : public io::SeekableInputStream {
public:
    struct Warm {
        int64_t offset;
        size_t length;
        bool urgent;
        std::weak_ptr<io::CacheWarmHandle> handle;
    };

    StatusOr<int64_t> read(void*, int64_t) override { return 0; }
    Status skip(int64_t) override { return Status::OK(); }
    Status seek(int64_t) override { return Status::OK(); }
    StatusOr<int64_t> position() override { return 0; }
    StatusOr<int64_t> get_size() override { return 0; }

    StatusOr<std::shared_ptr<io::CacheWarmHandle>> warm_cache_async(int64_t offset, size_t length,
                                                                    bool urgent) override {
        if (!supported) {
            return Status::NotSupported("warm_cache_async");
        }
        auto handle = std::make_shared<Handle>();
        warms.push_back(Warm{offset, length, urgent, handle});
        return handle;
    }

    bool supported = true;
    std::vector<Warm> warms;

private:
    class Handle final : public io::CacheWarmHandle {
    public:
        size_t submitted_blocks() const override { return 0; }
        size_t pending_blocks() const override { return 0; }
    };
};

struct Page {
    rowid_t first;
    rowid_t last;
    PagePointer pointer;
};

class SegmentPrefetchWindowTest : public ::testing::Test {
protected:
    static constexpr size_t kRows = 200000;
    static constexpr size_t kBlockSize = 16 * 1024;

    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_OK(_fs->create_dir("/prefetch_window"));

        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_num_short_key_columns(1);
        auto* k = pb.add_column();
        k->set_unique_id(0);
        k->set_name("k");
        k->set_type("INT");
        k->set_is_key(true);
        k->set_is_nullable(false);
        k->set_length(4);
        k->set_index_length(4);
        k->set_aggregation("NONE");
        auto* v = pb.add_column();
        v->set_unique_id(1);
        v->set_name("v");
        v->set_type("BIGINT");
        v->set_is_key(false);
        v->set_is_nullable(false);
        v->set_length(8);
        v->set_aggregation("NONE");
        _schema = TabletSchema::create(pb);

        const std::string filename = "/prefetch_window/0.dat";
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));
        SegmentWriterOptions opts;
        SegmentWriter writer(std::move(wfile), 0, _schema, opts);
        ASSERT_OK(writer.init());
        auto chunk = ChunkFactory::new_chunk(ChunkHelper::convert_schema(_schema), kRows);
        uint64_t state = 1;
        for (size_t rid = 0; rid < kRows; ++rid) {
            // Values that do not compress, so that the column takes many pages.
            state = state * 6364136223846793005ULL + 1442695040888963407ULL;
            auto cols = chunk->columns();
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(rid)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int64_t>(state)));
        }
        ASSERT_OK(writer.append_chunk(*chunk));
        uint64_t file_size, index_size, footer_position;
        ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

        ASSIGN_OR_ABORT(_segment, Segment::open(_fs, FileInfo{filename}, 0, _schema));
        // Initializing a column iterator loads the ordinal index the window walks.
        ASSIGN_OR_ABORT(_iter, _segment->new_column_iterator(_schema->column(1), nullptr));
        ASSIGN_OR_ABORT(_rfile, _fs->new_random_access_file(filename));
        ColumnIteratorOptions iter_opts;
        iter_opts.stats = &_iter_stats;
        iter_opts.read_file = _rfile.get();
        ASSERT_OK(_iter->init(iter_opts));
        _reader = const_cast<ColumnReader*>(_segment->column(1));
        ASSERT_GT(_reader->num_data_pages(), 8);

        OrdinalPageIndexIterator it;
        ASSERT_OK(_reader->seek_to_first(&it));
        for (; it.valid(); it.next()) {
            _pages.push_back(
                    Page{static_cast<rowid_t>(it.first_ordinal()), static_cast<rowid_t>(it.last_ordinal()), it.page()});
        }
        ASSERT_EQ(_reader->num_data_pages(), _pages.size());
    }

    std::unique_ptr<SegmentPrefetchWindow> make_window(const SparseRange<>* range, rowid_t start_row,
                                                       size_t ahead_blocks, double late_ratio = 2.0,
                                                       int64_t object_offset = 0) {
        return std::make_unique<SegmentPrefetchWindow>(std::vector<SegmentPrefetchWindow::Column>{{_reader, &_stream}},
                                                       range, start_row, object_offset, ahead_blocks, late_ratio,
                                                       kBlockSize, &_stats);
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    TabletSchemaCSPtr _schema;
    std::shared_ptr<Segment> _segment;
    std::unique_ptr<ColumnIterator> _iter;
    std::unique_ptr<RandomAccessFile> _rfile;
    OlapReaderStatistics _iter_stats;
    ColumnReader* _reader = nullptr;
    std::vector<Page> _pages;
    RecordingStream _stream;
    OlapReaderStatistics _stats;
};

// The window asks for the pages in row order and stops once the reader has enough blocks ahead of it.
TEST_F(SegmentPrefetchWindowTest, advance_bounds_read_ahead) {
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 4);
    window->advance(0);

    ASSERT_FALSE(_stream.warms.empty());
    ASSERT_LT(_stream.warms.size(), _pages.size());
    EXPECT_GE(window->ahead_blocks(), 4u);
    for (size_t i = 0; i < _stream.warms.size(); ++i) {
        EXPECT_FALSE(_stream.warms[i].urgent);
        // Offsets are those of the segment file; a page sharing its first block with the previous page starts at
        // the next block.
        EXPECT_GE(_stream.warms[i].offset, static_cast<int64_t>(_pages[i].pointer.offset));
        EXPECT_EQ(static_cast<int64_t>(_pages[i].pointer.offset + _pages[i].pointer.size),
                  _stream.warms[i].offset + static_cast<int64_t>(_stream.warms[i].length));
    }
    EXPECT_EQ(_stream.warms.size(), _stats.remote_prefetch_ranges);

    // Topping up again without progress asks for nothing more.
    const size_t asked = _stream.warms.size();
    window->advance(0);
    EXPECT_EQ(asked, _stream.warms.size());
}

// Once the reader is past a page, its warm-up is withdrawn and the window moves on.
TEST_F(SegmentPrefetchWindowTest, advance_releases_consumed_pages) {
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 4);
    window->advance(0);
    const size_t asked = _stream.warms.size();
    ASSERT_FALSE(_stream.warms[0].handle.expired());

    window->advance(_pages[2].last + 1);
    EXPECT_TRUE(_stream.warms[0].handle.expired());
    EXPECT_TRUE(_stream.warms[1].handle.expired());
    EXPECT_GT(_stream.warms.size(), asked);

    window->advance(kRows);
    EXPECT_TRUE(window->exhausted());
    EXPECT_EQ(0u, window->ahead_blocks());
    window.reset();
    for (const auto& w : _stream.warms) {
        EXPECT_TRUE(w.handle.expired());
    }
}

// Only the pages holding rows of the scan range are warmed.
TEST_F(SegmentPrefetchWindowTest, scan_range_skips_pages) {
    const Page& a = _pages[1];
    const Page& b = _pages[6];
    SparseRange<> range;
    range.add(Range<>(a.first, a.first + 1));
    range.add(Range<>(b.first, b.first + 1));
    auto window = make_window(&range, 0, 1024);
    window->advance(0);

    ASSERT_EQ(2u, _stream.warms.size());
    EXPECT_EQ(static_cast<int64_t>(a.pointer.offset + a.pointer.size),
              _stream.warms[0].offset + static_cast<int64_t>(_stream.warms[0].length));
    EXPECT_EQ(static_cast<int64_t>(b.pointer.offset + b.pointer.size),
              _stream.warms[1].offset + static_cast<int64_t>(_stream.warms[1].length));
    EXPECT_TRUE(window->exhausted());
}

// A window created mid-scan (a context switch) starts at the reader's row.
TEST_F(SegmentPrefetchWindowTest, start_row_skips_earlier_pages) {
    SparseRange<> range(0, kRows);
    const Page& start = _pages[5];
    auto window = make_window(&range, start.first, 1024);
    window->advance(start.first);
    ASSERT_FALSE(_stream.warms.empty());
    EXPECT_EQ(static_cast<int64_t>(start.pointer.offset), _stream.warms[0].offset);
    EXPECT_EQ(_pages.size() - 5, _stream.warms.size());
}

// In a bundle file the blocks are counted where the segment sits in the object, but the stream is asked in segment
// offsets: the bundle stream adds its own base.
TEST_F(SegmentPrefetchWindowTest, object_offset_keeps_segment_offsets) {
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 1024, 2.0, 3 * kBlockSize + 100);
    window->advance(0);
    ASSERT_EQ(_pages.size(), _stream.warms.size());
    const Page& last = _pages.back();
    EXPECT_EQ(static_cast<int64_t>(last.pointer.offset + last.pointer.size),
              _stream.warms.back().offset + static_cast<int64_t>(_stream.warms.back().length));
    for (const auto& w : _stream.warms) {
        EXPECT_GE(w.offset, 0);
    }
}

// A stream that cannot warm (not a starlet file, or the scheduler is off) turns the window off for good.
TEST_F(SegmentPrefetchWindowTest, not_supported_disables_window) {
    _stream.supported = false;
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 4);
    window->advance(0);
    EXPECT_TRUE(window->disabled());
    EXPECT_TRUE(window->exhausted());
    EXPECT_EQ(0u, window->ahead_blocks());
    _stream.supported = true;
    window->advance(_pages[3].first);
    rowid_t row = 10;
    window->warm_rows({{_reader, &_stream}}, &row, 1);
    EXPECT_TRUE(_stream.warms.empty());
    EXPECT_EQ(0, _stats.remote_prefetch_ranges);
}

// The late-materialized rows of a chunk are warmed as a demand, contiguous pages in one range, and the previous
// batch is released.
TEST_F(SegmentPrefetchWindowTest, warm_rows_is_urgent_and_replaces_previous_batch) {
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 0);
    std::vector<rowid_t> rows{_pages[0].first, _pages[1].first, _pages[4].first};
    window->warm_rows({{_reader, &_stream}}, rows.data(), rows.size());
    ASSERT_EQ(2u, _stream.warms.size());
    EXPECT_TRUE(_stream.warms[0].urgent);
    EXPECT_EQ(static_cast<int64_t>(_pages[0].pointer.offset), _stream.warms[0].offset);
    EXPECT_EQ(static_cast<int64_t>(_pages[1].pointer.offset + _pages[1].pointer.size),
              _stream.warms[0].offset + static_cast<int64_t>(_stream.warms[0].length));
    EXPECT_EQ(static_cast<int64_t>(_pages[4].pointer.offset), _stream.warms[1].offset);

    std::vector<rowid_t> next{_pages[6].first};
    window->warm_rows({{_reader, &_stream}}, next.data(), next.size());
    EXPECT_TRUE(_stream.warms[0].handle.expired());
    EXPECT_TRUE(_stream.warms[1].handle.expired());
    EXPECT_FALSE(_stream.warms[2].handle.expired());
    EXPECT_FALSE(window->reads_ahead_late_columns());
}

// Late rows that touch most of the pages they span switch the late columns to read-ahead after enough batches.
TEST_F(SegmentPrefetchWindowTest, dense_late_rows_switch_to_read_ahead) {
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 4, 0.5);
    for (int batch = 0; batch < 4; ++batch) {
        EXPECT_FALSE(window->reads_ahead_late_columns());
        std::vector<rowid_t> rows{_pages[batch].first, _pages[batch].last};
        window->warm_rows({{_reader, &_stream}}, rows.data(), rows.size());
    }
    EXPECT_TRUE(window->reads_ahead_late_columns());
    EXPECT_EQ(1, _stats.remote_prefetch_late_read_ahead);
    const size_t asked = _stream.warms.size();
    window->advance(_pages[4].first);
    EXPECT_GT(_stream.warms.size(), asked);
    EXPECT_FALSE(_stream.warms.back().urgent);
}

// Sparse late rows, or a ratio above 1, never switch.
TEST_F(SegmentPrefetchWindowTest, sparse_late_rows_stay_on_demand) {
    SparseRange<> range(0, kRows);
    auto sparse = make_window(&range, 0, 4, 0.5);
    auto never = make_window(&range, 0, 4, 1.5);
    for (int batch = 0; batch < 8; ++batch) {
        std::vector<rowid_t> rows{_pages[0].first, _pages.back().first};
        sparse->warm_rows({{_reader, &_stream}}, rows.data(), rows.size());
        std::vector<rowid_t> dense{_pages[batch].first};
        never->warm_rows({{_reader, &_stream}}, dense.data(), dense.size());
    }
    EXPECT_FALSE(sparse->reads_ahead_late_columns());
    EXPECT_FALSE(never->reads_ahead_late_columns());
    EXPECT_EQ(0, _stats.remote_prefetch_late_read_ahead);
}

} // namespace
} // namespace starrocks
