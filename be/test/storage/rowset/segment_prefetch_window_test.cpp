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

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "common/config_lake_fwd.h"
#include "fs/fs_memory.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/read_ahead_budget.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/storage_metrics.h"
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
                                                       int64_t object_offset = 0, size_t block_size = kBlockSize) {
        return std::make_unique<SegmentPrefetchWindow>(
                std::vector<SegmentPrefetchWindow::Column>{{_reader, &_stream, object_offset}}, range, start_row,
                ahead_blocks, late_ratio, block_size, &_stats, ReadAheadBudget::instance());
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
    // Exactly the window: a page larger than what is left of it is asked for in part.
    EXPECT_EQ(4u, window->ahead_blocks());
    EXPECT_EQ(static_cast<int64_t>(_pages[0].pointer.offset), _stream.warms[0].offset);
    int64_t prev_end = 0;
    size_t blocks = 0;
    for (const auto& w : _stream.warms) {
        EXPECT_FALSE(w.urgent);
        // Offsets are those of the segment file, in file order; a page sharing its first block with the previous
        // page starts at the next block.
        EXPECT_GE(w.offset, prev_end);
        prev_end = w.offset + static_cast<int64_t>(w.length);
        blocks += static_cast<size_t>((prev_end - 1) / kBlockSize - w.offset / static_cast<int64_t>(kBlockSize) + 1);
    }
    EXPECT_EQ(4u, blocks);
    EXPECT_EQ(_stream.warms.size(), _stats.remote_prefetch_ranges);

    // Topping up again without progress asks for nothing more.
    const size_t asked = _stream.warms.size();
    window->advance(0);
    EXPECT_EQ(asked, _stream.warms.size());
}

// The windows of a node together hold no more blocks ahead than lake_scan_read_ahead_max_blocks; a window held back
// asks for the rest once another one has released its blocks.
TEST_F(SegmentPrefetchWindowTest, node_limit_bounds_all_windows) {
    auto& node = StorageMetrics::instance()->lake_scan_read_ahead_blocks;
    auto& capped = StorageMetrics::instance()->lake_scan_read_ahead_capped_total;
    const int64_t base = node.value();
    const int64_t old_limit = config::lake_scan_read_ahead_max_blocks;
    DeferOp restore([&] { config::lake_scan_read_ahead_max_blocks = old_limit; });
    config::lake_scan_read_ahead_max_blocks = base + 6;

    SparseRange<> range(0, kRows);
    auto first = make_window(&range, 0, 4);
    first->advance(0);
    EXPECT_EQ(4u, first->ahead_blocks());
    EXPECT_EQ(base + 4, node.value());
    EXPECT_TRUE(ReadAheadBudget::instance()->has_room());

    // The second window gets only what is left of the node's blocks.
    const int64_t capped_before = capped.value();
    auto second = make_window(&range, 0, 4);
    second->advance(0);
    EXPECT_EQ(2u, second->ahead_blocks());
    EXPECT_EQ(base + 6, node.value());
    EXPECT_GT(capped.value(), capped_before);
    EXPECT_FALSE(ReadAheadBudget::instance()->has_room());

    first.reset();
    EXPECT_EQ(base + 2, node.value());
    second->advance(0);
    EXPECT_EQ(4u, second->ahead_blocks());
    EXPECT_EQ(base + 4, node.value());
    second.reset();
    EXPECT_EQ(base, node.value());
}

// A window that stops warming gives its blocks back to the node.
TEST_F(SegmentPrefetchWindowTest, disabled_window_releases_node_blocks) {
    auto& node = StorageMetrics::instance()->lake_scan_read_ahead_blocks;
    const int64_t base = node.value();
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 64);
    window->advance(0);
    // Several pages ahead, so that passing the first one leaves blocks held.
    ASSERT_GT(_stream.warms.size(), 2u);
    EXPECT_EQ(base + 64, node.value());
    auto& windows = StorageMetrics::instance()->lake_scan_read_ahead_windows;
    const int64_t reading = windows.value();
    _stream.supported = false;
    window->advance(_pages[1].first);
    EXPECT_TRUE(window->disabled());
    EXPECT_EQ(0u, window->ahead_blocks());
    EXPECT_EQ(base, node.value());
    // It no longer takes a share of the node limit, and is not counted twice when it goes away.
    EXPECT_EQ(reading - 1, windows.value());
    window.reset();
    EXPECT_EQ(reading - 1, windows.value());
}

// The segment readers that read ahead share lake_scan_read_ahead_max_blocks equally, whichever asks first; one that reads
// ahead alone keeps its own window again.
TEST_F(SegmentPrefetchWindowTest, node_limit_is_shared_equally) {
    auto& node = StorageMetrics::instance()->lake_scan_read_ahead_blocks;
    auto& windows = StorageMetrics::instance()->lake_scan_read_ahead_windows;
    auto& capped = StorageMetrics::instance()->lake_scan_read_ahead_capped_total;
    ASSERT_EQ(0, node.value());
    ASSERT_EQ(0, windows.value());
    const int64_t old_limit = config::lake_scan_read_ahead_max_blocks;
    DeferOp restore([&] { config::lake_scan_read_ahead_max_blocks = old_limit; });
    config::lake_scan_read_ahead_max_blocks = 8;

    SparseRange<> range(0, kRows);
    auto first = make_window(&range, 0, 8);
    auto second = make_window(&range, 0, 8);
    EXPECT_EQ(2, windows.value());
    // The first to ask takes its share, not all the node has left.
    const int64_t capped_before = capped.value();
    first->advance(0);
    EXPECT_EQ(4u, first->ahead_blocks());
    EXPECT_GT(capped.value(), capped_before);
    second->advance(0);
    EXPECT_EQ(4u, second->ahead_blocks());
    EXPECT_EQ(8, node.value());

    second.reset();
    EXPECT_EQ(1, windows.value());
    EXPECT_EQ(4, node.value());
    first->advance(0);
    EXPECT_EQ(8u, first->ahead_blocks());
    first.reset();
    EXPECT_EQ(0, windows.value());
    EXPECT_EQ(0, node.value());
}

// A window without a budget (compaction) keeps only its own bound: it neither joins the node budget nor takes blocks
// from it, even while the node is full.
TEST_F(SegmentPrefetchWindowTest, window_without_budget_stays_out_of_node_limit) {
    auto& node = StorageMetrics::instance()->lake_scan_read_ahead_blocks;
    auto& windows = StorageMetrics::instance()->lake_scan_read_ahead_windows;
    ASSERT_EQ(0, node.value());
    ASSERT_EQ(0, windows.value());
    const int64_t old_limit = config::lake_scan_read_ahead_max_blocks;
    DeferOp restore([&] { config::lake_scan_read_ahead_max_blocks = old_limit; });
    config::lake_scan_read_ahead_max_blocks = 4;

    SparseRange<> range(0, kRows);
    auto query = make_window(&range, 0, 8);
    query->advance(0);
    EXPECT_EQ(4u, query->ahead_blocks());
    EXPECT_FALSE(ReadAheadBudget::instance()->has_room());

    SegmentPrefetchWindow compaction({{_reader, &_stream, 0}}, &range, 0, 8, 2.0, kBlockSize, &_stats, nullptr);
    compaction.advance(0);
    EXPECT_EQ(8u, compaction.ahead_blocks());
    EXPECT_EQ(4, node.value());
    EXPECT_EQ(1, windows.value());
    query.reset();
    EXPECT_EQ(0, node.value());
    EXPECT_EQ(0, windows.value());
}

// A grant gives its blocks back when released, moved over, or destroyed.
TEST(ReadAheadBudgetTest, grant_gives_back_its_blocks) {
    const int64_t old_limit = config::lake_scan_read_ahead_max_blocks;
    DeferOp restore([&] { config::lake_scan_read_ahead_max_blocks = old_limit; });
    config::lake_scan_read_ahead_max_blocks = 10;
    ReadAheadBudget budget;
    {
        auto member = budget.join();
        auto grant = budget.acquire(6, 0, 100);
        EXPECT_EQ(6u, grant.blocks());
        // No more than the node has left.
        auto rest = budget.acquire(6, 6, 100);
        EXPECT_EQ(4u, rest.blocks());
        EXPECT_EQ(0u, budget.acquire(1, 10, 100).blocks());
        EXPECT_FALSE(budget.has_room());
        // Moving a grant over another gives the overwritten blocks back.
        rest = std::move(grant);
        EXPECT_EQ(0u, grant.blocks());
        EXPECT_EQ(6, budget.held_blocks());
        rest.release();
        EXPECT_EQ(0, budget.held_blocks());
        EXPECT_EQ(1, budget.members());
    }
    EXPECT_EQ(0, budget.held_blocks());
    EXPECT_EQ(0, budget.members());
}

// Concurrent members never hold more than the limit together, nor more than their share each.
TEST(ReadAheadBudgetTest, concurrent_grants_stay_within_limit) {
    const int64_t old_limit = config::lake_scan_read_ahead_max_blocks;
    DeferOp restore([&] { config::lake_scan_read_ahead_max_blocks = old_limit; });
    constexpr int kThreads = 8;
    constexpr int64_t kLimit = 64;
    config::lake_scan_read_ahead_max_blocks = kLimit;
    ReadAheadBudget budget;
    std::vector<ReadAheadBudget::Member> members;
    for (int i = 0; i < kThreads; ++i) {
        members.push_back(budget.join());
    }
    std::atomic<bool> over{false};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            std::vector<ReadAheadBudget::Grant> held;
            size_t blocks = 0;
            for (int i = 0; i < 20000; ++i) {
                auto grant = budget.acquire(3, blocks, 1000);
                blocks += grant.blocks();
                if (grant.blocks() > 0) {
                    held.push_back(std::move(grant));
                }
                if (budget.held_blocks() > kLimit || blocks > static_cast<size_t>(kLimit / kThreads)) {
                    over = true;
                }
                if ((i + t) % 3 == 0 && !held.empty()) {
                    blocks -= held.back().blocks();
                    held.pop_back();
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_FALSE(over.load());
    EXPECT_EQ(0, budget.held_blocks());
}

// Once the reader is past a page, its warm-up is withdrawn and the window moves on.
TEST_F(SegmentPrefetchWindowTest, advance_releases_consumed_pages) {
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 4);
    window->advance(0);
    const size_t asked = _stream.warms.size();
    ASSERT_LT(asked, _pages.size());
    ASSERT_FALSE(_stream.warms[0].handle.expired());

    // Every page asked for so far ends before the reader's new row: all of them are released, and the window asks
    // for pages further on (skipping what was left of a page asked for in part).
    const int64_t asked_end = _stream.warms.back().offset + static_cast<int64_t>(_stream.warms.back().length);
    size_t next_page = 0;
    while (next_page < _pages.size() && static_cast<int64_t>(_pages[next_page].pointer.offset) < asked_end) {
        next_page++;
    }
    ASSERT_LT(next_page, _pages.size());
    const rowid_t past = _pages[next_page].first;
    window->advance(past);
    for (size_t i = 0; i < asked; ++i) {
        EXPECT_TRUE(_stream.warms[i].handle.expired()) << i;
    }
    EXPECT_GT(_stream.warms.size(), asked);
    EXPECT_FALSE(_stream.warms.back().handle.expired());

    window->advance(kRows);
    EXPECT_TRUE(window->exhausted());
    EXPECT_EQ(0u, window->ahead_blocks());
    window.reset();
    for (const auto& w : _stream.warms) {
        EXPECT_TRUE(w.handle.expired());
    }
}

// A page larger than the window is asked for a window at a time; the reader passing it drops the rest.
TEST_F(SegmentPrefetchWindowTest, page_larger_than_window_is_asked_in_part) {
    constexpr size_t kSmallBlock = 1024;
    const Page& page = _pages[0];
    ASSERT_GT(page.pointer.size, 4 * kSmallBlock);
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 1, 2.0, 0, kSmallBlock);
    window->advance(0);
    ASSERT_EQ(1u, _stream.warms.size());
    EXPECT_EQ(static_cast<int64_t>(page.pointer.offset), _stream.warms[0].offset);
    EXPECT_EQ(kSmallBlock - page.pointer.offset % kSmallBlock, _stream.warms[0].length);
    EXPECT_EQ(1u, window->ahead_blocks());
    // Still on the page: nothing more until the reader releases a block.
    window->advance(page.last);
    EXPECT_EQ(1u, _stream.warms.size());

    // Past the page: its warm-up is released and the window moves to the next page, a block of it.
    window->advance(_pages[1].first);
    EXPECT_TRUE(_stream.warms[0].handle.expired());
    ASSERT_EQ(2u, _stream.warms.size());
    EXPECT_EQ(static_cast<int64_t>(_pages[1].pointer.offset), _stream.warms[1].offset);
    EXPECT_EQ(1u, window->ahead_blocks());
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

// A window created past the end of its scan range has nothing to warm.
TEST_F(SegmentPrefetchWindowTest, start_row_past_scan_range_is_exhausted) {
    SparseRange<> range(0, _pages[2].first);
    auto window = make_window(&range, _pages[3].first, 1024);
    EXPECT_TRUE(window->exhausted());
    window->advance(_pages[3].first);
    EXPECT_TRUE(_stream.warms.empty());
}

// Pages sharing one cache block are warmed once, and the warm-up stays until the reader is past the last of them.
TEST_F(SegmentPrefetchWindowTest, pages_in_one_block_share_a_warm_up) {
    SparseRange<> range(0, kRows);
    auto window = make_window(&range, 0, 1024, 2.0, 0, 1L << 30);
    window->advance(0);
    ASSERT_EQ(1u, _stream.warms.size());
    EXPECT_EQ(1u, window->ahead_blocks());
    EXPECT_TRUE(window->exhausted());

    window->advance(_pages.back().first);
    EXPECT_FALSE(_stream.warms[0].handle.expired());
    window->advance(kRows);
    EXPECT_TRUE(_stream.warms[0].handle.expired());
    EXPECT_EQ(0u, window->ahead_blocks());
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

// Each column counts blocks at its own file's offset in its object: a delta column group overlay reads a file of
// its own, not the bundled segment's.
TEST_F(SegmentPrefetchWindowTest, columns_keep_their_own_object_offsets) {
    constexpr int64_t kBigBlock = 1L << 30;
    SparseRange<> range(0, kRows);
    // Page 3 of the second column crosses into the next block; the first column's pages all fit in block 0.
    const int64_t crossing = kBigBlock - static_cast<int64_t>(_pages[3].pointer.offset) - 1;
    SegmentPrefetchWindow window({{_reader, &_stream, 0}, {_reader, &_stream, crossing}}, &range, 0, 1024, 2.0,
                                 kBigBlock, &_stats, ReadAheadBudget::instance());
    window.advance(0);
    ASSERT_EQ(3u, _stream.warms.size());
    size_t from_next_block = 0;
    for (const auto& w : _stream.warms) {
        if (w.offset == static_cast<int64_t>(_pages[3].pointer.offset) + 1) {
            from_next_block++;
        }
    }
    EXPECT_EQ(1u, from_next_block);
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

// Late rows on a stream that cannot warm turn the window off, whether the failing range is followed by more pages
// of the batch or is its last one.
TEST_F(SegmentPrefetchWindowTest, warm_rows_not_supported_disables_window) {
    _stream.supported = false;
    SparseRange<> range(0, kRows);
    auto gapped = make_window(&range, 0, 0);
    std::vector<rowid_t> rows{_pages[0].first, _pages[4].first};
    gapped->warm_rows({{_reader, &_stream}}, rows.data(), rows.size());
    EXPECT_TRUE(gapped->disabled());

    auto single = make_window(&range, 0, 0);
    rowid_t row = _pages[2].first;
    single->warm_rows({{_reader, &_stream}}, &row, 1);
    EXPECT_TRUE(single->disabled());
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
    // One row on a page far from the previous batch's: each batch alone spans one page, but the batches between
    // them (with no row left) span the gap, so 4 touched pages out of 9 stay below the ratio.
    auto gapped = make_window(&range, 0, 4, 0.5);
    for (int page : {0, 3, 6, 8}) {
        rowid_t row = _pages[page].first;
        gapped->warm_rows({{_reader, &_stream}}, &row, 1);
    }
    // Several batches on one page count it once: 2 pages touched out of 7.
    auto repeated = make_window(&range, 0, 4, 0.5);
    for (rowid_t row : {_pages[0].first, _pages[0].first + 1, _pages[0].first + 2, _pages[6].first}) {
        repeated->warm_rows({{_reader, &_stream}}, &row, 1);
    }
    auto never = make_window(&range, 0, 4, 1.5);
    for (int batch = 0; batch < 8; ++batch) {
        std::vector<rowid_t> dense{_pages[batch].first};
        never->warm_rows({{_reader, &_stream}}, dense.data(), dense.size());
    }
    EXPECT_FALSE(gapped->reads_ahead_late_columns());
    EXPECT_FALSE(repeated->reads_ahead_late_columns());
    EXPECT_FALSE(never->reads_ahead_late_columns());
    EXPECT_EQ(0, _stats.remote_prefetch_late_read_ahead);
}

} // namespace
} // namespace starrocks
