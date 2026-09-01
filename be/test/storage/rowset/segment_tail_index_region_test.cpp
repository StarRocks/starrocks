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

// Covers config::enable_segment_tail_index_region: the segment writer gathers every column's
// ordinal index and page zone map into one contiguous run immediately before the footer, instead
// of interleaving each column's indexes after that column's own data pages.
//
// The two layouts must be indistinguishable to a reader -- every index is located through an
// absolute PagePointer either way -- so each test that asserts something about the layout also
// reads the whole segment back and checks the values.

#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "common/config_exec_fwd.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_memory.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage_primitive/chunk_iterator.h"

namespace starrocks {

namespace {

// Enough rows that every column spans several 64KB data pages. With a single page per column the
// ordinal index degenerates to `is_root_data_page` -- no index page is written at all -- and every
// layout assertion below would pass vacuously.
constexpr size_t kNumRows = 100000;

struct WriteResult {
    SegmentFooterPB footer;
    uint64_t file_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
};

// Every byte range the read path must load before it can touch a data page: the ordinal index of
// each column, and the page zone map of each column that has one. Reported as offsets in the same
// space as the rest of the footer's PagePointers.
std::vector<uint64_t> collect_small_index_offsets(const SegmentFooterPB& footer) {
    std::vector<uint64_t> offsets;
    for (const auto& column : footer.columns()) {
        for (const auto& index : column.indexes()) {
            if (index.type() == ORDINAL_INDEX && index.ordinal_index().has_root_page()) {
                offsets.push_back(index.ordinal_index().root_page().root_page().offset());
            } else if (index.type() == ZONE_MAP_INDEX && index.zone_map_index().has_page_zone_maps()) {
                const auto& zone_maps = index.zone_map_index().page_zone_maps();
                if (zone_maps.has_ordinal_index_meta()) {
                    offsets.push_back(zone_maps.ordinal_index_meta().root_page().offset());
                }
                if (zone_maps.has_value_index_meta()) {
                    offsets.push_back(zone_maps.value_index_meta().root_page().offset());
                }
            }
        }
    }
    return offsets;
}

uint64_t span_of(const std::vector<uint64_t>& offsets) {
    auto [min_it, max_it] = std::minmax_element(offsets.begin(), offsets.end());
    return *max_it - *min_it;
}

} // namespace

class SegmentTailIndexRegionTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kSegmentDir).ok());
        _saved_region = config::enable_segment_tail_index_region;
        _saved_shared_stream = config::enable_segment_shared_small_index_stream;
        _saved_data_page_prefetch = config::enable_segment_data_page_concurrent_prefetch;
        _saved_data_page_prefetch_concurrency = config::segment_data_page_prefetch_concurrency;
        _saved_segment_lookahead = config::segment_iterator_lookahead;
    }

    void TearDown() override {
        config::enable_segment_tail_index_region = _saved_region;
        config::enable_segment_shared_small_index_stream = _saved_shared_stream;
        config::enable_segment_data_page_concurrent_prefetch = _saved_data_page_prefetch;
        config::segment_data_page_prefetch_concurrency = _saved_data_page_prefetch_concurrency;
        config::segment_iterator_lookahead = _saved_segment_lookahead;
    }

    static std::shared_ptr<TabletSchema> make_schema() {
        return TabletSchemaHelper::create_tablet_schema(
                {create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(3), create_int_value_pb(4)});
    }

    // Row i holds (i, i+1, i+2, i+3) so read-back can be checked without a side table.
    void append_rows(SegmentWriter* writer, const TabletSchemaCSPtr& tablet_schema,
                     const std::vector<uint32_t>& column_indexes) {
        auto schema = ChunkHelper::convert_schema(tablet_schema, column_indexes);
        const int32_t chunk_size = config::vector_chunk_size;
        auto chunk = ChunkFactory::new_chunk(schema, chunk_size);
        for (size_t base = 0; base < kNumRows; base += chunk_size) {
            chunk->reset();
            auto cols = chunk->columns();
            for (int32_t j = 0; j < chunk_size && base + j < kNumRows; ++j) {
                const auto row = static_cast<int32_t>(base + j);
                for (size_t c = 0; c < column_indexes.size(); ++c) {
                    cols[c]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(row + column_indexes[c])));
                }
            }
            ASSERT_OK(writer->append_chunk(*chunk));
        }
    }

    // Horizontal write: one column group covering every column, a single finalize_columns().
    WriteResult write_horizontal(const std::string& file_name, const TabletSchemaCSPtr& tablet_schema) {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        SegmentWriterOptions opts;
        opts.num_rows_per_block = 10;
        SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);
        CHECK_OK(writer.init());
        append_rows(&writer, tablet_schema, {0, 1, 2, 3});

        WriteResult result;
        CHECK_OK(writer.finalize(&result.file_size, &result.index_size, &result.footer_position));
        result.footer = read_footer(file_name);
        return result;
    }

    // Vertical write: one column group at a time, finalize_columns() per group, one shared footer.
    WriteResult write_vertical(const std::string& file_name, const TabletSchemaCSPtr& tablet_schema) {
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
        SegmentWriterOptions opts;
        opts.num_rows_per_block = 10;
        SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

        WriteResult result;
        const std::vector<std::vector<uint32_t>> groups = {{0, 1}, {2}, {3}};
        for (size_t g = 0; g < groups.size(); ++g) {
            CHECK_OK(writer.init(groups[g], /*has_key=*/g == 0));
            append_rows(&writer, tablet_schema, groups[g]);
            CHECK_OK(writer.finalize_columns(&result.index_size));
        }
        CHECK_OK(writer.finalize_footer(&result.file_size, &result.footer_position));
        result.footer = read_footer(file_name);
        return result;
    }

    SegmentFooterPB read_footer(const std::string& file_name) {
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file_name));
        SegmentFooterPB footer;
        size_t footer_length_hint = 16 * 1024;
        CHECK_OK(Segment::parse_segment_footer(rfile.get(), &footer, &footer_length_hint, nullptr).status());
        return footer;
    }

    // Full scan of the segment, asserting the (i, i+1, i+2, i+3) pattern on every row.
    void verify_all_rows(const std::string& file_name, const TabletSchemaCSPtr& tablet_schema) {
        ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema));
        ASSERT_EQ(kNumRows, segment->num_rows());

        SegmentReadOptions seg_options;
        seg_options.fs = _fs;
        OlapReaderStatistics stats;
        seg_options.stats = &stats;
        auto schema = ChunkHelper::convert_schema(tablet_schema);
        ASSIGN_OR_ABORT(auto iter, segment->new_iterator(schema, seg_options));

        auto chunk = ChunkFactory::new_chunk(schema, config::vector_chunk_size);
        size_t count = 0;
        while (true) {
            chunk->reset();
            auto st = iter->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                for (int32_t c = 0; c < 4; ++c) {
                    ASSERT_EQ(static_cast<int32_t>(count) + c, chunk->get(i)[c].get_int32());
                }
                ++count;
            }
        }
        ASSERT_EQ(kNumRows, count);
    }

    const std::string kSegmentDir = "/segment_tail_index_region_test";
    std::shared_ptr<MemoryFileSystem> _fs;
    bool _saved_region = false;
    bool _saved_shared_stream = false;
    bool _saved_data_page_prefetch = false;
    int32_t _saved_data_page_prefetch_concurrency = 0;
    int32_t _saved_segment_lookahead = 0;
};

// With the region on, every small index sits between the last data page and the footer, and the
// declared range covers exactly that gap.
TEST_F(SegmentTailIndexRegionTest, RegionCoversEverySmallIndex) {
    auto tablet_schema = make_schema();
    config::enable_segment_tail_index_region = true;

    const std::string file_name = kSegmentDir + "/region_on";
    auto result = write_horizontal(file_name, tablet_schema);

    ASSERT_TRUE(result.footer.has_small_index_region_offset());
    ASSERT_TRUE(result.footer.has_small_index_region_size());
    const uint64_t region_begin = result.footer.small_index_region_offset();
    const uint64_t region_end = region_begin + result.footer.small_index_region_size();

    // The region is closed after the short key index, right where the footer starts.
    EXPECT_EQ(result.footer_position, region_end);

    auto offsets = collect_small_index_offsets(result.footer);
    ASSERT_FALSE(offsets.empty()) << "no ordinal/zone map index was written -- assertions would be vacuous";
    for (uint64_t offset : offsets) {
        EXPECT_GE(offset, region_begin);
        EXPECT_LT(offset, region_end);
    }

    // The short key index is on the same critical path, so it belongs to the region too.
    ASSERT_TRUE(result.footer.has_short_key_index_page());
    EXPECT_GE(result.footer.short_key_index_page().offset(), region_begin);
    EXPECT_LT(result.footer.short_key_index_page().offset(), region_end);

    // Data comes first and the region is a tail: region_begin is past the start of the file,
    // and region_end is the footer (asserted above), so nothing follows it.
    //
    // Deliberately not a size ratio. This fixture sets num_rows_per_block to 10, so 100k rows
    // give every column 10k pages; the ordinal index and the 10k-entry short key index then
    // outweigh four columns of ints by construction. The region measures 257906 bytes of a
    // 270482-byte file here, which says nothing about the layout and everything about the page
    // size the test chose.
    EXPECT_GT(region_begin, 0u);

    verify_all_rows(file_name, tablet_schema);
}

// A narrow scan has only one segment-file handle and often only one cache block. It cannot
// overlap page fills within the segment, but its one fill can overlap the fills of other
// segments prepared by UnionIterator.
TEST_F(SegmentTailIndexRegionTest, SingleTaskDataPagePrefetchRequiresLookaheadPrepare) {
    auto tablet_schema = make_schema();
    config::enable_segment_tail_index_region = true;
    config::enable_segment_data_page_concurrent_prefetch = true;
    config::segment_data_page_prefetch_concurrency = 8;
    config::segment_iterator_lookahead = 2;

    const std::string file_name = kSegmentDir + "/single_task_data_page_prefetch";
    write_horizontal(file_name, tablet_schema);
    ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema));
    auto read_schema = ChunkHelper::convert_schema(tablet_schema, {0});

    auto read_once = [&](bool prepare, OlapReaderStatistics* stats) {
        SegmentReadOptions options;
        options.fs = _fs;
        options.stats = stats;
        ASSIGN_OR_ABORT(auto iter, segment->new_iterator(read_schema, options));
        if (prepare) {
            ASSERT_OK(iter->prepare_for_read());
        }
        auto chunk = ChunkFactory::new_chunk(read_schema, config::vector_chunk_size);
        ASSERT_OK(iter->get_next(chunk.get()));
        ASSERT_GT(chunk->num_rows(), 0);
    };

    OlapReaderStatistics direct_stats;
    read_once(false, &direct_stats);
    EXPECT_EQ(0, direct_stats.data_page_prefetch_segments);
    EXPECT_EQ(0, direct_stats.data_page_prefetch_tasks);

    OlapReaderStatistics prepared_stats;
    read_once(true, &prepared_stats);
    EXPECT_EQ(1, prepared_stats.data_page_prefetch_segments);
    EXPECT_EQ(1, prepared_stats.data_page_prefetch_tasks);
    EXPECT_EQ(1, prepared_stats.data_page_prefetch_lookahead_segments);
    EXPECT_GT(prepared_stats.data_page_prefetch_blocks, 0);
}

// With the region off nothing changes: no footer fields, and the small indexes stay spread across
// the file instead of clustering at the tail.
TEST_F(SegmentTailIndexRegionTest, LegacyLayoutIsUnchanged) {
    auto tablet_schema = make_schema();

    config::enable_segment_tail_index_region = false;
    const std::string legacy_file = kSegmentDir + "/region_off";
    auto legacy = write_horizontal(legacy_file, tablet_schema);

    EXPECT_FALSE(legacy.footer.has_small_index_region_offset());
    EXPECT_FALSE(legacy.footer.has_small_index_region_size());

    config::enable_segment_tail_index_region = true;
    const std::string region_file = kSegmentDir + "/region_on_for_span";
    auto region = write_horizontal(region_file, tablet_schema);

    // The whole point of the change: gathered indexes span a fraction of what scattered ones do.
    auto legacy_offsets = collect_small_index_offsets(legacy.footer);
    auto region_offsets = collect_small_index_offsets(region.footer);
    ASSERT_EQ(legacy_offsets.size(), region_offsets.size());
    ASSERT_FALSE(legacy_offsets.empty());
    EXPECT_GT(span_of(legacy_offsets), 2 * span_of(region_offsets));

    // Same bytes accounted for as index either way, and the same data read back.
    EXPECT_EQ(legacy.index_size, region.index_size);
    verify_all_rows(legacy_file, tablet_schema);
    verify_all_rows(region_file, tablet_schema);
}

// A vertical writer finalizes one column group at a time, so an early group's indexes reach the
// tail only by surviving until the last group's data is on disk. This matters well beyond
// compaction ergonomics: CompactionUtils::choose_compaction_algorithm picks VERTICAL for any table
// wider than vertical_compaction_max_columns_per_group (5) with more than one source rowset, so if
// the vertical path fell back to the legacy layout the region would disappear from essentially
// every wide table at its first real compaction.
TEST_F(SegmentTailIndexRegionTest, VerticalWriteAlsoProducesRegion) {
    auto tablet_schema = make_schema();
    config::enable_segment_tail_index_region = true;

    const std::string file_name = kSegmentDir + "/vertical_region";
    auto result = write_vertical(file_name, tablet_schema);

    ASSERT_TRUE(result.footer.has_small_index_region_offset());
    ASSERT_TRUE(result.footer.has_small_index_region_size());
    const uint64_t region_begin = result.footer.small_index_region_offset();
    const uint64_t region_end = region_begin + result.footer.small_index_region_size();
    EXPECT_EQ(result.footer_position, region_end);

    // Every column, including those from the FIRST group, must have landed in the tail region --
    // that is the whole point, and the case a per-group write would get wrong.
    auto offsets = collect_small_index_offsets(result.footer);
    ASSERT_FALSE(offsets.empty());
    for (uint64_t offset : offsets) {
        EXPECT_GE(offset, region_begin);
        EXPECT_LT(offset, region_end);
    }

    // The short key index belongs to the region too. _has_key is reassigned by every init() and
    // the LAST vertical group holds value columns, so a naive `if (_has_key)` at region-write time
    // skips it entirely and leaves the footer with no short_key_index_page.
    ASSERT_TRUE(result.footer.has_short_key_index_page());
    EXPECT_GE(result.footer.short_key_index_page().offset(), region_begin);
    EXPECT_LT(result.footer.short_key_index_page().offset(), region_end);

    verify_all_rows(file_name, tablet_schema);
}

// The vertical and horizontal writers must agree: same rows in, same layout guarantees out.
TEST_F(SegmentTailIndexRegionTest, VerticalAndHorizontalRegionsAgree) {
    auto tablet_schema = make_schema();
    config::enable_segment_tail_index_region = true;

    const std::string h_file = kSegmentDir + "/agree_horizontal";
    const std::string v_file = kSegmentDir + "/agree_vertical";
    auto h = write_horizontal(h_file, tablet_schema);
    auto v = write_vertical(v_file, tablet_schema);

    EXPECT_EQ(collect_small_index_offsets(h.footer).size(), collect_small_index_offsets(v.footer).size());
    EXPECT_EQ(h.footer_position, h.footer.small_index_region_offset() + h.footer.small_index_region_size());
    EXPECT_EQ(v.footer_position, v.footer.small_index_region_offset() + v.footer.small_index_region_size());

    verify_all_rows(h_file, tablet_schema);
    verify_all_rows(v_file, tablet_schema);
}

// The prefetch is worth paying for only when the region reaches past the file's last cache block.
// The footer is at the very end and is always read first, and a block cache serves that read by
// fetching the whole block -- so a region sitting inside it is already warm, and walking it again
// is pure cost. A 17-column segment's region is ~74 KB and always lands there; a 105-column one is
// ~1.8 MB and does not.
TEST_F(SegmentTailIndexRegionTest, FooterReadCoversASmallRegion) {
    constexpr uint64_t kBlock = 1024 * 1024;

    // Region wholly inside the last block: 74 KB before a 40 MB file's end.
    EXPECT_TRUE(Segment::small_index_region_covered_by_footer_read(40 * 1024 * 1024 - 74 * 1024, 40 * 1024 * 1024,
                                                                   kBlock, 0));
    // Region reaching back past the block boundary: 1.8 MB before the same end.
    EXPECT_FALSE(Segment::small_index_region_covered_by_footer_read(40 * 1024 * 1024 - 1800 * 1024, 40 * 1024 * 1024,
                                                                    kBlock, 0));

    // Exactly on the boundary: the last block of a 2 MB file starts at 1 MB, so a region starting
    // there is covered and one byte earlier is not.
    EXPECT_TRUE(Segment::small_index_region_covered_by_footer_read(kBlock, 2 * kBlock, kBlock, 0));
    EXPECT_FALSE(Segment::small_index_region_covered_by_footer_read(kBlock - 1, 2 * kBlock, kBlock, 0));

    // A file that does not fill one block has only that block, so anything in it is covered.
    EXPECT_TRUE(Segment::small_index_region_covered_by_footer_read(0, 4096, kBlock, 0));

    // A small bundled slice can straddle two physical cache blocks. Its footer warms the second
    // block, not necessarily the whole slice: 896 KB base + 256 KB segment crosses at 1 MB.
    constexpr uint64_t kBundleOffset = 896 * 1024;
    constexpr uint64_t kSegmentSize = 256 * 1024;
    EXPECT_FALSE(Segment::small_index_region_covered_by_footer_read(64 * 1024, kSegmentSize, kBlock, kBundleOffset));
    EXPECT_TRUE(Segment::small_index_region_covered_by_footer_read(128 * 1024, kSegmentSize, kBlock, kBundleOffset));

    // Degenerate inputs must not skip the prefetch: with no block size there is no cache to
    // reason about, and an unknown file size tells us nothing.
    EXPECT_FALSE(Segment::small_index_region_covered_by_footer_read(0, 4096, 0, 0));
    EXPECT_FALSE(Segment::small_index_region_covered_by_footer_read(0, 0, kBlock, 0));
}

// A real segment written by this test is far smaller than one cache block, so its region must be
// judged covered -- the case the gate exists to catch.
TEST_F(SegmentTailIndexRegionTest, RealSmallSegmentRegionIsCovered) {
    auto tablet_schema = make_schema();
    config::enable_segment_tail_index_region = true;

    auto result = write_horizontal(kSegmentDir + "/region_covered", tablet_schema);
    ASSERT_TRUE(result.footer.has_small_index_region_offset());
    ASSERT_LT(result.file_size, 1024 * 1024) << "test segment outgrew a cache block; pick smaller data";

    EXPECT_TRUE(Segment::small_index_region_covered_by_footer_read(result.footer.small_index_region_offset(),
                                                                   result.file_size, 1024 * 1024, 0));
}

// Small index reads are routed to a shared stream only when one was supplied; everything else
// keeps reading through the column's own file, as it always has.
TEST_F(SegmentTailIndexRegionTest, IndexFileFallsBackToTheColumnFile) {
    // Distinct non-null addresses are enough: index_file() only chooses between them.
    auto* column_file = reinterpret_cast<io::SeekableInputStream*>(0x1000);
    auto* shared_file = reinterpret_cast<io::SeekableInputStream*>(0x2000);

    ColumnIteratorOptions opts;
    opts.read_file = column_file;
    EXPECT_EQ(column_file, opts.index_file());

    opts.index_read_file = shared_file;
    EXPECT_EQ(shared_file, opts.index_file());
    EXPECT_EQ(column_file, opts.read_file) << "data pages must not follow the small indexes";
}

// Serving every column's small indexes from one buffered stream must not change what is read.
// Both layouts, both settings: the shared stream is an IO consolidation, not a format.
TEST_F(SegmentTailIndexRegionTest, SharedSmallIndexStreamReadsTheSameRows) {
    auto tablet_schema = make_schema();

    for (bool region : {true, false}) {
        config::enable_segment_tail_index_region = region;
        const std::string base = kSegmentDir + (region ? "/shared_region_on" : "/shared_region_off");

        for (bool shared : {true, false}) {
            config::enable_segment_shared_small_index_stream = shared;
            const std::string file_name = base + (shared ? "_shared" : "_percolumn");
            (void)write_horizontal(file_name, tablet_schema);
            verify_all_rows(file_name, tablet_schema);
        }
    }
}

} // namespace starrocks
