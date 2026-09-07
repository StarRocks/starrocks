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

// Covers config::lake_enable_segment_tail_index_region: the segment writer gathers the short key index
// and then every column's ordinal index into one contiguous run immediately before the footer,
// instead of writing each column's ordinal index after that column's own data pages. Page zone
// maps stay inline either way, and the ordinal indexes go last inside the region so that the cache
// block parsing the footer already fetches covers as many of them as it can.
//
// The two layouts must be indistinguishable to a reader -- every index is located through an
// absolute PagePointer either way -- so each test that asserts something about the layout also
// reads the whole segment back and checks the values.

#include <gtest/gtest.h>

#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "common/config_exec_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/system/master_info.h"
#include "fs/fs_memory.h"
#include "storage/chunk_helper.h"
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

// The one index the read path must load for EVERY projected column before it can touch a data
// page, and the only one this layout moves. Reported as offsets in the same space as the rest of
// the footer's PagePointers.
std::vector<uint64_t> collect_ordinal_index_offsets(const SegmentFooterPB& footer) {
    std::vector<uint64_t> offsets;
    for (const auto& column : footer.columns()) {
        for (const auto& index : column.indexes()) {
            if (index.type() == ORDINAL_INDEX && index.ordinal_index().has_root_page()) {
                offsets.push_back(index.ordinal_index().root_page().root_page().offset());
            }
        }
    }
    return offsets;
}

// Page zone maps, which this layout deliberately leaves inline next to their column's data.
std::vector<uint64_t> collect_zone_map_offsets(const SegmentFooterPB& footer) {
    std::vector<uint64_t> offsets;
    for (const auto& column : footer.columns()) {
        for (const auto& index : column.indexes()) {
            if (index.type() == ZONE_MAP_INDEX && index.zone_map_index().has_page_zone_maps()) {
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
        _saved_region = config::lake_enable_segment_tail_index_region;
        // The layout is shared-data only and reads the run mode the FE reports over the
        // heartbeat, which nothing sets in a unit test. Report shared-data so the writer takes
        // the path under test; restored in TearDown so no other suite inherits it.
        _saved_master_info = get_master_info();
        TMasterInfo shared_data = _saved_master_info;
        shared_data.__set_run_mode(TRunMode::SHARED_DATA);
        ASSERT_TRUE(update_master_info(shared_data));
    }

    void TearDown() override {
        config::lake_enable_segment_tail_index_region = _saved_region;
        (void)update_master_info(_saved_master_info);
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
    TMasterInfo _saved_master_info;
};

// With the region on, every small index sits between the last data page and the footer, and the
// declared range covers exactly that gap.
TEST_F(SegmentTailIndexRegionTest, RegionCoversEverySmallIndex) {
    auto tablet_schema = make_schema();
    config::lake_enable_segment_tail_index_region = true;

    const std::string file_name = kSegmentDir + "/region_on";
    auto result = write_horizontal(file_name, tablet_schema);

    ASSERT_TRUE(result.footer.has_small_index_region_offset());
    ASSERT_TRUE(result.footer.has_small_index_region_size());
    const uint64_t region_begin = result.footer.small_index_region_offset();
    const uint64_t region_end = region_begin + result.footer.small_index_region_size();

    // Adjacency to the footer is the point: parsing the footer already fetches the file's final
    // cache block, and the ordinal indexes are what every projected column loads. So the region
    // must end exactly where the footer begins.
    EXPECT_EQ(result.footer_position, region_end);

    auto offsets = collect_ordinal_index_offsets(result.footer);
    ASSERT_FALSE(offsets.empty()) << "no ordinal index was written -- assertions would be vacuous";
    for (uint64_t offset : offsets) {
        EXPECT_GE(offset, region_begin);
        EXPECT_LT(offset, region_end);
    }

    // The short key index does not move: it keeps the position it has always had, at the end of
    // the column group that carried the keys, which is before the region.
    ASSERT_TRUE(result.footer.has_short_key_index_page());
    EXPECT_LT(result.footer.short_key_index_page().offset(), region_begin)
            << "the short key index must keep its original position, outside the region";

    // Page zone maps deliberately stay OUT of the region, inline next to their column's data,
    // where they ride in a cache block a predicate scan is reading anyway. Hoisting them would
    // give up that free ride and buy nothing.
    auto zone_map_offsets = collect_zone_map_offsets(result.footer);
    ASSERT_FALSE(zone_map_offsets.empty());
    for (uint64_t offset : zone_map_offsets) {
        EXPECT_LT(offset, region_begin) << "page zone maps must stay inline, not join the region";
    }

    // "Data pages stayed outside the region" is already established above: every page zone map is
    // written immediately after its own column's data pages, and all of them are asserted to be
    // below region_begin. A second check on the region's share of the file was tried and removed
    // -- on a synthetic segment of 100k narrow rows the data is small enough that a legitimate
    // region is a large fraction of it, so the assertion failed for reasons that have nothing to
    // do with the layout being correct.

    verify_all_rows(file_name, tablet_schema);
}

// A writer that continues an existing segment -- what SegmentRewriter does for a partial-update
// rewrite -- must not advertise a region. It only appends the remaining value columns, so the
// copied columns keep the ordinal index pages they were written with, and a region gathered from
// the appended ones alone would describe a range covering only part of the segment's indexes.
TEST_F(SegmentTailIndexRegionTest, ContinuedSegmentPublishesNoRegion) {
    auto tablet_schema = make_schema();
    config::lake_enable_segment_tail_index_region = true;

    // The footer handed to init() describes what is ALREADY in the file, and init() merges those
    // columns into the one being built. So the donor must be a genuine partial segment holding
    // only the key columns, disjoint from the value columns the rewrite appends -- exactly the
    // split rewrite_partial_update() produces. Overlapping the two puts a column in the footer
    // twice, which _verify_footer() rejects.
    const std::vector<uint32_t> already_written = {0, 1};
    const std::vector<uint32_t> appended = {2, 3};

    const std::string donor_name = kSegmentDir + "/continued_donor";
    SegmentFooterPB donor_footer;
    {
        ASSIGN_OR_ABORT(auto donor_wfile, _fs->new_writable_file(donor_name));
        SegmentWriterOptions donor_opts;
        donor_opts.num_rows_per_block = 10;
        SegmentWriter donor_writer(std::move(donor_wfile), 0, tablet_schema, donor_opts);
        ASSERT_OK(donor_writer.init(already_written, /*has_key=*/true));
        append_rows(&donor_writer, tablet_schema, already_written);
        uint64_t donor_file_size = 0;
        uint64_t donor_index_size = 0;
        uint64_t donor_footer_position = 0;
        ASSERT_OK(donor_writer.finalize(&donor_file_size, &donor_index_size, &donor_footer_position));
        donor_footer = read_footer(donor_name);
    }
    // The donor is an ordinary write, so it does carry a region. That is what keeps the
    // EXPECT_FALSE below meaningful instead of vacuous: the config is on and this path is live.
    ASSERT_TRUE(donor_footer.has_small_index_region_offset());

    const std::string file_name = kSegmentDir + "/continued";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);
    // Same shape as SegmentRewriter::rewrite_partial_update: append the remaining value columns
    // on top of a footer describing what is already there.
    ASSERT_OK(writer.init(appended, /*has_key=*/false, &donor_footer));
    append_rows(&writer, tablet_schema, appended);

    uint64_t index_size = 0;
    uint64_t file_size = 0;
    uint64_t footer_position = 0;
    ASSERT_OK(writer.finalize_columns(&index_size));
    ASSERT_OK(writer.finalize_footer(&file_size, &footer_position));

    const auto continued_footer = read_footer(file_name);
    EXPECT_FALSE(continued_footer.has_small_index_region_offset())
            << "a continued segment must keep the original layout, not advertise a partial region";
    EXPECT_FALSE(continued_footer.has_small_index_region_size());
}

// With the region off nothing changes: no footer fields, and the small indexes stay spread across
// the file instead of clustering at the tail.
TEST_F(SegmentTailIndexRegionTest, LegacyLayoutIsUnchanged) {
    auto tablet_schema = make_schema();

    config::lake_enable_segment_tail_index_region = false;
    const std::string legacy_file = kSegmentDir + "/region_off";
    auto legacy = write_horizontal(legacy_file, tablet_schema);

    EXPECT_FALSE(legacy.footer.has_small_index_region_offset());
    EXPECT_FALSE(legacy.footer.has_small_index_region_size());

    config::lake_enable_segment_tail_index_region = true;
    const std::string region_file = kSegmentDir + "/region_on_for_span";
    auto region = write_horizontal(region_file, tablet_schema);

    // The whole point of the change: gathered indexes span a fraction of what scattered ones do.
    auto legacy_offsets = collect_ordinal_index_offsets(legacy.footer);
    auto region_offsets = collect_ordinal_index_offsets(region.footer);
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
    config::lake_enable_segment_tail_index_region = true;

    const std::string file_name = kSegmentDir + "/vertical_region";
    auto result = write_vertical(file_name, tablet_schema);

    ASSERT_TRUE(result.footer.has_small_index_region_offset());
    ASSERT_TRUE(result.footer.has_small_index_region_size());
    const uint64_t region_begin = result.footer.small_index_region_offset();
    const uint64_t region_end = region_begin + result.footer.small_index_region_size();
    EXPECT_EQ(result.footer_position, region_end);

    // Every column, including those from the FIRST group, must have landed in the tail region --
    // that is the whole point, and the case a per-group write would get wrong.
    auto offsets = collect_ordinal_index_offsets(result.footer);
    ASSERT_FALSE(offsets.empty());
    for (uint64_t offset : offsets) {
        EXPECT_GE(offset, region_begin);
        EXPECT_LT(offset, region_end);
    }

    // The short key index still gets written, and still from the group that carried the keys --
    // the first one. It is not part of the region: only the ordinal indexes move, and they are
    // deferred past every later group's data, which is what a vertical writer has to get right.
    ASSERT_TRUE(result.footer.has_short_key_index_page());
    EXPECT_LT(result.footer.short_key_index_page().offset(), region_begin)
            << "the short key index must keep its original position, outside the region";

    verify_all_rows(file_name, tablet_schema);
}

// The vertical and horizontal writers must agree: same rows in, same layout guarantees out.
TEST_F(SegmentTailIndexRegionTest, VerticalAndHorizontalRegionsAgree) {
    auto tablet_schema = make_schema();
    config::lake_enable_segment_tail_index_region = true;

    const std::string h_file = kSegmentDir + "/agree_horizontal";
    const std::string v_file = kSegmentDir + "/agree_vertical";
    auto h = write_horizontal(h_file, tablet_schema);
    auto v = write_vertical(v_file, tablet_schema);

    EXPECT_EQ(collect_ordinal_index_offsets(h.footer).size(), collect_ordinal_index_offsets(v.footer).size());
    EXPECT_EQ(h.footer_position, h.footer.small_index_region_offset() + h.footer.small_index_region_size());
    EXPECT_EQ(v.footer_position, v.footer.small_index_region_offset() + v.footer.small_index_region_size());

    verify_all_rows(h_file, tablet_schema);
    verify_all_rows(v_file, tablet_schema);
}

} // namespace starrocks
