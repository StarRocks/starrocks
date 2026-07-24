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

// End-to-end coverage for the split morsel queue + segment group over full-sort-key-index
// segments, plus the mixed-encoding unsplit fallback. Each test writes real segments (config
// on -> full-key, config off -> legacy), drives the queue's try_get to end-of-queue, reads the
// emitted morsels back through the segment iterator, and asserts the union of returned rows
// equals a plain full scan of the same data.

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/global_dict/types.h"
#include "common/config_rowset_fwd.h"
#include "exec_primitive/pipeline/scan/split_morsel_ticket_checker.h"
#include "fs/fs_memory.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/segment.pb.h"
#include "storage/base/short_key_index.h"
#include "storage/base_tablet.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/query/split_morsel_queue.h"
#include "storage/query/split_scan_morsel.h"
#include "storage/rowset/base_rowset.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/page_pointer.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/seek_range.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage_primitive/olap_tuple.h"
#include "storage_primitive/range.h"
#include "types/datum.h"

namespace starrocks::pipeline {

namespace {

using Row = std::pair<int32_t, int32_t>;

// (c0 int key, c1 int key, v int value); sort key = (c0, c1); only 1 short key column, so the
// legacy short key index covers c0 alone while the full-key index covers (c0, c1). This makes the
// full-key vs. legacy distinction exercise both the codec and the indexed-column-count difference.
std::shared_ptr<TabletSchema> make_schema() {
    std::vector<ColumnPB> cols;
    cols.push_back(create_int_key_pb(/*id=*/1));
    cols.push_back(create_int_key_pb(/*id=*/2));
    cols.push_back(create_int_value_pb(/*id=*/3));
    auto unique = TabletSchemaHelper::create_tablet_schema(cols, /*num_short_key_columns=*/1);
    return std::shared_ptr<TabletSchema>(std::move(unique));
}

// Same shape as make_schema() -- SAME sort-key column unique-ids (1, 2) -- but the FIRST sort-key
// column has |first_key_type| instead of INT. Used to model metadata-only fast schema evolution:
// segments stay full-key under the historical INT schema while the current tablet schema's sort-key
// type diverges. The unique-ids match so the divergence is a type-only mismatch, exactly what the
// logical-split compatibility check must catch.
std::shared_ptr<TabletSchema> make_schema_with_first_key_type(const std::string& first_key_type, int length) {
    ColumnPB c0;
    c0.set_unique_id(1);
    c0.set_name("1");
    c0.set_type(first_key_type);
    c0.set_is_key(true);
    c0.set_is_nullable(true);
    c0.set_length(length);
    c0.set_index_length(length);

    std::vector<ColumnPB> cols;
    cols.push_back(c0);
    cols.push_back(create_int_key_pb(/*id=*/2));
    cols.push_back(create_int_value_pb(/*id=*/3));
    auto unique = TabletSchemaHelper::create_tablet_schema(cols, /*num_short_key_columns=*/1);
    return std::shared_ptr<TabletSchema>(std::move(unique));
}

// A chunk of |n| rows for i in [begin, begin+n): c0 = i / 10, c1 = i, v = 0. Monotonic in (c0, c1)
// and in c1, so a segment is internally sorted and disjoint key windows keep the tablet globally
// sorted across segments.
ChunkUniquePtr make_chunk(const std::shared_ptr<TabletSchema>& schema, int begin, int n) {
    auto s = ChunkHelper::convert_schema(schema);
    auto chunk = ChunkFactory::new_chunk(s, n);
    auto cols = chunk->columns();
    for (int k = 0; k < n; ++k) {
        int i = begin + k;
        cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i / 10)));
        cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
        cols[2]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
    }
    return chunk;
}

TScanRange make_scan_range(int64_t tablet_id) {
    TScanRange scan_range;
    TInternalScanRange internal;
    internal.tablet_id = tablet_id;
    internal.version = "1";
    internal.partition_id = 1;
    scan_range.__set_internal_scan_range(internal);
    return scan_range;
}

// Minimal BaseRowset that just wraps pre-built, pre-index-loaded segments.
class FakeRowset : public BaseRowset {
public:
    FakeRowset(std::vector<SegmentSharedPtr> segments, bool overlapped, int64_t id)
            : _segments(std::move(segments)), _overlapped(overlapped) {
        _rowset_id.init(id);
    }
    RowsetId rowset_id() const override { return _rowset_id; }
    int64_t num_rows() const override {
        int64_t n = 0;
        for (const auto& s : _segments) {
            if (s != nullptr) n += s->num_rows();
        }
        return n;
    }
    bool is_overlapped() const override { return _overlapped; }
    std::vector<SegmentSharedPtr> get_segments() override { return _segments; }
    bool has_data_files() const override { return true; }
    int64_t start_version() const override { return 1; }
    int64_t end_version() const override { return 1; }

private:
    std::vector<SegmentSharedPtr> _segments;
    bool _overlapped;
    RowsetId _rowset_id;
};

// Minimal non-cloud-native BaseTablet: only tablet_id / num_rows / belonged_to_cloud_native matter
// to the logical split queue.
class FakeTablet : public BaseTablet {
public:
    FakeTablet(int64_t id, int64_t num_rows) : _id(id), _num_rows(num_rows) {}
    int64_t tablet_id() const override { return _id; }
    size_t num_rows() const override { return _num_rows; }
    StatusOr<bool> has_delete_predicates(const Version& /*version*/) override { return false; }
    bool belonged_to_cloud_native() const override { return false; }

private:
    int64_t _id;
    size_t _num_rows;
};

class LogicalSplitMorselQueueFullKeyTest : public ::testing::Test {
public:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_OK(_fs->create_dir(kDir));
        _saved_write = config::enable_full_sort_key_index;
        _saved_read = config::enable_full_sort_key_index_read;
        // The read gate is on by default; individual tests flip it to exercise rollback / anti-drift.
        config::enable_full_sort_key_index_read = true;
    }

    void TearDown() override {
        config::enable_full_sort_key_index = _saved_write;
        config::enable_full_sort_key_index_read = _saved_read;
    }

    // Write one segment to |path| (write config gate decides whether the full page is additionally
    // written) WITHOUT opening it.
    void write_segment_file(const std::shared_ptr<TabletSchema>& schema, const std::string& path, uint32_t seg_id,
                            int begin, int n, bool full_key) {
        const bool old = config::enable_full_sort_key_index;
        config::enable_full_sort_key_index = full_key;
        DeferOp restore([&] { config::enable_full_sort_key_index = old; });

        auto chunk = make_chunk(schema, begin, n);
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(path));
        SegmentWriterOptions opts;
        opts.num_rows_per_block = 10;
        SegmentWriter writer(std::move(wfile), seg_id, schema, opts);
        CHECK_OK(writer.init(true));
        CHECK_OK(writer.append_chunk(*chunk));
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        CHECK_OK(writer.finalize(&file_size, &index_size, &footer_position));
    }

    // Write one segment (config gate decides full-key vs. legacy) and return the opened,
    // index-loaded segment.
    SegmentSharedPtr write_segment(const std::shared_ptr<TabletSchema>& schema, uint32_t seg_id, int begin, int n,
                                   bool full_key) {
        std::string path = kDir + "/seg_" + std::to_string(_next_file_seq++) + ".dat";
        write_segment_file(schema, path, seg_id, begin, n, full_key);
        ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{path}, seg_id, schema));
        CHECK_OK(segment->load_index());
        return segment;
    }

    // Write a dual-page segment, then rewrite footer field 11 to point at a geometry-valid,
    // arity-valid, but KEY-ORDER-CORRUPT full page (entries not byte-wise non-decreasing). Usability
    // validation must reject it, so the logical split degrades to legacy morsels. Returns the opened,
    // index-loaded segment.
    SegmentSharedPtr write_segment_with_corrupt_full_page(const std::shared_ptr<TabletSchema>& schema, uint32_t seg_id,
                                                          int begin, int n) {
        std::string base = kDir + "/corrupt_base_" + std::to_string(_next_file_seq++) + ".dat";
        write_segment_file(schema, base, seg_id, begin, n, /*full_key=*/true);

        // Read the legacy page footer to mirror its geometry into the corrupt full page.
        SegmentFooterPB footer;
        {
            ASSIGN_OR_ABORT(auto rf, _fs->new_random_access_file(base));
            CHECK(Segment::parse_segment_footer(rf.get(), &footer, nullptr, nullptr).ok());
        }
        ShortKeyFooterPB legacy;
        {
            ASSIGN_OR_ABORT(auto rf, _fs->new_random_access_file(base));
            PageReadOptions opts;
            opts.read_file = rf.get();
            opts.page_pointer = PagePointer(footer.short_key_index_page());
            opts.codec = nullptr;
            OlapReaderStatistics stats;
            opts.stats = &stats;
            PageHandle handle;
            Slice body;
            PageFooterPB pf;
            CHECK(PageIO::read_and_decompress_page(opts, &handle, &body, &pf).ok());
            legacy = pf.short_key_page_footer();
        }
        const uint32_t items = legacy.num_items();
        CHECK_GE(items, 2u);

        // Copy the data region (everything but the trailing footer) verbatim so the legacy page +
        // column pages keep their offsets.
        std::string dst = kDir + "/corrupt_" + std::to_string(_next_file_seq++) + ".dat";
        std::unique_ptr<WritableFile> wf;
        {
            ASSIGN_OR_ABORT(auto rf, _fs->new_random_access_file(base));
            ASSIGN_OR_ABORT(auto file_size, rf->get_size());
            SegmentFooterPB tmp;
            ASSIGN_OR_ABORT(auto footer_total, Segment::parse_segment_footer(rf.get(), &tmp, nullptr, nullptr));
            const size_t data_len = static_cast<size_t>(file_size) - footer_total;
            std::string data;
            data.resize(data_len);
            CHECK(rf->read_at_fully(0, data.data(), data_len).ok());
            ASSIGN_OR_ABORT(wf, _fs->new_writable_file(dst));
            CHECK_OK(wf->append(Slice(data)));
        }

        // Build a full page with the same geometry/arity but non-monotonic keys: the first key is the
        // largest, so key(1) < key(0) fails the byte-wise non-decreasing check.
        ShortKeyIndexBuilder builder(seg_id, legacy.num_rows_per_block());
        auto be32 = [](uint32_t v) {
            std::string s(4, '\0');
            s[0] = static_cast<char>((v >> 24) & 0xFF);
            s[1] = static_cast<char>((v >> 16) & 0xFF);
            s[2] = static_cast<char>((v >> 8) & 0xFF);
            s[3] = static_cast<char>(v & 0xFF);
            return s;
        };
        CHECK_OK(builder.add_item(be32(items))); // largest, out of order
        for (uint32_t i = 1; i < items; ++i) {
            CHECK_OK(builder.add_item(be32(i)));
        }
        std::vector<Slice> body;
        PageFooterPB pf;
        CHECK_OK(builder.finalize(legacy.num_segment_rows(), &body, &pf, SHORT_KEY_ENCODING_FULL_SORT_KEY,
                                  /*num_sort_key_columns=*/static_cast<uint32_t>(schema->sort_key_idxes().size())));
        PagePointer pp;
        CHECK_OK(PageIO::write_page(wf.get(), body, pf, &pp));
        pp.to_proto(footer.mutable_full_sort_key_index_page());
        CHECK_OK(Segment::write_segment_footer(wf.get(), footer));
        CHECK_OK(wf->close());

        ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{dst}, seg_id, schema));
        CHECK_OK(segment->load_index());
        return segment;
    }

    // Drain a positioned segment iterator into (c0, c1) rows.
    std::vector<Row> drain(const ChunkIteratorPtr& it) {
        CHECK_OK(it->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
        CHECK_OK(it->init_output_schema(std::unordered_set<uint32_t>()));
        auto chunk = ChunkFactory::new_chunk(it->output_schema(), 64);
        std::vector<Row> rows;
        while (true) {
            chunk->reset();
            auto st = it->get_next(chunk.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            for (size_t r = 0; r < chunk->num_rows(); ++r) {
                rows.emplace_back(chunk->get_column_by_index(0)->get(r).get_int32(),
                                  chunk->get_column_by_index(1)->get(r).get_int32());
            }
        }
        it->close();
        return rows;
    }

    // Read one segment fully (no split option) -> its rows.
    std::vector<Row> read_full(const SegmentSharedPtr& segment, const std::shared_ptr<TabletSchema>& schema) {
        Schema read_schema = ChunkHelper::get_full_sort_key_schema(schema);
        OlapReaderStatistics stats;
        SegmentReadOptions o;
        o.fs = _fs;
        o.stats = &stats;
        return drain(new_segment_iterator(segment, read_schema, o));
    }

    // Read one segment restricted to |short_key_ranges| -> the rows landing in those key ranges.
    std::vector<Row> read_with_short_key_ranges(const SegmentSharedPtr& segment,
                                                const std::shared_ptr<TabletSchema>& schema,
                                                const std::vector<ShortKeyRangeOptionPtr>& short_key_ranges) {
        Schema read_schema = ChunkHelper::get_full_sort_key_schema(schema);
        OlapReaderStatistics stats;
        SegmentReadOptions o;
        o.fs = _fs;
        o.stats = &stats;
        o.short_key_ranges = short_key_ranges;
        return drain(new_segment_iterator(segment, read_schema, o));
    }

    // Read one segment restricted to a rowid |range| -> its rows.
    std::vector<Row> read_with_rowid_range(const SegmentSharedPtr& segment, const std::shared_ptr<TabletSchema>& schema,
                                           const SparseRangePtr& range) {
        Schema read_schema = ChunkHelper::get_full_sort_key_schema(schema);
        OlapReaderStatistics stats;
        SegmentReadOptions o;
        o.fs = _fs;
        o.stats = &stats;
        o.rowid_range_option = range;
        return drain(new_segment_iterator(segment, read_schema, o));
    }

protected:
    std::shared_ptr<MemoryFileSystem> _fs;
    const std::string kDir = "/logical_split_full_key_test";
    uint32_t _next_file_seq = 0;
    bool _saved_write = true;
    bool _saved_read = true;
};

std::vector<Row> sorted(std::vector<Row> rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

std::vector<Row> expected_rows(int begin, int end) {
    std::vector<Row> rows;
    for (int i = begin; i < end; ++i) {
        rows.emplace_back(i / 10, i);
    }
    return rows;
}

} // namespace

// (b) Uniform full-key tablet: the logically split morsels' short-key ranges, read back over every
// segment of every rowset, cover exactly the full scan. The largest (source) rowset has two
// segments; a second rowset's keys sit above the source group's window, so its rows land in the
// last (open-ended) split range -- proving the source-group boundaries apply correctly to other
// rowsets.
TEST_F(LogicalSplitMorselQueueFullKeyTest, uniform_full_key_logical_split_matches_full_scan) {
    auto schema = make_schema();

    // Rowset A (source group, non-overlapped, 2 segments): i in [0, 50) and [50, 100).
    std::vector<SegmentSharedPtr> a_segs = {write_segment(schema, 0, 0, 50, /*full_key=*/true),
                                            write_segment(schema, 1, 50, 50, /*full_key=*/true)};
    // Dual-page segments, read gate on -> the logical split takes the full-key path.
    ASSERT_TRUE(a_segs[0]->has_full_sort_key_index_page());
    ASSERT_TRUE(a_segs[0]->use_full_sort_key_index());
    auto rowset_a = std::make_shared<FakeRowset>(a_segs, /*overlapped=*/false, /*id=*/1);
    // Rowset B (non-overlapped, 1 segment): i in [100, 150) -- above A's window.
    std::vector<SegmentSharedPtr> b_segs = {write_segment(schema, 0, 100, 50, /*full_key=*/true)};
    auto rowset_b = std::make_shared<FakeRowset>(b_segs, /*overlapped=*/false, /*id=*/2);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset_a, rowset_b}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(10001, 150)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(10001)));
    // splitted_scan_rows small -> multiple morsels.
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    std::vector<Row> collected;
    int num_morsels = 0;
    while (true) {
        ASSIGN_OR_ABORT(auto morsel, queue.try_get());
        if (morsel == nullptr) break;
        ++num_morsels;
        auto* logical = dynamic_cast<LogicalSplitScanMorsel*>(morsel.get());
        ASSERT_NE(logical, nullptr) << "uniform tablet must be logically split";
        const auto& ranges = logical->get_short_key_ranges_option()->short_key_ranges;
        ASSERT_FALSE(ranges.empty());
        // Every raw-slice boundary must carry the full-key pin so the Slice consumer decodes it with
        // the full decoder regardless of the live read config.
        for (const auto& r : ranges) {
            if (!r->lower->short_key.empty()) {
                EXPECT_EQ(SHORT_KEY_ENCODING_FULL_SORT_KEY, r->lower->encoding);
            }
            if (!r->upper->short_key.empty()) {
                EXPECT_EQ(SHORT_KEY_ENCODING_FULL_SORT_KEY, r->upper->encoding);
            }
        }
        for (const auto& rowset : tablet_rowsets[0]) {
            for (const auto& segment : rowset->get_segments()) {
                auto rows = read_with_short_key_ranges(segment, schema, ranges);
                collected.insert(collected.end(), rows.begin(), rows.end());
            }
        }
    }
    EXPECT_GT(num_morsels, 1) << "small splitted_scan_rows should produce multiple splits";
    EXPECT_EQ(sorted(expected_rows(0, 150)), sorted(collected));
    EXPECT_TRUE(queue.empty());
}

// (b') Uniform full-key with an OVERLAPPED source rowset: the source group holds only the largest
// segment, but the boundaries must still cover the rowset's other segments. Union == full scan.
TEST_F(LogicalSplitMorselQueueFullKeyTest, uniform_full_key_overlapped_source_matches_full_scan) {
    auto schema = make_schema();

    // Overlapped rowset with two segments over the SAME key window [0, 100): the larger one (100
    // rows) becomes the source group; the smaller (30 rows) must still be covered by the boundaries.
    std::vector<SegmentSharedPtr> segs = {write_segment(schema, 0, 0, 100, /*full_key=*/true),
                                          write_segment(schema, 1, 0, 30, /*full_key=*/true)};
    auto rowset = std::make_shared<FakeRowset>(segs, /*overlapped=*/true, /*id=*/1);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(10002, 130)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(10002)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    std::vector<Row> collected;
    while (true) {
        ASSIGN_OR_ABORT(auto morsel, queue.try_get());
        if (morsel == nullptr) break;
        auto* logical = dynamic_cast<LogicalSplitScanMorsel*>(morsel.get());
        ASSERT_NE(logical, nullptr);
        const auto& ranges = logical->get_short_key_ranges_option()->short_key_ranges;
        for (const auto& segment : rowset->get_segments()) {
            auto rows = read_with_short_key_ranges(segment, schema, ranges);
            collected.insert(collected.end(), rows.begin(), rows.end());
        }
    }
    // Expected: [0,100) once + [0,30) again from the overlapping segment.
    std::vector<Row> expected = expected_rows(0, 100);
    auto extra = expected_rows(0, 30);
    expected.insert(expected.end(), extra.begin(), extra.end());
    EXPECT_EQ(sorted(expected), sorted(collected));
}

// Uniform LEGACY tablet (config off): unchanged behavior -- logically split, union == full scan.
TEST_F(LogicalSplitMorselQueueFullKeyTest, uniform_legacy_logical_split_matches_full_scan) {
    auto schema = make_schema();

    std::vector<SegmentSharedPtr> a_segs = {write_segment(schema, 0, 0, 50, /*full_key=*/false),
                                            write_segment(schema, 1, 50, 50, /*full_key=*/false)};
    auto rowset_a = std::make_shared<FakeRowset>(a_segs, /*overlapped=*/false, /*id=*/1);
    std::vector<SegmentSharedPtr> b_segs = {write_segment(schema, 0, 100, 50, /*full_key=*/false)};
    auto rowset_b = std::make_shared<FakeRowset>(b_segs, /*overlapped=*/false, /*id=*/2);
    ASSERT_FALSE(a_segs[0]->has_full_sort_key_index_page());

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset_a, rowset_b}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(10003, 150)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(10003)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    std::vector<Row> collected;
    while (true) {
        ASSIGN_OR_ABORT(auto morsel, queue.try_get());
        if (morsel == nullptr) break;
        auto* logical = dynamic_cast<LogicalSplitScanMorsel*>(morsel.get());
        ASSERT_NE(logical, nullptr);
        const auto& ranges = logical->get_short_key_ranges_option()->short_key_ranges;
        for (const auto& rowset : tablet_rowsets[0]) {
            for (const auto& segment : rowset->get_segments()) {
                auto rows = read_with_short_key_ranges(segment, schema, ranges);
                collected.insert(collected.end(), rows.begin(), rows.end());
            }
        }
    }
    EXPECT_EQ(sorted(expected_rows(0, 150)), sorted(collected));
}

// (c) MIXED encodings -> the tablet must NOT be logically split: exactly one ordinary ScanMorsel
// (no ShortKeyRangesOption) is emitted, and reading the whole tablet through it returns all rows.
// Covers both directions plus the overlapping-rowset and source-group-itself-mixed shapes.
TEST_F(LogicalSplitMorselQueueFullKeyTest, mixed_full_key_source_legacy_secondary_falls_back) {
    auto schema = make_schema();
    // Source (largest) rowset full-key; a smaller secondary rowset legacy.
    std::vector<SegmentSharedPtr> a_segs = {write_segment(schema, 0, 0, 100, /*full_key=*/true)};
    auto rowset_a = std::make_shared<FakeRowset>(a_segs, /*overlapped=*/false, /*id=*/1);
    std::vector<SegmentSharedPtr> b_segs = {write_segment(schema, 0, 100, 40, /*full_key=*/false)};
    auto rowset_b = std::make_shared<FakeRowset>(b_segs, /*overlapped=*/false, /*id=*/2);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset_a, rowset_b}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(20001, 140)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(20001)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    ASSIGN_OR_ABORT(auto morsel, queue.try_get());
    ASSERT_NE(morsel, nullptr);
    // Exactly ONE ordinary ScanMorsel: not a logical split, carrying no short key ranges but the
    // tablet's rowsets attached (so the downstream does a normal full-tablet scan).
    EXPECT_EQ(dynamic_cast<LogicalSplitScanMorsel*>(morsel.get()), nullptr);
    EXPECT_EQ(2u, morsel->rowsets().size());

    // The unsplit morsel means a plain full-tablet scan; read every segment fully.
    std::vector<Row> collected;
    for (const auto& rowset : tablet_rowsets[0]) {
        for (const auto& segment : rowset->get_segments()) {
            auto rows = read_full(segment, schema);
            collected.insert(collected.end(), rows.begin(), rows.end());
        }
    }
    EXPECT_EQ(sorted(expected_rows(0, 140)), sorted(collected));

    // No second morsel; queue advances past the tablet and drains.
    ASSIGN_OR_ABORT(auto next, queue.try_get());
    EXPECT_EQ(next, nullptr);
    EXPECT_TRUE(queue.empty());
}

TEST_F(LogicalSplitMorselQueueFullKeyTest, mixed_legacy_source_full_key_secondary_falls_back) {
    auto schema = make_schema();
    // Source (largest) rowset legacy; a smaller secondary rowset full-key (opposite direction).
    std::vector<SegmentSharedPtr> a_segs = {write_segment(schema, 0, 0, 100, /*full_key=*/false)};
    auto rowset_a = std::make_shared<FakeRowset>(a_segs, /*overlapped=*/false, /*id=*/1);
    std::vector<SegmentSharedPtr> b_segs = {write_segment(schema, 0, 100, 40, /*full_key=*/true)};
    auto rowset_b = std::make_shared<FakeRowset>(b_segs, /*overlapped=*/false, /*id=*/2);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset_a, rowset_b}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(20002, 140)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(20002)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    ASSIGN_OR_ABORT(auto morsel, queue.try_get());
    ASSERT_NE(morsel, nullptr);
    EXPECT_EQ(dynamic_cast<LogicalSplitScanMorsel*>(morsel.get()), nullptr);

    std::vector<Row> collected;
    for (const auto& rowset : tablet_rowsets[0]) {
        for (const auto& segment : rowset->get_segments()) {
            auto rows = read_full(segment, schema);
            collected.insert(collected.end(), rows.begin(), rows.end());
        }
    }
    EXPECT_EQ(sorted(expected_rows(0, 140)), sorted(collected));

    ASSIGN_OR_ABORT(auto next, queue.try_get());
    EXPECT_EQ(next, nullptr);
    EXPECT_TRUE(queue.empty());
}

// The overlapping-rowset mixed shape: the source group holds only the largest (full-key) segment,
// but a smaller legacy segment lives in the SAME rowset. The signature check spans every segment,
// not just the source group, so the disagreement is caught and the tablet falls back to unsplit.
TEST_F(LogicalSplitMorselQueueFullKeyTest, mixed_within_overlapped_source_rowset_falls_back) {
    auto schema = make_schema();
    std::vector<SegmentSharedPtr> segs = {write_segment(schema, 0, 0, 100, /*full_key=*/true),
                                          write_segment(schema, 1, 0, 30, /*full_key=*/false)};
    auto rowset = std::make_shared<FakeRowset>(segs, /*overlapped=*/true, /*id=*/1);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(20003, 130)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(20003)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    ASSIGN_OR_ABORT(auto morsel, queue.try_get());
    ASSERT_NE(morsel, nullptr);
    EXPECT_EQ(dynamic_cast<LogicalSplitScanMorsel*>(morsel.get()), nullptr);

    std::vector<Row> collected;
    for (const auto& segment : rowset->get_segments()) {
        auto rows = read_full(segment, schema);
        collected.insert(collected.end(), rows.begin(), rows.end());
    }
    std::vector<Row> expected = expected_rows(0, 100);
    auto extra = expected_rows(0, 30);
    expected.insert(expected.end(), extra.begin(), extra.end());
    EXPECT_EQ(sorted(expected), sorted(collected));

    ASSIGN_OR_ABORT(auto next, queue.try_get());
    EXPECT_EQ(next, nullptr);
    EXPECT_TRUE(queue.empty());
}

// The source group is itself mixed: the single largest (non-overlapped) rowset has two segments of
// different encodings -> unsplit fallback.
TEST_F(LogicalSplitMorselQueueFullKeyTest, mixed_within_source_group_falls_back) {
    auto schema = make_schema();
    std::vector<SegmentSharedPtr> segs = {write_segment(schema, 0, 0, 50, /*full_key=*/true),
                                          write_segment(schema, 1, 50, 50, /*full_key=*/false)};
    auto rowset = std::make_shared<FakeRowset>(segs, /*overlapped=*/false, /*id=*/1);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(20004, 100)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(20004)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    ASSIGN_OR_ABORT(auto morsel, queue.try_get());
    ASSERT_NE(morsel, nullptr);
    EXPECT_EQ(dynamic_cast<LogicalSplitScanMorsel*>(morsel.get()), nullptr);

    std::vector<Row> collected;
    for (const auto& segment : rowset->get_segments()) {
        auto rows = read_full(segment, schema);
        collected.insert(collected.end(), rows.begin(), rows.end());
    }
    EXPECT_EQ(sorted(expected_rows(0, 100)), sorted(collected));

    ASSIGN_OR_ABORT(auto next, queue.try_get());
    EXPECT_EQ(next, nullptr);
    EXPECT_TRUE(queue.empty());
}

// Schema-evolution seam (a): every segment is uniformly full-key under the HISTORICAL INT sort key,
// but the CURRENT tablet schema's first sort-key column is BIGINT. The boundaries would be encoded
// with the current 8-byte BIGINT schema and compared against the segments' 4-byte INT index bytes,
// mis-bracketing and dropping rows. The current-schema signature check must catch the divergence and
// fall back to a single unsplit ScanMorsel; the full scan through it must still return every row.
TEST_F(LogicalSplitMorselQueueFullKeyTest, uniform_full_key_current_schema_bigint_falls_back) {
    auto historical = make_schema();
    auto current = make_schema_with_first_key_type("BIGINT", /*length=*/8);

    // Segments written full-key under the historical INT schema.
    std::vector<SegmentSharedPtr> segs = {write_segment(historical, 0, 0, 100, /*full_key=*/true)};
    ASSERT_TRUE(segs[0]->has_full_sort_key_index_page());
    auto rowset = std::make_shared<FakeRowset>(segs, /*overlapped=*/false, /*id=*/1);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(50001, 100)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(50001)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    // Current tablet schema diverges from the segments' historical schema.
    queue.set_tablet_schema(current);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    ASSIGN_OR_ABORT(auto morsel, queue.try_get());
    ASSERT_NE(morsel, nullptr);
    // Unsplit fallback: a plain ScanMorsel carrying no ShortKeyRangesOption.
    EXPECT_EQ(dynamic_cast<LogicalSplitScanMorsel*>(morsel.get()), nullptr);
    EXPECT_EQ(1u, morsel->rowsets().size());

    // A plain full-tablet scan must return all rows -- no drops from a wrong coarse bracket.
    std::vector<Row> collected;
    for (const auto& segment : rowset->get_segments()) {
        auto rows = read_full(segment, historical);
        collected.insert(collected.end(), rows.begin(), rows.end());
    }
    EXPECT_EQ(sorted(expected_rows(0, 100)), sorted(collected));

    ASSIGN_OR_ABORT(auto next, queue.try_get());
    EXPECT_EQ(next, nullptr);
    EXPECT_TRUE(queue.empty());
}

// Schema-evolution seam (b): every segment is uniformly full-key under the HISTORICAL INT sort key,
// but the CURRENT tablet schema's first sort-key column is DOUBLE -- which has no registered key
// coder, so encoding a boundary with it would deref get_key_coder(DOUBLE) == nullptr and crash. The
// current-schema encodability check must reject the tablet up front (unsplit fallback) so no boundary
// is ever encoded with the DOUBLE schema. Reaching end-of-queue without crashing is the core assertion.
TEST_F(LogicalSplitMorselQueueFullKeyTest, uniform_full_key_current_schema_double_falls_back_no_crash) {
    auto historical = make_schema();
    auto current = make_schema_with_first_key_type("DOUBLE", /*length=*/8);

    std::vector<SegmentSharedPtr> segs = {write_segment(historical, 0, 0, 100, /*full_key=*/true)};
    ASSERT_TRUE(segs[0]->has_full_sort_key_index_page());
    auto rowset = std::make_shared<FakeRowset>(segs, /*overlapped=*/false, /*id=*/1);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(50002, 100)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(50002)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(current);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    ASSIGN_OR_ABORT(auto morsel, queue.try_get());
    ASSERT_NE(morsel, nullptr);
    // Unsplit fallback: no boundary was encoded with the unencodable DOUBLE schema.
    EXPECT_EQ(dynamic_cast<LogicalSplitScanMorsel*>(morsel.get()), nullptr);
    EXPECT_EQ(1u, morsel->rowsets().size());

    std::vector<Row> collected;
    for (const auto& segment : rowset->get_segments()) {
        auto rows = read_full(segment, historical);
        collected.insert(collected.end(), rows.begin(), rows.end());
    }
    EXPECT_EQ(sorted(expected_rows(0, 100)), sorted(collected));

    ASSIGN_OR_ABORT(auto next, queue.try_get());
    EXPECT_EQ(next, nullptr);
    EXPECT_TRUE(queue.empty());
}

// Multi-tablet drive to end-of-queue: a uniform full-key tablet (multiple splits), a mixed tablet
// (one unsplit morsel), and an empty tablet (no morsel). Assert no tablet is skipped or repeated,
// morsels stay grouped by tablet in order, and per-tablet ticket accounting is complete (enter
// count == morsels emitted, with exactly the last split flagged).
TEST_F(LogicalSplitMorselQueueFullKeyTest, multi_tablet_ticket_accounting_and_ordering) {
    auto schema = make_schema();

    // Tablet 30001: uniform full-key, splits into multiple morsels.
    std::vector<SegmentSharedPtr> t1 = {write_segment(schema, 0, 0, 100, /*full_key=*/true)};
    auto rs1 = std::make_shared<FakeRowset>(t1, /*overlapped=*/false, /*id=*/1);
    // Tablet 30002: mixed -> one unsplit morsel.
    std::vector<SegmentSharedPtr> t2 = {write_segment(schema, 0, 0, 60, /*full_key=*/true),
                                        write_segment(schema, 1, 60, 60, /*full_key=*/false)};
    auto rs2 = std::make_shared<FakeRowset>(t2, /*overlapped=*/false, /*id=*/2);
    // Tablet 30003: empty (no rows) -> no morsel, no ticket.
    auto rs3 = std::make_shared<FakeRowset>(std::vector<SegmentSharedPtr>{}, /*overlapped=*/false, /*id=*/3);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rs1}, {rs2}, {rs3}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(30001, 100),
                                                std::make_shared<FakeTablet>(30002, 120),
                                                std::make_shared<FakeTablet>(30003, 0)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(30001)));
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(30002)));
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(30003)));
    LogicalSplitMorselQueue queue(std::move(morsels), 3, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    auto ticket_checker = std::make_shared<SplitMorselTicketChecker>();
    queue.set_ticket_checker(ticket_checker);

    std::vector<int64_t> emitted_tablet_ids;
    while (true) {
        ASSIGN_OR_ABORT(auto morsel, queue.try_get());
        if (morsel == nullptr) break;
        emitted_tablet_ids.push_back(morsel->get_olap_scan_range()->tablet_id);
    }
    EXPECT_TRUE(queue.empty());

    // The empty tablet emits nothing; the other two are present and grouped in order.
    ASSERT_FALSE(emitted_tablet_ids.empty());
    // Grouped by tablet and non-decreasing (no interleave/repeat-after-leaving).
    std::vector<int64_t> distinct;
    for (auto id : emitted_tablet_ids) {
        if (distinct.empty() || distinct.back() != id) {
            // Must not have appeared earlier (no skip-back / repeat).
            EXPECT_EQ(std::find(distinct.begin(), distinct.end(), id), distinct.end())
                    << "tablet " << id << " emitted in two separate groups";
            distinct.push_back(id);
        }
    }
    EXPECT_EQ((std::vector<int64_t>{30001, 30002}), distinct);

    // The mixed tablet emitted exactly one morsel.
    EXPECT_EQ(1, std::count(emitted_tablet_ids.begin(), emitted_tablet_ids.end(), int64_t{30002}));
    // The uniform tablet emitted more than one.
    EXPECT_GT(std::count(emitted_tablet_ids.begin(), emitted_tablet_ids.end(), int64_t{30001}), 1);

    // Per-tablet ticket accounting: leaving exactly (#morsels emitted) times must balance only on the
    // final leave -- proving enter_count == morsels emitted and the last split carried is_last.
    for (int64_t tablet_id : {int64_t{30001}, int64_t{30002}}) {
        auto n = std::count(emitted_tablet_ids.begin(), emitted_tablet_ids.end(), tablet_id);
        ASSERT_GT(n, 0);
        EXPECT_TRUE(ticket_checker->are_all_ready(tablet_id)) << "last split not flagged for " << tablet_id;
        for (int k = 0; k < n; ++k) {
            bool balanced = ticket_checker->leave(tablet_id);
            if (k + 1 < n) {
                EXPECT_FALSE(balanced) << "balanced too early for tablet " << tablet_id << " at leave " << k;
            } else {
                EXPECT_TRUE(balanced) << "not balanced after all leaves for tablet " << tablet_id;
            }
        }
    }
}

// (a) Physical split over full-key segments returns the same key-range-restricted rows as legacy.
// The physical split queue produces coarse, block-aligned rowid ranges from a full-key-aware seek;
// reading those ranges back and applying the query key predicate must match a config-off (legacy) run
// and the analytic expectation.
TEST_F(LogicalSplitMorselQueueFullKeyTest, physical_split_full_key_matches_legacy) {
    auto schema = make_schema();

    // c0 in [2, 5) -> rows i in [20, 50).
    auto run = [&](bool full_key) -> std::vector<Row> {
        std::vector<SegmentSharedPtr> a_segs = {write_segment(schema, 0, 0, 50, full_key),
                                                write_segment(schema, 1, 50, 50, full_key)};
        auto rowset = std::make_shared<FakeRowset>(a_segs, /*overlapped=*/false, /*id=*/1);
        std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};
        std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(40001, 100)};

        Morsels morsels;
        morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(40001)));
        PhysicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/15);
        queue.set_tablets(tablets);
        queue.set_tablet_rowsets(tablet_rowsets);
        queue.set_tablet_schema(schema);
        queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

        std::vector<OlapTuple> start{OlapTuple(std::vector<std::string>{"2"})};
        std::vector<OlapTuple> end{OlapTuple(std::vector<std::string>{"5"})};
        queue.set_key_ranges(TabletReaderParams::RangeStartOperation::GE, TabletReaderParams::RangeEndOperation::LT,
                             start, end);

        std::vector<Row> collected;
        while (true) {
            ASSIGN_OR_ABORT(auto morsel, queue.try_get());
            if (morsel == nullptr) break;
            auto* phys = dynamic_cast<PhysicalSplitScanMorsel*>(morsel.get());
            CHECK(phys != nullptr);
            auto rowid_range = phys->get_rowid_range_option();
            CHECK(rowid_range != nullptr);
            for (const auto& segment : rowset->get_segments()) {
                if (!rowid_range->contains_rowset(rowset.get())) continue;
                auto split = rowid_range->get_segment_rowid_range(rowset.get(), segment.get());
                if (split.row_id_range == nullptr) continue;
                auto rows = read_with_rowid_range(segment, schema, split.row_id_range);
                collected.insert(collected.end(), rows.begin(), rows.end());
            }
        }
        return sorted(collected);
    };

    auto full_key_rows = run(true);
    auto legacy_rows = run(false);

    // The physical split emits block-aligned (coarse) rowid ranges, not exact key-range rows: a block
    // whose boundary straddles the range is taken whole. The lower bound therefore backs up one block,
    // so the c0=1 boundary block leaks in for BOTH encodings, and the two encodings do NOT produce
    // byte-identical coarse ranges -- the full-key index brackets the exclusive upper (c0 < 5) tighter
    // than the legacy short-key index, which cannot distinguish "< 5" from "<= 5" on its only indexed
    // column and so conservatively keeps the c0=5 block. That divergence is benign: a coarse bracket may
    // over-include; it must only never drop an in-range row. The correctness invariant is that the
    // full-key bracket never includes a row the legacy bracket excludes.
    EXPECT_TRUE(std::includes(legacy_rows.begin(), legacy_rows.end(), full_key_rows.begin(), full_key_rows.end()))
            << "full-key coarse bracket must not include any row the legacy bracket excludes";

    // A real scan applies the query key predicate (the fine filter) on top of the coarse range. Once
    // restricted to the key range c0 in [2, 5), both encodings return exactly the in-range rows --
    // proving neither coarse bracket dropped an in-range row and the two agree on the final result.
    auto restrict_to_key_range = [](const std::vector<Row>& rows) {
        std::vector<Row> out;
        for (const auto& r : rows) {
            if (r.first >= 2 && r.first < 5) out.push_back(r);
        }
        return out;
    };
    EXPECT_EQ(sorted(expected_rows(20, 50)), restrict_to_key_range(full_key_rows));
    EXPECT_EQ(sorted(expected_rows(20, 50)), restrict_to_key_range(legacy_rows));
}

// (b) Dual-page segments (full page present) but the read gate is OFF before init: the logical split
// degrades to legacy morsels (TRUNCATED pin), and the union still equals the full scan. This is the
// go-forward rollback for logical split -- no segment rewrite, the always-present legacy page is used.
TEST_F(LogicalSplitMorselQueueFullKeyTest, dual_page_read_off_uses_legacy_morsels) {
    config::enable_full_sort_key_index_read = false;
    auto schema = make_schema();

    std::vector<SegmentSharedPtr> segs = {write_segment(schema, 0, 0, 50, /*full_key=*/true),
                                          write_segment(schema, 1, 50, 50, /*full_key=*/true)};
    // The full page is present, but the read gate is off, so the split must not use it.
    ASSERT_TRUE(segs[0]->has_full_sort_key_index_page());
    ASSERT_FALSE(segs[0]->use_full_sort_key_index());
    auto rowset = std::make_shared<FakeRowset>(segs, /*overlapped=*/false, /*id=*/1);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(60001, 100)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(60001)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    std::vector<Row> collected;
    while (true) {
        ASSIGN_OR_ABORT(auto morsel, queue.try_get());
        if (morsel == nullptr) break;
        auto* logical = dynamic_cast<LogicalSplitScanMorsel*>(morsel.get());
        ASSERT_NE(logical, nullptr);
        const auto& ranges = logical->get_short_key_ranges_option()->short_key_ranges;
        for (const auto& r : ranges) {
            if (!r->lower->short_key.empty()) EXPECT_EQ(SHORT_KEY_ENCODING_TRUNCATED, r->lower->encoding);
            if (!r->upper->short_key.empty()) EXPECT_EQ(SHORT_KEY_ENCODING_TRUNCATED, r->upper->encoding);
        }
        for (const auto& segment : rowset->get_segments()) {
            auto rows = read_with_short_key_ranges(segment, schema, ranges);
            collected.insert(collected.end(), rows.begin(), rows.end());
        }
    }
    EXPECT_EQ(sorted(expected_rows(0, 100)), sorted(collected));
}

// (c) ANTI-DRIFT: flip the read config AFTER morsel creation but BEFORE the Slice consumer runs. The
// consumer honors the encoding PINNED into each ShortKeyOption at creation, not the live config, so
// the result stays correct for both flip directions (ON->OFF and OFF->ON).
TEST_F(LogicalSplitMorselQueueFullKeyTest, pinned_encoding_survives_read_config_flip) {
    auto schema = make_schema();

    auto run = [&](bool create_read_on, int64_t tablet_id) {
        config::enable_full_sort_key_index_read = create_read_on;

        std::vector<SegmentSharedPtr> segs = {write_segment(schema, 0, 0, 50, /*full_key=*/true),
                                              write_segment(schema, 1, 50, 50, /*full_key=*/true)};
        auto rowset = std::make_shared<FakeRowset>(segs, /*overlapped=*/false, /*id=*/1);
        std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};
        std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(tablet_id, 100)};

        Morsels morsels;
        morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(tablet_id)));
        LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
        queue.set_tablets(tablets);
        queue.set_tablet_rowsets(tablet_rowsets);
        queue.set_tablet_schema(schema);
        queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

        // Create ALL morsels (this pins the encoding per boundary) under create_read_on.
        const ShortKeyEncodingPB expect_pin =
                create_read_on ? SHORT_KEY_ENCODING_FULL_SORT_KEY : SHORT_KEY_ENCODING_TRUNCATED;
        std::vector<std::vector<ShortKeyRangeOptionPtr>> all_ranges;
        while (true) {
            ASSIGN_OR_ABORT(auto morsel, queue.try_get());
            if (morsel == nullptr) break;
            auto* logical = dynamic_cast<LogicalSplitScanMorsel*>(morsel.get());
            ASSERT_NE(logical, nullptr);
            const auto& ranges = logical->get_short_key_ranges_option()->short_key_ranges;
            for (const auto& r : ranges) {
                if (!r->lower->short_key.empty()) EXPECT_EQ(expect_pin, r->lower->encoding);
                if (!r->upper->short_key.empty()) EXPECT_EQ(expect_pin, r->upper->encoding);
            }
            all_ranges.push_back(ranges);
        }

        // FLIP the read config after creation, before consumption. The pin must win.
        config::enable_full_sort_key_index_read = !create_read_on;

        std::vector<Row> collected;
        for (const auto& ranges : all_ranges) {
            for (const auto& segment : rowset->get_segments()) {
                auto rows = read_with_short_key_ranges(segment, schema, ranges);
                collected.insert(collected.end(), rows.begin(), rows.end());
            }
        }
        EXPECT_EQ(sorted(expected_rows(0, 100)), sorted(collected)) << "create_read_on=" << create_read_on;
    };

    run(/*create_read_on=*/true, 70001);  // pinned FULL, consumed with read off.
    run(/*create_read_on=*/false, 70002); // pinned TRUNCATED, consumed with read on.
}

// (d) A geometry-valid, arity-valid, but KEY-ORDER-CORRUPT full page fails usability validation, so
// decide_tablet_encoding sees use_full_sort_key_index()==false and the split degrades to legacy
// morsels. The union equals the full scan with no gaps/overlaps/duplicates/out-of-range rows.
TEST_F(LogicalSplitMorselQueueFullKeyTest, key_order_corrupt_full_page_falls_back_to_legacy) {
    config::enable_full_sort_key_index_read = true;
    auto schema = make_schema();

    auto corrupt = write_segment_with_corrupt_full_page(schema, /*seg_id=*/0, /*begin=*/0, /*n=*/100);
    // Present but permanently unusable: usability validation rejects the misordered page.
    EXPECT_TRUE(corrupt->has_full_sort_key_index_page());
    EXPECT_FALSE(corrupt->use_full_sort_key_index());

    auto rowset = std::make_shared<FakeRowset>(std::vector<SegmentSharedPtr>{corrupt}, /*overlapped=*/false, /*id=*/1);
    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(80001, 100)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(80001)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    std::vector<Row> collected;
    while (true) {
        ASSIGN_OR_ABORT(auto morsel, queue.try_get());
        if (morsel == nullptr) break;
        auto* logical = dynamic_cast<LogicalSplitScanMorsel*>(morsel.get());
        ASSERT_NE(logical, nullptr) << "corrupt full page must still logically split via the legacy page";
        const auto& ranges = logical->get_short_key_ranges_option()->short_key_ranges;
        for (const auto& r : ranges) {
            if (!r->lower->short_key.empty()) EXPECT_EQ(SHORT_KEY_ENCODING_TRUNCATED, r->lower->encoding);
            if (!r->upper->short_key.empty()) EXPECT_EQ(SHORT_KEY_ENCODING_TRUNCATED, r->upper->encoding);
        }
        for (const auto& segment : rowset->get_segments()) {
            auto rows = read_with_short_key_ranges(segment, schema, ranges);
            collected.insert(collected.end(), rows.begin(), rows.end());
        }
    }
    EXPECT_EQ(sorted(expected_rows(0, 100)), sorted(collected));
}

} // namespace starrocks::pipeline
