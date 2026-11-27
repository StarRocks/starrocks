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

// End-to-end coverage for the logical split morsel queue over real segments: writes segments,
// drives the queue's try_get to end-of-queue, reads the emitted morsels back through the segment
// iterator, and asserts the union of returned rows equals a plain full scan of the same data.
// This is the wrong-rows guard for logical splitting -- the other morsel queue tests never execute
// a split.

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/global_dict/types.h"
#include "exec_primitive/pipeline/scan/split_morsel_ticket_checker.h"
#include "fs/fs_memory.h"
#include "gen_cpp/InternalService_types.h"
#include "storage/base_tablet.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/query/split_morsel_queue.h"
#include "storage/query/split_scan_morsel.h"
#include "storage/rowset/base_rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage_primitive/chunk_iterator.h"
#include "types/datum.h"

namespace starrocks::pipeline {

namespace {

using Row = std::pair<int32_t, int32_t>;

// (c0 int key, c1 int key, v int value); sort key = (c0, c1) with only 1 short key column, so the
// short key index covers c0 alone while a split range's endpoints still have to bracket rows
// correctly in (c0, c1) space.
std::shared_ptr<TabletSchema> make_schema() {
    std::vector<ColumnPB> cols;
    cols.push_back(create_int_key_pb(/*id=*/1));
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

TScanRange make_scan_range(int64_t tablet_id, int64_t version = 1) {
    TScanRange scan_range;
    TInternalScanRange internal;
    internal.tablet_id = tablet_id;
    internal.version = std::to_string(version);
    internal.partition_id = 1;
    scan_range.__set_internal_scan_range(internal);
    return scan_range;
}

// Minimal BaseRowset that just wraps pre-built, pre-index-loaded segments.
class FakeRowset : public BaseRowset {
public:
    FakeRowset(std::vector<SegmentSharedPtr> segments, bool overlapped, int64_t id, Version version = Version(1, 1))
            : _segments(std::move(segments)) {
        _rowset_id.init(id);
        RowsetMetaPB pb;
        pb.set_rowset_id(_rowset_id.to_string());
        pb.set_num_segments(_segments.size());
        pb.set_start_version(version.first);
        pb.set_end_version(version.second);
        pb.set_segments_overlap_pb(overlapped ? OVERLAPPING : NONOVERLAPPING);
        _meta = std::make_shared<RowsetMeta>(pb);
    }
    RowsetId rowset_id() const override { return _rowset_id; }
    int64_t num_rows() const override {
        int64_t n = 0;
        for (const auto& s : _segments) {
            if (s != nullptr) n += s->num_rows();
        }
        return n;
    }
    bool is_overlapped() const override { return _meta->is_segments_overlapping(); }
    std::vector<SegmentSharedPtr> get_segments() override { return _segments; }
    bool has_data_files() const override { return true; }
    int64_t start_version() const override { return _meta->start_version(); }
    int64_t end_version() const override { return _meta->end_version(); }

private:
    std::vector<SegmentSharedPtr> _segments;
    RowsetMetaSharedPtr _meta;
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

class LogicalSplitMorselQueueTest : public ::testing::Test {
public:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_OK(_fs->create_dir(kDir));
    }

    // Write one segment and return it opened and index-loaded.
    SegmentSharedPtr write_segment(const std::shared_ptr<TabletSchema>& schema, uint32_t seg_id, int begin, int n) {
        std::string path = kDir + "/seg_" + std::to_string(_next_file_seq++) + ".dat";
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

        ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{path}, seg_id, schema));
        CHECK_OK(segment->load_index());
        return segment;
    }

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

    // Read one segment restricted to |short_key_ranges| -> the rows landing in those key ranges.
    std::vector<Row> read_with_short_key_ranges(const SegmentSharedPtr& segment,
                                                const std::shared_ptr<TabletSchema>& schema,
                                                const std::vector<ShortKeyRangeOptionPtr>& short_key_ranges) {
        // (c0, c1): the sort-key columns, in sort-key order.
        Schema read_schema = ChunkHelper::convert_schema(schema, std::vector<ColumnId>{0, 1});
        OlapReaderStatistics stats;
        SegmentReadOptions o;
        o.fs = _fs;
        o.stats = &stats;
        o.short_key_ranges = short_key_ranges;
        return drain(new_segment_iterator(segment, read_schema, o));
    }

protected:
    std::shared_ptr<MemoryFileSystem> _fs;
    const std::string kDir = "/logical_split_test";
    uint32_t _next_file_seq = 0;
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

// The logically split morsels' short-key ranges, read back over every segment of every rowset,
// cover exactly the full scan -- no dropped and no duplicated row. The largest (source) rowset has
// two segments; a second rowset's keys sit above the source group's window, so its rows land in the
// last (open-ended) split range, proving the source-group boundaries apply correctly to other
// rowsets.
TEST_F(LogicalSplitMorselQueueTest, logical_split_matches_full_scan) {
    auto schema = make_schema();

    std::vector<SegmentSharedPtr> a_segs = {write_segment(schema, 0, 0, 50), write_segment(schema, 1, 50, 50)};
    auto rowset_a = std::make_shared<FakeRowset>(a_segs, /*overlapped=*/false, /*id=*/1);
    std::vector<SegmentSharedPtr> b_segs = {write_segment(schema, 0, 100, 50)};
    auto rowset_b = std::make_shared<FakeRowset>(b_segs, /*overlapped=*/false, /*id=*/2);

    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset_a, rowset_b}};
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(10003, 150)};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(10003)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, /*splitted_scan_rows=*/25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    int num_splits = 0;
    std::vector<Row> collected;
    while (true) {
        ASSIGN_OR_ABORT(auto morsel, queue.try_get());
        if (morsel == nullptr) break;
        auto* logical = dynamic_cast<LogicalSplitScanMorsel*>(morsel.get());
        ASSERT_NE(logical, nullptr);
        ++num_splits;
        const auto& ranges = logical->get_short_key_ranges_option()->short_key_ranges;
        for (const auto& rowset : tablet_rowsets[0]) {
            for (const auto& segment : rowset->get_segments()) {
                auto rows = read_with_short_key_ranges(segment, schema, ranges);
                collected.insert(collected.end(), rows.begin(), rows.end());
            }
        }
    }
    // The tablet really was split -- otherwise the union property below is vacuous.
    ASSERT_GT(num_splits, 1);
    EXPECT_EQ(sorted(expected_rows(0, 150)), sorted(collected));
}

TEST_F(LogicalSplitMorselQueueTest, version_overwrite_preserves_overlapping_segments) {
    auto schema = make_schema();
    std::vector<SegmentSharedPtr> segments = {write_segment(schema, 0, 50, 100), write_segment(schema, 1, 0, 100)};
    auto rowset = std::make_shared<FakeRowset>(segments, true, 1, Version(0, 2));
    std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(10003, 200)};
    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(10003, 2)));
    LogicalSplitMorselQueue queue(std::move(morsels), 1, 25);
    queue.set_tablets(tablets);
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(schema);
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

    std::vector<Row> expected;
    for (const auto& segment : segments) {
        auto rows = read_with_short_key_ranges(segment, schema, {});
        expected.insert(expected.end(), rows.begin(), rows.end());
    }
    std::vector<Row> actual;
    int num_splits = 0;
    while (true) {
        ASSIGN_OR_ABORT(auto morsel, queue.try_get());
        if (morsel == nullptr) break;
        auto* logical = dynamic_cast<LogicalSplitScanMorsel*>(morsel.get());
        ASSERT_NE(logical, nullptr);
        ++num_splits;
        for (const auto& segment : segments) {
            auto rows = read_with_short_key_ranges(segment, schema,
                                                   logical->get_short_key_ranges_option()->short_key_ranges);
            actual.insert(actual.end(), rows.begin(), rows.end());
        }
    }
    ASSERT_GT(num_splits, 1);
    EXPECT_EQ(sorted(expected), sorted(actual));
}

namespace {

// A rowset whose segments fail to load, the way a lake Rowset behaves when a segment open fails
// (e.g. a StarMgr GetShard timeout): get_segments() swallows the error and returns an empty list,
// get_segments_checked() returns it. num_rows() comes from the rowset metadata, so it stays non-zero.
class LoadFailingRowset : public BaseRowset {
public:
    LoadFailingRowset(bool overlapped, int64_t num_rows, Status load_error)
            : _overlapped(overlapped), _num_rows(num_rows), _load_error(std::move(load_error)) {
        _rowset_id.init(1);
    }
    RowsetId rowset_id() const override { return _rowset_id; }
    int64_t num_rows() const override { return _num_rows; }
    bool is_overlapped() const override { return _overlapped; }
    std::vector<SegmentSharedPtr> get_segments() override { return {}; }
    StatusOr<std::vector<SegmentSharedPtr>> get_segments_checked() override { return _load_error; }
    bool has_data_files() const override { return true; }
    int64_t start_version() const override { return 1; }
    int64_t end_version() const override { return 1; }

private:
    bool _overlapped;
    int64_t _num_rows;
    Status _load_error;
    RowsetId _rowset_id;
};

} // namespace

// RB-2037: a segment-load failure of the largest rowset must fail the query with the load error. An
// overlapped rowset used to turn the empty get_segments() result into a null largest segment and
// crash the CN in Segment::load_index(); a non-overlapped one was split as if it had no segment.
TEST_F(LogicalSplitMorselQueueTest, segment_load_failure_fails_query) {
    auto schema = make_schema();
    const std::string kLoadError = "starlet err grpc.GetShard(shardId=888596) error: Deadline Exceeded";
    for (bool overlapped : {true, false}) {
        SCOPED_TRACE(overlapped ? "overlapped" : "non-overlapped");
        auto rowset = std::make_shared<LoadFailingRowset>(overlapped, 100, Status::InternalError(kLoadError));
        std::vector<BaseTabletSharedPtr> tablets = {std::make_shared<FakeTablet>(10003, 100)};
        std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets = {{rowset}};

        Morsels morsels;
        morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(10003)));
        LogicalSplitMorselQueue queue(std::move(morsels), 1, 25);
        queue.set_tablets(tablets);
        queue.set_tablet_rowsets(tablet_rowsets);
        queue.set_tablet_schema(schema);
        queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());

        auto morsel_or = queue.try_get();
        ASSERT_FALSE(morsel_or.ok());
        EXPECT_NE(morsel_or.status().to_string().find(kLoadError), std::string::npos) << morsel_or.status();
    }
}

} // namespace starrocks::pipeline
