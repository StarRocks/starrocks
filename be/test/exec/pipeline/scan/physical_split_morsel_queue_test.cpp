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

#include <gtest/gtest.h>

#include "exec/pipeline/scan/morsel.h"
#include "fs/fs.h"
#include "gen_cpp/InternalService_types.h"
#include "storage/lake/tablet.h"
#include "storage/rowset/base_rowset.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

class PhysicalSplitMorselQueueTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

// Test case: PhysicalSplitMorselQueue crashes when tablet has no rowsets
// This test directly verifies the boundary checks in PhysicalSplitMorselQueue methods:
// _cur_rowset, _cur_segment, _is_last_split_of_current_morsel, _next_segment, _init_segment
TEST_F(PhysicalSplitMorselQueueTest, test_empty_rowset) {
    // Create scan range for the morsel
    TScanRange scan_range;
    TInternalScanRange internal_scan_range;
    internal_scan_range.tablet_id = 10001;
    internal_scan_range.version = "1";
    internal_scan_range.partition_id = 1;
    scan_range.__set_internal_scan_range(internal_scan_range);

    // Create morsels
    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, scan_range));

    // Create PhysicalSplitMorselQueue
    PhysicalSplitMorselQueue queue(std::move(morsels), 1, 1024);

    // Create a lake tablet for testing (nullptr TabletManager is allowed)
    auto tablet = std::make_shared<lake::Tablet>(nullptr, 10001);
    std::vector<BaseTabletSharedPtr> tablets;
    tablets.push_back(tablet);
    queue.set_tablets(tablets);

    // Set up tablet_rowsets with empty rowsets (key scenario for issue #70280)
    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets;
    tablet_rowsets.emplace_back(); // Empty rowsets for the tablet
    queue.set_tablet_rowsets(tablet_rowsets);

    // Set a non-null tablet schema to avoid null dereference in _init_segment
    auto tablet_schema = std::make_shared<TabletSchema>();
    queue.set_tablet_schema(tablet_schema);

    // Call try_get, this should not crash with empty rowsets
    auto result = queue.try_get();
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value(), nullptr);
    ASSERT_TRUE(queue.empty());
}

namespace {
// A BaseRowset whose get_segments() returns an EMPTY list on its first call and a non-empty segment list
// on every call after that. This models a *transient* Rowset::get_segments() failure: lake Rowset returns
// {} on a load failure and does NOT cache it, so the immediate retry re-loads and can succeed. This is the
// exact precondition for issue #75203 -- the first (failing) call makes _init_segment() early-return before
// assigning _segment_range_iter, and the second (succeeding) call lets checks 1-3 fall through to check 4.
class TransientEmptyRowset : public BaseRowset {
public:
    explicit TransientEmptyRowset(SegmentSharedPtr segment) : _segment(std::move(segment)) {}
    RowsetId rowset_id() const override { return RowsetId{}; }
    int64_t num_rows() const override { return 100; }
    bool is_overlapped() const override { return false; }
    std::vector<SegmentSharedPtr> get_segments() override {
        if (_calls++ == 0) {
            return {}; // transient failure: empty list, deliberately uncached
        }
        return {_segment}; // retry succeeds
    }
    bool has_data_files() const override { return true; }
    int64_t start_version() const override { return 0; }
    int64_t end_version() const override { return 0; }

private:
    SegmentSharedPtr _segment;
    int _calls = 0;
};
} // namespace

// Path-level regression for issue #75203: reproduce the crash through the REAL
// PhysicalSplitMorselQueue::_try_get_split_from_single_tablet control flow, driven by a transient
// get_segments() failure (empty then non-empty).
//
// Trace: check 1 (!_has_init_any_segment) fires first; _next_segment() flips it true and _init_segment()
// calls _cur_segment() -> get_segments() call #1 -> {} -> segment==nullptr -> early-return WITHOUT setting
// _segment_range_iter (its _range stays nullptr). Re-eval: check 1 false; check 2 (_cur_segment()==nullptr)
// calls get_segments() #2 -> real segment -> false; check 3 (num_rows()==0) -> false; control reaches
// check 4 (!_segment_range_iter.has_more()). WITHOUT any fix, has_more() dereferences the null _range and
// crashes. WITH the source-level fix (get_segments_checked() primed once in _init_segment), the segment
// list is materialized before check 4, so the range iterator is initialized and the segment's rows are
// actually scanned. try_get() then returns a real split morsel instead of silently dropping the segment,
// which is what the bare has_more() null-guard alone would do.
TEST_F(PhysicalSplitMorselQueueTest, test_transient_get_segments_null_range_75203) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    auto* col = schema_pb.add_column();
    col->set_unique_id(0);
    col->set_name("c0");
    col->set_type("INT");
    col->set_is_key(true);
    col->set_is_nullable(false);
    col->set_length(4);
    col->set_index_length(4);
    auto tablet_schema = TabletSchema::create(schema_pb);
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString("/tmp/sr75203_transient"));

    // Bare segment reporting rows > 0 so check 3 (`_cur_segment()->num_rows() == 0`) is false on the retry.
    auto segment = std::make_shared<Segment>(fs, FileInfo{"/tmp/sr75203_transient/0.dat"}, /*seg_id=*/0, tablet_schema,
                                             /*tablet_manager=*/nullptr);
    segment->set_num_rows(100);

    // Build the scan range inline (this branch has no make_scan_range() helper).
    TScanRange scan_range;
    TInternalScanRange internal_scan_range;
    internal_scan_range.tablet_id = 10001;
    internal_scan_range.version = "1";
    internal_scan_range.partition_id = 1;
    scan_range.__set_internal_scan_range(internal_scan_range);

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, scan_range));
    PhysicalSplitMorselQueue queue(std::move(morsels), /*degree_of_parallelism=*/1, /*splitted_scan_rows=*/1024);

    auto tablet = std::make_shared<lake::Tablet>(nullptr, 10001);
    std::vector<BaseTabletSharedPtr> tablets{tablet};
    queue.set_tablets(tablets);

    // Exactly one rowset, whose first get_segments() transiently returns empty.
    std::vector<std::vector<BaseRowsetSharedPtr>> tablet_rowsets;
    tablet_rowsets.push_back({std::make_shared<TransientEmptyRowset>(segment)});
    queue.set_tablet_rowsets(tablet_rowsets);
    queue.set_tablet_schema(tablet_schema);

    // WITHOUT any fix: SIGSEGV in SparseRangeIterator::has_more(). WITH the source-level fix: the prime
    // materializes the segment, so the split iterator is initialized and the segment's rows are scanned
    // rather than dropped -- try_get() returns a real morsel instead of nullptr.
    auto result = queue.try_get();
    ASSERT_TRUE(result.ok());
    ASSERT_NE(result.value(), nullptr);
}

} // namespace starrocks::pipeline
