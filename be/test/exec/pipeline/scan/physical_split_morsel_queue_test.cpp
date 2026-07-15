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

#include "base/testutil/assert.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec_primitive/pipeline/scan/split_morsel_ticket_checker.h"
#include "fs/fs_factory.h"
#include "gen_cpp/InternalService_types.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/types_fwd.h"
#include "storage/query/split_morsel_queue.h"
#include "storage/query/split_morsel_queue_builder.h"
#include "storage/query/split_scan_morsel.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema.h"

namespace starrocks::pipeline {

class PhysicalSplitMorselQueueTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

namespace {

TScanRange make_scan_range(int64_t tablet_id = 10001) {
    TScanRange scan_range;
    TInternalScanRange internal_scan_range;
    internal_scan_range.tablet_id = tablet_id;
    internal_scan_range.version = "1";
    internal_scan_range.partition_id = 1;
    scan_range.__set_internal_scan_range(internal_scan_range);
    return scan_range;
}

// Build a root ScanMorsel carrying a lake split context (given rowid-range source + prepared state).
// With an empty PreparedTabletReadState the pre-refinement candidate resolves to DEAD (rowset_index 0
// is out of range), so it is never enqueued -- which is exactly what these gate-off/segment-free unit
// tests exercise. Producing a real PRE_REFINEMENT_COARSE split from a LIVE candidate needs a real
// segment and is covered end to end by the follow-up connector PR.
MorselPtr make_lake_split_morsel(LakeSplitContext::RowidRangeSource source,
                                 lake::PreparedTabletReadStatePtr tablet_state,
                                 lake::PreparedSegmentReadStatePtr segment_state) {
    auto morsel = std::make_unique<ScanMorsel>(1, make_scan_range());
    auto ctx = std::make_unique<LakeSplitContext>();
    ctx->rowid_range = std::make_shared<RowidRangeOption>();
    ctx->rowid_range_source = source;
    ctx->prepared_tablet_read_state = std::move(tablet_state);
    ctx->prepared_segment_read_state = std::move(segment_state);
    morsel->set_split_context(std::move(ctx));
    return morsel;
}

} // namespace

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

// Boundary/contract test for the lake prepared-physical-split morsel queue: the root morsels are handed
// out from the internal queue, and once it drains -- with no pre-refinement candidates registered --
// try_get yields nullptr and the queue reports empty.
TEST_F(PhysicalSplitMorselQueueTest, test_lake_prepared_physical_split_basic) {
    TScanRange scan_range;
    TInternalScanRange internal_scan_range;
    internal_scan_range.tablet_id = 10001;
    internal_scan_range.version = "1";
    internal_scan_range.partition_id = 1;
    scan_range.__set_internal_scan_range(internal_scan_range);

    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, scan_range));
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, scan_range));

    LakePreparedPhysicalSplitMorselQueue queue(std::move(morsels), /*has_more_scan_ranges=*/false,
                                               /*splitted_scan_rows=*/1024, /*degree_of_parallelism=*/2);

    EXPECT_EQ(MorselQueue::LAKE_PREPARED_PHYSICAL_SPLIT, queue.type());
    EXPECT_EQ(2u, queue.max_degree_of_parallelism());
    EXPECT_TRUE(queue.could_attch_ticket_checker());
    queue.set_ticket_checker(std::make_shared<SplitMorselTicketChecker>());
    EXPECT_FALSE(queue.empty());

    // The two root morsels are handed out from the internal queue.
    auto r1 = queue.try_get();
    ASSERT_TRUE(r1.ok());
    ASSERT_NE(r1.value(), nullptr);
    auto r2 = queue.try_get();
    ASSERT_TRUE(r2.ok());
    ASSERT_NE(r2.value(), nullptr);

    // Queue drained, no pre-refinement candidates -> try_get yields nullptr and the queue is empty.
    auto r3 = queue.try_get();
    ASSERT_TRUE(r3.ok());
    ASSERT_EQ(r3.value(), nullptr);
    ASSERT_TRUE(queue.empty());
}

// An INITIAL_COARSE morsel whose prepared state is present but empty: the ctor funnels it through
// append_morsels -> enqueue_pre_refinement_candidate, which builds the candidate and finds it DEAD
// (rowset_index 0 is out of range for the empty PreparedTabletReadState), so nothing is enqueued.
// The root morsel is still handed out; then the queue drains to nullptr and reports not-ready.
TEST_F(PhysicalSplitMorselQueueTest, test_lake_prepared_physical_split_dead_candidate) {
    Morsels morsels;
    morsels.emplace_back(make_lake_split_morsel(LakeSplitContext::RowidRangeSource::INITIAL_COARSE,
                                                std::make_shared<lake::PreparedTabletReadState>(),
                                                std::make_shared<lake::PreparedSegmentReadState>()));
    LakePreparedPhysicalSplitMorselQueue queue(std::move(morsels), /*has_more_scan_ranges=*/false,
                                               /*splitted_scan_rows=*/1024, /*degree_of_parallelism=*/2);

    auto ready1 = queue.ready_for_next();
    ASSERT_TRUE(ready1.ok());
    EXPECT_TRUE(ready1.value()); // a root morsel is queued

    auto r1 = queue.try_get();
    ASSERT_TRUE(r1.ok());
    ASSERT_NE(r1.value(), nullptr);

    auto r2 = queue.try_get(); // no LIVE pre-refinement candidate -> nullptr
    ASSERT_TRUE(r2.ok());
    ASSERT_EQ(r2.value(), nullptr);
    EXPECT_TRUE(queue.empty());

    auto ready2 = queue.ready_for_next();
    ASSERT_TRUE(ready2.ok());
    EXPECT_FALSE(ready2.value());
}

// enqueue_pre_refinement_candidate must skip morsels that are not eligible candidates: no split context,
// a non-INITIAL_COARSE source, or a null prepared state. None register a candidate; all root morsels are
// still handed out and the queue then drains.
TEST_F(PhysicalSplitMorselQueueTest, test_lake_prepared_physical_split_enqueue_skips_non_candidates) {
    Morsels morsels;
    // (a) plain ScanMorsel with no split context.
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range()));
    // (b) a REGULAR (non-INITIAL_COARSE) lake split context.
    morsels.emplace_back(make_lake_split_morsel(LakeSplitContext::RowidRangeSource::REGULAR,
                                                std::make_shared<lake::PreparedTabletReadState>(),
                                                std::make_shared<lake::PreparedSegmentReadState>()));
    // (c) INITIAL_COARSE but with a null prepared state.
    morsels.emplace_back(make_lake_split_morsel(LakeSplitContext::RowidRangeSource::INITIAL_COARSE, nullptr, nullptr));
    LakePreparedPhysicalSplitMorselQueue queue(std::move(morsels), /*has_more_scan_ranges=*/false,
                                               /*splitted_scan_rows=*/1024, /*degree_of_parallelism=*/3);

    for (int i = 0; i < 3; ++i) {
        auto r = queue.try_get();
        ASSERT_TRUE(r.ok());
        ASSERT_NE(r.value(), nullptr);
    }
    auto drained = queue.try_get();
    ASSERT_TRUE(drained.ok());
    ASSERT_EQ(drained.value(), nullptr);
    EXPECT_TRUE(queue.empty());
}

// unget stashes a morsel so the next try_get returns it before consuming the queue again.
TEST_F(PhysicalSplitMorselQueueTest, test_lake_prepared_physical_split_unget) {
    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range()));
    LakePreparedPhysicalSplitMorselQueue queue(std::move(morsels), /*has_more_scan_ranges=*/false,
                                               /*splitted_scan_rows=*/1024, /*degree_of_parallelism=*/1);

    auto r1 = queue.try_get();
    ASSERT_TRUE(r1.ok());
    ASSERT_NE(r1.value(), nullptr);

    queue.unget(std::move(r1.value()));
    auto r2 = queue.try_get(); // returns the ungot morsel
    ASSERT_TRUE(r2.ok());
    ASSERT_NE(r2.value(), nullptr);

    auto r3 = queue.try_get();
    ASSERT_TRUE(r3.ok());
    ASSERT_EQ(r3.value(), nullptr);
}

// append_morsels adds more root morsels to a live queue; all are handed out then the queue drains.
TEST_F(PhysicalSplitMorselQueueTest, test_lake_prepared_physical_split_append_morsels) {
    Morsels initial;
    initial.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range()));
    LakePreparedPhysicalSplitMorselQueue queue(std::move(initial), /*has_more_scan_ranges=*/false,
                                               /*splitted_scan_rows=*/1024, /*degree_of_parallelism=*/2);

    Morsels more;
    more.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range()));
    more.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range()));
    ASSERT_TRUE(queue.append_morsels(std::move(more)).ok());

    int count = 0;
    while (true) {
        auto r = queue.try_get();
        ASSERT_TRUE(r.ok());
        if (r.value() == nullptr) break;
        ++count;
    }
    EXPECT_EQ(3, count); // 1 initial + 2 appended
    EXPECT_TRUE(queue.empty());
}

// The builder produces a LakePreparedPhysicalSplitMorselQueue that hands out its root morsels.
TEST_F(PhysicalSplitMorselQueueTest, test_lake_prepared_physical_split_builder) {
    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range()));
    auto builder = make_lake_prepared_physical_split_morsel_queue_builder(
            std::move(morsels), /*has_more_scan_ranges=*/false, /*max_degree_of_parallelism=*/2,
            /*splitted_scan_rows=*/1024);
    ASSERT_NE(builder, nullptr);
    EXPECT_TRUE(builder->can_uniform_distribute());

    auto queue_or = builder->build();
    ASSERT_TRUE(queue_or.ok());
    auto queue = std::move(queue_or.value());
    ASSERT_NE(queue, nullptr);
    EXPECT_EQ(MorselQueue::LAKE_PREPARED_PHYSICAL_SPLIT, queue->type());

    auto r = queue->try_get();
    ASSERT_TRUE(r.ok());
    ASSERT_NE(r.value(), nullptr);

    // build_from_morsels builds another queue from a fresh morsel set.
    auto builder2 = make_lake_prepared_physical_split_morsel_queue_builder(
            Morsels{}, /*has_more_scan_ranges=*/false, /*max_degree_of_parallelism=*/2, /*splitted_scan_rows=*/1024);
    Morsels more;
    more.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range()));
    auto q2 = builder2->build_from_morsels(std::move(more));
    ASSERT_TRUE(q2.ok());
    ASSERT_NE(q2.value(), nullptr);
    EXPECT_EQ(MorselQueue::LAKE_PREPARED_PHYSICAL_SPLIT, q2.value()->type());
}

// A LIVE pre-refinement candidate: an INITIAL_COARSE morsel carrying a prepared read state whose coarse
// cursor is open. Once the root morsel drains, try_get allocates PRE_REFINEMENT_COARSE splits from the
// coarse range until it is exhausted. The Rowset/Segment are bare (never opened) -- allocate only stores
// their pointers in the RowidRangeOption, so no on-disk segment is needed.
TEST_F(PhysicalSplitMorselQueueTest, test_lake_prepared_physical_split_live_pre_refinement) {
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
    RowsetMetadataPB rowset_meta; // must outlive the bare Rowset below
    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString("/tmp/p3c_live_pre_refinement"));

    auto rowset = std::make_shared<lake::Rowset>(/*tablet_mgr=*/nullptr, /*tablet_id=*/10001, &rowset_meta,
                                                 /*index=*/0, tablet_schema);
    auto segment = std::make_shared<Segment>(fs, FileInfo{"/tmp/p3c_live_pre_refinement/0.dat"}, /*seg_id=*/0,
                                             tablet_schema, /*tablet_manager=*/nullptr);

    auto tablet_state = std::make_shared<lake::PreparedTabletReadState>();
    tablet_state->rowsets = {rowset};
    tablet_state->rowset_segments = {{segment}};
    auto segment_state = std::make_shared<lake::PreparedSegmentReadState>();
    tablet_state->rowset_prepared_states = {{segment_state}};

    // Open the coarse cursor over [0, 100) -- mirrors init_coarse_split_allocation_state -- so the
    // INITIAL_COARSE morsel resolves to a LIVE candidate.
    {
        std::lock_guard<std::mutex> guard(segment_state->coarse_range_lock);
        segment_state->coarse_scan_range.add(Range<>(0, 100));
        segment_state->coarse_scan_range_iter = segment_state->coarse_scan_range.new_iterator();
        segment_state->allocated_coarse_ranges.clear();
        segment_state->coarse_split_allocation_closed = false;
    }

    Morsels morsels;
    {
        auto morsel = std::make_unique<ScanMorsel>(1, make_scan_range());
        auto ctx = std::make_unique<LakeSplitContext>();
        ctx->rowid_range = std::make_shared<RowidRangeOption>();
        ctx->rowid_range_source = LakeSplitContext::RowidRangeSource::INITIAL_COARSE;
        ctx->prepared_tablet_read_state = tablet_state;
        ctx->prepared_segment_read_state = segment_state;
        ctx->rowset_index = 0;
        ctx->segment_index = 0;
        morsel->set_split_context(std::move(ctx));
        morsels.emplace_back(std::move(morsel));
    }
    LakePreparedPhysicalSplitMorselQueue queue(std::move(morsels), /*has_more_scan_ranges=*/false,
                                               /*splitted_scan_rows=*/40, /*degree_of_parallelism=*/2);

    // First the root INITIAL_COARSE morsel is handed out (and re-enqueued as a LIVE candidate).
    auto r0 = queue.try_get();
    ASSERT_TRUE(r0.ok());
    ASSERT_NE(r0.value(), nullptr);

    // Then PRE_REFINEMENT_COARSE splits are allocated from the coarse range until it is exhausted.
    int pre_refine = 0;
    int guard = 0;
    while (true) {
        ASSERT_LT(guard++, 100) << "runaway pre-refinement loop";
        auto r = queue.try_get();
        ASSERT_TRUE(r.ok());
        if (r.value() == nullptr) {
            break;
        }
        auto* ctx = dynamic_cast<LakeSplitContext*>(r.value()->get_split_context());
        ASSERT_NE(ctx, nullptr);
        EXPECT_EQ(LakeSplitContext::RowidRangeSource::PRE_REFINEMENT_COARSE, ctx->rowid_range_source);
        EXPECT_NE(ctx->rowid_range, nullptr);
        EXPECT_EQ(ctx->prepared_tablet_read_state, tablet_state);
        ++pre_refine;
    }
    EXPECT_GT(pre_refine, 0) << "a LIVE candidate must yield PRE_REFINEMENT_COARSE splits";
    EXPECT_EQ(100u, segment_state->allocated_coarse_ranges.span_size()); // whole coarse range allocated
    EXPECT_TRUE(queue.empty());
}

// prepare_olap_scan_ranges returns one TInternalScanRange* per queued root morsel.
TEST_F(PhysicalSplitMorselQueueTest, test_lake_prepared_physical_split_prepare_olap_scan_ranges) {
    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(777)));
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, make_scan_range(888)));
    LakePreparedPhysicalSplitMorselQueue queue(std::move(morsels), /*has_more_scan_ranges=*/false,
                                               /*splitted_scan_rows=*/1024, /*degree_of_parallelism=*/2);
    auto ranges = queue.prepare_olap_scan_ranges();
    EXPECT_EQ(2u, ranges.size());
}

} // namespace starrocks::pipeline
