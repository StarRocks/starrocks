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

#include "storage/lake/tablet_reshard_helper.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>
#include <numeric>
#include <optional>
#include <random>

#include "storage/datum_variant.h"
#include "storage/types.h"
#include "storage/variant_tuple.h"

namespace starrocks::lake {

using namespace tablet_reshard_helper;

class TabletReshardHelperTest : public ::testing::Test {};

namespace {

int64_t sum_of(const std::vector<int64_t>& v) {
    return std::accumulate(v.begin(), v.end(), int64_t{0});
}

} // namespace

// -----------------------------------------------------------------------------
// Proportional anchor helpers. allocate_proportionally and
// cap_and_redistribute_dels back the per-rowset anchor pass in
// tablet_splitter (split_tablet → get_tablet_split_ranges → apply_rowset_anchor)
// so re-splits preserve `Σ children == parent` exactly. End-to-end conservation
// under split is covered by LakeTabletReshardTest in tablet_reshard_test.cpp.
// -----------------------------------------------------------------------------

TEST_F(TabletReshardHelperTest, allocate_proportionally_basic_conservation) {
    std::vector<int64_t> out(3, 0);
    allocate_proportionally(/*total=*/12, /*weights=*/{1, 2, 3}, &out);
    EXPECT_EQ(12, sum_of(out));
    EXPECT_EQ((std::vector<int64_t>{2, 4, 6}), out);
}

TEST_F(TabletReshardHelperTest, allocate_proportionally_largest_remainder) {
    // total=10, weights=[1,1,1] → base=3 each, leftover=1 → first index gets +1.
    std::vector<int64_t> out(3, 0);
    allocate_proportionally(/*total=*/10, /*weights=*/{1, 1, 1}, &out);
    EXPECT_EQ(10, sum_of(out));
    EXPECT_EQ((std::vector<int64_t>{4, 3, 3}), out);
}

TEST_F(TabletReshardHelperTest, allocate_proportionally_tiebreak_deterministic) {
    // Equal weights and tied remainders fall to the lowest indices. Run twice
    // and compare to confirm reproducibility.
    std::vector<int64_t> a(5, 0), b(5, 0);
    allocate_proportionally(7, {1, 1, 1, 1, 1}, &a);
    allocate_proportionally(7, {1, 1, 1, 1, 1}, &b);
    EXPECT_EQ(7, sum_of(a));
    EXPECT_EQ((std::vector<int64_t>{2, 2, 1, 1, 1}), a);
    EXPECT_EQ(a, b);
}

TEST_F(TabletReshardHelperTest, allocate_proportionally_zero_total) {
    std::vector<int64_t> out(4, 99);
    allocate_proportionally(0, {3, 5, 7, 9}, &out);
    EXPECT_EQ((std::vector<int64_t>{0, 0, 0, 0}), out);
}

TEST_F(TabletReshardHelperTest, allocate_proportionally_zero_weights_uniform_fallback) {
    std::vector<int64_t> out(3, 0);
    allocate_proportionally(10, {0, 0, 0}, &out);
    EXPECT_EQ(10, sum_of(out));
    // floor(10/3)=3 each, remainder 1 → first index gets +1.
    EXPECT_EQ((std::vector<int64_t>{4, 3, 3}), out);
}

TEST_F(TabletReshardHelperTest, allocate_proportionally_overflow_safe_at_extreme_scale) {
    // total * weight overflows int64 without __int128 (1e10 * 1e10 = 1e20).
    constexpr int64_t kTotal = 10'000'000'000LL;
    std::vector<int64_t> weights{kTotal, kTotal};
    std::vector<int64_t> out(2, 0);
    allocate_proportionally(kTotal, weights, &out);
    EXPECT_EQ(kTotal, sum_of(out));
    EXPECT_EQ(kTotal / 2, out[0]);
    EXPECT_EQ(kTotal / 2, out[1]);
}

// Regression: when Σ weights overflows int64_t (each weight ≈ INT64_MAX/2 + 1,
// sum > INT64_MAX), the largest-remainder routine must keep remainders in
// __int128 — truncating to int64_t there would corrupt the tie-break order.
// Hare-Niemeyer for two equal weights and an even total still gives an even
// split; the test asserts both Σ conservation and the deterministic
// per-bucket value.
TEST_F(TabletReshardHelperTest, allocate_proportionally_handles_sum_weights_above_int64_max) {
    constexpr int64_t kHalfPlus = (std::numeric_limits<int64_t>::max() / 2) + 1;
    // 2 * kHalfPlus exceeds INT64_MAX (overflows in int64), but sum_w stays
    // safe in __int128.
    std::vector<int64_t> weights{kHalfPlus, kHalfPlus};
    std::vector<int64_t> out(2, 0);
    allocate_proportionally(/*total=*/100, weights, &out);
    EXPECT_EQ(100, sum_of(out));
    EXPECT_EQ(50, out[0]);
    EXPECT_EQ(50, out[1]);
}

TEST_F(TabletReshardHelperTest, cap_and_redistribute_dels_no_overflow_is_no_op) {
    std::vector<int64_t> dels{2, 3, 1};
    cap_and_redistribute_dels(/*rows=*/{10, 10, 10}, &dels);
    EXPECT_EQ((std::vector<int64_t>{2, 3, 1}), dels);
}

TEST_F(TabletReshardHelperTest, cap_and_redistribute_dels_basic_overflow) {
    // dels[0] over-allocated by 5; redistribute to bucket 2 which has the
    // largest headroom. Σ preserved at 20.
    std::vector<int64_t> dels{15, 5, 0};
    cap_and_redistribute_dels(/*rows=*/{10, 10, 10}, &dels);
    EXPECT_EQ(20, sum_of(dels));
    EXPECT_EQ(10, dels[0]);
    // Bucket 1 has headroom 5, bucket 2 has 10 — headroom desc puts 2 ahead of 1
    // (10 > 5). So overflow=5 goes to bucket 2 first.
    EXPECT_EQ(5, dels[1]);
    EXPECT_EQ(5, dels[2]);
    for (size_t i = 0; i < dels.size(); ++i) {
        EXPECT_LE(dels[i], 10);
    }
}

TEST_F(TabletReshardHelperTest, cap_and_redistribute_dels_skewed_headrooms_break_by_index) {
    // Σ pre-cap dels = 20, Σ rows = 25 (feasible). After cap dels[0] from 10
    // to 5, overflow=5; headroom=[0,5,5] → buckets 1 and 2 tie, index-asc
    // tie-break sends overflow to bucket 1 first.
    std::vector<int64_t> dels{10, 5, 5}; // pre-cap, Σ=20
    cap_and_redistribute_dels(/*rows=*/{5, 10, 10}, &dels);
    EXPECT_EQ(20, sum_of(dels));
    EXPECT_EQ(5, dels[0]);
    EXPECT_EQ(10, dels[1]); // takes the full overflow=5 (tie-break by index)
    EXPECT_EQ(5, dels[2]);
}

TEST_F(TabletReshardHelperTest, cap_and_redistribute_dels_invalid_parent_input_capped_to_rows_sum) {
    // Σ pre-cap dels (30) > Σ rows (15). Best-effort contract: per-bucket cap
    // honored, Σ dels post equals Σ rows (= 15).
    std::vector<int64_t> dels{20, 5, 5};
    cap_and_redistribute_dels(/*rows=*/{5, 5, 5}, &dels);
    EXPECT_EQ(15, sum_of(dels));
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_LE(dels[i], 5);
    }
}

TEST_F(TabletReshardHelperTest, cap_and_redistribute_dels_fuzz_conservation_and_cap) {
    // 1000 random trials with feasible inputs (Σ dels ≤ Σ rows). Helper must
    // preserve Σ exactly and respect per-bucket cap.
    std::mt19937_64 rng(42);
    for (int trial = 0; trial < 1000; ++trial) {
        const int n = 1 + (rng() % 8);
        std::vector<int64_t> rows(n, 0);
        std::vector<int64_t> dels(n, 0);
        int64_t total_rows = 0;
        for (int i = 0; i < n; ++i) {
            rows[i] = static_cast<int64_t>(rng() % 1'000'000);
            total_rows += rows[i];
        }
        // Pick parent.dels in [0, total_rows]; distribute it arbitrarily across
        // buckets (may individually exceed rows[i]).
        const int64_t parent_dels = total_rows == 0 ? 0 : static_cast<int64_t>(rng() % (total_rows + 1));
        int64_t remaining = parent_dels;
        for (int i = 0; i < n - 1 && remaining > 0; ++i) {
            const int64_t take = static_cast<int64_t>(rng() % (remaining + 1));
            dels[i] = take;
            remaining -= take;
        }
        if (n > 0) dels[n - 1] = remaining;

        cap_and_redistribute_dels(rows, &dels);
        EXPECT_EQ(parent_dels, sum_of(dels)) << "trial=" << trial;
        for (int i = 0; i < n; ++i) {
            EXPECT_LE(dels[i], rows[i]) << "trial=" << trial << " bucket=" << i;
            EXPECT_GE(dels[i], 0) << "trial=" << trial << " bucket=" << i;
        }
    }
}

// Verify that set_all_data_files_shared(TxnLogPB*) marks all SST-related fields
// as shared=true, including the ssts fields in OpWrite and OpCompaction that were
// previously missed.
TEST_F(TabletReshardHelperTest, test_set_all_data_files_shared_covers_ssts) {
    TxnLogPB txn_log;

    // Setup OpWrite with rowset, del_files, and ssts (all shared=false initially)
    {
        auto* op_write = txn_log.mutable_op_write();
        auto* rowset = op_write->mutable_rowset();
        rowset->add_segments("seg1.dat");
        rowset->add_segments("seg2.dat");
        auto* del = rowset->add_del_files();
        del->set_name("del1.dat");

        auto* sst1 = op_write->add_ssts();
        sst1->set_name("write_sst1.sst");
        sst1->set_shared(false);
        auto* sst2 = op_write->add_ssts();
        sst2->set_name("write_sst2.sst");
        sst2->set_shared(false);
    }

    // Setup OpCompaction with output_rowset, output_sstable, output_sstables, and ssts
    {
        auto* op_compaction = txn_log.mutable_op_compaction();
        auto* output_rowset = op_compaction->mutable_output_rowset();
        output_rowset->add_segments("compact_seg.dat");

        op_compaction->mutable_output_sstable()->set_filename("output.sst");
        op_compaction->mutable_output_sstable()->set_shared(false);

        auto* out_sst = op_compaction->add_output_sstables();
        out_sst->set_filename("output2.sst");
        out_sst->set_shared(false);

        auto* sst1 = op_compaction->add_ssts();
        sst1->set_name("compact_sst1.sst");
        sst1->set_shared(false);
        auto* sst2 = op_compaction->add_ssts();
        sst2->set_name("compact_sst2.sst");
        sst2->set_shared(false);
    }

    // Setup OpSchemaChange with rowsets and delvec_meta
    {
        auto* op_schema_change = txn_log.mutable_op_schema_change();
        auto* rowset = op_schema_change->add_rowsets();
        rowset->add_segments("sc_seg.dat");
        auto* del = rowset->add_del_files();
        del->set_name("sc_del.dat");
        auto& delvec_file = (*op_schema_change->mutable_delvec_meta()->mutable_version_to_file())[1];
        delvec_file.set_name("delvec1.dat");
        delvec_file.set_shared(false);
    }

    // Call the function under test
    set_all_data_files_shared(&txn_log);

    // Verify OpWrite
    {
        const auto& op_write = txn_log.op_write();
        // Rowset shared_segments
        ASSERT_EQ(op_write.rowset().shared_segments_size(), 2);
        EXPECT_TRUE(op_write.rowset().shared_segments(0));
        EXPECT_TRUE(op_write.rowset().shared_segments(1));
        // Del files
        EXPECT_TRUE(op_write.rowset().del_files(0).shared());
        // SSTs in OpWrite
        ASSERT_EQ(op_write.ssts_size(), 2);
        EXPECT_TRUE(op_write.ssts(0).shared());
        EXPECT_TRUE(op_write.ssts(1).shared());
    }

    // Verify OpCompaction
    {
        const auto& op_compaction = txn_log.op_compaction();
        // Output rowset shared_segments
        ASSERT_EQ(op_compaction.output_rowset().shared_segments_size(), 1);
        EXPECT_TRUE(op_compaction.output_rowset().shared_segments(0));
        // Output sstable
        EXPECT_TRUE(op_compaction.output_sstable().shared());
        // Output sstables (repeated)
        ASSERT_EQ(op_compaction.output_sstables_size(), 1);
        EXPECT_TRUE(op_compaction.output_sstables(0).shared());
        // SSTs in OpCompaction
        ASSERT_EQ(op_compaction.ssts_size(), 2);
        EXPECT_TRUE(op_compaction.ssts(0).shared());
        EXPECT_TRUE(op_compaction.ssts(1).shared());
    }

    // Verify OpSchemaChange
    {
        const auto& op_schema_change = txn_log.op_schema_change();
        ASSERT_EQ(op_schema_change.rowsets(0).shared_segments_size(), 1);
        EXPECT_TRUE(op_schema_change.rowsets(0).shared_segments(0));
        EXPECT_TRUE(op_schema_change.rowsets(0).del_files(0).shared());
        const auto& vtf = op_schema_change.delvec_meta().version_to_file();
        auto it = vtf.find(1);
        ASSERT_NE(it, vtf.end());
        EXPECT_TRUE(it->second.shared());
    }
}

// Verify that set_all_data_files_shared populates op_write.shared_dels (parallel to
// op_write.dels) so that new del files produced by an in-flight write that is
// cross-published during tablet split are persisted with shared=true in the child
// tablets' rowset.del_files.
TEST_F(TabletReshardHelperTest, test_set_all_data_files_shared_populates_shared_dels) {
    TxnLogPB txn_log;
    auto* op_write = txn_log.mutable_op_write();
    op_write->add_dels("del1.del");
    op_write->add_dels("del2.del");
    op_write->add_dels("del3.del");
    // Start with shared_dels empty (pre-split state).
    ASSERT_EQ(op_write->shared_dels_size(), 0);

    set_all_data_files_shared(&txn_log);

    ASSERT_EQ(txn_log.op_write().shared_dels_size(), 3);
    EXPECT_TRUE(txn_log.op_write().shared_dels(0));
    EXPECT_TRUE(txn_log.op_write().shared_dels(1));
    EXPECT_TRUE(txn_log.op_write().shared_dels(2));
}

// Verify that set_all_data_files_shared marks every SST-related field inside each
// op_parallel_compaction.subtask_compactions entry. Each subtask is a full OpCompaction
// republished via publish_primary_compaction and whose ssts get merged back into parent
// compactions (see tablet_parallel_compaction_manager.cpp), so missing any field would
// leak a non-shared flag into later metadata.
TEST_F(TabletReshardHelperTest, test_set_all_data_files_shared_covers_parallel_compaction_subtasks) {
    TxnLogPB txn_log;
    auto* op_parallel = txn_log.mutable_op_parallel_compaction();

    // Two subtasks, each a full OpCompaction with output_rowset, output_sstable(s), and ssts
    for (int i = 0; i < 2; ++i) {
        auto* subtask = op_parallel->add_subtask_compactions();

        auto* output_rowset = subtask->mutable_output_rowset();
        output_rowset->add_segments("sub_seg.dat");
        auto* del = output_rowset->add_del_files();
        del->set_name("sub_del.dat");

        subtask->mutable_output_sstable()->set_filename("sub_output.sst");
        subtask->mutable_output_sstable()->set_shared(false);

        auto* out_sst = subtask->add_output_sstables();
        out_sst->set_filename("sub_output_multi.sst");
        out_sst->set_shared(false);

        auto* sst = subtask->add_ssts();
        sst->set_name("sub_ingested.sst");
        sst->set_shared(false);
    }

    // Top-level parallel compaction output fields
    op_parallel->mutable_output_sstable()->set_filename("parallel_output.sst");
    op_parallel->mutable_output_sstable()->set_shared(false);
    auto* top_out_sst = op_parallel->add_output_sstables();
    top_out_sst->set_filename("parallel_output_multi.sst");
    top_out_sst->set_shared(false);

    set_all_data_files_shared(&txn_log);

    ASSERT_EQ(txn_log.op_parallel_compaction().subtask_compactions_size(), 2);
    for (const auto& subtask : txn_log.op_parallel_compaction().subtask_compactions()) {
        // Nested rowset data files
        ASSERT_EQ(subtask.output_rowset().shared_segments_size(), 1);
        EXPECT_TRUE(subtask.output_rowset().shared_segments(0));
        EXPECT_TRUE(subtask.output_rowset().del_files(0).shared());
        // Nested SST fields
        EXPECT_TRUE(subtask.output_sstable().shared());
        ASSERT_EQ(subtask.output_sstables_size(), 1);
        EXPECT_TRUE(subtask.output_sstables(0).shared());
        ASSERT_EQ(subtask.ssts_size(), 1);
        EXPECT_TRUE(subtask.ssts(0).shared()) << "subtask.ssts must be marked shared";
    }

    // Top-level parallel compaction fields still covered
    EXPECT_TRUE(txn_log.op_parallel_compaction().output_sstable().shared());
    ASSERT_EQ(txn_log.op_parallel_compaction().output_sstables_size(), 1);
    EXPECT_TRUE(txn_log.op_parallel_compaction().output_sstables(0).shared());
}

// Verify that set_all_data_files_shared treats every op_replication.op_writes[i]
// identically to a top-level op_write: populating shared_dels parallel to dels,
// marking ssts shared, and marking rowset.shared_segments / del_files.shared. PK
// incremental replication publishes each op_write through the same apply_opwrite
// path (see txn_log_applier.cpp apply_write_log), so dropping any of these fields
// here would re-open the same cross-publish leaks fixed for the top-level op_write.
TEST_F(TabletReshardHelperTest, test_set_all_data_files_shared_covers_op_replication) {
    TxnLogPB txn_log;
    auto* op_replication = txn_log.mutable_op_replication();

    // Two replication op_writes with dels, rowset segments, and ssts.
    for (int i = 0; i < 2; ++i) {
        auto* op_write = op_replication->add_op_writes();

        auto* rowset = op_write->mutable_rowset();
        rowset->add_segments("rep_seg.dat");
        auto* del = rowset->add_del_files();
        del->set_name("rep_existing.del");

        op_write->add_dels("rep_new1.del");
        op_write->add_dels("rep_new2.del");
        // shared_dels starts empty — must be populated by set_all_data_files_shared.
        ASSERT_EQ(op_write->shared_dels_size(), 0);

        auto* sst = op_write->add_ssts();
        sst->set_name("rep_sst.sst");
        sst->set_shared(false);
    }

    set_all_data_files_shared(&txn_log);

    ASSERT_EQ(txn_log.op_replication().op_writes_size(), 2);
    for (const auto& op_write : txn_log.op_replication().op_writes()) {
        // Rowset segment & existing del_file shared flags
        ASSERT_EQ(op_write.rowset().shared_segments_size(), 1);
        EXPECT_TRUE(op_write.rowset().shared_segments(0));
        EXPECT_TRUE(op_write.rowset().del_files(0).shared());
        // New-del shared_dels parallel to dels
        ASSERT_EQ(op_write.shared_dels_size(), 2);
        EXPECT_TRUE(op_write.shared_dels(0));
        EXPECT_TRUE(op_write.shared_dels(1));
        // ssts shared flag
        ASSERT_EQ(op_write.ssts_size(), 1);
        EXPECT_TRUE(op_write.ssts(0).shared());
    }
}

// Verify update_rowset_data_stats scales num_dels alongside num_rows / data_size so that
// children of a split tablet do not inherit the parent's full delete cardinality. Without
// this, get_tablet_stats subtracts the full D from each child's partial num_rows and the
// partition row count collapses to 0.
TEST_F(TabletReshardHelperTest, test_update_rowset_data_stats_scales_num_dels) {
    RowsetMetadataPB rowset;
    rowset.set_num_rows(10);
    rowset.set_data_size(1000);
    rowset.set_num_dels(7);

    // Split into 3 children: num_dels = 7/3 + remainder -> children get 3, 2, 2.
    std::vector<int64_t> child_num_dels;
    int64_t total_num_dels = 0;
    for (int i = 0; i < 3; ++i) {
        RowsetMetadataPB child = rowset;
        update_rowset_data_stats(&child, /*split_count=*/3, /*split_index=*/i);
        child_num_dels.push_back(child.num_dels());
        total_num_dels += child.num_dels();
        EXPECT_LE(child.num_dels(), child.num_rows()) << "child " << i;
    }
    EXPECT_EQ(7, total_num_dels);
    EXPECT_THAT(child_num_dels, ::testing::ElementsAre(3, 2, 2));
}

// When num_dels/split_count would exceed num_rows (extreme case, e.g. parent with almost
// all rows deleted and uneven row split), the child must clamp num_dels to its share of
// rows so that live_rows = num_rows - num_dels stays >= 0 downstream.
TEST_F(TabletReshardHelperTest, test_update_rowset_data_stats_clamps_num_dels_to_num_rows) {
    RowsetMetadataPB rowset;
    rowset.set_num_rows(3);
    rowset.set_data_size(300);
    rowset.set_num_dels(9); // pathologically large

    RowsetMetadataPB child = rowset;
    update_rowset_data_stats(&child, /*split_count=*/3, /*split_index=*/0);
    EXPECT_EQ(1, child.num_rows());
    // 9/3 = 3, clamped to num_rows=1.
    EXPECT_EQ(1, child.num_dels());
}

// Verify update_txn_log_data_stats scales num_dels across every op_* branch that already
// scales num_rows / data_size (op_write / op_compaction / op_schema_change / op_replication /
// op_parallel_compaction). Parallel tests pin down the set of branches that produce output
// rowsets during split's cross-publish, so dropping any branch here would resurrect the
// original bug for that flow.
TEST_F(TabletReshardHelperTest, test_update_txn_log_data_stats_scales_num_dels_all_branches) {
    auto make_rowset = [](RowsetMetadataPB* r) {
        r->set_num_rows(8);
        r->set_data_size(800);
        r->set_num_dels(4);
    };

    TxnLogPB txn_log;
    make_rowset(txn_log.mutable_op_write()->mutable_rowset());
    make_rowset(txn_log.mutable_op_compaction()->mutable_output_rowset());
    make_rowset(txn_log.mutable_op_schema_change()->add_rowsets());
    make_rowset(txn_log.mutable_op_replication()->add_op_writes()->mutable_rowset());
    make_rowset(txn_log.mutable_op_parallel_compaction()->add_subtask_compactions()->mutable_output_rowset());

    // split_count=2 split_index=0 -> num_dels should halve to 2 everywhere.
    update_txn_log_data_stats(&txn_log, /*split_count=*/2, /*split_index=*/0);

    EXPECT_EQ(2, txn_log.op_write().rowset().num_dels());
    EXPECT_EQ(2, txn_log.op_compaction().output_rowset().num_dels());
    EXPECT_EQ(2, txn_log.op_schema_change().rowsets(0).num_dels());
    EXPECT_EQ(2, txn_log.op_replication().op_writes(0).rowset().num_dels());
    EXPECT_EQ(2, txn_log.op_parallel_compaction().subtask_compactions(0).output_rowset().num_dels());
}

// Rowsets that predate the num_dels field (has_num_dels() == false) must not be written
// at split time; otherwise the new child would record "0 deletes" and permanently lose
// the parent's accurate-mode fallback, masking a pre-existing replication / recover bug
// whose fix is tracked separately.
TEST_F(TabletReshardHelperTest, test_update_rowset_data_stats_skips_when_num_dels_absent) {
    RowsetMetadataPB rowset;
    rowset.set_num_rows(10);
    rowset.set_data_size(1000);
    // num_dels intentionally unset.

    update_rowset_data_stats(&rowset, /*split_count=*/2, /*split_index=*/0);
    EXPECT_FALSE(rowset.has_num_dels());
}

// ---------------------------------------------------------------------------
// Range-helper tests (PR-1).
// ---------------------------------------------------------------------------

// Build a TabletRangePB from optional int32 lower/upper bounds. nullopt means
// unbounded on that side. Callers pass int values for the sort-key value
// component; this matches the existing generate_sort_key style in
// tablet_reshard_test.cpp without pulling in the heavy fixture.
namespace {

TabletRangePB make_range(std::optional<int> lower, bool lower_included, std::optional<int> upper, bool upper_included) {
    TabletRangePB pb;
    auto write_int = [](TuplePB* tuple_pb, int v) {
        DatumVariant variant(get_type_info(LogicalType::TYPE_INT), Datum(v));
        VariantTuple t;
        t.append(variant);
        t.to_proto(tuple_pb);
    };
    if (lower) {
        write_int(pb.mutable_lower_bound(), *lower);
        pb.set_lower_bound_included(lower_included);
    }
    if (upper) {
        write_int(pb.mutable_upper_bound(), *upper);
        pb.set_upper_bound_included(upper_included);
    }
    return pb;
}

// Convenience: closed-open [lower, upper).
TabletRangePB co_range(std::optional<int> lower, std::optional<int> upper) {
    return make_range(lower, /*lower_included=*/true, upper, /*upper_included=*/false);
}

bool ranges_pb_equal(const TabletRangePB& a, const TabletRangePB& b) {
    if (a.has_lower_bound() != b.has_lower_bound()) return false;
    if (a.has_upper_bound() != b.has_upper_bound()) return false;
    if (a.has_lower_bound() && a.lower_bound().DebugString() != b.lower_bound().DebugString()) return false;
    if (a.has_upper_bound() && a.upper_bound().DebugString() != b.upper_bound().DebugString()) return false;
    if (a.lower_bound_included() != b.lower_bound_included()) return false;
    if (a.upper_bound_included() != b.upper_bound_included()) return false;
    return true;
}

} // namespace

// ranges_are_contiguous --------------------------------------------------------

TEST_F(TabletReshardHelperTest, test_ranges_are_contiguous_overlap) {
    EXPECT_TRUE(ranges_are_contiguous(co_range(0, 15), co_range(10, 20)));
    EXPECT_TRUE(ranges_are_contiguous(co_range(10, 20), co_range(0, 15)));
}

TEST_F(TabletReshardHelperTest, test_ranges_are_contiguous_touching_included) {
    // [0, 10] + [10, 20) → contiguous (10 is in left side).
    EXPECT_TRUE(ranges_are_contiguous(make_range(0, true, 10, true), make_range(10, false, 20, false)));
    // [0, 10) + [10, 20) → contiguous (10 is in right side, lower_included).
    EXPECT_TRUE(ranges_are_contiguous(co_range(0, 10), co_range(10, 20)));
    // Reverse argument order.
    EXPECT_TRUE(ranges_are_contiguous(co_range(10, 20), co_range(0, 10)));
}

TEST_F(TabletReshardHelperTest, test_ranges_are_contiguous_touching_excluded) {
    // [0, 10) + (10, 20) → gap at value 10 (excluded both sides).
    EXPECT_FALSE(ranges_are_contiguous(co_range(0, 10), make_range(10, false, 20, false)));
}

TEST_F(TabletReshardHelperTest, test_ranges_are_contiguous_gap) {
    EXPECT_FALSE(ranges_are_contiguous(co_range(0, 10), co_range(20, 30)));
    EXPECT_FALSE(ranges_are_contiguous(co_range(20, 30), co_range(0, 10)));
}

TEST_F(TabletReshardHelperTest, test_ranges_are_contiguous_unbounded_same_side) {
    // (-∞, 10) + (-∞, 20) → both reach -∞, overlap → contiguous.
    EXPECT_TRUE(ranges_are_contiguous(co_range(std::nullopt, 10), co_range(std::nullopt, 20)));
    // [10, +∞) + [20, +∞) → both reach +∞, overlap → contiguous.
    EXPECT_TRUE(ranges_are_contiguous(make_range(10, true, std::nullopt, false),
                                      make_range(20, true, std::nullopt, false)));
}

TEST_F(TabletReshardHelperTest, test_ranges_are_contiguous_unbounded_inner) {
    // (-∞, +∞) covers everything, contiguous with anything non-empty.
    EXPECT_TRUE(ranges_are_contiguous(make_range(std::nullopt, false, std::nullopt, false), co_range(0, 10)));
    // (-∞, 10) + [20, +∞) → strict gap [10, 20).
    EXPECT_FALSE(ranges_are_contiguous(co_range(std::nullopt, 10), make_range(20, true, std::nullopt, false)));
    // (-∞, 10) + [10, +∞) → touch at 10, contiguous.
    EXPECT_TRUE(ranges_are_contiguous(make_range(std::nullopt, false, 10, false),
                                      make_range(10, true, std::nullopt, false)));
}

// sort_and_merge_adjacent_ranges -----------------------------------------------

TEST_F(TabletReshardHelperTest, test_sort_and_merge_adjacent_basic) {
    auto result = sort_and_merge_adjacent_ranges({co_range(20, 30), co_range(0, 10)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(2u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(co_range(0, 10), result.value()[0]));
    EXPECT_TRUE(ranges_pb_equal(co_range(20, 30), result.value()[1]));
}

TEST_F(TabletReshardHelperTest, test_sort_and_merge_adjacent_overlapping) {
    auto result = sort_and_merge_adjacent_ranges({co_range(5, 15), co_range(0, 10), co_range(12, 20)});
    ASSERT_TRUE(result.ok());
    // [0,15) ∪ [12,20) merge to [0,20). [5,15) overlaps with [0,15) → [0,15).
    ASSERT_EQ(1u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(co_range(0, 20), result.value()[0]));
}

TEST_F(TabletReshardHelperTest, test_sort_and_merge_adjacent_touching) {
    // [0,10) and [10,20) touch at 10 with right side included → merge.
    auto result = sort_and_merge_adjacent_ranges({co_range(10, 20), co_range(0, 10)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(co_range(0, 20), result.value()[0]));
}

TEST_F(TabletReshardHelperTest, test_sort_and_merge_adjacent_unbounded) {
    auto result =
            sort_and_merge_adjacent_ranges({co_range(std::nullopt, 10), make_range(10, true, std::nullopt, false)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1u, result.value().size());
    EXPECT_FALSE(result.value()[0].has_lower_bound());
    EXPECT_FALSE(result.value()[0].has_upper_bound());
}

// compute_disjoint_gaps_within --------------------------------------------------

TEST_F(TabletReshardHelperTest, test_compute_disjoint_gaps_within_full_coverage) {
    auto result = compute_disjoint_gaps_within(co_range(0, 30), {co_range(0, 30)});
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(result.value().empty());
}

TEST_F(TabletReshardHelperTest, test_compute_disjoint_gaps_within_internal_gap) {
    auto result = compute_disjoint_gaps_within(co_range(0, 30), {co_range(0, 10), co_range(20, 30)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(co_range(10, 20), result.value()[0]));
}

TEST_F(TabletReshardHelperTest, test_compute_disjoint_gaps_within_left_edge) {
    // bound=[0,30), children=[10,30). Gap = [0,10).
    auto result = compute_disjoint_gaps_within(co_range(0, 30), {co_range(10, 30)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(co_range(0, 10), result.value()[0]));
}

TEST_F(TabletReshardHelperTest, test_compute_disjoint_gaps_within_right_edge) {
    auto result = compute_disjoint_gaps_within(co_range(0, 30), {co_range(0, 20)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(co_range(20, 30), result.value()[0]));
}

TEST_F(TabletReshardHelperTest, test_compute_disjoint_gaps_within_outside_clipped) {
    // bound=[0,30), children include a slice extending below bound → clip.
    auto result = compute_disjoint_gaps_within(co_range(0, 30), {co_range(-5, 10), co_range(10, 30)});
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(result.value().empty()); // children fully cover bound after clipping.
}

TEST_F(TabletReshardHelperTest, test_compute_disjoint_gaps_within_no_children) {
    auto result = compute_disjoint_gaps_within(co_range(0, 30), {});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(co_range(0, 30), result.value()[0]));
}

// compute_non_contributed_ranges (unbounded universe) --------------------------

TEST_F(TabletReshardHelperTest, test_compute_non_contributed_ranges_full_coverage) {
    // Children = (-∞, +∞). Result empty.
    auto result = compute_non_contributed_ranges({make_range(std::nullopt, false, std::nullopt, false)});
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(result.value().empty());
}

TEST_F(TabletReshardHelperTest, test_compute_non_contributed_ranges_internal_gap) {
    auto result = compute_non_contributed_ranges({co_range(0, 10), co_range(20, 30)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(3u, result.value().size());
    // (-∞, 0)
    EXPECT_FALSE(result.value()[0].has_lower_bound());
    EXPECT_TRUE(ranges_pb_equal(co_range(std::nullopt, 0), result.value()[0]));
    // [10, 20)
    EXPECT_TRUE(ranges_pb_equal(co_range(10, 20), result.value()[1]));
    // [30, +∞)
    EXPECT_TRUE(ranges_pb_equal(make_range(30, true, std::nullopt, false), result.value()[2]));
}

TEST_F(TabletReshardHelperTest, test_compute_non_contributed_ranges_left_edge_only) {
    // First-child-compaction case: children = [K1, +∞). Result = (-∞, K1).
    auto result = compute_non_contributed_ranges({make_range(10, true, std::nullopt, false)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(co_range(std::nullopt, 10), result.value()[0]));
}

TEST_F(TabletReshardHelperTest, test_compute_non_contributed_ranges_right_edge_only) {
    // Last-child-compaction case: children = (-∞, K2). Result = [K2, +∞).
    auto result = compute_non_contributed_ranges({co_range(std::nullopt, 20)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(make_range(20, true, std::nullopt, false), result.value()[0]));
}

TEST_F(TabletReshardHelperTest, test_compute_non_contributed_ranges_both_edges) {
    auto result = compute_non_contributed_ranges({co_range(10, 20)});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(2u, result.value().size());
    EXPECT_TRUE(ranges_pb_equal(co_range(std::nullopt, 10), result.value()[0]));
    EXPECT_TRUE(ranges_pb_equal(make_range(20, true, std::nullopt, false), result.value()[1]));
}

TEST_F(TabletReshardHelperTest, test_compute_non_contributed_ranges_empty_children) {
    auto result = compute_non_contributed_ranges({});
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1u, result.value().size());
    // (-∞, +∞)
    EXPECT_FALSE(result.value()[0].has_lower_bound());
    EXPECT_FALSE(result.value()[0].has_upper_bound());
}

// effective_child_local_range --------------------------------------------------

TEST_F(TabletReshardHelperTest, test_effective_child_local_range_rowset_present) {
    RowsetMetadataPB rowset;
    *rowset.mutable_range() = co_range(0, 10);
    TabletMetadataPB ctx;
    *ctx.mutable_range() = co_range(0, 30);

    const auto& result = effective_child_local_range(rowset, ctx);
    EXPECT_TRUE(ranges_pb_equal(co_range(0, 10), result));
}

TEST_F(TabletReshardHelperTest, test_effective_child_local_range_rowset_absent_ctx_present) {
    RowsetMetadataPB rowset; // no range
    TabletMetadataPB ctx;
    *ctx.mutable_range() = co_range(0, 30);

    const auto& result = effective_child_local_range(rowset, ctx);
    EXPECT_TRUE(ranges_pb_equal(co_range(0, 30), result));
}

TEST_F(TabletReshardHelperTest, test_effective_child_local_range_both_absent) {
    RowsetMetadataPB rowset;
    TabletMetadataPB ctx;

    const auto& result = effective_child_local_range(rowset, ctx);
    EXPECT_FALSE(result.has_lower_bound());
    EXPECT_FALSE(result.has_upper_bound());
}

} // namespace starrocks::lake
