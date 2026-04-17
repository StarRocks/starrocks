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

namespace starrocks::lake {

using namespace tablet_reshard_helper;

class TabletReshardHelperTest : public ::testing::Test {};

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

} // namespace starrocks::lake
