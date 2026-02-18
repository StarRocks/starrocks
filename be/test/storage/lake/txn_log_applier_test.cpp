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
#include "storage/lake/txn_log_applier.h"

#include <gtest/gtest.h>

#include "runtime/exec_env.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks {
namespace lake {

// Helper to build non-primary key metadata
MutableTabletMetadataPtr build_non_pk_metadata(int64_t id) {
    auto meta = std::make_shared<TabletMetadata>();
    meta->set_id(id);
    meta->set_version(1);
    meta->set_next_rowset_id(0);
    auto* schema = meta->mutable_schema();
    schema->set_id(100);
    schema->set_keys_type(DUP_KEYS);
    return meta;
}

// Helper to build primary key metadata
MutableTabletMetadataPtr build_pk_metadata(int64_t id) {
    auto meta = std::make_shared<TabletMetadata>();
    meta->set_id(id);
    meta->set_version(1);
    meta->set_next_rowset_id(0);
    auto* schema = meta->mutable_schema();
    schema->set_id(200);
    schema->set_keys_type(PRIMARY_KEYS);
    return meta;
}

// Helper to build primary key metadata with LOCAL persistent index enabled.
// This simulates the scenario where the source cluster has local persistent index enabled,
// which would trigger prepare_primary_index() in finish() for non-replication transactions.
MutableTabletMetadataPtr build_pk_metadata_with_local_persistent_index(int64_t id) {
    auto meta = std::make_shared<TabletMetadata>();
    meta->set_id(id);
    meta->set_version(1);
    meta->set_next_rowset_id(0);
    auto* schema = meta->mutable_schema();
    schema->set_id(200);
    schema->set_keys_type(PRIMARY_KEYS);
    // Enable local persistent index - this is the key configuration that would
    // trigger prepare_primary_index() call in finish() method
    meta->set_enable_persistent_index(true);
    meta->set_persistent_index_type(PersistentIndexTypePB::LOCAL);
    return meta;
}

// Create an op_write txn log
std::shared_ptr<TxnLogPB> make_op_write_log(int64_t tablet_id, int64_t txn_id, int64_t num_rows, int64_t data_size,
                                            const std::vector<std::string>& segments) {
    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(tablet_id);
    log->set_txn_id(txn_id);
    auto* opw = log->mutable_op_write();
    auto* rowset = opw->mutable_rowset();
    rowset->set_num_rows(num_rows);
    rowset->set_data_size(data_size);
    for (auto& s : segments) {
        rowset->add_segments(s);
        rowset->add_segment_size(123); // dummy
    }
    return log;
}

// Build a Tablet instance (minimal requirements for non-primary key path)
bool make_tablet(int64_t tablet_id, Tablet* out_tablet) {
    auto mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    if (mgr == nullptr) return false;
    auto meta = std::make_shared<TabletMetadata>();
    meta->set_id(tablet_id);
    meta->set_version(1);
    meta->mutable_schema()->set_id(1);
    meta->mutable_schema()->set_keys_type(DUP_KEYS);
    (void)mgr->put_tablet_metadata(meta);
    *out_tablet = Tablet(mgr, tablet_id); // 修改参数顺序
    return true;
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeBasic) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 10001); // 修改参数顺序
    auto meta = build_non_pk_metadata(10001);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    TxnLogVector logs;
    logs.push_back(make_op_write_log(10001, 10, 5, 100, {"seg_a"}));
    logs.push_back(make_op_write_log(10001, 11, 7, 140, {"seg_b1", "seg_b2"}));
    logs.push_back(make_op_write_log(10001, 12, 3, 60, {"seg_c"}));

    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    const auto& rs = meta->rowsets(0);
    EXPECT_EQ(5 + 7 + 3, rs.num_rows());
    EXPECT_EQ(100 + 140 + 60, rs.data_size());
    EXPECT_EQ(4, rs.segments_size());
    EXPECT_EQ(0u, rs.id());
    EXPECT_EQ(4u, meta->next_rowset_id()); // 批量合并仍消耗3个额外rowset id
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeSparseSegmentIdStep) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 10004);
    auto meta = build_non_pk_metadata(10004);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(10004);
    log->set_txn_id(13);
    auto* rowset = log->mutable_op_write()->mutable_rowset();
    rowset->set_num_rows(5);
    rowset->set_data_size(100);
    rowset->add_segments("seg_sparse_a");
    rowset->add_segments("seg_sparse_b");
    rowset->add_segment_size(50);
    rowset->add_segment_size(50);
    rowset->add_segment_metas()->set_segment_idx(0);
    rowset->add_segment_metas()->set_segment_idx(4);

    TxnLogVector logs{log};
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    EXPECT_EQ(0u, meta->rowsets(0).id());
    EXPECT_EQ(5u, meta->next_rowset_id());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeRemapSegmentId) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 10005);
    auto meta = build_non_pk_metadata(10005);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log1 = std::make_shared<TxnLogPB>();
    log1->set_tablet_id(10005);
    log1->set_txn_id(14);
    auto* rowset1 = log1->mutable_op_write()->mutable_rowset();
    rowset1->set_num_rows(3);
    rowset1->set_data_size(30);
    rowset1->add_segments("seg_a");
    rowset1->add_segment_size(30);
    rowset1->add_segment_metas()->set_segment_idx(0);

    auto log2 = std::make_shared<TxnLogPB>();
    log2->set_tablet_id(10005);
    log2->set_txn_id(15);
    auto* rowset2 = log2->mutable_op_write()->mutable_rowset();
    rowset2->set_num_rows(4);
    rowset2->set_data_size(40);
    rowset2->add_segments("seg_b");
    rowset2->add_segment_size(40);
    rowset2->add_segment_metas()->set_segment_idx(0);

    TxnLogVector logs{log1, log2};
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    const auto& merged = meta->rowsets(0);
    ASSERT_EQ(2, merged.segments_size());
    ASSERT_EQ(2, merged.segment_metas_size());
    EXPECT_EQ(0, merged.segment_metas(0).segment_idx());
    EXPECT_EQ(1, merged.segment_metas(1).segment_idx());
    EXPECT_EQ(2u, meta->next_rowset_id());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchApplyEmptyVector) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 10002); // 修改参数顺序
    auto meta = build_non_pk_metadata(10002);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    TxnLogVector logs;
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchDeletePredicateUnsupported) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 10003); // 修改参数顺序
    auto meta = build_non_pk_metadata(10003);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log1 = make_op_write_log(10003, 20, 10, 100, {"seg1"});
    auto log2 = std::make_shared<TxnLogPB>();
    log2->set_tablet_id(10003);
    log2->set_txn_id(21);
    auto* opw = log2->mutable_op_write();
    auto* rowset = opw->mutable_rowset();
    rowset->set_num_rows(5);
    rowset->set_data_size(50);
    rowset->add_segments("seg2");
    rowset->add_segment_size(50);
    rowset->mutable_delete_predicate()->set_version(1);

    TxnLogVector logs{log1, log2};
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.is_not_supported()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

TEST(TxnLogApplierBatchTest, PrimaryKeyBatchRejectsNonWriteOp) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 20001); // 修改参数顺序
    auto meta = build_pk_metadata(20001);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log1 = std::make_shared<TxnLogPB>();
    log1->set_tablet_id(20001);
    log1->set_txn_id(30);
    (void)log1->mutable_op_schema_change();

    auto log2 = make_op_write_log(20001, 31, 4, 40, {"pks1"});

    TxnLogVector logs{log1, log2};
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.is_not_supported()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

TEST(TxnLogApplierBatchTest, PrimaryKeyBatchRejectsLogWithoutWrite) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 20002); // 修改参数顺序
    auto meta = build_pk_metadata(20002);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(20002);
    log->set_txn_id(40);

    TxnLogVector logs{log};
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.is_not_supported()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

// Create a lake replication txn log with tablet_metadata for PK table
// This simulates a lake-to-lake replication scenario
std::shared_ptr<TxnLogPB> make_lake_replication_log_with_tablet_metadata(int64_t tablet_id, int64_t txn_id,
                                                                         int64_t num_rows, int64_t data_size,
                                                                         int64_t next_rowset_id) {
    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(tablet_id);
    log->set_txn_id(txn_id);
    auto* op_replication = log->mutable_op_replication();

    // Set txn_meta with TXN_REPLICATED state
    auto* txn_meta = op_replication->mutable_txn_meta();
    txn_meta->set_txn_id(txn_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_snapshot_version(2);
    txn_meta->set_data_version(0); // data_version=0 means full snapshot

    // Set tablet_metadata - this is the key field that distinguishes lake replication
    auto* tablet_metadata = op_replication->mutable_tablet_metadata();
    tablet_metadata->set_id(tablet_id);
    tablet_metadata->set_next_rowset_id(next_rowset_id);
    auto* rowset = tablet_metadata->add_rowsets();
    rowset->set_id(0);
    rowset->set_num_rows(num_rows);
    rowset->set_data_size(data_size);
    rowset->add_segments("replicated_seg1");
    rowset->add_segment_size(data_size);

    return log;
}

// Test that PK table with lake replication log (has tablet_metadata) skips prepare_primary_index in finish().
// Background: When replicating primary key tables from a source cluster with local persistent index enabled,
// all pk index need rebuilding on the target. Before the fix, during finish() phase, if LOCAL type persistent
// index is used, prepare_primary_index() would be called, which uses base_version to fetch delvecs from old
// metadata. But the old metadata doesn't contain delvec info for rowsets replicated from source cluster,
// causing duplicate key errors like:
// "Already exist: FixedMutableIndex<20> insert found duplicate key, new(rssid=X rowid=0), old(rssid=Y rowid=Z)"
// The fix: For lake pk table replication txns, finish() should just put or cache metadata, skipping pk index rebuild.
TEST(TxnLogApplierBatchTest, PrimaryKeyLakeReplicationFinishSkipsPrepareIndex) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 30001);
    // Use metadata with LOCAL persistent index enabled - this is the scenario that would
    // trigger prepare_primary_index() in finish() for normal transactions
    auto meta = build_pk_metadata_with_local_persistent_index(30001);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    // Create a lake replication log with tablet_metadata
    // This sets _is_lake_replication = true in apply_replication_log()
    auto log = make_lake_replication_log_with_tablet_metadata(30001, 50, 100, 2048, 10);

    // Apply the replication log - this marks the transaction as lake replication
    Status apply_st = applier->apply(*log);
    EXPECT_TRUE(apply_st.ok()) << apply_st.to_string();

    // Verify that the metadata was updated with the replicated rowsets
    EXPECT_EQ(1, meta->rowsets_size());
    EXPECT_EQ(100, meta->rowsets(0).num_rows());
    EXPECT_EQ(2048, meta->rowsets(0).data_size());
    EXPECT_EQ(10u, meta->next_rowset_id());

    // Call finish() - this is the critical code path being tested.
    // Before the fix, finish() would call prepare_primary_index() for LOCAL persistent index,
    // which would fail with duplicate key error because delvec info is missing.
    // After the fix, finish() should detect _is_lake_replication and skip prepare_primary_index(),
    // directly writing metadata instead.
    Status finish_st = applier->finish();
    EXPECT_TRUE(finish_st.ok()) << finish_st.to_string();

    // Verify the version was correctly updated in finish()
    EXPECT_EQ(2, meta->version());
}

// Create a lake replication txn log WITHOUT tablet_metadata (shared-nothing cluster migration)
std::shared_ptr<TxnLogPB> make_replication_log_without_tablet_metadata(int64_t tablet_id, int64_t txn_id,
                                                                       int64_t num_rows, int64_t data_size) {
    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(tablet_id);
    log->set_txn_id(txn_id);
    auto* op_replication = log->mutable_op_replication();

    // Set txn_meta with TXN_REPLICATED state
    auto* txn_meta = op_replication->mutable_txn_meta();
    txn_meta->set_txn_id(txn_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_snapshot_version(2);
    txn_meta->set_data_version(0);

    // Add op_writes instead of tablet_metadata - traditional replication path
    auto* op_write = op_replication->add_op_writes();
    auto* rowset = op_write->mutable_rowset();
    rowset->set_id(0);
    rowset->set_num_rows(num_rows);
    rowset->set_data_size(data_size);
    rowset->add_segments("trad_replicated_seg1");
    rowset->add_segment_size(data_size);

    return log;
}

// Test that non-PK table with lake replication log (has tablet_metadata) works correctly
TEST(TxnLogApplierBatchTest, NonPrimaryKeyLakeReplicationApply) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 30002);
    auto meta = build_non_pk_metadata(30002);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    // Create a lake replication log with tablet_metadata
    auto log = make_lake_replication_log_with_tablet_metadata(30002, 60, 200, 4096, 15);

    // Apply the replication log
    Status st = applier->apply(*log);
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Verify that the metadata was updated with the replicated rowsets
    EXPECT_EQ(1, meta->rowsets_size());
    EXPECT_EQ(200, meta->rowsets(0).num_rows());
    EXPECT_EQ(4096, meta->rowsets(0).data_size());
    EXPECT_EQ(15u, meta->next_rowset_id());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyReplicationWithoutTabletMetaSparseSegmentIdStep) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 30003);
    auto meta = build_non_pk_metadata(30003);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(30003);
    log->set_txn_id(61);
    auto* op_replication = log->mutable_op_replication();

    auto* txn_meta = op_replication->mutable_txn_meta();
    txn_meta->set_txn_id(61);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_snapshot_version(2);
    txn_meta->set_data_version(0);

    auto* op_write = op_replication->add_op_writes();
    auto* rowset = op_write->mutable_rowset();
    rowset->set_id(0);
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    rowset->add_segments("rep_seg1");
    rowset->add_segments("rep_seg2");
    rowset->add_segment_size(50);
    rowset->add_segment_size(50);
    rowset->add_segment_metas()->set_segment_idx(0);
    rowset->add_segment_metas()->set_segment_idx(6);

    Status st = applier->apply(*log);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    EXPECT_EQ(0u, meta->rowsets(0).id());
    EXPECT_EQ(7u, meta->next_rowset_id());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyFullReplicationWithoutTabletMetaClearsStaleDcgMeta) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 30004);
    auto meta = build_non_pk_metadata(30004);
    meta->set_next_rowset_id(10);
    auto& stale_dcg = (*meta->mutable_dcg_meta()->mutable_dcgs())[123];
    stale_dcg.add_column_files("stale_dcg_file1.cols");
    stale_dcg.add_column_files("stale_dcg_file2.cols");
    stale_dcg.add_shared_files(false);
    stale_dcg.add_shared_files(true);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(30004);
    log->set_txn_id(62);
    auto* op_replication = log->mutable_op_replication();
    auto* txn_meta = op_replication->mutable_txn_meta();
    txn_meta->set_txn_id(62);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_snapshot_version(2);
    txn_meta->set_data_version(0);

    auto* op_write = op_replication->add_op_writes();
    auto* rowset = op_write->mutable_rowset();
    rowset->set_id(5); // source rssid base
    rowset->set_num_rows(10);
    rowset->set_data_size(100);
    rowset->add_segments("rep_seg1");
    rowset->add_segment_size(100);

    auto& incoming_dcg = (*op_replication->mutable_dcg_meta()->mutable_dcgs())[5];
    incoming_dcg.add_column_files("new_dcg_file.cols");
    incoming_dcg.add_shared_files(false);

    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);
    Status st = applier->apply(*log);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->dcg_meta().dcgs_size());
    EXPECT_TRUE(meta->dcg_meta().dcgs().count(10));
    EXPECT_FALSE(meta->dcg_meta().dcgs().count(123));
    EXPECT_EQ("new_dcg_file.cols", meta->dcg_meta().dcgs().at(10).column_files(0));

    bool found_stale1 = false;
    bool found_stale2 = false;
    for (int i = 0; i < meta->orphan_files_size(); i++) {
        const auto& orphan = meta->orphan_files(i);
        if (orphan.name() == "stale_dcg_file1.cols") {
            found_stale1 = true;
            EXPECT_FALSE(orphan.shared());
        }
        if (orphan.name() == "stale_dcg_file2.cols") {
            found_stale2 = true;
            EXPECT_TRUE(orphan.shared());
        }
    }
    EXPECT_TRUE(found_stale1);
    EXPECT_TRUE(found_stale2);
}

TEST(TxnLogApplierBatchTest, PKFullReplicationWithDcg) {
    // --- Sub-case 1: Non-lake path (offset-based DCG remap) ---
    {
        Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 50001);
        auto meta = build_pk_metadata(50001);
        meta->set_next_rowset_id(10);
        // Pre-existing stale DCG
        auto& stale_dcg = (*meta->mutable_dcg_meta()->mutable_dcgs())[99];
        stale_dcg.add_column_files("stale_pk.cols");
        stale_dcg.add_shared_files(true);
        auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

        auto log = std::make_shared<TxnLogPB>();
        log->set_tablet_id(50001);
        log->set_txn_id(200);
        auto* op_rep = log->mutable_op_replication();
        auto* txn_meta = op_rep->mutable_txn_meta();
        txn_meta->set_txn_id(200);
        txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
        txn_meta->set_snapshot_version(2);
        txn_meta->set_data_version(0);

        // Rowset: source id=3, 2 segments
        auto* op_write = op_rep->add_op_writes();
        auto* rowset = op_write->mutable_rowset();
        rowset->set_id(3);
        rowset->set_num_rows(50);
        rowset->set_data_size(2048);
        rowset->add_segments("pk_full_seg1.dat");
        rowset->add_segments("pk_full_seg2.dat");
        rowset->add_segment_size(1024);
        rowset->add_segment_size(1024);

        // DCG on source rssid 4 → offset to 4 + 10 = 14
        auto& dcg = (*op_rep->mutable_dcg_meta()->mutable_dcgs())[4];
        dcg.add_column_files("pk_full_dcg.cols");
        dcg.add_versions(1);
        auto* ucids = dcg.add_unique_column_ids();
        ucids->add_column_ids(5);

        ASSERT_TRUE(applier->apply(*log).ok());

        // Rowset offset: id = 3 + 10 = 13
        ASSERT_EQ(1, meta->rowsets_size());
        EXPECT_EQ(13u, meta->rowsets(0).id());
        // DCG offset: 4 + 10 = 14
        ASSERT_EQ(1, meta->dcg_meta().dcgs_size());
        EXPECT_TRUE(meta->dcg_meta().dcgs().count(14));
        EXPECT_EQ("pk_full_dcg.cols", meta->dcg_meta().dcgs().at(14).column_files(0));
        EXPECT_EQ(5, meta->dcg_meta().dcgs().at(14).unique_column_ids(0).column_ids(0));
        // Stale DCG → orphan files
        EXPECT_FALSE(meta->dcg_meta().dcgs().count(99));
        bool found_stale = false;
        for (int i = 0; i < meta->orphan_files_size(); i++) {
            if (meta->orphan_files(i).name() == "stale_pk.cols") {
                found_stale = true;
                EXPECT_TRUE(meta->orphan_files(i).shared());
            }
        }
        EXPECT_TRUE(found_stale);
    }

    // --- Sub-case 2: Lake path (tablet_metadata copy) ---
    {
        Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 50002);
        auto meta = build_pk_metadata(50002);
        meta->set_next_rowset_id(5);
        auto& stale_dcg = (*meta->mutable_dcg_meta()->mutable_dcgs())[88];
        stale_dcg.add_column_files("stale_lake_pk.cols");
        auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

        auto log = std::make_shared<TxnLogPB>();
        log->set_tablet_id(50002);
        log->set_txn_id(201);
        auto* op_rep = log->mutable_op_replication();
        auto* txn_meta = op_rep->mutable_txn_meta();
        txn_meta->set_txn_id(201);
        txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
        txn_meta->set_snapshot_version(2);
        txn_meta->set_data_version(0);

        auto* tablet_metadata = op_rep->mutable_tablet_metadata();
        tablet_metadata->set_id(50002);
        tablet_metadata->set_next_rowset_id(20);
        auto* rep_rowset = tablet_metadata->add_rowsets();
        rep_rowset->set_id(0);
        rep_rowset->set_num_rows(100);
        rep_rowset->set_data_size(4096);
        rep_rowset->add_segments("lake_pk_seg1.dat");
        rep_rowset->add_segment_size(4096);
        auto& lake_dcg = (*tablet_metadata->mutable_dcg_meta()->mutable_dcgs())[5];
        lake_dcg.add_column_files("lake_pk_dcg.cols");
        lake_dcg.add_versions(2);

        ASSERT_TRUE(applier->apply(*log).ok());

        ASSERT_EQ(1, meta->rowsets_size());
        EXPECT_EQ(0u, meta->rowsets(0).id());
        EXPECT_EQ(20u, meta->next_rowset_id());
        // DCG directly copied from tablet_metadata
        ASSERT_EQ(1, meta->dcg_meta().dcgs_size());
        EXPECT_TRUE(meta->dcg_meta().dcgs().count(5));
        EXPECT_FALSE(meta->dcg_meta().dcgs().count(88));
        // Stale DCG → orphan files
        bool found = false;
        for (int i = 0; i < meta->orphan_files_size(); i++) {
            if (meta->orphan_files(i).name() == "stale_lake_pk.cols") found = true;
        }
        EXPECT_TRUE(found);
    }
}

TEST(TxnLogApplierBatchTest, PKIncrementalReplicationWithDcg) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 50005);
    auto meta = build_pk_metadata(50005);
    meta->set_next_rowset_id(10);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(50005);
    log->set_txn_id(204);
    auto* op_rep = log->mutable_op_replication();
    auto* txn_meta = op_rep->mutable_txn_meta();
    txn_meta->set_txn_id(204);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_snapshot_version(4);
    txn_meta->set_data_version(3);
    txn_meta->set_incremental_snapshot(true);

    // op_write 1: num_rows=50, 2 segments → included, remap {5→10, 6→11}, target→12
    auto* op_write1 = op_rep->add_op_writes();
    auto* rowset1 = op_write1->mutable_rowset();
    rowset1->set_id(5);
    rowset1->set_num_rows(50);
    rowset1->set_data_size(200);
    rowset1->add_segments("pk_seg1");
    rowset1->add_segments("pk_seg2");
    rowset1->add_segment_size(100);
    rowset1->add_segment_size(100);

    // op_write 2: empty (num_rows=0, no dels, no delete_pred) → skipped
    auto* op_write2 = op_rep->add_op_writes();
    auto* rowset2 = op_write2->mutable_rowset();
    rowset2->set_id(20);
    rowset2->set_num_rows(0);
    rowset2->set_data_size(0);

    // op_write 3: has dels → included, remap {30→12}, target→13
    auto* op_write3 = op_rep->add_op_writes();
    auto* rowset3 = op_write3->mutable_rowset();
    rowset3->set_id(30);
    rowset3->set_num_rows(0);
    rowset3->set_data_size(50);
    rowset3->add_segments("pk_seg3");
    rowset3->add_segment_size(50);
    op_write3->add_dels("pk_del1");

    // DCG entries: 6→11, 30→12, 99 not in remap → kept as 99
    auto* dcg_meta = op_rep->mutable_dcg_meta();
    (*dcg_meta->mutable_dcgs())[6].add_column_files("pk_dcg_6.cols");
    (*dcg_meta->mutable_dcgs())[30].add_column_files("pk_dcg_30.cols");
    (*dcg_meta->mutable_dcgs())[99].add_column_files("pk_dcg_99.cols");

    ASSERT_TRUE(applier->apply(*log).ok());

    // Verify DCG remap: 6→11, 30→12, 99→99
    ASSERT_EQ(3, meta->dcg_meta().dcgs_size());
    EXPECT_TRUE(meta->dcg_meta().dcgs().count(11));
    EXPECT_EQ("pk_dcg_6.cols", meta->dcg_meta().dcgs().at(11).column_files(0));
    EXPECT_TRUE(meta->dcg_meta().dcgs().count(12));
    EXPECT_EQ("pk_dcg_30.cols", meta->dcg_meta().dcgs().at(12).column_files(0));
    EXPECT_TRUE(meta->dcg_meta().dcgs().count(99));
    EXPECT_EQ("pk_dcg_99.cols", meta->dcg_meta().dcgs().at(99).column_files(0));
    // next_rowset_id: 10 + 2 (op1 step) + 1 (op3 step) = 13
    EXPECT_EQ(13u, meta->next_rowset_id());
}

TEST(TxnLogApplierBatchTest, NonPKFullReplicationWithDcg) {
    // --- Sub-case 1: Non-lake path (rssid_remap) ---
    {
        Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 50003);
        auto meta = build_non_pk_metadata(50003);
        meta->set_next_rowset_id(10);
        auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

        auto log = std::make_shared<TxnLogPB>();
        log->set_tablet_id(50003);
        log->set_txn_id(202);
        auto* op_rep = log->mutable_op_replication();
        auto* txn_meta = op_rep->mutable_txn_meta();
        txn_meta->set_txn_id(202);
        txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
        txn_meta->set_snapshot_version(2);
        txn_meta->set_data_version(0);

        // Rowset 1: source id=5, 2 segs → remap {5→10, 6→11}, target→12
        auto* op_write1 = op_rep->add_op_writes();
        auto* rowset1 = op_write1->mutable_rowset();
        rowset1->set_id(5);
        rowset1->set_num_rows(50);
        rowset1->set_data_size(200);
        rowset1->add_segments("full_seg1");
        rowset1->add_segments("full_seg2");
        rowset1->add_segment_size(100);
        rowset1->add_segment_size(100);

        // Rowset 2: source id=8, 1 seg → remap {8→12}, target→13
        auto* op_write2 = op_rep->add_op_writes();
        auto* rowset2 = op_write2->mutable_rowset();
        rowset2->set_id(8);
        rowset2->set_num_rows(30);
        rowset2->set_data_size(100);
        rowset2->add_segments("full_seg3");
        rowset2->add_segment_size(100);

        // DCG on source rssid 6 → remap to 11
        (*op_rep->mutable_dcg_meta()->mutable_dcgs())[6].add_column_files("nonpk_full_dcg.cols");

        ASSERT_TRUE(applier->apply(*log).ok());

        ASSERT_EQ(2, meta->rowsets_size());
        EXPECT_EQ(10u, meta->rowsets(0).id());
        EXPECT_EQ(12u, meta->rowsets(1).id());
        ASSERT_EQ(1, meta->dcg_meta().dcgs_size());
        EXPECT_TRUE(meta->dcg_meta().dcgs().count(11));
        EXPECT_EQ("nonpk_full_dcg.cols", meta->dcg_meta().dcgs().at(11).column_files(0));
    }

    // --- Sub-case 2: Lake path (tablet_metadata copy + stale DCG cleanup) ---
    {
        Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 50004);
        auto meta = build_non_pk_metadata(50004);
        meta->set_next_rowset_id(10);
        auto& stale_dcg = (*meta->mutable_dcg_meta()->mutable_dcgs())[88];
        stale_dcg.add_column_files("stale_nonpk.cols");
        auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

        auto log = std::make_shared<TxnLogPB>();
        log->set_tablet_id(50004);
        log->set_txn_id(203);
        auto* op_rep = log->mutable_op_replication();
        auto* txn_meta = op_rep->mutable_txn_meta();
        txn_meta->set_txn_id(203);
        txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
        txn_meta->set_snapshot_version(2);
        txn_meta->set_data_version(0);

        auto* tablet_metadata = op_rep->mutable_tablet_metadata();
        tablet_metadata->set_id(50004);
        tablet_metadata->set_next_rowset_id(25);
        auto* rep_rowset = tablet_metadata->add_rowsets();
        rep_rowset->set_id(0);
        rep_rowset->set_num_rows(200);
        rep_rowset->set_data_size(8192);
        rep_rowset->add_segments("lake_nonpk_seg1.dat");
        rep_rowset->add_segment_size(8192);
        (*tablet_metadata->mutable_dcg_meta()->mutable_dcgs())[3].add_column_files("lake_nonpk_dcg.cols");

        ASSERT_TRUE(applier->apply(*log).ok());

        ASSERT_EQ(1, meta->rowsets_size());
        EXPECT_EQ(200, meta->rowsets(0).num_rows());
        EXPECT_EQ(25u, meta->next_rowset_id());
        ASSERT_EQ(1, meta->dcg_meta().dcgs_size());
        EXPECT_TRUE(meta->dcg_meta().dcgs().count(3));
        EXPECT_FALSE(meta->dcg_meta().dcgs().count(88));
        bool found = false;
        for (int i = 0; i < meta->orphan_files_size(); i++) {
            if (meta->orphan_files(i).name() == "stale_nonpk.cols") found = true;
        }
        EXPECT_TRUE(found);
    }
}

TEST(TxnLogApplierBatchTest, NonPKIncrementalReplicationWithDcg) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 40001);
    auto meta = build_non_pk_metadata(40001);
    meta->set_next_rowset_id(10);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(40001);
    log->set_txn_id(100);
    auto* op_rep = log->mutable_op_replication();
    auto* txn_meta = op_rep->mutable_txn_meta();
    txn_meta->set_txn_id(100);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_snapshot_version(4);
    txn_meta->set_data_version(3);
    txn_meta->set_incremental_snapshot(true);

    // op_write 1: num_rows=50, 2 segs → included, remap {5→10, 6→11}, target→12
    auto* op_write1 = op_rep->add_op_writes();
    auto* rowset1 = op_write1->mutable_rowset();
    rowset1->set_id(5);
    rowset1->set_num_rows(50);
    rowset1->set_data_size(200);
    rowset1->add_segments("seg1");
    rowset1->add_segments("seg2");
    rowset1->add_segment_size(100);
    rowset1->add_segment_size(100);

    // op_write 2: empty (num_rows=0, no delete_pred) → skipped
    auto* op_write2 = op_rep->add_op_writes();
    auto* rowset2 = op_write2->mutable_rowset();
    rowset2->set_id(100);
    rowset2->set_num_rows(0);
    rowset2->set_data_size(0);

    // op_write 3: has delete_predicate, num_rows=0 → included, remap {20→12}, target→13
    auto* op_write3 = op_rep->add_op_writes();
    auto* rowset3 = op_write3->mutable_rowset();
    rowset3->set_id(20);
    rowset3->set_num_rows(0);
    rowset3->set_data_size(50);
    rowset3->add_segments("seg3");
    rowset3->add_segment_size(50);
    rowset3->mutable_delete_predicate()->set_version(1);

    // op_write 4: no rowset → skipped
    op_rep->add_op_writes();

    // DCG: 6→11, 20→12, 77 not in remap → kept as 77
    auto* dcg_meta = op_rep->mutable_dcg_meta();
    (*dcg_meta->mutable_dcgs())[6].add_column_files("dcg_6.cols");
    (*dcg_meta->mutable_dcgs())[20].add_column_files("dcg_20.cols");
    (*dcg_meta->mutable_dcgs())[77].add_column_files("dcg_77.cols");

    ASSERT_TRUE(applier->apply(*log).ok());

    // Rowsets: op1 (50 rows) + op3 (delete_pred), op2/op4 skipped
    ASSERT_EQ(2, meta->rowsets_size());
    EXPECT_EQ(10u, meta->rowsets(0).id());
    EXPECT_EQ(50, meta->rowsets(0).num_rows());
    EXPECT_EQ(12u, meta->rowsets(1).id());
    EXPECT_TRUE(meta->rowsets(1).has_delete_predicate());
    // next_rowset_id: 10 + 2 + 1 = 13
    EXPECT_EQ(13u, meta->next_rowset_id());
    // DCG remap: 6→11, 20→12, 77→77
    ASSERT_EQ(3, meta->dcg_meta().dcgs_size());
    EXPECT_EQ("dcg_6.cols", meta->dcg_meta().dcgs().at(11).column_files(0));
    EXPECT_EQ("dcg_20.cols", meta->dcg_meta().dcgs().at(12).column_files(0));
    EXPECT_EQ("dcg_77.cols", meta->dcg_meta().dcgs().at(77).column_files(0));
}

} // namespace lake
} // namespace starrocks
