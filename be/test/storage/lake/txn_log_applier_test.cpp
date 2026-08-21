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

#include <set>
#include <string>
#include <vector>

#include "exec/exec_env.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/storage_env.h"

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
    // Production op_writes mint a uid at delta_writer time; emulate that here so
    // batch-apply's strict-uid invariant in apply_op_write_batch holds.
    tablet_reshard_helper::ensure_rowset_uid(rowset);
    for (auto& s : segments) {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename(s);
        sm->set_size(123); // dummy
    }
    return log;
}

// Create an op_write txn log with bundle file offsets (bundled data files)
std::shared_ptr<TxnLogPB> make_op_write_log_with_bundle(int64_t tablet_id, int64_t txn_id, int64_t num_rows,
                                                        int64_t data_size, const std::vector<std::string>& segments,
                                                        const std::vector<int64_t>& bundle_offsets) {
    auto log = make_op_write_log(tablet_id, txn_id, num_rows, data_size, segments);
    auto* rowset = log->mutable_op_write()->mutable_rowset();
    for (int i = 0; i < static_cast<int>(bundle_offsets.size()); i++) {
        if (i < rowset->segment_metas_size()) {
            rowset->mutable_segment_metas(i)->set_bundle_file_offset(bundle_offsets[i]);
        } else {
            rowset->add_segment_metas()->set_bundle_file_offset(bundle_offsets[i]);
        }
    }
    return log;
}

// Build a Tablet instance (minimal requirements for non-primary key path)
bool make_tablet(int64_t tablet_id, Tablet* out_tablet) {
    auto mgr = StorageEnv::GetInstance()->lake_tablet_manager();
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
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10001); // 修改参数顺序
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
    EXPECT_EQ(4, rs.segment_metas_size());
    EXPECT_EQ(0u, rs.id());
    EXPECT_EQ(4u, meta->next_rowset_id()); // 批量合并仍消耗3个额外rowset id
}

// Regression: a cross-published op_write whose num_rows scaled to 0 on this child (but still
// carries a segment) must NOT be dropped, and the merged uid must be taken from it (the first
// op_write carrying segments) so split children converge on one identity. Gating on num_rows
// instead would skip seg_zero, set num_rows-driven uid to log1's, and diverge across children.
TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchZeroNumRowsKeepsSegmentAndUid) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10020);
    auto meta = build_non_pk_metadata(10020);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log0 = make_op_write_log(10020, 20, /*num_rows=*/0, /*data_size=*/0, {"seg_zero"});
    auto log1 = make_op_write_log(10020, 21, /*num_rows=*/10, /*data_size=*/200, {"seg_data"});
    const PUniqueId first_uid = log0->op_write().rowset().uid();

    TxnLogVector logs{log0, log1};
    Status st = applier->apply(logs);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    const auto& rs = meta->rowsets(0);
    EXPECT_EQ(2, rs.segment_metas_size()) << "the num_rows==0 op_write's segment must be retained";
    EXPECT_EQ(10, rs.num_rows()) << "num_rows still summed faithfully (0 + 10)";
    ASSERT_TRUE(rs.has_uid());
    EXPECT_EQ(first_uid.hi(), rs.uid().hi());
    EXPECT_EQ(first_uid.lo(), rs.uid().lo());
}

// Regression for the early-exit: if EVERY contributing op_write scaled to num_rows==0 but
// segments are present, the merged rowset must still be created (the early-exit keys on
// segments, not total_num_rows) — otherwise the whole cross-published txn's data is dropped.
TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchAllZeroNumRowsKeepsSegments) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10021);
    auto meta = build_non_pk_metadata(10021);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    TxnLogVector logs;
    logs.push_back(make_op_write_log(10021, 22, 0, 0, {"seg_x"}));
    logs.push_back(make_op_write_log(10021, 23, 0, 0, {"seg_y"}));

    Status st = applier->apply(logs);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size()) << "rowset must be created even when total num_rows is 0";
    EXPECT_EQ(2, meta->rowsets(0).segment_metas_size());
    EXPECT_EQ(0, meta->rowsets(0).num_rows());
}

// The single-log path had the hole the batch path above was already built to avoid: a cross
// published rowset arrives with the rowset-level num_rows apportioned across the siblings, so a
// sibling holding this tablet's rows can legitimately see 0. The per-segment counts are not
// apportioned, so they are what settles it -- keying presence off the rowset-level count drops the
// segments and the rows are gone while the transaction reports success.
TEST(TxnLogApplierBatchTest, NonPrimaryKeySingleLogZeroNumRowsKeepsSegments) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10120);
    auto meta = build_non_pk_metadata(10120);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = make_op_write_log(10120, 30, /*num_rows=*/0, /*data_size=*/0, {"seg_zero"});
    log->mutable_op_write()->mutable_rowset()->mutable_segment_metas(0)->set_num_rows(7);
    Status st = applier->apply(*log);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size()) << "a rowset whose segments hold rows must be attached whatever num_rows says";
    EXPECT_EQ(1, meta->rowsets(0).segment_metas_size());
    EXPECT_EQ(0, meta->rowsets(0).num_rows()) << "the apportioned statistic is kept as-is";
}

// The counterpart: nothing to attach when the op really is empty.
TEST(TxnLogApplierBatchTest, NonPrimaryKeySingleLogNoSegmentsAttachesNothing) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10121);
    auto meta = build_non_pk_metadata(10121);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = make_op_write_log(10121, 31, /*num_rows=*/0, /*data_size=*/0, {});
    Status st = applier->apply(*log);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

// An empty write still produces a segment, and it reports 0 rows. That is not a cross publish and
// there is nothing to attach -- the segment count alone would not tell the two apart, which is why
// this path asks the per-segment counts. (The batch path is deliberately left as it was: it keys
// off the segment count and merges, so an empty segment costs it a file reference, not a rowset.)
TEST(TxnLogApplierBatchTest, NonPrimaryKeySingleLogEmptySegmentAttachesNothing) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10122);
    auto meta = build_non_pk_metadata(10122);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = make_op_write_log(10122, 32, /*num_rows=*/0, /*data_size=*/0, {"seg_empty"});
    log->mutable_op_write()->mutable_rowset()->mutable_segment_metas(0)->set_num_rows(0);
    Status st = applier->apply(*log);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

// A legacy rowset whose segment_metas were back-filled from the deprecated parallel arrays carries
// no per-segment count at all. Nothing proves it empty, so it is kept.
TEST(TxnLogApplierBatchTest, NonPrimaryKeySingleLogUncountedSegmentIsKept) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10123);
    auto meta = build_non_pk_metadata(10123);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = make_op_write_log(10123, 33, /*num_rows=*/0, /*data_size=*/0, {"seg_uncounted"});
    ASSERT_FALSE(log->op_write().rowset().segment_metas(0).has_num_rows());
    Status st = applier->apply(*log);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1, meta->rowsets_size());
    EXPECT_EQ(1, meta->rowsets(0).segment_metas_size());
}

// The primary key applier's skip predicate moved the same way, so check it did not broaden: an
// op_write that really is empty -- a segment was written but holds no rows, no del files, no delete
// predicate -- must still be skipped. Note the delete-only case never depended on this predicate;
// dels_meta_size() > 0 short circuits it.
//
// The assertion has teeth because the skip returns BEFORE prepare_primary_index(): had the predicate
// let this through, publish would go on to open "seg_empty", which does not exist.
TEST(TxnLogApplierBatchTest, PrimaryKeySingleLogEmptySegmentIsSkipped) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 30010);
    auto meta = build_pk_metadata(30010);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = make_op_write_log(30010, 60, /*num_rows=*/0, /*data_size=*/0, {"seg_empty"});
    log->mutable_op_write()->mutable_rowset()->mutable_segment_metas(0)->set_num_rows(0);
    ASSERT_EQ(0, log->op_write().dels_meta_size());
    ASSERT_FALSE(log->op_write().rowset().has_delete_predicate());

    Status st = applier->apply(*log);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeSparseSegmentIdStep) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10004);
    auto meta = build_non_pk_metadata(10004);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(10004);
    log->set_txn_id(13);
    auto* rowset = log->mutable_op_write()->mutable_rowset();
    rowset->set_num_rows(5);
    rowset->set_data_size(100);
    tablet_reshard_helper::ensure_rowset_uid(rowset);
    {
        auto* sm0 = rowset->add_segment_metas();
        sm0->set_filename("seg_sparse_a");
        sm0->set_size(50);
        sm0->set_segment_idx(0);
        auto* sm1 = rowset->add_segment_metas();
        sm1->set_filename("seg_sparse_b");
        sm1->set_size(50);
        sm1->set_segment_idx(4);
    }

    TxnLogVector logs{log};
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    EXPECT_EQ(0u, meta->rowsets(0).id());
    EXPECT_EQ(5u, meta->next_rowset_id());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeRemapSegmentId) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10005);
    auto meta = build_non_pk_metadata(10005);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    auto log1 = std::make_shared<TxnLogPB>();
    log1->set_tablet_id(10005);
    log1->set_txn_id(14);
    auto* rowset1 = log1->mutable_op_write()->mutable_rowset();
    rowset1->set_num_rows(3);
    rowset1->set_data_size(30);
    tablet_reshard_helper::ensure_rowset_uid(rowset1);
    {
        auto* sm = rowset1->add_segment_metas();
        sm->set_filename("seg_a");
        sm->set_size(30);
        sm->set_segment_idx(0);
    }

    auto log2 = std::make_shared<TxnLogPB>();
    log2->set_tablet_id(10005);
    log2->set_txn_id(15);
    auto* rowset2 = log2->mutable_op_write()->mutable_rowset();
    rowset2->set_num_rows(4);
    rowset2->set_data_size(40);
    tablet_reshard_helper::ensure_rowset_uid(rowset2);
    {
        auto* sm = rowset2->add_segment_metas();
        sm->set_filename("seg_b");
        sm->set_size(40);
        sm->set_segment_idx(0);
    }

    TxnLogVector logs{log1, log2};
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    const auto& merged = meta->rowsets(0);
    ASSERT_EQ(2, merged.segment_metas_size());
    EXPECT_EQ(0, merged.segment_metas(0).segment_idx());
    EXPECT_EQ(1, merged.segment_metas(1).segment_idx());
    EXPECT_EQ(2u, meta->next_rowset_id());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchApplyEmptyVector) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10002); // 修改参数顺序
    auto meta = build_non_pk_metadata(10002);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    TxnLogVector logs;
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchDeletePredicateUnsupported) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10003); // 修改参数顺序
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
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("seg2");
        sm->set_size(50);
    }
    rowset->mutable_delete_predicate()->set_version(1);

    TxnLogVector logs{log1, log2};
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.is_not_supported()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

TEST(TxnLogApplierBatchTest, PrimaryKeyBatchRejectsNonWriteOp) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 20001); // 修改参数顺序
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
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 20002); // 修改参数顺序
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
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("replicated_seg1");
        sm->set_size(data_size);
    }

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
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 30001);
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
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("trad_replicated_seg1");
        sm->set_size(data_size);
    }

    return log;
}

// Test that non-PK table with lake replication log (has tablet_metadata) works correctly
TEST(TxnLogApplierBatchTest, NonPrimaryKeyLakeReplicationApply) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 30002);
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

// Build an incremental replication log carrying |op_writes| plus a dcg_meta keyed by SOURCE rssids.
// Incremental is the shape that routes through build_rssid_remap; the version arithmetic
// (snapshot - data + base == new) has to line up or apply_replication_log rejects the log outright.
std::shared_ptr<TxnLogPB> make_incremental_replication_log(int64_t tablet_id, int64_t txn_id, int64_t base_version,
                                                           int64_t new_version,
                                                           const std::vector<TxnLogPB_OpWrite>& op_writes,
                                                           const std::vector<uint32_t>& dcg_source_rssids) {
    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(tablet_id);
    log->set_txn_id(txn_id);
    auto* op_replication = log->mutable_op_replication();

    auto* txn_meta = op_replication->mutable_txn_meta();
    txn_meta->set_txn_id(txn_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_incremental_snapshot(true);
    txn_meta->set_data_version(10);
    txn_meta->set_snapshot_version(10 + (new_version - base_version));

    for (const auto& op_write : op_writes) {
        op_replication->add_op_writes()->CopyFrom(op_write);
    }
    for (uint32_t src_rssid : dcg_source_rssids) {
        // The payload only has to be distinguishable; the column file name records which source
        // rssid the entry came from so the assertions can follow it through the remap.
        DeltaColumnGroupVerPB dcg_ver;
        dcg_ver.add_column_files("src_" + std::to_string(src_rssid) + ".cols");
        (*op_replication->mutable_dcg_meta()->mutable_dcgs())[src_rssid] = dcg_ver;
    }
    return log;
}

TxnLogPB_OpWrite make_replication_op_write(uint32_t source_rowset_id, int64_t rowset_num_rows,
                                           const std::vector<int64_t>& per_segment_num_rows) {
    TxnLogPB_OpWrite op_write;
    auto* rowset = op_write.mutable_rowset();
    rowset->set_id(source_rowset_id);
    rowset->set_num_rows(rowset_num_rows);
    for (size_t i = 0; i < per_segment_num_rows.size(); i++) {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("repl_seg_" + std::to_string(source_rowset_id) + "_" + std::to_string(i));
        sm->set_size(123);
        sm->set_num_rows(per_segment_num_rows[i]);
    }
    return op_write;
}

// build_rssid_remap decides which op_writes advance the target rssid, and it MUST reach the same
// verdict as the applier that attaches the rowsets -- its comment says so, because the two run over
// the same op_writes and the remap is what re-keys the replicated delta column groups. A drift makes
// every dcg entry after the first disagreement point at an rssid no attached rowset owns: silently
// wrong columns, no error anywhere.
//
// #77511 moved that predicate (num_rows alone -> ask the segments) in both places at once. This
// pins the agreement so a future one-sided edit fails here instead of in production.
//
// The three op_writes below exercise all three verdicts:
//   src 100: rowset num_rows > 0                      -> attached, step 1
//   src 200: a segment written but holding no rows     -> skipped, must NOT advance the target id
//   src 300: num_rows apportioned to 0, segments hold rows -> attached, step 2
TEST(TxnLogApplierBatchTest, NonPrimaryKeyIncrementalReplicationRssidRemapMatchesAttachedRowsets) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 30010);
    auto meta = build_non_pk_metadata(30010);
    ASSERT_EQ(0u, meta->next_rowset_id());
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    std::vector<TxnLogPB_OpWrite> op_writes{
            make_replication_op_write(/*source_rowset_id=*/100, /*rowset_num_rows=*/5, {5}),
            make_replication_op_write(/*source_rowset_id=*/200, /*rowset_num_rows=*/0, {0}),
            make_replication_op_write(/*source_rowset_id=*/300, /*rowset_num_rows=*/0, {7, 0}),
    };
    // One dcg per source rssid the writes own, plus one (999) that no write owns.
    auto log = make_incremental_replication_log(30010, 70, /*base_version=*/1, /*new_version=*/2, op_writes,
                                                {100, 200, 300, 301, 999});

    Status st = applier->apply(*log);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Two rowsets attached at 0 and 1; the empty one contributed nothing, so 300 lands at 1 (not 2)
    // and owns rssids 1 and 2 for its two segments.
    ASSERT_EQ(2, meta->rowsets_size());
    EXPECT_EQ(0u, meta->rowsets(0).id());
    EXPECT_EQ(1u, meta->rowsets(1).id());
    EXPECT_EQ(3u, meta->next_rowset_id());

    // Every rssid the attached rowsets own.
    std::set<uint32_t> owned_rssids;
    for (const auto& rowset : meta->rowsets()) {
        for (uint32_t i = 0; i < get_rowset_id_step(rowset); i++) {
            owned_rssids.insert(rowset.id() + i);
        }
    }
    EXPECT_EQ(std::set<uint32_t>({0, 1, 2}), owned_rssids);

    // The dcgs of a skipped write, and of a source rssid nobody owns, are left on their source key
    // (apply_replication_dcg_meta keeps unmapped keys unchanged), so filter to the remapped ones and
    // require each to land on an rssid an attached rowset actually owns. This is the assertion that
    // catches a predicate drift: including the skipped write in the remap would push 300 to 2 and 301
    // to 3, and 3 is owned by nothing.
    const auto& dcgs = meta->dcg_meta().dcgs();
    auto source_of = [&](uint32_t key) {
        auto it = dcgs.find(key);
        return it == dcgs.end() ? std::string() : it->second.column_files(0);
    };
    EXPECT_EQ("src_100.cols", source_of(0)) << "source rssid 100 must be remapped onto the first attached rowset";
    EXPECT_EQ("src_300.cols", source_of(1)) << "the skipped write must not have advanced the target rssid";
    EXPECT_EQ("src_301.cols", source_of(2));
    // Untouched keys: the skipped write's own rssid and the orphan one.
    EXPECT_EQ("src_200.cols", source_of(200));
    EXPECT_EQ("src_999.cols", source_of(999));
    EXPECT_EQ(5u, dcgs.size()) << "no dcg entry may be dropped or collide";
}

// Test that bundle_file_offsets from multiple TxnLogs are correctly merged into the combined rowset.
// This verifies multi-statement transaction support for file bundling.
TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeBundleFileOffsets) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10010);
    auto meta = build_non_pk_metadata(10010);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    // Simulate two statements in a multi-statement transaction, each with bundled segments
    TxnLogVector logs;
    logs.push_back(make_op_write_log_with_bundle(10010, 10, 5, 100, {"seg_a"}, {0}));
    logs.push_back(make_op_write_log_with_bundle(10010, 11, 7, 140, {"seg_b"}, {1024}));
    logs.push_back(make_op_write_log_with_bundle(10010, 12, 3, 60, {"seg_c"}, {2048}));

    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    const auto& rs = meta->rowsets(0);
    EXPECT_EQ(15, rs.num_rows());
    EXPECT_EQ(300, rs.data_size());
    EXPECT_EQ(3, rs.segment_metas_size());

    // Verify bundle_file_offsets are preserved with 1:1 correspondence
    ASSERT_EQ(3, rs.segment_metas_size());
    ASSERT_TRUE(rs.segment_metas(0).has_bundle_file_offset());
    ASSERT_TRUE(rs.segment_metas(1).has_bundle_file_offset());
    ASSERT_TRUE(rs.segment_metas(2).has_bundle_file_offset());
    EXPECT_EQ(0, rs.segment_metas(0).bundle_file_offset());
    EXPECT_EQ(1024, rs.segment_metas(1).bundle_file_offset());
    EXPECT_EQ(2048, rs.segment_metas(2).bundle_file_offset());
}

// Test that when no TxnLogs have bundle_file_offsets, the merged rowset also has none.
TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeNoBundleOffsets) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10011);
    auto meta = build_non_pk_metadata(10011);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    TxnLogVector logs;
    logs.push_back(make_op_write_log(10011, 10, 5, 100, {"seg_a"}));
    logs.push_back(make_op_write_log(10011, 11, 7, 140, {"seg_b"}));

    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    const auto& rs = meta->rowsets(0);
    EXPECT_EQ(2, rs.segment_metas_size());
    EXPECT_FALSE(rs.segment_metas(0).has_bundle_file_offset());
    EXPECT_FALSE(rs.segment_metas(1).has_bundle_file_offset());
}

// Test that mixed bundle_file_offsets (some TxnLogs with, some without) returns error to prevent
// data corruption — silently dropping offsets would leave bundled segment paths unresolvable.
TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeMixedBundleOffsetsReturnsError) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10012);
    auto meta = build_non_pk_metadata(10012);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    TxnLogVector logs;
    // First log has bundle offsets
    logs.push_back(make_op_write_log_with_bundle(10012, 10, 5, 100, {"seg_a"}, {0}));
    // Second log does NOT have bundle offsets
    logs.push_back(make_op_write_log(10012, 11, 7, 140, {"seg_b"}));

    Status st = applier->apply(logs);
    EXPECT_TRUE(st.is_internal_error()) << st.to_string();
    EXPECT_NE(std::string::npos, st.to_string().find("Inconsistent bundle_file_offsets"));
}

// Test reverse order: first TxnLog has no offsets, second has offsets.
// This must also be detected as inconsistent and return error.
TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeMixedBundleOffsetsReverseReturnsError) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10013);
    auto meta = build_non_pk_metadata(10013);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    TxnLogVector logs;
    // First log does NOT have bundle offsets
    logs.push_back(make_op_write_log(10013, 10, 5, 100, {"seg_a", "seg_b"}));
    // Second log has bundle offsets
    logs.push_back(make_op_write_log_with_bundle(10013, 11, 7, 140, {"seg_c"}, {0}));

    Status st = applier->apply(logs);
    EXPECT_TRUE(st.is_internal_error()) << st.to_string();
    EXPECT_NE(std::string::npos, st.to_string().find("Inconsistent bundle_file_offsets"));
}

// Test that a single TxnLog with mismatched offset/segment count returns error.
TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeBundleOffsetSizeMismatchReturnsError) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10014);
    auto meta = build_non_pk_metadata(10014);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    TxnLogVector logs;
    // 2 segments but only 1 offset → mismatch within single TxnLog
    logs.push_back(make_op_write_log_with_bundle(10014, 10, 5, 100, {"seg_a", "seg_b"}, {0}));

    Status st = applier->apply(logs);
    EXPECT_TRUE(st.is_internal_error()) << st.to_string();
    EXPECT_NE(std::string::npos, st.to_string().find("mismatch"));
}

// Legacy/upgrade path: a txn log written before the uid field existed carries no producer
// uid. Batch-apply must NOT hard-fail on it (that would strand a pending multi-statement
// txn across a rolling upgrade) — instead the merged rowset backfills a fresh uid so the
// publish succeeds. Such a txn predates range distribution and is never cross-published, so
// the backfilled (non-deterministic) uid is safe. We clear the uid that make_op_write_log
// auto-stamps to simulate the legacy log.
TEST(TxnLogApplierBatchTest, NonPrimaryKeyBatchMergeNoUidBackfills) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 10015);
    auto meta = build_non_pk_metadata(10015);
    auto applier = new_txn_log_applier(tablet, meta, 2, false, true);

    TxnLogVector logs;
    auto log = make_op_write_log(10015, 10, 5, 100, {"seg_a"});
    log->mutable_op_write()->mutable_rowset()->clear_uid(); // simulate a legacy pre-uid txn log
    logs.push_back(std::move(log));

    Status st = applier->apply(logs);
    EXPECT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1, meta->rowsets_size());
    EXPECT_TRUE(meta->rowsets(0).has_uid()) << "merged rowset must backfill a uid for a legacy log";
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyReplicationWithoutTabletMetaSparseSegmentIdStep) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 30003);
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
    {
        auto* sm0 = rowset->add_segment_metas();
        sm0->set_filename("rep_seg1");
        sm0->set_size(50);
        sm0->set_segment_idx(0);
        auto* sm1 = rowset->add_segment_metas();
        sm1->set_filename("rep_seg2");
        sm1->set_size(50);
        sm1->set_segment_idx(6);
    }

    Status st = applier->apply(*log);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1, meta->rowsets_size());
    EXPECT_EQ(0u, meta->rowsets(0).id());
    EXPECT_EQ(7u, meta->next_rowset_id());
}

TEST(TxnLogApplierBatchTest, NonPrimaryKeyFullReplicationWithoutTabletMetaClearsStaleDcgMeta) {
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 30004);
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
    {
        auto* sm = rowset->add_segment_metas();
        sm->set_filename("rep_seg1");
        sm->set_size(100);
    }

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
        Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 50001);
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
        {
            auto* sm0 = rowset->add_segment_metas();
            sm0->set_filename("pk_full_seg1.dat");
            sm0->set_size(1024);
            auto* sm1 = rowset->add_segment_metas();
            sm1->set_filename("pk_full_seg2.dat");
            sm1->set_size(1024);
        }

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
        Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 50002);
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
        {
            auto* sm = rep_rowset->add_segment_metas();
            sm->set_filename("lake_pk_seg1.dat");
            sm->set_size(4096);
        }
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
    // NOTE: PK incremental replication calls apply_write_log which requires prepare_primary_index
    // for non-empty op_writes. In this lightweight test environment (no real data files),
    // prepare_primary_index would fail. Therefore we use empty op_writes (num_rows=0, no dels,
    // no delete_predicate) which are skipped by apply_write_log, and verify that DCG entries
    // from op_replication are correctly applied to metadata (pass-through without remapping).
    // The full DCG rssid remapping logic is tested by NonPKIncrementalReplicationWithDcg,
    // which uses the same shared apply_replication_dcg_meta function.
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 50005);
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

    // All op_writes are empty (num_rows=0, no dels, no delete_pred) → skipped by apply_write_log.
    // rssid_remap will be empty, so DCG keys are preserved as-is.
    auto* op_write1 = op_rep->add_op_writes();
    auto* rowset1 = op_write1->mutable_rowset();
    rowset1->set_id(5);
    rowset1->set_num_rows(0);
    rowset1->set_data_size(0);

    auto* op_write2 = op_rep->add_op_writes();
    auto* rowset2 = op_write2->mutable_rowset();
    rowset2->set_id(20);
    rowset2->set_num_rows(0);
    rowset2->set_data_size(0);

    // DCG entries: no remapping (rssid_remap is empty), all keys preserved as-is
    auto* dcg_meta = op_rep->mutable_dcg_meta();
    (*dcg_meta->mutable_dcgs())[6].add_column_files("pk_dcg_6.cols");
    (*dcg_meta->mutable_dcgs())[30].add_column_files("pk_dcg_30.cols");
    (*dcg_meta->mutable_dcgs())[99].add_column_files("pk_dcg_99.cols");

    ASSERT_TRUE(applier->apply(*log).ok());

    // Verify DCG entries preserved with original keys (no remapping since all op_writes are empty)
    ASSERT_EQ(3, meta->dcg_meta().dcgs_size());
    EXPECT_TRUE(meta->dcg_meta().dcgs().count(6));
    EXPECT_EQ("pk_dcg_6.cols", meta->dcg_meta().dcgs().at(6).column_files(0));
    EXPECT_TRUE(meta->dcg_meta().dcgs().count(30));
    EXPECT_EQ("pk_dcg_30.cols", meta->dcg_meta().dcgs().at(30).column_files(0));
    EXPECT_TRUE(meta->dcg_meta().dcgs().count(99));
    EXPECT_EQ("pk_dcg_99.cols", meta->dcg_meta().dcgs().at(99).column_files(0));
    // next_rowset_id unchanged since all op_writes were skipped
    EXPECT_EQ(10u, meta->next_rowset_id());
}

TEST(TxnLogApplierBatchTest, NonPKFullReplicationWithDcg) {
    // --- Sub-case 1: Non-lake path (rssid_remap) ---
    {
        Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 50003);
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
        {
            auto* sm0 = rowset1->add_segment_metas();
            sm0->set_filename("full_seg1");
            sm0->set_size(100);
            auto* sm1 = rowset1->add_segment_metas();
            sm1->set_filename("full_seg2");
            sm1->set_size(100);
        }

        // Rowset 2: source id=8, 1 seg → remap {8→12}, target→13
        auto* op_write2 = op_rep->add_op_writes();
        auto* rowset2 = op_write2->mutable_rowset();
        rowset2->set_id(8);
        rowset2->set_num_rows(30);
        rowset2->set_data_size(100);
        {
            auto* sm = rowset2->add_segment_metas();
            sm->set_filename("full_seg3");
            sm->set_size(100);
        }

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
        Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 50004);
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
        {
            auto* sm = rep_rowset->add_segment_metas();
            sm->set_filename("lake_nonpk_seg1.dat");
            sm->set_size(8192);
        }
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
    Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), 40001);
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
    {
        auto* sm0 = rowset1->add_segment_metas();
        sm0->set_filename("seg1");
        sm0->set_size(100);
        auto* sm1 = rowset1->add_segment_metas();
        sm1->set_filename("seg2");
        sm1->set_size(100);
    }

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
    {
        auto* sm = rowset3->add_segment_metas();
        sm->set_filename("seg3");
        sm->set_size(50);
    }
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
