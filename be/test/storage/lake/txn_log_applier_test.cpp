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
std::shared_ptr<TxnLogPB> make_op_write_log(int64_t txn_id, int64_t num_rows, int64_t data_size,
                                            const std::vector<std::string>& segments) {
    auto log = std::make_shared<TxnLogPB>();
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
    logs.push_back(make_op_write_log(10, 5, 100, {"seg_a"}));
    logs.push_back(make_op_write_log(11, 7, 140, {"seg_b1", "seg_b2"}));
    logs.push_back(make_op_write_log(12, 3, 60, {"seg_c"}));

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

    auto log1 = make_op_write_log(20, 10, 100, {"seg1"});
    auto log2 = std::make_shared<TxnLogPB>();
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
    log1->set_txn_id(30);
    (void)log1->mutable_op_schema_change();

    auto log2 = make_op_write_log(31, 4, 40, {"pks1"});

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
    log->set_txn_id(40);

    TxnLogVector logs{log};
    Status st = applier->apply(logs);
    EXPECT_TRUE(st.is_not_supported()) << st.to_string();
    EXPECT_EQ(0, meta->rowsets_size());
}

// Create a lake replication txn log with tablet_metadata for PK table
// This simulates a lake-to-lake replication scenario
std::shared_ptr<TxnLogPB> make_lake_replication_log_with_tablet_metadata(int64_t txn_id, int64_t num_rows,
                                                                         int64_t data_size, int64_t next_rowset_id) {
    auto log = std::make_shared<TxnLogPB>();
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
    auto log = make_lake_replication_log_with_tablet_metadata(50, 100, 2048, 10);

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
std::shared_ptr<TxnLogPB> make_replication_log_without_tablet_metadata(int64_t txn_id, int64_t num_rows,
                                                                       int64_t data_size) {
    auto log = std::make_shared<TxnLogPB>();
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
    auto log = make_lake_replication_log_with_tablet_metadata(60, 200, 4096, 15);

    // Apply the replication log
    Status st = applier->apply(*log);
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Verify that the metadata was updated with the replicated rowsets
    EXPECT_EQ(1, meta->rowsets_size());
    EXPECT_EQ(200, meta->rowsets(0).num_rows());
    EXPECT_EQ(4096, meta->rowsets(0).data_size());
    EXPECT_EQ(15u, meta->next_rowset_id());
}

} // namespace lake
} // namespace starrocks
