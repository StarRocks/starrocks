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

// Regression for the metadata-reclaim change on the non-PK path: when
// NonPrimaryKeyTxnLogApplier archives compaction input rowsets into
// compaction_inputs, the delete_predicate must be dropped from the archived copy
// (covers both the std::move(*iter) branch and the last_input_rowset branch).
TEST(TxnLogApplierCompactionTest, NonPKCompactionDropsDeletePredicate) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 40050);
    auto meta = build_non_pk_metadata(40050);
    meta->set_next_rowset_id(10);

    // Two adjacent live rowsets, each carrying a delete_predicate.
    auto* r0 = meta->add_rowsets();
    r0->set_id(1);
    r0->set_overlapped(false);
    r0->set_num_rows(10);
    r0->set_data_size(100);
    r0->add_segments("seg0.dat");
    r0->mutable_delete_predicate()->set_version(1);
    auto* r1 = meta->add_rowsets();
    r1->set_id(2);
    r1->set_overlapped(false);
    r1->set_num_rows(20);
    r1->set_data_size(200);
    r1->add_segments("seg1.dat");
    r1->mutable_delete_predicate()->set_version(2);

    ASSERT_TRUE(meta->rowsets(0).has_delete_predicate());
    ASSERT_TRUE(meta->rowsets(1).has_delete_predicate());

    auto applier = new_txn_log_applier(tablet, meta, 2, false);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(40050);
    log->set_txn_id(200);
    auto* op_compaction = log->mutable_op_compaction();
    op_compaction->add_input_rowsets(1);
    op_compaction->add_input_rowsets(2);
    auto* output = op_compaction->mutable_output_rowset();
    output->set_num_rows(30);
    output->set_data_size(300);
    output->add_segments("out.dat");
    op_compaction->set_compact_version(2);

    ASSERT_TRUE(applier->apply(*log).ok());

    // Input rowsets archived into compaction_inputs must NOT retain delete_predicate.
    ASSERT_EQ(2, meta->compaction_inputs_size());
    EXPECT_FALSE(meta->compaction_inputs(0).has_delete_predicate());
    EXPECT_FALSE(meta->compaction_inputs(1).has_delete_predicate());
}

// Regression: non-PK NON-LAKE replication (full snapshot, no tablet_metadata) swaps all superseded
// old rowsets into compaction_inputs — delete_predicate must be dropped across the Swap too.
TEST(TxnLogApplierCompactionTest, NonPKNonLakeReplicationDropsDeletePredicate) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 40061);
    auto meta = build_non_pk_metadata(40061);
    meta->set_next_rowset_id(10);
    auto* r0 = meta->add_rowsets();
    r0->set_id(1);
    r0->set_num_rows(10);
    r0->add_segments("old0.dat");
    r0->mutable_delete_predicate()->set_version(1);
    auto* r1 = meta->add_rowsets();
    r1->set_id(2);
    r1->set_num_rows(20);
    r1->add_segments("old1.dat");
    r1->mutable_delete_predicate()->set_version(2);
    ASSERT_TRUE(meta->rowsets(0).has_delete_predicate());
    ASSERT_TRUE(meta->rowsets(1).has_delete_predicate());

    auto applier = new_txn_log_applier(tablet, meta, 2, false);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(40061);
    log->set_txn_id(301);
    auto* op_rep = log->mutable_op_replication();
    auto* txn_meta = op_rep->mutable_txn_meta();
    txn_meta->set_txn_id(301);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_snapshot_version(2);
    txn_meta->set_data_version(0);
    txn_meta->set_incremental_snapshot(false); // full snapshot, no tablet_metadata -> non-lake Swap path

    ASSERT_TRUE(applier->apply(*log).ok());

    // All superseded old rowsets are swapped into compaction_inputs without their delete_predicate.
    ASSERT_EQ(2, meta->compaction_inputs_size());
    EXPECT_FALSE(meta->compaction_inputs(0).has_delete_predicate());
    EXPECT_FALSE(meta->compaction_inputs(1).has_delete_predicate());
}

// Regression: PK NON-LAKE replication (full snapshot, no tablet_metadata) swaps all superseded old
// rowsets into compaction_inputs — delete_predicate must be dropped across the Swap too.
TEST(TxnLogApplierCompactionTest, PKNonLakeReplicationDropsDeletePredicate) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 40071);
    auto meta = build_pk_metadata(40071);
    meta->set_next_rowset_id(10);
    auto* r0 = meta->add_rowsets();
    r0->set_id(1);
    r0->set_num_rows(10);
    r0->add_segments("old0.dat");
    r0->mutable_delete_predicate()->set_version(1);
    auto* r1 = meta->add_rowsets();
    r1->set_id(2);
    r1->set_num_rows(20);
    r1->add_segments("old1.dat");
    r1->mutable_delete_predicate()->set_version(2);
    ASSERT_TRUE(meta->rowsets(0).has_delete_predicate());
    ASSERT_TRUE(meta->rowsets(1).has_delete_predicate());

    auto applier = new_txn_log_applier(tablet, meta, 2, false);

    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(40071);
    log->set_txn_id(401);
    auto* op_rep = log->mutable_op_replication();
    auto* txn_meta = op_rep->mutable_txn_meta();
    txn_meta->set_txn_id(401);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_snapshot_version(2);
    txn_meta->set_data_version(0);
    txn_meta->set_incremental_snapshot(false); // full snapshot, no tablet_metadata -> non-lake Swap path

    ASSERT_TRUE(applier->apply(*log).ok());

    // All superseded old rowsets are swapped into compaction_inputs without their delete_predicate.
    ASSERT_EQ(2, meta->compaction_inputs_size());
    EXPECT_FALSE(meta->compaction_inputs(0).has_delete_predicate());
    EXPECT_FALSE(meta->compaction_inputs(1).has_delete_predicate());
}

} // namespace lake
} // namespace starrocks
