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

#include "runtime/exec_env.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log_applier.h"

namespace starrocks {
namespace lake {

namespace {

// Build a non-PK TabletMetadata with one user column. The unique_id must be
// set so that schema-flag self-healing in apply_add_index has a target.
MutableTabletMetadataPtr make_npk_meta(int64_t id, int64_t version) {
    auto meta = std::make_shared<TabletMetadata>();
    meta->set_id(id);
    meta->set_version(version);
    meta->set_next_rowset_id(0);
    auto* schema = meta->mutable_schema();
    schema->set_id(100);
    schema->set_keys_type(DUP_KEYS);
    auto* col = schema->add_column();
    col->set_unique_id(7);
    col->set_name("c1");
    col->set_type("INT");
    return meta;
}

MutableTabletMetadataPtr make_pk_meta(int64_t id, int64_t version) {
    auto meta = std::make_shared<TabletMetadata>();
    meta->set_id(id);
    meta->set_version(version);
    meta->set_next_rowset_id(0);
    auto* schema = meta->mutable_schema();
    schema->set_id(200);
    schema->set_keys_type(PRIMARY_KEYS);
    return meta;
}

} // namespace

// ADD INDEX -> NPK applier path: exercises NonPrimaryKeyTxnLogApplier::apply
// transient MetaFileBuilder branch (lines 894-905 in txn_log_applier.cpp) and
// MetaFileBuilder::apply_add_index. After apply, idg_meta carries the new
// segment entry and table_indices reconciles the new index.
TEST(TxnLogApplierIdgTest, npk_apply_op_add_index_populates_idg_meta) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 70001);
    auto meta = make_npk_meta(70001, /*version=*/4);
    auto applier = new_txn_log_applier(tablet, meta, /*new_version=*/5, /*rebuild_pindex=*/false,
                                       /*skip_write_tablet_metadata=*/true);

    TxnLogPB log;
    log.set_tablet_id(70001);
    log.set_txn_id(7100);
    auto* op = log.mutable_op_add_index();
    op->set_alter_version(5);
    auto* se = op->add_segment_entries();
    se->set_segment_id(0);
    auto* entry = se->mutable_entry();
    entry->set_index_file("idx_a.idx");
    entry->set_version(5);
    auto* k = entry->add_keys();
    k->set_col_unique_id(7);
    k->set_index_type(BITMAP);
    auto* new_ix = op->add_new_indexes();
    new_ix->set_index_id(901);
    new_ix->set_index_type(BITMAP);
    new_ix->add_col_unique_id(7);

    Status st = applier->apply(log);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(meta->has_idg_meta());
    ASSERT_EQ(1u, meta->idg_meta().idgs().size());
    ASSERT_NE(meta->idg_meta().idgs().find(0), meta->idg_meta().idgs().end());
    EXPECT_EQ("idx_a.idx", meta->idg_meta().idgs().at(0).entries(0).index_file());
    ASSERT_EQ(1, meta->schema().table_indices_size());
    EXPECT_EQ(901, meta->schema().table_indices(0).index_id());
    EXPECT_TRUE(meta->schema().column(0).has_bitmap_index());
}

// DROP INDEX -> NPK applier path: ensures the apply path tombstones the
// matching TabletIndexPB into dropped_table_indices via the transient
// MetaFileBuilder.
TEST(TxnLogApplierIdgTest, npk_apply_op_drop_index_records_tombstone) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 70002);
    auto meta = make_npk_meta(70002, /*version=*/6);
    // Pre-populate schema with the index that will be dropped.
    auto* ix = meta->mutable_schema()->add_table_indices();
    ix->set_index_id(902);
    ix->set_index_type(BITMAP);
    ix->add_col_unique_id(7);

    auto applier = new_txn_log_applier(tablet, meta, /*new_version=*/7, /*rebuild_pindex=*/false,
                                       /*skip_write_tablet_metadata=*/true);
    TxnLogPB log;
    log.set_tablet_id(70002);
    log.set_txn_id(7200);
    auto* op = log.mutable_op_drop_index();
    op->set_drop_version(7);
    auto* d = op->add_dropped();
    d->set_index_id(902);
    d->set_col_unique_id(7);
    d->set_index_type(BITMAP);

    Status st = applier->apply(log);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, meta->schema().table_indices_size());
    ASSERT_EQ(1, meta->schema().dropped_table_indices_size());
    EXPECT_EQ(902, meta->schema().dropped_table_indices(0).index_id());
}

// Lake replication carries idg_meta from source TabletMetadata into the
// target during a full snapshot (tablet_metadata branch, lines 1353-1360).
TEST(TxnLogApplierIdgTest, npk_apply_replication_carries_idg_meta) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 70003);
    auto meta = make_npk_meta(70003, /*version=*/1);
    auto applier = new_txn_log_applier(tablet, meta, /*new_version=*/2, /*rebuild_pindex=*/false,
                                       /*skip_write_tablet_metadata=*/true);

    TxnLogPB log;
    log.set_tablet_id(70003);
    log.set_txn_id(7300);
    auto* op_repl = log.mutable_op_replication();
    auto* tm = op_repl->mutable_txn_meta();
    tm->set_txn_id(7300);
    tm->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    tm->set_snapshot_version(2);
    tm->set_data_version(0); // full snapshot
    auto* src = op_repl->mutable_tablet_metadata();
    src->set_id(70003);
    src->set_next_rowset_id(20);
    auto& idg_ver = (*src->mutable_idg_meta()->mutable_idgs())[3];
    auto* e = idg_ver.add_entries();
    e->set_index_file("rep_new.idx");
    e->set_version(2);

    Status st = applier->apply(log);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(meta->has_idg_meta());
    ASSERT_NE(meta->idg_meta().idgs().find(3), meta->idg_meta().idgs().end());
    EXPECT_EQ("rep_new.idx", meta->idg_meta().idgs().at(3).entries(0).index_file());
}

// Lake replication moves stale IDG entries to orphan_files unless the new
// metadata still references the same index_file (lines 1391-1398, plus
// collect_idg_orphan_files path).
TEST(TxnLogApplierIdgTest, npk_apply_replication_collects_old_idx_orphans) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 70004);
    auto meta = make_npk_meta(70004, /*version=*/1);
    // Pre-existing IDG entries on the *target*; the unreferenced one must
    // become an orphan, the still-referenced one must be skipped.
    auto& stale = (*meta->mutable_idg_meta()->mutable_idgs())[5];
    auto* unref = stale.add_entries();
    unref->set_index_file("stale_unref.idx");
    unref->set_file_size(123);
    unref->set_shared_file(false);
    auto* still_ref = stale.add_entries();
    still_ref->set_index_file("kept.idx");
    still_ref->set_shared_file(false);

    auto applier = new_txn_log_applier(tablet, meta, /*new_version=*/2, /*rebuild_pindex=*/false,
                                       /*skip_write_tablet_metadata=*/true);
    TxnLogPB log;
    log.set_tablet_id(70004);
    log.set_txn_id(7400);
    auto* op_repl = log.mutable_op_replication();
    auto* tm = op_repl->mutable_txn_meta();
    tm->set_txn_id(7400);
    tm->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    tm->set_snapshot_version(2);
    tm->set_data_version(0);
    auto* src = op_repl->mutable_tablet_metadata();
    src->set_id(70004);
    src->set_next_rowset_id(20);
    auto& src_ver = (*src->mutable_idg_meta()->mutable_idgs())[9];
    src_ver.add_entries()->set_index_file("kept.idx"); // shares filename with target

    Status st = applier->apply(log);
    ASSERT_TRUE(st.ok()) << st.to_string();

    bool found_orphan = false;
    bool kept_appeared_as_orphan = false;
    for (const auto& f : meta->orphan_files()) {
        if (f.name() == "stale_unref.idx") {
            found_orphan = true;
            EXPECT_EQ(123, f.size());
        }
        if (f.name() == "kept.idx") kept_appeared_as_orphan = true;
    }
    EXPECT_TRUE(found_orphan);
    EXPECT_FALSE(kept_appeared_as_orphan);
}

// PK replication: idg_meta carry-over (lines 743-745) plus orphan collection
// for the unreferenced .idx file (lines 807-811, 833).
TEST(TxnLogApplierIdgTest, pk_apply_replication_carries_idg_meta) {
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), 70005);
    auto meta = make_pk_meta(70005, /*version=*/1);
    auto& stale = (*meta->mutable_idg_meta()->mutable_idgs())[4];
    stale.add_entries()->set_index_file("pk_stale.idx");

    auto applier = new_txn_log_applier(tablet, meta, /*new_version=*/2, /*rebuild_pindex=*/false,
                                       /*skip_write_tablet_metadata=*/true);
    TxnLogPB log;
    log.set_tablet_id(70005);
    log.set_txn_id(7500);
    auto* op_repl = log.mutable_op_replication();
    auto* tm = op_repl->mutable_txn_meta();
    tm->set_txn_id(7500);
    tm->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    tm->set_snapshot_version(2);
    tm->set_data_version(0);
    auto* src = op_repl->mutable_tablet_metadata();
    src->set_id(70005);
    src->set_next_rowset_id(30);
    auto& src_ver = (*src->mutable_idg_meta()->mutable_idgs())[8];
    src_ver.add_entries()->set_index_file("pk_new.idx");

    Status st = applier->apply(log);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(meta->has_idg_meta());
    ASSERT_NE(meta->idg_meta().idgs().find(8), meta->idg_meta().idgs().end());
    EXPECT_EQ("pk_new.idx", meta->idg_meta().idgs().at(8).entries(0).index_file());

    bool stale_orphaned = false;
    for (const auto& f : meta->orphan_files()) {
        if (f.name() == "pk_stale.idx") stale_orphaned = true;
    }
    EXPECT_TRUE(stale_orphaned);
}

} // namespace lake
} // namespace starrocks
