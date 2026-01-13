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

// Test for apply_tablet_metadata_from_replication function
class TabletMetadataReplicationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create source tablet metadata
        source_meta_ = std::make_shared<TabletMetadata>();
        source_meta_->set_id(1000);
        source_meta_->set_version(5);
        source_meta_->set_next_rowset_id(100);
        source_meta_->set_cumulative_point(2);

        // Add some rowsets to source
        auto* rowset1 = source_meta_->add_rowsets();
        rowset1->set_id(10);
        rowset1->set_num_rows(100);

        auto* rowset2 = source_meta_->add_rowsets();
        rowset2->set_id(20);
        rowset2->set_num_rows(200);

        // Add dcg_meta
        source_meta_->mutable_dcg_meta()->set_dcgs("test_dcg_data");

        // Add sstable_meta
        auto* sstable = source_meta_->mutable_sstable_meta()->add_sstables();
        sstable->set_filename("test.sst");
        sstable->set_filesize(1024);

        // Add delvec_meta
        source_meta_->mutable_delvec_meta()->set_version(1);

        // Create target metadata with some old rowsets
        target_meta_ = std::make_shared<TabletMetadata>();
        target_meta_->set_id(2000);
        target_meta_->set_version(1);

        // Add old rowsets (some will be kept, some will be moved to compaction_inputs)
        auto* old_rowset1 = old_rowsets_.Add();
        old_rowset1->set_id(5); // This will be moved to compaction_inputs (not in new rowsets)

        auto* old_rowset2 = old_rowsets_.Add();
        old_rowset2->set_id(10); // This will be kept (exists in new rowsets)

        auto* old_rowset3 = old_rowsets_.Add();
        old_rowset3->set_id(15); // This will be moved to compaction_inputs (not in new rowsets)
    }

    MutableTabletMetadataPtr source_meta_;
    MutableTabletMetadataPtr target_meta_;
    RepeatedPtrField<RowsetMetadataPB> old_rowsets_;
};

TEST_F(TabletMetadataReplicationTest, BasicMetadataCopy) {
    Status st = apply_tablet_metadata_from_replication(target_meta_, *source_meta_, old_rowsets_);
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Check rowsets are copied
    EXPECT_EQ(2, target_meta_->rowsets_size());
    EXPECT_EQ(10u, target_meta_->rowsets(0).id());
    EXPECT_EQ(20u, target_meta_->rowsets(1).id());

    // Check metadata fields are copied
    EXPECT_EQ(100u, target_meta_->next_rowset_id());
    EXPECT_EQ(0u, target_meta_->cumulative_point());

    // Check dcg_meta is copied
    EXPECT_TRUE(target_meta_->has_dcg_meta());
    EXPECT_EQ("test_dcg_data", target_meta_->dcg_meta().dcgs());

    // Check sstable_meta is copied
    EXPECT_TRUE(target_meta_->has_sstable_meta());
    EXPECT_EQ(1, target_meta_->sstable_meta().sstables_size());
    EXPECT_EQ("test.sst", target_meta_->sstable_meta().sstables(0).filename());

    // Check delvec_meta is copied
    EXPECT_TRUE(target_meta_->has_delvec_meta());
    EXPECT_EQ(1, target_meta_->delvec_meta().version());
}

TEST_F(TabletMetadataReplicationTest, CompactionInputsHandling) {
    Status st = apply_tablet_metadata_from_replication(target_meta_, *source_meta_, old_rowsets_);
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Check compaction_inputs: rowsets with id 5 and 15 should be added (10 is kept in new rowsets)
    EXPECT_EQ(2, target_meta_->compaction_inputs_size());
    std::unordered_set<uint32_t> compaction_input_ids;
    for (const auto& rowset : target_meta_->compaction_inputs()) {
        compaction_input_ids.insert(rowset.id());
    }
    EXPECT_TRUE(compaction_input_ids.count(5));
    EXPECT_TRUE(compaction_input_ids.count(15));
    EXPECT_FALSE(compaction_input_ids.count(10));
}

TEST_F(TabletMetadataReplicationTest, EmptyRowsets) {
    // Create source metadata with no rowsets
    auto empty_source = std::make_shared<TabletMetadata>();
    empty_source->set_id(3000);
    empty_source->set_next_rowset_id(200);

    RepeatedPtrField<RowsetMetadataPB> empty_old_rowsets;

    Status st = apply_tablet_metadata_from_replication(target_meta_, *empty_source, empty_old_rowsets);
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Rowsets should remain unchanged (original rowsets cleared since rowsets_size() == 0)
    EXPECT_EQ(0, target_meta_->rowsets_size());
    EXPECT_EQ(200u, target_meta_->next_rowset_id());
    EXPECT_EQ(0, target_meta_->compaction_inputs_size());
}

TEST_F(TabletMetadataReplicationTest, NoOldRowsets) {
    RepeatedPtrField<RowsetMetadataPB> empty_old_rowsets;

    Status st = apply_tablet_metadata_from_replication(target_meta_, *source_meta_, empty_old_rowsets);
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Check rowsets are copied
    EXPECT_EQ(2, target_meta_->rowsets_size());

    // No old rowsets, so compaction_inputs should be empty
    EXPECT_EQ(0, target_meta_->compaction_inputs_size());
}

TEST_F(TabletMetadataReplicationTest, PartialMetadataCopy) {
    // Create source with only some metadata types
    auto partial_source = std::make_shared<TabletMetadata>();
    partial_source->set_id(4000);
    partial_source->set_next_rowset_id(300);

    // Add only dcg_meta, no other metadata types
    partial_source->mutable_dcg_meta()->set_dcgs("partial_dcg");

    // Add one rowset
    auto* rowset = partial_source->add_rowsets();
    rowset->set_id(30);

    Status st = apply_tablet_metadata_from_replication(target_meta_, *partial_source, old_rowsets_);
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Check rowset is copied
    EXPECT_EQ(1, target_meta_->rowsets_size());
    EXPECT_EQ(30u, target_meta_->rowsets(0).id());

    // Check dcg_meta is copied
    EXPECT_TRUE(target_meta_->has_dcg_meta());
    EXPECT_EQ("partial_dcg", target_meta_->dcg_meta().dcgs());

    // Check other metadata types are not set (should be empty)
    EXPECT_FALSE(target_meta_->has_sstable_meta());
    EXPECT_FALSE(target_meta_->has_delvec_meta());

    // Check compaction_inputs still works
    EXPECT_EQ(2, target_meta_->compaction_inputs_size());
}

} // namespace lake
} // namespace starrocks
