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

#include <cstdint>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet.h"
#include "storage/lake/txn_log_applier.h"

namespace starrocks::lake {

namespace {

MutableTabletMetadataPtr new_non_pk_metadata(int64_t tablet_id) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(1);
    metadata->set_next_rowset_id(1);
    metadata->mutable_schema()->set_id(1001);
    metadata->mutable_schema()->set_keys_type(DUP_KEYS);
    return metadata;
}

RowsetMetadataPB build_rowset(uint32_t rowset_id, const std::string& segment_name,
                              const std::string& del_file_name = "") {
    RowsetMetadataPB rowset;
    rowset.set_id(rowset_id);
    rowset.set_num_rows(10);
    rowset.set_data_size(100);
    rowset.add_segments(segment_name);
    rowset.add_segment_size(100);
    if (!del_file_name.empty()) {
        auto* del_file = rowset.add_del_files();
        del_file->set_name(del_file_name);
    }
    return rowset;
}

TxnLogPB build_full_lake_replication_log(int64_t tablet_id, int64_t txn_id,
                                         const std::vector<RowsetMetadataPB>& new_rowsets, uint32_t next_rowset_id) {
    TxnLogPB log;
    log.set_tablet_id(tablet_id);
    log.set_txn_id(txn_id);
    auto* op_replication = log.mutable_op_replication();

    auto* txn_meta = op_replication->mutable_txn_meta();
    txn_meta->set_txn_id(txn_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_visible_version(1);
    txn_meta->set_snapshot_version(2);
    txn_meta->set_data_version(0);
    txn_meta->set_incremental_snapshot(false);

    auto* tablet_metadata = op_replication->mutable_tablet_metadata();
    tablet_metadata->set_next_rowset_id(next_rowset_id);
    for (const auto& rowset : new_rowsets) {
        tablet_metadata->add_rowsets()->CopyFrom(rowset);
    }
    return log;
}

} // namespace

TEST(TxnLogApplierLakeReplicationTest, FullReplicationSkipsCompactionInputsWhenSegmentOverlaps) {
    constexpr int64_t kTabletId = 902001;
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), kTabletId);
    auto metadata = new_non_pk_metadata(kTabletId);
    metadata->add_rowsets()->CopyFrom(
            build_rowset(10, "0000000000000010_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat", "shared.del"));

    auto applier = new_txn_log_applier(tablet, metadata, 2, false, true);

    std::vector<RowsetMetadataPB> new_rowsets;
    new_rowsets.emplace_back(
            build_rowset(20, "0000000000000020_aaaaaaaa-bbbb-cccc-dddd-000000000001.dat", "shared.del"));
    TxnLogPB log = build_full_lake_replication_log(kTabletId, 70001, new_rowsets, 21);

    ASSERT_OK(applier->apply(log));
    EXPECT_EQ(1, metadata->rowsets_size());
    EXPECT_EQ(20, metadata->rowsets(0).id());
    EXPECT_EQ(0, metadata->compaction_inputs_size());
}

TEST(TxnLogApplierLakeReplicationTest, FullReplicationMovesOnlyNonOverlappedOldRowsetToCompactionInputs) {
    constexpr int64_t kTabletId = 902002;
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), kTabletId);
    auto metadata = new_non_pk_metadata(kTabletId);
    metadata->add_rowsets()->CopyFrom(build_rowset(11, "0000000000000011_deadbeef-dead-beef-dead-beef00000001.dat"));

    auto applier = new_txn_log_applier(tablet, metadata, 2, false, true);

    std::vector<RowsetMetadataPB> new_rowsets;
    new_rowsets.emplace_back(build_rowset(21, "0000000000000021_cafebabe-cafe-babe-cafe-babe00000001.dat"));
    TxnLogPB log = build_full_lake_replication_log(kTabletId, 70002, new_rowsets, 22);

    ASSERT_OK(applier->apply(log));
    ASSERT_EQ(1, metadata->compaction_inputs_size());
    EXPECT_EQ(11, metadata->compaction_inputs(0).id());
    EXPECT_EQ("0000000000000011_deadbeef-dead-beef-dead-beef00000001.dat", metadata->compaction_inputs(0).segments(0));
}

TEST(TxnLogApplierLakeReplicationTest, FullReplicationSkipsCompactionInputsWhenRowsetIdOverlaps) {
    constexpr int64_t kTabletId = 902003;
    Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), kTabletId);
    auto metadata = new_non_pk_metadata(kTabletId);
    metadata->add_rowsets()->CopyFrom(build_rowset(30, "0000000000000030_old-old0-old0-old0-old000000001.dat"));

    auto applier = new_txn_log_applier(tablet, metadata, 2, false, true);

    std::vector<RowsetMetadataPB> new_rowsets;
    new_rowsets.emplace_back(build_rowset(30, "0000000000000031_new-new0-new0-new0-new000000001.dat"));
    TxnLogPB log = build_full_lake_replication_log(kTabletId, 70003, new_rowsets, 31);

    ASSERT_OK(applier->apply(log));
    EXPECT_EQ(0, metadata->compaction_inputs_size());
}

} // namespace starrocks::lake
