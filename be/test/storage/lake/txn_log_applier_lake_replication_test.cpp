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
#include "storage/lake/replication_schema_utils.h"
#include "storage/lake/tablet.h"
#include "storage/lake/txn_log_applier.h"
#include "storage/storage_env.h"

namespace starrocks::lake {

namespace {

MutableTabletMetadataPtr new_metadata(int64_t tablet_id, KeysType keys_type) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(tablet_id);
    metadata->set_version(1);
    metadata->set_next_rowset_id(1);
    metadata->mutable_schema()->set_id(1001);
    metadata->mutable_schema()->set_keys_type(keys_type);
    return metadata;
}

RowsetMetadataPB build_rowset(uint32_t rowset_id, const std::vector<std::string>& segment_names,
                              const std::vector<std::string>& del_file_names = {}) {
    RowsetMetadataPB rowset;
    rowset.set_id(rowset_id);
    rowset.set_num_rows(10);
    rowset.set_data_size(segment_names.size() * 100);
    for (const auto& segment_name : segment_names) {
        auto* segment = rowset.add_segment_metas();
        segment->set_filename(segment_name);
        segment->set_size(100);
    }
    for (const auto& del_file_name : del_file_names) {
        auto* del_file = rowset.add_del_files();
        del_file->set_name(del_file_name);
    }
    return rowset;
}

RowsetMetadataPB build_rowset(uint32_t rowset_id, const std::string& segment_name,
                              const std::string& del_file_name = "") {
    std::vector<std::string> del_file_names;
    if (!del_file_name.empty()) {
        del_file_names.emplace_back(del_file_name);
    }
    return build_rowset(rowset_id, std::vector<std::string>{segment_name}, del_file_names);
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

TEST(TxnLogApplierLakeReplicationTest, NormalizedReplicationReplacesSchemaTrackingAndCollectsRewrittenFiles) {
    for (auto keys_type : {DUP_KEYS, PRIMARY_KEYS}) {
        constexpr int64_t kTabletId = 902010;
        Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), kTabletId);
        auto metadata = new_metadata(kTabletId, keys_type);
        metadata->add_rowsets()->CopyFrom(build_rowset(10, "old.dat", "shared.del"));
        (*metadata->mutable_historical_schemas())[900].CopyFrom(metadata->schema());
        (*metadata->mutable_rowset_to_schema())[10] = 900;
        auto log = build_full_lake_replication_log(kTabletId, 70010, {build_rowset(10, "rewritten.dat", "shared.del")},
                                                   11);
        auto* op = log.mutable_op_replication();
        op->set_normalize_rowset_schema(true);
        op->mutable_tablet_metadata()->mutable_schema()->CopyFrom(metadata->schema());
        normalize_replication_rowset_schemas(op->mutable_tablet_metadata());
        // Round-trip the durable marker; publish intentionally does not consult the admission flag.
        TxnLogPB replay;
        ASSERT_TRUE(replay.ParseFromString(log.SerializeAsString()));
        auto applier = new_txn_log_applier(tablet, metadata, 2, false, true);
        ASSERT_OK(applier->apply(replay));
        EXPECT_EQ(1, metadata->historical_schemas_size());
        EXPECT_EQ(metadata->schema().id(), metadata->rowset_to_schema().at(10));
        ASSERT_EQ(1, metadata->compaction_inputs_size());
        EXPECT_EQ("old.dat", metadata->compaction_inputs(0).segment_metas(0).filename());
        EXPECT_TRUE(metadata->compaction_inputs(0).del_files().empty());
        EXPECT_EQ("rewritten.dat", metadata->rowsets(0).segment_metas(0).filename());
    }
}

TEST(TxnLogApplierLakeReplicationTest, NormalizedReplicationRejectsSchemaChangeBeforeAnyMutation) {
    for (auto keys_type : {DUP_KEYS, PRIMARY_KEYS}) {
        constexpr int64_t kTabletId = 902011;
        Tablet tablet(StorageEnv::GetInstance()->lake_tablet_manager(), kTabletId);
        auto metadata = new_metadata(kTabletId, keys_type);
        metadata->add_rowsets()->CopyFrom(build_rowset(10, "old.dat"));
        auto log = build_full_lake_replication_log(kTabletId, 70011, {build_rowset(20, "new.dat")}, 21);
        auto* op = log.mutable_op_replication();
        op->set_normalize_rowset_schema(true);
        op->mutable_tablet_metadata()->mutable_schema()->CopyFrom(metadata->schema());
        normalize_replication_rowset_schemas(op->mutable_tablet_metadata());
        metadata->mutable_schema()->set_schema_version(2);
        auto applier = new_txn_log_applier(tablet, metadata, 2, false, true);
        const auto before = metadata->SerializeAsString();
        EXPECT_TRUE(applier->apply(log).is_not_supported());
        EXPECT_EQ(before, metadata->SerializeAsString());
    }
}

} // namespace starrocks::lake
