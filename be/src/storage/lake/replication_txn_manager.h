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

#pragma once

#include "common/status.h"
#include "fs/encryption.h"
#include "gen_cpp/AgentService_types.h"
#include "gutil/macros.h"
#include "lake_replication_txn_manager.h"
#include "storage/delta_column_group.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_meta.h"
#include "tablet_manager.h"

namespace starrocks {
class ThreadPool;
} // namespace starrocks

using starrocks::FileConverterCreatorFunc;

namespace starrocks::lake {

class TabletManager;

class ReplicationTxnManager {
public:
    explicit ReplicationTxnManager(lake::TabletManager* tablet_manager) : _tablet_manager(tablet_manager) {
        _lake_replication_txn_manager = std::make_unique<LakeReplicationTxnManager>(tablet_manager);
    }

    Status remote_snapshot(const TRemoteSnapshotRequest& request, TSnapshotInfo* src_snapshot_info);

    Status replicate_snapshot(const TReplicateSnapshotRequest& request, ThreadPool* replicate_file_thread_pool);

    Status clear_snapshots(const TxnLogPtr& txn_slog);

    DISALLOW_COPY_AND_MOVE(ReplicationTxnManager);

    static FileConverterCreatorFunc build_file_converters(
            const TabletManager* tablet_manager, const TReplicateSnapshotRequest& request,
            const std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map,
            std::unordered_map<uint32_t, uint32_t>& column_unique_id_map, std::vector<std::string>& files_to_delete);

    // Convert DeltaColumnGroupSnapshotPB from shared-nothing non-PK table snapshot
    // into DeltaColumnGroupMetadataPB for shared-data replication.
    // Also populates filename_map with old->new .cols filename mappings.
    static Status convert_dcg_meta_for_non_pk(
            const DeltaColumnGroupSnapshotPB& dcg_snapshot_pb,
            const std::unordered_map<std::string, uint32_t>& rowset_id_to_seg_id, TTransactionId transaction_id,
            DeltaColumnGroupMetadataPB* dcg_meta,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>* filename_map);

    // Convert DeltaColumnGroupList from shared-nothing PK table snapshot
    // into DeltaColumnGroupMetadataPB for shared-data replication.
    // Also populates filename_map with old->new .cols filename mappings.
    static Status convert_dcg_meta_for_pk(
            const std::unordered_map<uint32_t, DeltaColumnGroupList>& delta_column_groups,
            TTransactionId transaction_id, DeltaColumnGroupMetadataPB* dcg_meta,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>* filename_map);

    // Convert column unique IDs in DCG metadata using the provided column_unique_id_map.
    static Status convert_dcg_column_unique_ids(DeltaColumnGroupMetadataPB* dcg_meta,
                                                const std::unordered_map<uint32_t, uint32_t>& column_unique_id_map);

private:
    Status make_remote_snapshot(const TRemoteSnapshotRequest& request, const std::vector<Version>* missed_versions,
                                const std::vector<int64_t>* missing_version_ranges, TBackend* src_backend,
                                std::string* src_snapshot_path);

    Status replicate_remote_snapshot(const TReplicateSnapshotRequest& request, const TSnapshotInfo& src_snapshot_info,
                                     const TabletMetadataPtr& tablet_metadata);

    static Status convert_rowset_meta(
            const RowsetMeta& rowset_meta, TTransactionId transaction_id, TxnLogPB::OpWrite* op_write,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>* segment_filename_map);

    static Status convert_delete_predicate_pb(DeletePredicatePB* delete_predicate);

private:
    lake::TabletManager* _tablet_manager;
    std::unique_ptr<LakeReplicationTxnManager> _lake_replication_txn_manager;
};

} // namespace starrocks::lake
