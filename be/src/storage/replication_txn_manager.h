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

#include "storage/storage_engine.h"

namespace starrocks {

class ReplicationTxnManager {
public:
    explicit ReplicationTxnManager() {}

    Status remote_snapshot(const TRemoteSnapshotRequest& request, std::string* src_snapshot_path,
                           bool* incremental_snapshot);

    Status replicate_snapshot(const TReplicateSnapshotRequest& request);

    Status get_txn_related_tablets(const TTransactionId transaction_id, TPartitionId partition_id,
                                   std::vector<TTabletId>* tablet_ids);

    Status publish_txn(TTransactionId transaction_id, TPartitionId partition_id, const TabletSharedPtr& tablet,
                       int64_t version);

    void clear_txn(TTransactionId transaction_id) { clear_txn_snapshots(transaction_id); }

    void clear_expired_snapshots();

    DISALLOW_COPY_AND_MOVE(ReplicationTxnManager);

private:
    Status make_remote_snapshot(const TRemoteSnapshotRequest& request, const std::vector<Version>* missed_versions,
                                const std::vector<int64_t>* missing_version_ranges, TBackend* src_backend,
                                std::string* src_snapshot_path);

    Status replicate_remote_snapshot(const TReplicateSnapshotRequest& request,
                                     const TRemoteSnapshotInfo& src_snapshot_info,
                                     const std::string& tablet_snapshot_dir_path, Tablet* tablet);

    Status convert_snapshot_for_none_primary(const std::string& tablet_snapshot_path,
                                             const TReplicateSnapshotRequest& request);

    Status convert_snapshot_for_primary(const std::string& tablet_snapshot_path,
                                        const TReplicateSnapshotRequest& request, Tablet* tablet);

    Status publish_snapshot(Tablet* tablet, const string& snapshot_dir, int64_t snapshot_version,
                            bool incremental_snapshot);

    Status publish_snapshot_for_primary(Tablet* tablet, const std::string& snapshot_dir);

    Status publish_incremental_meta(Tablet* tablet, const TabletMeta& cloned_tablet_meta, int64_t snapshot_version);

    Status publish_full_meta(Tablet* tablet, TabletMeta* cloned_tablet_meta,
                             std::vector<RowsetMetaSharedPtr>& rs_to_clone);

    void clear_txn_snapshots(TTransactionId transaction_id);

    Status save_tablet_txn_meta(DataDir* data_dir, TTransactionId transaction_id, TPartitionId partition_id,
                                TTabletId tablet_id, const ReplicationTxnMetaPB& txn_meta);

    Status save_tablet_txn_meta(const std::string& tablet_txn_dir_path, const ReplicationTxnMetaPB& txn_meta);

    Status load_tablet_txn_meta(DataDir* data_dir, TTransactionId transaction_id, TPartitionId partition_id,
                                TTabletId tablet_id, ReplicationTxnMetaPB& txn_meta);

    Status load_tablet_txn_meta(const std::string& tablet_txn_dir_path, ReplicationTxnMetaPB& txn_meta);

    StatusOr<TabletSharedPtr> get_tablet(TTabletId tablet_id);
};

} // namespace starrocks
