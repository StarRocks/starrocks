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
#include "gen_cpp/AgentService_types.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_meta.h"

namespace starrocks::lake {

class TabletManager;

class ReplicationTxnManager {
public:
    explicit ReplicationTxnManager(lake::TabletManager* tablet_manager) : _tablet_manager(tablet_manager) {}

    Status remote_snapshot(const TRemoteSnapshotRequest& request, std::string* src_snapshot_path,
                           bool* incremental_snapshot);

    Status replicate_snapshot(const TReplicateSnapshotRequest& request);

    Status clear_snapshots(const TxnLogPtr& txn_slog);

    DISALLOW_COPY_AND_MOVE(ReplicationTxnManager);

private:
    Status make_remote_snapshot(const TRemoteSnapshotRequest& request, const std::vector<Version>* missed_versions,
                                const std::vector<int64_t>* missing_version_ranges, TBackend* src_backend,
                                std::string* src_snapshot_path);

    StatusOr<TxnLogPtr> replicate_remote_snapshot(const TReplicateSnapshotRequest& request,
                                                  const TRemoteSnapshotInfo& src_snapshot_info,
                                                  const TabletMetadataPtr& tablet_metadata);

    Status convert_rowset_meta(const RowsetMeta& rowset_meta, TTransactionId transaction_id,
                               TxnLogPB::OpWrite* op_write,
                               std::unordered_map<std::string, std::string>* segment_filename_map);

private:
    lake::TabletManager* _tablet_manager;
};

} // namespace starrocks::lake
