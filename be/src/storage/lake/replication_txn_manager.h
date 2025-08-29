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
#include "storage/lake/remote_starlet_location_provider.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/replication_utils.h"
#include "storage/rowset/rowset_meta.h"

namespace starrocks {
class FileSystem;
} // namespace starrocks

using starrocks::FileConverterCreatorFunc;

namespace starrocks::lake {

class TabletManager;

struct RowsetMetaComparator {
    bool operator()(const RowsetMetadataPB& rs_a, const RowsetMetadataPB& rs_b) const { return rs_a.id() < rs_b.id(); }
};

class ReplicationTxnManager {
public:
    explicit ReplicationTxnManager(lake::TabletManager* tablet_manager)
            : _tablet_manager(tablet_manager),
              _remote_location_provider(std::make_unique<lake::RemoteStarletLocationProvider>()) {}

    Status remote_snapshot(const TRemoteSnapshotRequest& request, TSnapshotInfo* src_snapshot_info);

    Status replicate_snapshot(const TReplicateSnapshotRequest& request);

    Status clear_snapshots(const TxnLogPtr& txn_slog);

    DISALLOW_COPY_AND_MOVE(ReplicationTxnManager);

private:
    Status make_remote_snapshot(const TRemoteSnapshotRequest& request, const std::vector<Version>* missed_versions,
                                const std::vector<int64_t>* missing_version_ranges, TBackend* src_backend,
                                std::string* src_snapshot_path);

    Status replicate_remote_snapshot(const TReplicateSnapshotRequest& request, const TSnapshotInfo& src_snapshot_info,
                                     const TabletMetadataPtr& tablet_metadata);

    Status replicate_lake_remote_storage(const TReplicateSnapshotRequest& request);

    StatusOr<TabletMetadataPtr> build_source_tablet_meta(int64_t src_tablet_id, int64_t version,
                                                         const std::string& meta_dir,
                                                         const std::shared_ptr<FileSystem>& shared_src_fs);

    // incrementally build mutable rowset metadata for txn log,
    Status convert_lake_replicate_rowset_meta(
            const RowsetMetadataPB& src_rowset_meta, RowsetMetadataPB* new_rowset_meta, TTransactionId txn_id,
            int64_t src_tablet_id, int64_t target_tablet_id, const std::string& src_data_dir,
            const std::unordered_set<std::string>& existed_filename_uuids,
            std::unordered_map<std::string, size_t>& segment_name_to_size_map,
            std::map<std::string, std::string>& file_locations,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>>& filename_map);

    Status convert_lake_replicate_sstable_meta(
            const std::shared_ptr<TabletMetadataPB>& new_metadata, const TabletMetadataPtr& src_current_tablet_meta,
            int64_t txn_id, int64_t target_tablet_id, const std::string& src_data_dir,
            const std::unordered_set<std::string>& existed_filename_uuids,
            std::map<std::string, std::string>& file_locations,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>>& filename_map);

    FileConverterCreatorFunc build_file_converters(
            const TReplicateSnapshotRequest& request,
            const std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>>& filename_map,
            std::unordered_map<uint32_t, uint32_t>& column_unique_id_map,
            std::vector<std::string>& files_to_delete) const;

private:
    static Status convert_rowset_meta(
            const RowsetMeta& rowset_meta, TTransactionId transaction_id, TxnLogPB::OpWrite* op_write,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionInfo>>* segment_filename_map);

    static Status convert_delete_predicate_pb(DeletePredicatePB* delete_predicate);

private:
    lake::TabletManager* _tablet_manager;
    std::unique_ptr<lake::RemoteStarletLocationProvider> _remote_location_provider;
    std::unordered_map<int64_t, std::shared_ptr<FileSystem>> _faked_starlet_fs_map;
};

} // namespace starrocks::lake
