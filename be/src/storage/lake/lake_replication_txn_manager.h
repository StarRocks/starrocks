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
#include "storage/lake/remote_starlet_location_provider.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/replication_utils.h"

namespace starrocks {
struct FileEncryptionPair;
class TabletMetadataPB;
class FileSystem;
} // namespace starrocks

using starrocks::FileConverterCreatorFunc;
namespace starrocks::lake {

class TabletManager;

class LakeReplicationTxnManager {
public:
    explicit LakeReplicationTxnManager(lake::TabletManager* tablet_manager)
            : _tablet_manager(tablet_manager),
              _remote_location_provider(std::make_unique<lake::RemoteStarletLocationProvider>()) {}

    Status replicate_lake_remote_storage(const TReplicateSnapshotRequest& request);

    StatusOr<TabletMetadataPtr> build_source_tablet_meta(int64_t src_tablet_id, int64_t version,
                                                         const std::string& meta_dir,
                                                         const std::shared_ptr<FileSystem>& shared_src_fs);

    // Helper function to build existed filename UUIDs map from target tablet metadata
    // For files that replicated from source storage, we keep the `uuid` part of file name, and use it to decide if the file
    // is already replicated to target storage. Map from UUID to target filename.
    // Also, in order to support file encryption, we also need to keep the encryption meta.
    Status build_existed_filename_uuids_map(
            const TabletMetadataPtr& target_data_version_tablet_meta,
            std::unordered_map<std::string, std::pair<std::string, std::string>>& existed_filename_uuids);

    // Helper function to create replication txn log with converted metadata
    StatusOr<std::shared_ptr<TabletMetadataPB>> convert_and_build_new_tablet_meta(
            const TabletMetadataPtr& src_tablet_meta, int64_t src_tablet_id, int64_t target_tablet_id,
            TTransactionId txn_id, int64_t data_version, const std::string& src_data_dir,
            std::unordered_map<std::string, size_t>& segment_name_to_size_map,
            std::map<std::string, std::string>& file_locations,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map);

    // Helper functions for filename conversion and building file mappings
    StatusOr<bool> determine_final_filename(
            const std::string& src_filename, TTransactionId txn_id,
            const std::unordered_map<std::string, std::pair<std::string, std::string>>& existed_filename_uuids,
            std::string& final_filename, int64_t target_tablet_id, const std::string& src_data_dir,
            std::map<std::string, std::string>& file_locations,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map);

private:
    TabletManager* _tablet_manager;
    std::unique_ptr<lake::RemoteStarletLocationProvider> _remote_location_provider;
    std::unordered_map<int64_t, std::shared_ptr<FileSystem>> _faked_starlet_fs_map;
};

} // namespace starrocks::lake
