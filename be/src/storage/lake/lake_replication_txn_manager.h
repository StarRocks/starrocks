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

#include <atomic>
#include <functional>
#include <vector>

#include "common/status.h"
#include "fs/encryption.h"
#include "gen_cpp/AgentService_types.h"
#include "storage/lake/remote_starlet_location_provider.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/replication_utils.h"

namespace starrocks {
struct FileEncryptionPair;
struct WritableFileOptions;
class TabletMetadataPB;
class FileSystem;
class ThreadPool;
} // namespace starrocks

using starrocks::FileConverterCreatorFunc;
namespace starrocks::lake {

class TabletManager;

class LakeReplicationTxnManager {
public:
    using ReplicationTask = std::function<Status()>;

    explicit LakeReplicationTxnManager(lake::TabletManager* tablet_manager)
            : _tablet_manager(tablet_manager)
#ifdef USE_STAROS
              ,
              _remote_location_provider(std::make_unique<RemoteStarletLocationProvider>())
#endif
    {
    }

    Status replicate_lake_remote_storage(const TReplicateSnapshotRequest& request,
                                         ThreadPool* replicate_file_thread_pool);

    StatusOr<TabletMetadataPtr> build_source_tablet_meta(int64_t src_tablet_id, int64_t version,
                                                         const std::string& meta_dir,
                                                         const std::shared_ptr<FileSystem>& shared_src_fs);

    // Try to build source tablet meta with fallback for legacy path formats from older StarRocks versions.
    // If the metadata is not found with the current path format, it will try legacy formats in order:
    // - Current format: db{db_id}/{table_id}/{partition_id}/meta
    // - Legacy format without db_id: {table_id}/{partition_id}/meta
    // - Very old format without db_id and partition_id: {table_id}/meta
    // If successful with a legacy format, src_meta_dir and src_data_dir will be updated to the working paths.
    StatusOr<TabletMetadataPtr> try_build_source_tablet_meta_with_fallback(
            int64_t src_tablet_id, int64_t version, int64_t src_db_id, TTransactionId txn_id, std::string& src_meta_dir,
            std::string& src_data_dir, const std::shared_ptr<FileSystem>& shared_src_fs);

    // Helper function to build existed filename UUIDs map from target tablet metadata
    // For files that replicated from source storage, we keep the `uuid` part of file name, and use it to decide if the file
    // is already replicated to target storage. Map from UUID to target filename.
    // Also, in order to support file encryption, we also need to keep the encryption meta.
    Status build_existed_filename_uuids_map(
            const TabletMetadataPtr& target_data_version_tablet_meta,
            std::unordered_map<std::string, std::pair<std::string, std::string>>& existed_filename_uuids);

    // Helper function to create replication txn log with converted metadata
    // generate and replace file names to adapt for target storage
    StatusOr<std::shared_ptr<TabletMetadataPB>> convert_and_build_new_tablet_meta(
            const TabletMetadataPtr& src_tablet_meta, const TabletMetadataPtr& target_tablet_meta,
            int64_t src_tablet_id, int64_t target_tablet_id, TTransactionId txn_id, int64_t data_version,
            const std::string& src_data_dir, std::unordered_map<std::string, size_t>& segment_name_to_size_map,
            std::map<std::string, std::string>& file_locations,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map);

    // Helper functions for filename conversion and building file mappings
    StatusOr<bool> determine_final_filename(
            const std::string& src_filename, TTransactionId txn_id,
            const std::unordered_map<std::string, std::pair<std::string, std::string>>& existed_filename_uuids,
            std::string& final_filename, int64_t target_tablet_id, const std::string& src_data_dir,
            std::map<std::string, std::string>& file_locations,
            std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map);

    // Update segment sizes in tablet metadata after segment conversion
    // This is needed because segment file size may change after footer rewriting
    Status update_tablet_metadata_segment_sizes(const std::shared_ptr<TabletMetadataPB>& tablet_metadata,
                                                const std::unordered_map<std::string, size_t>& segment_size_changes);

    // Copy a non-segment file (e.g. .del, .sst, .delvec, .cols) with size verification and retry.
    // Gets source file size first, then copies with retry on failure or size mismatch.
    // Returns the actual file size after a successful copy.
    static StatusOr<size_t> copy_non_segment_file_with_retry(const std::string& src_file_location,
                                                             const std::shared_ptr<FileSystem>& shared_src_fs,
                                                             const std::string& target_file_location,
                                                             const WritableFileOptions& opts, int max_retry);

    // Decide whether to use parallel copy for current tablet files.
    static bool should_use_parallel_copy(size_t file_count, const ThreadPool* thread_pool);

private:
    TabletManager* _tablet_manager;
#ifdef USE_STAROS
    // Used for non-S3 storage types to construct relative paths
    // S3 storage type uses full path provided by FE instead
    std::unique_ptr<RemoteStarletLocationProvider> _remote_location_provider;
#endif
};

#ifdef USE_STAROS
// Helper function to convert S3 full path to starlet URI
// Only used for S3 storage type (which supports partitioned prefix feature)
std::string convert_s3_path_to_starlet_uri(std::string_view s3_path, int64_t shard_id);
#endif

// Helper function to remove the last path component before the final directory name (meta/data)
// Example: staros://123/bucket/path/56970/63453/meta -> staros://123/bucket/path/56970/meta
std::string remove_last_path_component(const std::string& path);

// Helper function to remove db{db_id}/ component from path
// Example: staros://123/bucket/path/db56764/56970/63453/meta -> staros://123/bucket/path/56970/63453/meta
std::string remove_db_id_component(const std::string& path, int64_t db_id);

} // namespace starrocks::lake
