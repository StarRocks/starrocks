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

#include "storage/lake/snapshot_file_syncer.h"

#include <string_view>

#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "glog/logging.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/remote_starlet_location_provider.h"
#include "storage/storage_env.h"

namespace starrocks::lake {

namespace {

template <typename Files, typename SrcFunc, typename DstFunc>
Status copy_files(const Files& files, SrcFunc&& make_src, DstFunc&& make_dst, bool skip_if_exists,
                  bool skip_if_not_exists) {
    for (const auto& file : files) {
        auto src_path = make_src(file);
        auto dst_path = make_dst(file);
        VLOG(3) << "src_file: " << src_path << ", dst_file: " << dst_path;

        ASSIGN_OR_RETURN(auto src_fs, FileSystemFactory::CreateSharedFromString(src_path));
        ASSIGN_OR_RETURN(auto dst_fs, FileSystemFactory::CreateSharedFromString(dst_path));

        if (skip_if_not_exists && src_fs->path_exists(src_path).is_not_found()) {
            continue;
        }

        if (skip_if_exists && dst_fs->path_exists(dst_path).ok()) {
            continue;
        }

        ASSIGN_OR_RETURN(auto input_file, src_fs->new_sequential_file(src_path));
        ASSIGN_OR_RETURN(auto output_file, dst_fs->new_writable_file(dst_path));
        RETURN_IF_ERROR(fs::copy(input_file.get(), output_file.get(), 1024 * 1024));
        RETURN_IF_ERROR(output_file->close());
    }
    return Status::OK();
}

} // namespace

Status SnapshotFileSyncer::upload(const TabletSnapshotInfo& snapshot_info, UploadSnapshotFilesResponsePB* response) {
    DCHECK(snapshot_info.tablet_snapshot != nullptr);
#ifdef USE_STAROS
    auto src_tablet_id = snapshot_info.tablet_snapshot->tablet_id();
    auto dst_tablet_id = snapshot_info.dest_tablet_id;
    auto db_id = snapshot_info.db_id;
    auto table_id = snapshot_info.table_id;
    auto physical_partition_id = snapshot_info.physical_partition_id;

    auto location_provider = StorageEnv::GetInstance()->lake_location_provider();
    auto remote_starlet_location_provider = StorageEnv::GetInstance()->remote_starlet_location_provider();

    RETURN_IF_ERROR(copy_files(
            snapshot_info.tablet_snapshot->new_data_files(),
            [&](const auto& name) { return join_path(location_provider->segment_root_location(src_tablet_id), name); },
            [&](const auto& name) {
                return remote_starlet_location_provider->data_file_location(dst_tablet_id, db_id, table_id,
                                                                            physical_partition_id, name);
            },
            true, false));

    RETURN_IF_ERROR(copy_files(
            snapshot_info.tablet_snapshot->new_metadata_files(),
            [&](const auto& name) { return join_path(location_provider->metadata_root_location(src_tablet_id), name); },
            [&](const auto& name) {
                return remote_starlet_location_provider->metadata_file_location(dst_tablet_id, db_id, table_id,
                                                                                physical_partition_id, name);
            },
            false, false));

    // fast schema evolution v2 don't not generate schema file any more, so the schema file may not exist in src bucket
    RETURN_IF_ERROR(copy_files(
            snapshot_info.tablet_snapshot->new_schema_files(),
            [&](const auto& name) { return join_path(location_provider->root_location(src_tablet_id), name); },
            [&](const auto& name) {
                return remote_starlet_location_provider->schema_file_location(dst_tablet_id, db_id, table_id,
                                                                              physical_partition_id, name);
            },
            true, true));
#endif
    return Status::OK();
}

Status SnapshotFileSyncer::delete_partition(int64_t tablet_id, int64_t db_id, int64_t table_id, int64_t partition_id,
                                            int64_t physical_partition_id) {
#if defined(USE_STAROS) && !defined(BE_TEST)
    auto remote_starlet_location_provider = StorageEnv::GetInstance()->remote_starlet_location_provider();
    auto tablet_root = remote_starlet_location_provider->root_location(tablet_id);
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(tablet_root));
    auto dir_path = remote_starlet_location_provider->partition_directory_location(tablet_id, db_id, table_id,
                                                                                   physical_partition_id);
    RETURN_IF_ERROR(fs->delete_dir_recursive(dir_path));
#endif
    return Status::OK();
}

Status SnapshotFileSyncer::delete_files(int64_t tablet_id, const ExternalClusterSnapshotLogPB& log_pb) {
#ifdef USE_STAROS
    auto remote_starlet_location_provider = StorageEnv::GetInstance()->remote_starlet_location_provider();
    auto tablet_root = remote_starlet_location_provider->root_location(tablet_id);
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(tablet_root));

    std::vector<std::string> files;
    files.reserve(log_pb.delete_data_files_size() + log_pb.delete_meta_files_size() +
                  log_pb.delete_schema_files_size());

    for (const auto& file : log_pb.delete_data_files()) {
        files.emplace_back(remote_starlet_location_provider->data_file_location(
                tablet_id, log_pb.db_id(), log_pb.table_id(), log_pb.physical_partition_id(), file));
    }
    for (const auto& file : log_pb.delete_meta_files()) {
        files.emplace_back(remote_starlet_location_provider->metadata_file_location(
                tablet_id, log_pb.db_id(), log_pb.table_id(), log_pb.physical_partition_id(), file));
    }
    for (const auto& file : log_pb.delete_schema_files()) {
        files.emplace_back(remote_starlet_location_provider->schema_file_location(
                tablet_id, log_pb.db_id(), log_pb.table_id(), log_pb.physical_partition_id(), file));
    }

    for (const auto& file : files) {
        LOG(INFO) << "delete file: " << file;
    }

    return fs->delete_files(files);
#else
    return Status::OK();
#endif
}

} // end namespace starrocks::lake
