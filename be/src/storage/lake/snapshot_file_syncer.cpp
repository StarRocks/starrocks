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

#include "fs/fs_util.h"
#include "glog/logging.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"

namespace starrocks::lake {

namespace {

template <typename Files, typename SrcFunc, typename DstFunc>
Status copy_files(const Files& files, SrcFunc&& make_src, DstFunc&& make_dst, bool skip_if_exists) {
    for (const auto& file : files) {
        auto src_path = make_src(file);
        auto dst_path = make_dst(file);
        VLOG(3) << "src_file: " << src_path << ", dst_file: " << dst_path;

        ASSIGN_OR_RETURN(auto src_fs, FileSystem::CreateSharedFromString(src_path));
        ASSIGN_OR_RETURN(auto dst_fs, FileSystem::CreateSharedFromString(dst_path));

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
    auto src_tablet_id = snapshot_info.tablet_snapshot->tablet_id();
    auto dst_tablet_id = snapshot_info.dest_tablet_id;
    auto db_id = snapshot_info.db_id;
    auto table_id = snapshot_info.table_id;
    auto physical_partition_id = snapshot_info.physical_partition_id;

    auto location_provider = _env->lake_location_provider();
    auto dst_tablet_root = location_provider->root_location(dst_tablet_id);
    auto dst_prefix = fmt::format("db{}/{}/{}", db_id, table_id, physical_partition_id);

    RETURN_IF_ERROR(copy_files(
            snapshot_info.tablet_snapshot->new_data_files(),
            [&](const auto& name) { return join_path(location_provider->segment_root_location(src_tablet_id), name); },
            [&](const auto& name) {
                return join_path(dst_tablet_root,
                                 fmt::format("{}/{}/{}", dst_prefix, lake::kSegmentDirectoryName, name));
            },
            true));

    RETURN_IF_ERROR(copy_files(
            snapshot_info.tablet_snapshot->new_metadata_files(),
            [&](const auto& name) { return join_path(location_provider->metadata_root_location(src_tablet_id), name); },
            [&](const auto& name) {
                return join_path(dst_tablet_root,
                                 fmt::format("{}/{}/{}", dst_prefix, lake::kMetadataDirectoryName, name));
            },
            false));

    RETURN_IF_ERROR(copy_files(
            snapshot_info.tablet_snapshot->new_schema_files(),
            [&](const auto& name) { return join_path(location_provider->root_location(src_tablet_id), name); },
            [&](const auto& name) { return join_path(dst_tablet_root, fmt::format("{}/{}", dst_prefix, name)); },
            true));

    return Status::OK();
}

} // end namespace starrocks::lake