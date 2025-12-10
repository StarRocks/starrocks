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

#include "fs/fs_util.h"
#include "glog/logging.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"

namespace starrocks::lake {

Status SnapshotFileSyncer::upload(const TabletSnapshotInfo& snapshot_info, UploadSnapshotFilesResponsePB* response) {
    DCHECK(snapshot_info.tablet_snapshot != nullptr);
    auto src_tablet_id = snapshot_info.tablet_snapshot->tablet_id();
    auto dst_tablet_id = snapshot_info.virtual_tablet_id;
    auto db_id = snapshot_info.db_id;
    auto table_id = snapshot_info.table_id;
    auto partition_id = snapshot_info.partition_id;
    //auto physical_partition_id = snapshot_info.physical_partition_id;

    auto location_provider = _env->lake_location_provider();
    for (auto& data_file : snapshot_info.tablet_snapshot->new_data_files()) {
        LOG(INFO) << "data_file: " << data_file;
        auto src_data_file = join_path(location_provider->segment_root_location(src_tablet_id), data_file);
        LOG(INFO) << "src_data_file: " << src_data_file;

        auto src_fs = FileSystem::CreateSharedFromString(src_data_file);
        if (!src_fs.ok()) {
            LOG(ERROR) << "create src fs failed, src_data_file: " << src_data_file
                       << ", status: " << src_fs.status().to_string();
            return src_fs.status();
        }

        auto input_file = (*src_fs)->new_sequential_file(src_data_file);
        if (!input_file.ok()) {
            LOG(ERROR) << "create input file failed, src_data_file: " << src_data_file
                       << ", status: " << input_file.status().to_string();
            return input_file.status();
        }

        auto dst_data_file = join_path(location_provider->root_location(dst_tablet_id),
                                       fmt::format("{}/{}/{}/{}/{}", db_id, table_id, partition_id, "data"/ data_file));
        LOG(INFO) << "dst_data_file: " << dst_data_file;
        auto dst_fs = FileSystem::CreateSharedFromString(dst_data_file);
        if (!dst_fs.ok()) {
            LOG(ERROR) << "create dst fs failed, dst_data_file: " << dst_data_file
                       << ", status: " << dst_fs.status().to_string();
            return dst_fs.status();
        }

        auto output_file = (*dst_fs)->new_writable_file(dst_data_file);
        if (!output_file.ok()) {
            LOG(ERROR) << "create output file failed, dst_data_file: " << dst_data_file
                       << ", status: " << output_file.status().to_string();
            return output_file.status();
        }

        auto res = fs::copy((*input_file).get(), (*output_file).get(), 1024 * 1024);
        if (!res.ok()) {
            LOG(ERROR) << "copy data file failed, src_data_file: " << src_data_file
                       << ", dst_data_file: " << dst_data_file << ", status: " << res.status().to_string();
            return res.status();
        } else {
            LOG(INFO) << "copy data file success, src_data_file: " << src_data_file
                      << ", dst_data_file: " << dst_data_file << ", size: " << res.value();
        }
        auto close_st = (*output_file)->close();
        if (!close_st.ok()) {
            LOG(ERROR) << "close output file failed, dst_data_file: " << dst_data_file
                       << ", status: " << close_st;
            return close_st;
        } else {
            LOG(INFO) << "close output file success, dst_data_file: " << dst_data_file;
        }
    }

    return Status::OK();
}

} // end namespace starrocks::lake