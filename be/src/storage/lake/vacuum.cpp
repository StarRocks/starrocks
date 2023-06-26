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

#include "storage/lake/vacuum.h"

#include <algorithm>
#include <ctime>
#include <string_view>
#include <unordered_map>

#include "common/config.h"
#include "common/status.h"
#include "fs/fs.h"
#include "gutil/stl_util.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_common.h"
#include "testutil/sync_point.h"
#include "util/raw_container.h"

namespace starrocks::lake {

static Status delete_file(FileSystem* fs, const std::string& path) {
    auto st = fs->delete_file(path);
    if (st.ok() && config::lake_print_delete_log) {
        LOG(INFO) << "Deleted " << path;
    } else if (!st.is_not_found()) {
        LOG(WARNING) << "Fail to delete " << path << ": " << st;
    }
    return st;
}

static Status delete_file_ignore_not_found(FileSystem* fs, const std::string& path) {
    return ignore_not_found(delete_file(fs, path));
}

static Status delete_rowset_files(FileSystem* fs, std::string_view data_dir, const RowsetMetadataPB& rowset) {
    for (const auto& segment : rowset.segments()) {
        auto seg_path = join_path(data_dir, segment);
        RETURN_IF_ERROR(delete_file_ignore_not_found(fs, seg_path));
    }
    return Status::OK();
}

static Status vacuum_tablet_metadata(TabletManager* tablet_mgr, std::string_view root_dir,
                                     const std::vector<int64_t>& tablet_ids, int64_t min_retain_version,
                                     int64_t grace_timestamp, int64_t* vacuumed_files, int64_t* vacuumed_file_size) {
    DCHECK(tablet_mgr != nullptr);
    DCHECK(std::is_sorted(tablet_ids.begin(), tablet_ids.end()));
    DCHECK(min_retain_version >= 0);
    DCHECK(grace_timestamp >= 0);
    DCHECK(vacuumed_files != nullptr);
    DCHECK(vacuumed_file_size != nullptr);

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_dir));

    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>> expired_tablets;
    //                 ^^^^^^^ tablet id
    //                                                ^^^^^^^^^^^^^^^^ version number and file size

    auto meta_dir = join_path(root_dir, kMetadataDirectoryName);
    auto data_dir = join_path(root_dir, kSegmentDirectoryName);
    RETURN_IF_ERROR(fs->iterate_dir2(meta_dir, [&](DirEntry entry) {
        TEST_SYNC_POINT_CALLBACK("vacuum_tablet_metadata:iterate_metadata", &entry);
        if (!is_tablet_metadata(entry.name)) {
            return true;
        }
        // NOTE: If mtime is unknown, the grace timestamp will be ignored.
        if (entry.mtime.value_or(0) >= grace_timestamp) {
            return true;
        }
        auto [tablet_id, version] = parse_tablet_metadata_filename(entry.name);
        if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
            return true;
        }
        if (version <= min_retain_version) {
            // We need to retain metadata files with version |min_retain_version|, but garbage files
            // recorded in the |min_retain_version| can be deleted. So metadata file with |min_retain_version| will
            // also be read.
            expired_tablets[tablet_id].emplace_back(version, entry.size.value_or(0));
        } else {
            // nothing to do
        }
        return true;
    }));

    for (auto& [tablet_id, versions] : expired_tablets) {
        DCHECK(!versions.empty());
        std::sort(versions.begin(), versions.end());

        // Find metadata files that has garbage data files and delete all those files
        for (int64_t garbage_version = versions.back().first; garbage_version >= versions[0].first; /**/) {
            auto path = join_path(meta_dir, tablet_metadata_filename(tablet_id, garbage_version));
            auto res = tablet_mgr->get_tablet_metadata(path, false);
            if (res.status().is_not_found()) {
                break;
            } else if (!res.ok()) {
                LOG(ERROR) << "Fail to read " << path << ": " << res.status();
                return res.status();
            } else {
                auto metadata = std::move(res).value();
                for (const auto& rowset : metadata->compaction_inputs()) {
                    RETURN_IF_ERROR(delete_rowset_files(fs.get(), data_dir, rowset));
                    *vacuumed_files += rowset.segments_size();
                    *vacuumed_file_size += rowset.data_size();
                }
                for (const auto& file : metadata->orphan_files()) {
                    RETURN_IF_ERROR(delete_file_ignore_not_found(fs.get(), join_path(data_dir, file.name())));
                    *vacuumed_files += 1;
                    *vacuumed_file_size += file.size();
                }
                if (metadata->has_prev_garbage_version()) {
                    garbage_version = metadata->prev_garbage_version();
                } else {
                    break;
                }
            }
        }

        // Do not delete the last version created before grace_timestamp.
        // Assuming grace_timestamp is the earliest possible initiation time of queries still in process, then the
        // earliest version to be accessed is the last version created before grace_timestamp. So retain this version.
        versions.pop_back();

        // TODO: batch delete
        // Note: Delete files with smaller version numbers first
        for (auto version : versions) {
            auto path = join_path(meta_dir, tablet_metadata_filename(tablet_id, version.first));
            RETURN_IF_ERROR(delete_file_ignore_not_found(fs.get(), path));
            *vacuumed_files += 1;
            *vacuumed_file_size += version.second;
        }
    }

    return Status::OK();
}

static Status vacuum_txn_log(std::string_view root_location, const std::vector<int64_t>& tablet_ids,
                             int64_t min_active_txn_id, int64_t* vacuumed_files, int64_t* vacuumed_file_size) {
    DCHECK(std::is_sorted(tablet_ids.begin(), tablet_ids.end()));
    DCHECK(vacuumed_files != nullptr);
    DCHECK(vacuumed_file_size != nullptr);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    auto log_dir = join_path(root_location, kTxnLogDirectoryName);
    return ignore_not_found(fs->iterate_dir2(log_dir, [&](DirEntry entry) {
        if (!is_txn_log(entry.name)) {
            return true;
        }
        auto [tablet_id, txn_id] = parse_txn_log_filename(entry.name);
        if (txn_id >= min_active_txn_id) {
            return true;
        }
        if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
            return true;
        }
        auto st = delete_file(fs.get(), join_path(log_dir, entry.name));
        if (st.ok()) {
            *vacuumed_files += 1;
            *vacuumed_file_size += entry.size.value_or(0);
        } else if (!st.is_not_found()) {
            // Stop execution
            return false;
        }
        return true;
    }));
}

Status vacuum_impl(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response) {
    if (UNLIKELY(tablet_mgr == nullptr)) {
        return Status::InvalidArgument("tablet_mgr is null");
    }
    if (UNLIKELY(request.tablet_ids_size() == 0)) {
        return Status::InvalidArgument("tablet_ids is empty");
    }
    if (UNLIKELY(request.min_retain_version() <= 0)) {
        return Status::InvalidArgument("value of min_retain_version is zero or negative");
    }
    if (UNLIKELY(request.grace_timestamp() <= 0)) {
        return Status::InvalidArgument("value of grace_timestamp is zero or nagative");
    }

    auto tablet_ids = std::vector<int64_t>(request.tablet_ids().begin(), request.tablet_ids().end());
    auto root_loc = tablet_mgr->tablet_root_location(tablet_ids[0]);
    auto min_retain_version = request.min_retain_version();
    auto grace_timestamp = request.grace_timestamp();
    auto min_active_txn_id = request.min_active_txn_id();

    int64_t vacuumed_files = 0;
    int64_t vacuumed_file_size = 0;

    std::sort(tablet_ids.begin(), tablet_ids.end());

    RETURN_IF_ERROR(vacuum_tablet_metadata(tablet_mgr, root_loc, tablet_ids, min_retain_version, grace_timestamp,
                                           &vacuumed_files, &vacuumed_file_size));
    RETURN_IF_ERROR(vacuum_txn_log(root_loc, tablet_ids, min_active_txn_id, &vacuumed_files, &vacuumed_file_size));
    response->set_vacuumed_files(vacuumed_files);
    response->set_vacuumed_file_size(vacuumed_file_size);
    return Status::OK();
}

void vacuum(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response) {
    auto st = vacuum_impl(tablet_mgr, request, response);
    st.to_protobuf(response->mutable_status());
}

Status vacuum_full_impl(TabletManager* tablet_mgr, const VacuumFullRequest& request, VacuumFullResponse* response) {
    return Status::NotSupported("vacuum_full not implemented yet");
}

void vacuum_full(TabletManager* tablet_mgr, const VacuumFullRequest& request, VacuumFullResponse* response) {
    auto st = vacuum_full_impl(tablet_mgr, request, response);
    st.to_protobuf(response->mutable_status());
}

} // namespace starrocks::lake
