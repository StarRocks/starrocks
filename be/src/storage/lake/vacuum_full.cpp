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

#include "storage/lake/vacuum_full.h"

#include <set>
#include <string_view>

#include "common/status.h"
#include "fs/fs.h"
#include "storage/lake/join_path.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/vacuum.h"
#include "storage/protobuf_file.h"

namespace starrocks::lake {

static Status vacuum_expired_tablet_metadata(TabletManager* tablet_mgr, std::string_view root_loc,
                                             int64_t grace_timestamp, int64_t* vacuumed_files,
                                             std::list<std::string>* meta_files,
                                             std::list<std::string>* bundle_meta_files,
                                             const std::unordered_set<int64_t>& retain_versions,
                                             int64_t min_check_version, int64_t max_check_version) {
    DCHECK(tablet_mgr != nullptr);
    DCHECK(meta_files != nullptr);
    DCHECK(bundle_meta_files != nullptr);
    if (grace_timestamp == 0) {
        // no expired metadata to be vacuumed
        return Status::OK();
    }

    auto meta_ver_checker = [&](const auto& version) {
        return version >= min_check_version && version < max_check_version && !retain_versions.contains(version);
    };

    auto metafile_delete_cb = [=](const std::vector<std::string>& files) {
        auto cache = tablet_mgr->metacache();
        DCHECK(cache != nullptr);
        for (const auto& path : files) {
            cache->erase(path);
        }
    };
    AsyncFileDeleter metafile_deleter(INT64_MAX, metafile_delete_cb);
    std::vector<std::string> expired_metas;
    std::vector<std::string> bundle_expired_metas;
    const auto metadata_root_location = join_path(root_loc, kMetadataDirectoryName);
    bool has_expired = false;
    for (const auto& name : *meta_files) {
        auto [tablet_id, version] = parse_tablet_metadata_filename(name);
        if (!meta_ver_checker(version)) {
            continue;
        }
        const string path = join_path(metadata_root_location, name);
        ASSIGN_OR_RETURN(auto metadata, tablet_mgr->get_tablet_metadata(tablet_id, version, false));
        if ((metadata->has_commit_time() && metadata->commit_time() < grace_timestamp) ||
            /* init version does not has commit time, delete it if there is any meta has been expired. */
            (has_expired && !metadata->has_commit_time())) {
            LOG(INFO) << "Try delete for full vacuum: " << path;
            RETURN_IF_ERROR(metafile_deleter.delete_file(path));
            expired_metas.push_back(name);
            has_expired = true;
        }
    }
    for (const auto& expired_meta : expired_metas) {
        meta_files->remove(expired_meta);
    }

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_loc));
    for (const auto& name : *bundle_meta_files) {
        auto [tablet_id, version] = parse_tablet_metadata_filename(name);
        if (!meta_ver_checker(version)) {
            continue;
        }
        auto path = join_path(metadata_root_location, name);
        bool need_clear = true;
        ASSIGN_OR_RETURN(auto metadatas, TabletManager::get_metas_from_bundle_tablet_metadata(path, fs.get()));
        // metadatas parsed from bundle files should alwasy have version >= 2
        for (const auto& metadata : metadatas) {
            if (metadata->commit_time() >= grace_timestamp) {
                need_clear = false;
                break;
            }
        }
        if (need_clear) {
            LOG(INFO) << "Try delete for full vacuum: " << path;
            RETURN_IF_ERROR(metafile_deleter.delete_file(path));
            bundle_expired_metas.push_back(name);
        }
    }
    for (const auto& bundle_expired_meta : bundle_expired_metas) {
        bundle_meta_files->remove(bundle_expired_meta);
    }

    RETURN_IF_ERROR(metafile_deleter.finish());
    (*vacuumed_files) += metafile_deleter.delete_count();
    return Status::OK();
}

// Deletes orphaned data files (data files which are not referenced by "relevant" metadata files)
// Data files written by transactions with txn_id >= min_active_txn_id will NOT be vacuumed
// Returns the number of files vacuumed and their total size
static StatusOr<std::pair<int64_t, int64_t>> vacuum_orphaned_datafiles(
        TabletManager* tablet_mgr, std::string_view root_loc, int64_t min_active_txn_id,
        const std::list<std::string>& meta_files, const std::list<std::string>& bundle_meta_files) {
    DCHECK(tablet_mgr != nullptr);
    const auto segment_root_location = join_path(root_loc, kSegmentDirectoryName);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_loc));
    ASSIGN_OR_RETURN(auto data_files_to_vacuum,
                     find_orphan_data_files(fs.get(), root_loc, 0, meta_files, bundle_meta_files, nullptr));
    const auto original_size = data_files_to_vacuum.size();
    std::erase_if(data_files_to_vacuum, [&](const auto& elem) {
        const auto& name = elem.first;
        const auto txn_id = extract_txn_id_prefix(name).value_or(0);
        return txn_id == 0 || txn_id >= min_active_txn_id;
    });
    LOG(INFO) << segment_root_location << ": "
              << "Removed " << original_size - data_files_to_vacuum.size()
              << " data files from consideration as orphans based on txn id";

    int64_t bytes_to_delete = 0;
    std::vector<std::string> files_to_delete;
    files_to_delete.reserve(data_files_to_vacuum.size());
    for (const auto& [name, dir_entry] : data_files_to_vacuum) {
        bytes_to_delete += dir_entry.size.value_or(0);
        files_to_delete.push_back(join_path(segment_root_location, name));
    }
    LOG(INFO) << segment_root_location << ": "
              << "Start to delete orphan data files: " << data_files_to_vacuum.size()
              << ", total size: " << bytes_to_delete;

    RETURN_IF_ERROR(do_delete_files(fs.get(), files_to_delete));

    return std::pair<int64_t, int64_t>(files_to_delete.size(), bytes_to_delete);
}

Status vacuum_full_impl(TabletManager* tablet_mgr, const VacuumFullRequest& request, VacuumFullResponse* response) {
    if (UNLIKELY(tablet_mgr == nullptr)) {
        return Status::InvalidArgument("tablet_mgr is null");
    }
    if (UNLIKELY(request.partition_id() == 0)) {
        return Status::InvalidArgument("partition_id is unset");
    }
    if (UNLIKELY(request.tablet_id() == 0)) {
        return Status::InvalidArgument("tablet_id is unset");
    }
    if (UNLIKELY(request.min_active_txn_id() <= 0)) {
        return Status::InvalidArgument("value of min_active_txn_id is unset or negative");
    }
    if (UNLIKELY(request.grace_timestamp() < 0)) {
        return Status::InvalidArgument("value of grace_timestamp is unset or negative");
    }
    if (UNLIKELY(request.min_check_version() < 0)) {
        return Status::InvalidArgument("value of min_check_version is unset or negative");
    }
    if (UNLIKELY(request.max_check_version() < 0)) {
        return Status::InvalidArgument("value of max_check_version is unset or negative");
    }

    const auto grace_timestamp = request.grace_timestamp();
    const auto min_active_txn_id = request.min_active_txn_id();
    std::unordered_set<int64_t> retain_versions;
    if (request.retain_versions_size() > 0) {
        retain_versions.insert(request.retain_versions().begin(), request.retain_versions().end());
    }
    int64_t min_check_version = request.min_check_version();
    int64_t max_check_version = request.max_check_version();

    int64_t vacuumed_files = 0;
    int64_t vacuumed_file_size = 0;

    const std::string root_loc = tablet_mgr->tablet_root_location(request.tablet_id());
    const auto metadata_root_location = join_path(root_loc, kMetadataDirectoryName);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_loc));
    ASSIGN_OR_RETURN(auto meta_files_and_bundle_files, list_meta_files(fs.get(), metadata_root_location));
    auto& meta_files = meta_files_and_bundle_files.first;
    auto& bundle_meta_files = meta_files_and_bundle_files.second;

    // 1. Delete all metadata files associated with the given partition which have tabletmeta.commit_time < grace_timestamp
    // and the checked version of tablet meta should not be found in retain_versions and should in [min_check_version, max_check_version)
    RETURN_IF_ERROR(vacuum_expired_tablet_metadata(tablet_mgr, root_loc, grace_timestamp, &vacuumed_files, &meta_files,
                                                   &bundle_meta_files, retain_versions, min_check_version,
                                                   max_check_version));

    // 2. Determine and delete orphaned data files. We use min_active_txn_id to filter out new data files, then we open
    // any remaining metadata files and note that the data files referenced are not orphans.
    ASSIGN_OR_RETURN(auto count_and_size,
                     vacuum_orphaned_datafiles(tablet_mgr, root_loc, min_active_txn_id, meta_files, bundle_meta_files));
    vacuumed_files += count_and_size.first;
    vacuumed_file_size += count_and_size.second;

    // 3. deleted txn log which have txn id < min_active_txn_id
    RETURN_IF_ERROR(vacuum_txn_log(root_loc, min_active_txn_id, &vacuumed_files, &vacuumed_file_size));

    response->set_vacuumed_files(vacuumed_files);
    response->set_vacuumed_file_size(vacuumed_file_size);
    return Status::OK();
}

void vacuum_full(TabletManager* tablet_mgr, const VacuumFullRequest& request, VacuumFullResponse* response) {
    auto st = vacuum_full_impl(tablet_mgr, request, response);
    st.to_protobuf(response->mutable_status());
}

} // namespace starrocks::lake
