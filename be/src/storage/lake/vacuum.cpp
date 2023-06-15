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

#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <ctime>
#include <string_view>
#include <unordered_map>

#include "common/config.h"
#include "common/status.h"
#include "fs/fs.h"
#include "fs/rapidjson_stream_adapter.h"
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
    TEST_SYNC_POINT("CloudNative::GC::delete_tablet_metadata:enter");

    DCHECK(tablet_mgr != nullptr);
    DCHECK(std::is_sorted(tablet_ids.begin(), tablet_ids.end()));
    DCHECK(min_retain_version >= 0);
    DCHECK(grace_timestamp >= 0);
    DCHECK(vacuumed_files != nullptr);
    DCHECK(vacuumed_file_size != nullptr);

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_dir));

    std::unordered_map<int64_t, std::vector<int64_t>> expired_tablets;

    auto meta_dir = join_path(root_dir, kMetadataDirectoryName);
    auto data_dir = join_path(root_dir, kSegmentDirectoryName);
    RETURN_IF_ERROR(fs->iterate_dir2(meta_dir, [&](DirEntry entry) {
        if (!is_tablet_metadata(entry.name)) {
            return true;
        }
        // NOTE: If mtime is unknown, the grace timestamp will be ignored.
        if (entry.mtime.value_or(0) >= grace_timestamp) {
            return true;
        }
        auto [tablet_id, version] = parse_tablet_metadata_filename(entry.name);
        if (version >= min_retain_version) {
            return true;
        }
        if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
            return true;
        }
        expired_tablets[tablet_id].emplace_back(version);
        *vacuumed_files += 1;
        *vacuumed_file_size += entry.size.value_or(0);
        return true;
    }));

    for (auto& [tablet_id, versions] : expired_tablets) {
        DCHECK(!versions.empty());
        std::sort(versions.begin(), versions.end());

        // Find metadata files that has garbage data files and delete all those files
        for (int64_t garbage_version = versions.back(); garbage_version >= versions[0]; /**/) {
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

        // TODO: batch delete
        // Note: Delete files with smaller version numbers first
        for (auto version : versions) {
            auto path = join_path(meta_dir, tablet_metadata_filename(tablet_id, version));
            RETURN_IF_ERROR(delete_file_ignore_not_found(fs.get(), path));
        }
    }

    TEST_SYNC_POINT("CloudNative::GC::delete_tablet_metadata:return");

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
    if (UNLIKELY(request.tablet_ids_size() == 0)) {
        return Status::InvalidArgument("tablet_ids is empty");
    }
    if (UNLIKELY(request.min_check_version() > request.max_check_version())) {
        return Status::InvalidArgument("min_check_version is greater than max_check_version");
    }
    if (UNLIKELY(request.min_active_txn_id() <= 0)) {
        return Status::InvalidArgument("min_active_txn_id is zero or negative");
    }
    auto root_loc = tablet_mgr->tablet_root_location(request.tablet_ids(0));
    int64_t vacuumed_files = 0;
    int64_t vacuumed_file_size = 0;

    const auto now = std::time(nullptr);
    const auto meta_loc = join_path(root_loc, kMetadataDirectoryName);
    const auto data_loc = join_path(root_loc, kSegmentDirectoryName);
    const auto log_loc = join_path(root_loc, kTxnLogDirectoryName);
    const auto min_active_txn_id = request.min_active_txn_id();
    const auto min_check_version = request.min_check_version();
    const auto max_check_version = request.max_check_version();
#ifndef BE_TEST
    const auto expire_seconds = std::max<int64_t>(config::lake_gc_segment_expire_seconds, 86400);
#else
    const auto expire_seconds = config::lake_gc_segment_expire_seconds;
#endif

    auto tablet_ids = std::vector<int64_t>(request.tablet_ids().begin(), request.tablet_ids().end());
    std::sort(tablet_ids.begin(), tablet_ids.end());

    std::set<std::string> referenced_files;
    std::set<std::string> orphan_files;

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_loc));

    for (auto tablet_id : tablet_ids) {
        for (auto v = min_check_version; v <= max_check_version; v++) {
            auto loc = join_path(meta_loc, tablet_metadata_filename(tablet_id, v));
            ASSIGN_OR_RETURN(auto metadata, tablet_mgr->get_tablet_metadata(loc, false));
            for (const auto& rowset : metadata->rowsets()) {
                referenced_files.insert(rowset.segments().begin(), rowset.segments().end());
            }
            if (metadata->has_delvec_meta()) {
                for (const auto& e : metadata->delvec_meta().version_to_file()) {
                    referenced_files.insert(e.second.name());
                }
            }
        }
    }

    int64_t grace_timestamp = ::time(nullptr);
#ifdef BE_TEST
    grace_timestamp += 3; // Make sure all tablet metadata with version less than "min_check_version" can be cacuumed;
#endif
    RETURN_IF_ERROR(vacuum_tablet_metadata(tablet_mgr, root_loc, tablet_ids, min_check_version, grace_timestamp,
                                           &vacuumed_files, &vacuumed_file_size));
    RETURN_IF_ERROR(vacuum_txn_log(root_loc, tablet_ids, min_active_txn_id, &vacuumed_files, &vacuumed_file_size));

    std::vector<std::string> txn_logs;
    RETURN_IF_ERROR(ignore_not_found(fs->iterate_dir(log_loc, [&](std::string_view name) {
        txn_logs.emplace_back(name);
        return true;
    })));

    for (const auto& log_name : txn_logs) {
        auto loc = join_path(log_loc, log_name);
        auto res = tablet_mgr->get_txn_log(loc, false);
        if (res.ok()) {
            auto txn_log = std::move(res).value();
            if (txn_log->has_op_write()) {
                const auto& op = txn_log->op_write();
                referenced_files.insert(op.rowset().segments().begin(), op.rowset().segments().end());
                referenced_files.insert(op.rewrite_segments().begin(), op.rewrite_segments().end());
                referenced_files.insert(op.dels().begin(), op.dels().end());
            }
            if (txn_log->has_op_compaction()) {
                const auto& op = txn_log->op_compaction();
                referenced_files.insert(op.output_rowset().segments().begin(), op.output_rowset().segments().end());
            }
            if (txn_log->has_op_schema_change()) {
                const auto& op = txn_log->op_schema_change();
                for (const auto& rowset : op.rowsets()) {
                    referenced_files.insert(rowset.segments().begin(), rowset.segments().end());
                }
                if (op.has_delvec_meta()) {
                    for (const auto& e : op.delvec_meta().version_to_file()) {
                        referenced_files.insert(e.second.name());
                    }
                }
            }
        } else if (!res.status().is_not_found()) {
            return res.status();
        }
    }

    RETURN_IF_ERROR(ignore_not_found(fs->iterate_dir2(data_loc, [&](DirEntry entry) {
        if (!is_segment(entry.name) && !is_del(entry.name) && !is_delvec(entry.name)) {
            LOG_EVERY_N(WARNING, 100) << "Unrecognized data file " << entry.name;
            return true;
        }
        bool is_orphan_file = false;
        std::string name(entry.name);
        if (referenced_files.count(name) > 0) {
            return true;
        }
        std::optional<int64_t> opt_txn_id = extract_txn_id_prefix(name);
        if (opt_txn_id.has_value()) {
            is_orphan_file = opt_txn_id.value() < min_active_txn_id;
        } else if (entry.mtime.has_value()) {
            is_orphan_file = (now >= (entry.mtime.value() + expire_seconds));
        } else {
            auto mtime_or = fs->get_file_modified_time(join_path(data_loc, name));
            is_orphan_file = mtime_or.ok() && (now >= (*mtime_or + expire_seconds));
        }

        if (is_orphan_file) {
            orphan_files.insert(name);
            vacuumed_files += 1;
            vacuumed_file_size += entry.size.value_or(0);
        }

        return true;
    })));

    // TODO: limit request rate
    // TODO: batch delete
    for (auto& file : orphan_files) {
        auto location = join_path(data_loc, file);
        RETURN_IF_ERROR(delete_file_ignore_not_found(fs.get(), location));
    }
    response->set_vacuumed_files(vacuumed_files);
    response->set_vacuumed_file_size(vacuumed_file_size);
    return Status::OK();
}

void vacuum_full(TabletManager* tablet_mgr, const VacuumFullRequest& request, VacuumFullResponse* response) {
    auto st = vacuum_full_impl(tablet_mgr, request, response);
    st.to_protobuf(response->mutable_status());
}

} // namespace starrocks::lake
