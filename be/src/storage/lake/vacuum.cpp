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

#include <butil/time.h>
#include <bvar/bvar.h>

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
#include "util/defer_op.h"
#include "util/raw_container.h"

namespace starrocks::lake {

static bvar::LatencyRecorder g_del_file_latency("lake_vacuum_del_file"); // unit: us
static bvar::Adder<uint64_t> g_del_fails("lake_vacuum_del_file_fails");
static bvar::Adder<uint64_t> g_deleted_files("lake_vacuum_deleted_files");
static bvar::LatencyRecorder g_metadata_travel_latency("lake_vacuum_metadata_travel"); // unit: ms
static bvar::LatencyRecorder g_vacuum_txnlog_latency("lake_vacuum_delete_txnlog");

static Status delete_files(FileSystem* fs, const std::vector<std::string>& paths) {
    if (paths.empty()) {
        return Status::OK();
    }

    auto wait_duration = config::experimental_lake_wait_per_delete_ms;
    if (wait_duration > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_duration));
    }

    if (config::lake_print_delete_log) {
        for (size_t i = 0, n = paths.size(); i < n; i++) {
            LOG(INFO) << "Deleting " << paths[i] << "(" << (i + 1) << '/' << n << ')';
        }
    }

    auto t0 = butil::gettimeofday_us();
    auto st = fs->delete_files(paths);
    TEST_SYNC_POINT_CALLBACK("vacuum.delete_files", &st);
    if (st.ok()) {
        auto t1 = butil::gettimeofday_us();
        g_del_file_latency << (t1 - t0);
        g_deleted_files << paths.size();
        VLOG(5) << "Deleted " << paths.size() << " files cost " << (t1 - t0) << "us";
    } else {
        LOG(WARNING) << "Fail to delete: " << st;
    }
    return st;
}

static void collect_garbage_files(const TabletMetadataPB& metadata, const std::string& base_dir,
                                  std::vector<std::string>* garbage_files, int64_t* garbage_data_size) {
    for (const auto& rowset : metadata.compaction_inputs()) {
        for (const auto& segment : rowset.segments()) {
            garbage_files->emplace_back(join_path(base_dir, segment));
        }
        *garbage_data_size += rowset.data_size();
    }
    for (const auto& file : metadata.orphan_files()) {
        garbage_files->emplace_back(join_path(base_dir, file.name()));
        *garbage_data_size += file.size();
    }
}

static Status collect_files_to_vacuum(TabletManager* tablet_mgr, std::string_view root_dir, int64_t tablet_id,
                                      int64_t grace_timestamp, int64_t min_retain_version,
                                      std::vector<std::string>* datafiles_to_vacuum,
                                      std::vector<std::string>* metafiles_to_vacuum, int64_t* total_datafile_size) {
    auto t0 = butil::gettimeofday_ms();
    auto meta_dir = join_path(root_dir, kMetadataDirectoryName);
    auto data_dir = join_path(root_dir, kSegmentDirectoryName);
    auto final_retain_version = min_retain_version;
    auto version = final_retain_version;
    // grace_timestamp <= 0 means no grace timestamp
    auto skip_check_grace_timestamp = grace_timestamp <= 0;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_dir));
    // Starting at |*final_retain_version|, read the tablet metadata forward along
    // the |prev_garbage_version| pointer until the tablet metadata does not exist.
    while (version > 0) {
        auto path = join_path(meta_dir, tablet_metadata_filename(tablet_id, version));
        auto res = tablet_mgr->get_tablet_metadata(path, false);
        TEST_SYNC_POINT_CALLBACK("collect_files_to_vacuum:get_tablet_metadata", &res);
        if (res.status().is_not_found()) {
            break;
        } else if (!res.ok()) {
            return res.status();
        } else {
            auto metadata = std::move(res).value();
            if (skip_check_grace_timestamp) {
                DCHECK_LE(version, final_retain_version);
                collect_garbage_files(*metadata, data_dir, datafiles_to_vacuum, total_datafile_size);
            } else {
                int64_t compare_time = 0;
                if (metadata->has_commit_time() && metadata->commit_time() > 0) {
                    compare_time = metadata->commit_time();
                } else {
                    ASSIGN_OR_RETURN(compare_time, fs->get_file_modified_time(path));
                    TEST_SYNC_POINT_CALLBACK("collect_files_to_vacuum:get_file_modified_time", &compare_time);
                }

                if (compare_time < grace_timestamp) {
                    // This is the first metadata we've encountered that was created or committed before
                    // the |grace_timestamp|, mark it as a version to retain prevents it from being deleted.
                    //
                    // Why not delete this version:
                    // Assuming |grace_timestamp| is the earliest possible initiation time of queries still
                    // in process, then the earliest version to be accessed is the last version created
                    // before |grace_timestamp|, so the last version created before |grace_timestamp| should
                    // be kept in case the query fails. And the |version| here is probably the last version
                    // created before grace_timestamp.
                    final_retain_version = version;

                    // From now on, all metadata encountered later will no longer need to be checked for
                    // |grace_timestamp| and will be considered ready for deletion.
                    skip_check_grace_timestamp = true;

                    // The metadata will be retained, but garbage files recorded in it can be deleted.
                    collect_garbage_files(*metadata, data_dir, datafiles_to_vacuum, total_datafile_size);
                } else {
                    DCHECK_LE(version, final_retain_version);
                    final_retain_version = version;
                }
            }

            CHECK_LT(metadata->prev_garbage_version(), version);
            version = metadata->prev_garbage_version();
        }
    }
    auto t1 = butil::gettimeofday_ms();
    g_metadata_travel_latency << (t1 - t0);

    if (!skip_check_grace_timestamp) {
        // All tablet metadata files encountered were created after the grace timestamp, there were no files to delete
        return Status::OK();
    }
    DCHECK_LE(version, final_retain_version);
    for (auto v = version + 1; v < final_retain_version; v++) {
        metafiles_to_vacuum->emplace_back(join_path(meta_dir, tablet_metadata_filename(tablet_id, v)));
    }
    return Status::OK();
}

static void erase_tablet_metadata_from_metacache(TabletManager* tablet_mgr, const std::vector<std::string>& metafiles) {
    auto cache = tablet_mgr->metacache();
    DCHECK(cache != nullptr);
    // Assuming the cache key for tablet metadata is the path to tablet metadata.
    // TODO: Refactor the code to extract a separate function that constructs the tablet metadata cache key
    for (const auto& path : metafiles) {
        cache->erase(path);
    }
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

    int64_t max_batch_delete_size = config::lake_vacuum_max_batch_delete_size;
    std::vector<std::string> datafiles_to_vacuum;
    std::vector<std::string> metafiles_to_vacuum;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_dir));
    for (auto tablet_id : tablet_ids) {
        RETURN_IF_ERROR(collect_files_to_vacuum(tablet_mgr, root_dir, tablet_id, grace_timestamp, min_retain_version,
                                                &datafiles_to_vacuum, &metafiles_to_vacuum, vacuumed_file_size));
        if (datafiles_to_vacuum.size() >= max_batch_delete_size ||
            metafiles_to_vacuum.size() >= max_batch_delete_size) {
            (*vacuumed_files) += (datafiles_to_vacuum.size() + metafiles_to_vacuum.size());
            RETURN_IF_ERROR(delete_files(fs.get(), datafiles_to_vacuum));
            RETURN_IF_ERROR(delete_files(fs.get(), metafiles_to_vacuum));
            erase_tablet_metadata_from_metacache(tablet_mgr, metafiles_to_vacuum);
            datafiles_to_vacuum.clear();
            metafiles_to_vacuum.clear();
        }
    }
    (*vacuumed_files) += (datafiles_to_vacuum.size() + metafiles_to_vacuum.size());
    RETURN_IF_ERROR(delete_files(fs.get(), datafiles_to_vacuum));
    RETURN_IF_ERROR(delete_files(fs.get(), metafiles_to_vacuum));
    erase_tablet_metadata_from_metacache(tablet_mgr, metafiles_to_vacuum);
    return Status::OK();
}

static Status vacuum_txn_log(std::string_view root_location, int64_t min_active_txn_id, int64_t* vacuumed_files,
                             int64_t* vacuumed_file_size) {
    auto t0 = butil::gettimeofday_s();
    DCHECK(vacuumed_files != nullptr);
    DCHECK(vacuumed_file_size != nullptr);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    std::vector<std::string> files_to_vacuum;
    auto ret = Status::OK();
    auto batch_size = config::lake_vacuum_max_batch_delete_size;
    auto log_dir = join_path(root_location, kTxnLogDirectoryName);
    auto iter_st = ignore_not_found(fs->iterate_dir2(log_dir, [&](DirEntry entry) {
        if (!is_txn_log(entry.name)) {
            return true;
        }
        auto [tablet_id, txn_id] = parse_txn_log_filename(entry.name);
        if (txn_id >= min_active_txn_id) {
            return true;
        }

        files_to_vacuum.emplace_back(join_path(log_dir, entry.name));
        *vacuumed_files += 1;
        *vacuumed_file_size += entry.size.value_or(0);

        if (files_to_vacuum.size() >= batch_size) {
            auto st = delete_files(fs.get(), files_to_vacuum);
            files_to_vacuum.clear();
            ret.update(st);
            return st.ok(); // Stop list if delete failed
        }
        return true;
    }));
    ret.update(iter_st);
    ret.update(delete_files(fs.get(), files_to_vacuum));

    auto t1 = butil::gettimeofday_s();
    g_vacuum_txnlog_latency << (t1 - t0);

    return ret;
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
    if (request.delete_txn_log()) {
        RETURN_IF_ERROR(vacuum_txn_log(root_loc, min_active_txn_id, &vacuumed_files, &vacuumed_file_size));
    }
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

// TODO: remote list objects requests
Status delete_tablets_impl(TabletManager* tablet_mgr, const std::string& root_dir,
                           const std::vector<int64_t>& tablet_ids) {
    DCHECK(tablet_mgr != nullptr);
    DCHECK(std::is_sorted(tablet_ids.begin(), tablet_ids.end()));

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_dir));

    std::unordered_map<int64_t, std::vector<int64_t>> tablet_versions;
    //                 ^^^^^^^ tablet id
    //                                     ^^^^^^^^^ version number

    auto meta_dir = join_path(root_dir, kMetadataDirectoryName);
    auto data_dir = join_path(root_dir, kSegmentDirectoryName);
    auto log_dir = join_path(root_dir, kTxnLogDirectoryName);

    std::vector<std::string> txn_logs;
    RETURN_IF_ERROR(ignore_not_found(fs->iterate_dir(log_dir, [&](std::string_view name) {
        if (is_txn_log(name)) {
            auto [tablet_id, txn_id] = parse_txn_log_filename(name);
            if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
                return true;
            }
        } else if (is_txn_vlog(name)) {
            auto [tablet_id, version] = parse_txn_vlog_filename(name);
            if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
                return true;
            }
        } else {
            return true;
        }

        txn_logs.emplace_back(name);

        return true;
    })));

    std::vector<std::string> files_to_vacuum;
    for (const auto& log_name : txn_logs) {
        auto res = tablet_mgr->get_txn_log(join_path(log_dir, log_name), false);
        if (res.status().is_not_found()) {
            continue;
        } else if (!res.ok()) {
            return res.status();
        } else {
            auto log = std::move(res).value();
            if (log->has_op_write()) {
                const auto& op = log->op_write();
                for (const auto& segment : op.rowset().segments()) {
                    files_to_vacuum.emplace_back(join_path(data_dir, segment));
                }
                for (const auto& f : op.dels()) {
                    files_to_vacuum.emplace_back((join_path(data_dir, f)));
                }
            }
            if (log->has_op_compaction()) {
                const auto& op = log->op_compaction();
                for (const auto& segment : op.output_rowset().segments()) {
                    files_to_vacuum.emplace_back((join_path(data_dir, segment)));
                }
            }
            if (log->has_op_schema_change()) {
                const auto& op = log->op_schema_change();
                for (const auto& rowset : op.rowsets()) {
                    for (const auto& segment : rowset.segments()) {
                        files_to_vacuum.emplace_back((join_path(data_dir, segment)));
                    }
                }
            }
            files_to_vacuum.emplace_back((join_path(log_dir, log_name)));
        }
    }
    RETURN_IF_ERROR(delete_files(fs.get(), files_to_vacuum));

    RETURN_IF_ERROR(ignore_not_found(fs->iterate_dir(meta_dir, [&](std::string_view name) {
        if (!is_tablet_metadata(name)) {
            return true;
        }
        auto [tablet_id, version] = parse_tablet_metadata_filename(name);
        if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
            return true;
        }
        tablet_versions[tablet_id].emplace_back(version);
        return true;
    })));

    for (auto& [tablet_id, versions] : tablet_versions) {
        DCHECK(!versions.empty());
        std::sort(versions.begin(), versions.end());

        TabletMetadataPtr latest_metadata = nullptr;

        // Find metadata files that has garbage data files and delete all those files
        files_to_vacuum.clear();
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
                if (latest_metadata == nullptr) {
                    latest_metadata = metadata;
                }
                int64_t dummy_file_size = 0;
                collect_garbage_files(*metadata, data_dir, &files_to_vacuum, &dummy_file_size);
                if (metadata->has_prev_garbage_version()) {
                    garbage_version = metadata->prev_garbage_version();
                } else {
                    break;
                }
            }
        }
        RETURN_IF_ERROR(delete_files(fs.get(), files_to_vacuum));

        // Delete all data files referenced in the latest version
        files_to_vacuum.clear();
        if (latest_metadata != nullptr) {
            for (const auto& rowset : latest_metadata->rowsets()) {
                for (const auto& segment : rowset.segments()) {
                    files_to_vacuum.emplace_back(join_path(data_dir, segment));
                }
            }
            if (latest_metadata->has_delvec_meta()) {
                for (const auto& [v, f] : latest_metadata->delvec_meta().version_to_file()) {
                    files_to_vacuum.emplace_back(join_path(data_dir, f.name()));
                }
            }
        }

        for (auto version : versions) {
            auto path = join_path(meta_dir, tablet_metadata_filename(tablet_id, version));
            files_to_vacuum.emplace_back(std::move(path));
        }
        RETURN_IF_ERROR(delete_files(fs.get(), files_to_vacuum));
    }

    return Status::OK();
}

void delete_tablets(TabletManager* tablet_mgr, const DeleteTabletRequest& request, DeleteTabletResponse* response) {
    DCHECK(tablet_mgr != nullptr);
    DCHECK(request.tablet_ids_size() > 0);
    DCHECK(response != nullptr);
    std::vector<int64_t> tablet_ids(request.tablet_ids().begin(), request.tablet_ids().end());
    std::sort(tablet_ids.begin(), tablet_ids.end());
    auto root_dir = tablet_mgr->tablet_root_location(tablet_ids[0]);
    auto st = delete_tablets_impl(tablet_mgr, root_dir, tablet_ids);
    st.to_protobuf(response->mutable_status());
}

} // namespace starrocks::lake
