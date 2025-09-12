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

#include <set>
#include <string_view>
#include <unordered_map>

#include "common/status.h"
#include "fs/fs.h"
#include "gutil/stl_util.h"
#include "gutil/strings/util.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_retain_info.h"
#include "storage/lake/update_manager.h"
#include "storage/protobuf_file.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"
#include "util/raw_container.h"

namespace starrocks::lake {

struct VacuumTabletMetaVerionRange {
    // range is [min_version, max_version)
    int64_t min_version = 0;
    int64_t max_version = 0;

    /*
    * if tablet a has version range [1, ..., 10) ,
    * and tablet b has version range [5, ..., 15),
    * then the merged version range is [1, ..., 10)
    *
    * The merge will calc the range of these two tablets both can delete,
    */
    void merge(int64_t min, int64_t max) {
        if (min_version == 0 && max_version == 0) {
            min_version = min;
            max_version = max;
        } else {
            min_version = std::min(min_version, min);
            // get the low watermark of the max version
            max_version = std::min(max_version, max);
        }
    }
};

static int get_num_delete_file_queued_tasks(void*) {
#ifndef BE_TEST
    auto tp = ExecEnv::GetInstance()->delete_file_thread_pool();
    return tp ? tp->num_queued_tasks() : 0;
#else
    return 0;
#endif
}

static int get_num_active_file_queued_tasks(void*) {
#ifndef BE_TEST
    auto tp = ExecEnv::GetInstance()->delete_file_thread_pool();
    return tp ? tp->active_threads() : 0;
#else
    return 0;
#endif
}

static bvar::LatencyRecorder g_del_file_latency("lake_vacuum_del_file"); // unit: us
static bvar::Adder<uint64_t> g_del_fails("lake_vacuum_del_file_fails");
static bvar::Adder<uint64_t> g_deleted_files("lake_vacuum_deleted_files");
static bvar::LatencyRecorder g_metadata_travel_latency("lake_vacuum_metadata_travel"); // unit: ms
static bvar::LatencyRecorder g_vacuum_txnlog_latency("lake_vacuum_delete_txnlog");
static bvar::PassiveStatus<int> g_queued_delete_file_tasks("lake_vacuum_queued_delete_file_tasks",
                                                           get_num_delete_file_queued_tasks, nullptr);
static bvar::PassiveStatus<int> g_active_delete_file_tasks("lake_vacuum_active_delete_file_tasks",
                                                           get_num_active_file_queued_tasks, nullptr);
namespace {

const char* const kDuplicateFilesError =
        "Duplicate files were returned from the remote storage. The most likely cause is an S3 or HDFS API "
        "compatibility issue with your remote storage implementation.";

std::future<Status> completed_future(Status value) {
    std::promise<Status> p;
    p.set_value(std::move(value));
    return p.get_future();
}

bool should_retry(const Status& st, int64_t attempted_retries) {
    if (attempted_retries >= config::lake_vacuum_retry_max_attempts) {
        return false;
    }
    if (st.is_resource_busy()) {
        return true;
    }
    auto message = st.message();
    return MatchPattern(message, config::lake_vacuum_retry_pattern.value());
}

int64_t calculate_retry_delay(int64_t attempted_retries) {
    int64_t min_delay = config::lake_vacuum_retry_min_delay_ms;
    return min_delay * (1 << attempted_retries);
}

Status delete_files_with_retry(FileSystem* fs, std::span<const std::string> paths) {
    for (int64_t attempted_retries = 0; /**/; attempted_retries++) {
        auto st = fs->delete_files(paths);
        if (!st.ok() && should_retry(st, attempted_retries)) {
            int64_t delay = calculate_retry_delay(attempted_retries);
            LOG(WARNING) << "Fail to delete: " << st << " will retry after " << delay << "ms";
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        } else {
            return st;
        }
    }
}

} // namespace

// Batch delete files with specified FileSystem object |fs|
Status do_delete_files(FileSystem* fs, const std::vector<std::string>& paths) {
    if (UNLIKELY(paths.empty())) {
        return Status::OK();
    }

    auto delete_single_batch = [fs](std::span<const std::string> batch) -> Status {
        auto wait_duration = config::experimental_lake_wait_per_delete_ms;
        if (wait_duration > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(wait_duration));
        }

        if (config::lake_print_delete_log) {
            for (size_t i = 0, n = batch.size(); i < n; i++) {
                LOG(INFO) << "Deleting " << batch[i] << "(" << (i + 1) << '/' << n << ')';
            }
        }

        auto t0 = butil::gettimeofday_us();
        auto st = delete_files_with_retry(fs, batch);
        if (st.ok()) {
            auto t1 = butil::gettimeofday_us();
            g_del_file_latency << (t1 - t0);
            g_deleted_files << batch.size();
            VLOG(5) << "Deleted " << batch.size() << " files cost " << (t1 - t0) << "us";
        } else {
            g_del_fails << 1;
            LOG(WARNING) << "Fail to delete: " << st;
        }
        return st;
    };

    auto batch_size = int64_t{config::lake_vacuum_min_batch_delete_size};
    auto batch_count = static_cast<int64_t>((paths.size() + batch_size - 1) / batch_size);
    for (auto i = int64_t{0}; i < batch_count; i++) {
        auto begin = paths.begin() + (i * batch_size);
        auto end = std::min(begin + batch_size, paths.end());
        RETURN_IF_ERROR(delete_single_batch(std::span<const std::string>(begin, end)));
    }
    return Status::OK();
}

// Batch delete files with automatically derived FileSystems.
// REQUIRE: All files in |paths| have the same file system scheme.
Status delete_files(const std::vector<std::string>& paths) {
    if (paths.empty()) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(paths[0]));
    return do_delete_files(fs.get(), paths);
}

void delete_files_async(std::vector<std::string> files_to_delete) {
    if (UNLIKELY(files_to_delete.empty())) {
        return;
    }
    auto task = [files_to_delete = std::move(files_to_delete)]() { (void)delete_files(files_to_delete); };
    auto tp = ExecEnv::GetInstance()->delete_file_thread_pool();
    auto st = tp->submit_func(std::move(task));
    LOG_IF(ERROR, !st.ok()) << st;
}

std::future<Status> delete_files_callable(std::vector<std::string> files_to_delete) {
    if (UNLIKELY(files_to_delete.empty())) {
        return completed_future(Status::OK());
    }
    auto task = std::make_shared<std::packaged_task<Status()>>(
            [files_to_delete = std::move(files_to_delete)]() { return delete_files(files_to_delete); });
    auto packaged_func = [task]() { (*task)(); };
    auto tp = ExecEnv::GetInstance()->delete_file_thread_pool();
    if (auto st = tp->submit_func(std::move(packaged_func)); !st.ok()) {
        return completed_future(std::move(st));
    }
    return task->get_future();
}

void run_clear_task_async(std::function<void()> task) {
    auto tp = ExecEnv::GetInstance()->delete_file_thread_pool();
    auto st = tp->submit_func(std::move(task));
    LOG_IF(ERROR, !st.ok()) << st;
}

static Status collect_garbage_files(const TabletMetadataPB& metadata, const std::string& base_dir,
                                    AsyncFileDeleter* deleter, AsyncBundleFileDeleter* bundle_file_deleter,
                                    int64_t* garbage_data_size, const TabletRetainInfo& retain_info) {
    for (const auto& rowset : metadata.compaction_inputs()) {
        if (retain_info.contains_rowset(rowset.id())) {
            continue;
        }

        for (int i = 0; i < rowset.segments_size(); ++i) {
            if (rowset.shared_segments_size() > 0 && rowset.shared_segments(i)) {
                continue;
            }

            if (rowset.bundle_file_offsets_size() > 0 && bundle_file_deleter != nullptr) {
                RETURN_IF_ERROR(bundle_file_deleter->delete_file(join_path(base_dir, rowset.segments(i))));
            } else {
                RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, rowset.segments(i))));
            }
        }

        for (const auto& del_file : rowset.del_files()) {
            if (del_file.shared()) {
                continue;
            }

            RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, del_file.name())));
        }
        *garbage_data_size += rowset.data_size();
    }

    for (const auto& file : metadata.orphan_files()) {
        if (file.shared()) {
            continue;
        }

        if (retain_info.contains_file(file.name())) {
            continue;
        }

        RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, file.name())));
        *garbage_data_size += file.size();
    }
    return Status::OK();
}

static Status collect_alive_bundle_files(TabletManager* tablet_mgr, const std::vector<TabletInfoPB>& tablet_infos,
                                         int64_t version, std::string_view root_dir, AsyncBundleFileDeleter* deleter) {
    auto data_dir = join_path(root_dir, kSegmentDirectoryName);
    for (const auto& tablet_info : tablet_infos) {
        auto tablet_id = tablet_info.tablet_id();
        auto res = tablet_mgr->get_tablet_metadata(tablet_id, version, false);
        TEST_SYNC_POINT_CALLBACK("collect_files_to_vacuum:get_tablet_metadata", &res);
        if (!res.ok()) {
            // must exist.
            return res.status();
        } else {
            auto metadata = std::move(res).value();
            for (const auto& rowset : metadata->rowsets()) {
                if (rowset.bundle_file_offsets_size() > 0) {
                    for (const auto& segment : rowset.segments()) {
                        RETURN_IF_ERROR(deleter->delay_delete(join_path(data_dir, segment)));
                    }
                }
            }
        }
    }
    return Status::OK();
}

static size_t collect_extra_files_size(const TabletMetadataPB& metadata, int64_t min_retain_version) {
    int64_t metadata_version = metadata.version();
    if (metadata_version > min_retain_version) {
        return 0;
    }
    size_t extra_file_size = 0;
    for (const auto& rowset : metadata.compaction_inputs()) {
        extra_file_size += rowset.data_size();
    }
    for (const auto& file : metadata.orphan_files()) {
        extra_file_size += file.size();
    }
    return extra_file_size;
}

static Status collect_files_to_vacuum(TabletManager* tablet_mgr, std::string_view root_dir, TabletInfoPB& tablet_info,
                                      int64_t grace_timestamp, int64_t min_retain_version,
                                      VacuumTabletMetaVerionRange* vacuum_version_range,
                                      AsyncFileDeleter* datafile_deleter, AsyncFileDeleter* metafile_deleter,
                                      AsyncBundleFileDeleter* bundle_file_deleter, int64_t* total_datafile_size,
                                      int64_t* vacuumed_version, int64_t* extra_datafile_size,
                                      const TabletRetainInfo& retain_info) {
    auto t0 = butil::gettimeofday_ms();
    auto meta_dir = join_path(root_dir, kMetadataDirectoryName);
    auto data_dir = join_path(root_dir, kSegmentDirectoryName);
    auto final_retain_version = min_retain_version;
    auto version = final_retain_version;
    auto tablet_id = tablet_info.tablet_id();
    auto min_version = std::max(1L, tablet_info.min_version());
    // grace_timestamp <= 0 means no grace timestamp
    auto skip_check_grace_timestamp = grace_timestamp <= 0;
    size_t extra_file_size = 0;
    int64_t prepare_vacuum_file_size = 0;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_dir));
    // Starting at |*final_retain_version|, read the tablet metadata forward along
    // the |prev_garbage_version| pointer until the tablet metadata does not exist.
    while (version >= min_version) {
        auto res = tablet_mgr->get_tablet_metadata(tablet_id, version, false);
        TEST_SYNC_POINT_CALLBACK("collect_files_to_vacuum:get_tablet_metadata", &res);
        if (res.status().is_not_found()) {
            break;
        } else if (!res.ok()) {
            return res.status();
        } else {
            auto metadata = std::move(res).value();
            extra_file_size += collect_extra_files_size(*metadata, min_retain_version);
            if (skip_check_grace_timestamp) {
                DCHECK_LE(version, final_retain_version);
                RETURN_IF_ERROR(collect_garbage_files(*metadata, data_dir, datafile_deleter, bundle_file_deleter,
                                                      &prepare_vacuum_file_size, retain_info));
            } else {
                int64_t compare_time = 0;
                if (metadata->has_commit_time() && metadata->commit_time() > 0) {
                    compare_time = metadata->commit_time();
                } else {
                    /*
                    * The path is not available since we get tablet metadata by tablet_id and version.
                    * We remove the code which is to get file modified time by path.
                    * This change will break some compatibility when upgraded from a old version which have no commit time.
                    * In that case, the compare_time is 0, making a result that the vacuum will keep the latest version.
                    * The incompatibility will be vanished after a few versions ingestion/compaction/GC.
                    */

                    // ASSIGN_OR_RETURN(compare_time, fs->get_file_modified_time(path));
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
                    RETURN_IF_ERROR(collect_garbage_files(*metadata, data_dir, datafile_deleter, bundle_file_deleter,
                                                          total_datafile_size, retain_info));
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
        // The final_retain_version is set to min_retain_version or minmum exist version which has garbage files.
        // So we set vacuumed_version to `final_retain_version - 1` to avoid the garbage files of final_retain_version can
        // not be deleted
        *vacuumed_version = final_retain_version - 1;
        return Status::OK();
    }
    *vacuumed_version = final_retain_version;
    DCHECK_LE(version, final_retain_version);
    if (vacuum_version_range == nullptr) {
        for (auto v = version + 1; v < final_retain_version; v++) {
            if (retain_info.contains_version(v)) {
                continue;
            }
            RETURN_IF_ERROR(metafile_deleter->delete_file(join_path(meta_dir, tablet_metadata_filename(tablet_id, v))));
        }
    } else {
        // The vacuum_version_range is used to collect the version range of the tablet metadata files to be deleted.
        // So we can decide the final version range to be deleted when aggregate partition is enabled.
        vacuum_version_range->merge(version + 1, final_retain_version);
    }
    tablet_info.set_min_version(final_retain_version);
    *total_datafile_size += prepare_vacuum_file_size;
    *extra_datafile_size += extra_file_size;
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
                                     std::vector<TabletInfoPB>& tablet_infos, int64_t min_retain_version,
                                     int64_t grace_timestamp, bool enable_file_bundling, int64_t* vacuumed_files,
                                     int64_t* vacuumed_file_size, int64_t* vacuumed_version, int64_t* extra_file_size,
                                     const std::unordered_set<int64_t>& retain_versions) {
    DCHECK(tablet_mgr != nullptr);
    DCHECK(std::is_sorted(tablet_infos.begin(), tablet_infos.end(),
                          [](const auto& a, const auto& b) { return a.tablet_id() < b.tablet_id(); }));
    DCHECK(min_retain_version >= 0);
    DCHECK(grace_timestamp >= 0);
    DCHECK(vacuumed_files != nullptr);
    DCHECK(vacuumed_file_size != nullptr);

    auto metafile_delete_cb = [=](const std::vector<std::string>& files) {
        erase_tablet_metadata_from_metacache(tablet_mgr, files);
    };
    std::unique_ptr<VacuumTabletMetaVerionRange> vacuum_version_range;
    if (enable_file_bundling) {
        vacuum_version_range = std::make_unique<VacuumTabletMetaVerionRange>();
    }
    AsyncBundleFileDeleter bundle_file_deleter(config::lake_vacuum_min_batch_delete_size);
    int64_t final_vacuum_version = std::numeric_limits<int64_t>::max();
    int64_t max_vacuum_version = 0;
    for (auto& tablet_info : tablet_infos) {
        TabletRetainInfo tablet_retain_info;
        RETURN_IF_ERROR(tablet_retain_info.init(tablet_info.tablet_id(), retain_versions, tablet_mgr));

        int64_t tablet_vacuumed_version = 0;
        AsyncFileDeleter datafile_deleter(config::lake_vacuum_min_batch_delete_size);
        AsyncFileDeleter metafile_deleter(INT64_MAX, metafile_delete_cb);
        RETURN_IF_ERROR(collect_files_to_vacuum(tablet_mgr, root_dir, tablet_info, grace_timestamp, min_retain_version,
                                                vacuum_version_range.get(), &datafile_deleter, &metafile_deleter,
                                                &bundle_file_deleter, vacuumed_file_size, &tablet_vacuumed_version,
                                                extra_file_size, tablet_retain_info));
        RETURN_IF_ERROR(datafile_deleter.finish());
        (*vacuumed_files) += datafile_deleter.delete_count();
        if (!enable_file_bundling) {
            RETURN_IF_ERROR(metafile_deleter.finish());
            (*vacuumed_files) += metafile_deleter.delete_count();
        }
        // set partition vacuumed_version to min tablet vacuumed version
        final_vacuum_version = std::min(final_vacuum_version, tablet_vacuumed_version);
        max_vacuum_version = std::max(max_vacuum_version, tablet_vacuumed_version);
    }
    // delete bundle files
    if (max_vacuum_version > 0 && !bundle_file_deleter.is_empty()) {
        RETURN_IF_ERROR(collect_alive_bundle_files(tablet_mgr, tablet_infos, max_vacuum_version, root_dir,
                                                   &bundle_file_deleter));
        RETURN_IF_ERROR(bundle_file_deleter.finish());
        (*vacuumed_files) += bundle_file_deleter.delete_count();
    }
    if (enable_file_bundling) {
        // collect meta files to vacuum at partition level
        AsyncFileDeleter metafile_deleter(INT64_MAX, metafile_delete_cb);
        auto meta_dir = join_path(root_dir, kMetadataDirectoryName);
        // a special case:
        // if a table enable file_bundling and finished alter job, the new created tablet will create initial tablet metadata
        // its own tablet_id to avoid overwriting the initial tablet metadata.
        // After that, we need to vacuum these metadata file using its own tablet_id
        if (vacuum_version_range->min_version <= 1) {
            for (auto& tablet_info : tablet_infos) {
                RETURN_IF_ERROR(metafile_deleter.delete_file(
                        join_path(meta_dir, tablet_metadata_filename(tablet_info.tablet_id(), 1))));
            }
        }
        for (auto v = vacuum_version_range->min_version; v < vacuum_version_range->max_version; v++) {
            if (retain_versions.find(v) != retain_versions.end()) {
                continue;
            }
            RETURN_IF_ERROR(metafile_deleter.delete_file(join_path(meta_dir, tablet_metadata_filename(0, v))));
        }
        RETURN_IF_ERROR(metafile_deleter.finish());
        (*vacuumed_files) += metafile_deleter.delete_count();
    }
    *vacuumed_version = final_vacuum_version;
    return Status::OK();
}

Status vacuum_txn_log(std::string_view root_location, int64_t min_active_txn_id, int64_t* vacuumed_files,
                      int64_t* vacuumed_file_size) {
    DCHECK(vacuumed_files != nullptr);
    DCHECK(vacuumed_file_size != nullptr);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    auto t0 = butil::gettimeofday_s();
    auto deleter = AsyncFileDeleter(config::lake_vacuum_min_batch_delete_size);
    auto ret = Status::OK();
    auto log_dir = join_path(root_location, kTxnLogDirectoryName);
    auto iter_st = ignore_not_found(fs->iterate_dir2(log_dir, [&](DirEntry entry) {
        if (is_txn_log(entry.name)) {
            auto [tablet_id, txn_id] = parse_txn_log_filename(entry.name);
            if (txn_id >= min_active_txn_id) {
                return true;
            }
        } else if (is_txn_slog(entry.name)) {
            auto [tablet_id, txn_id] = parse_txn_slog_filename(entry.name);
            if (txn_id >= min_active_txn_id) {
                return true;
            }
        } else if (is_combined_txn_log(entry.name)) {
            auto txn_id = parse_combined_txn_log_filename(entry.name);
            if (txn_id >= min_active_txn_id) {
                return true;
            }
        } else {
            return true;
        }

        *vacuumed_files += 1;
        *vacuumed_file_size += entry.size.value_or(0);

        auto st = deleter.delete_file(join_path(log_dir, entry.name));
        if (!st.ok()) {
            LOG(WARNING) << "Fail to delete " << join_path(log_dir, entry.name) << ": " << st;
            ret.update(st);
        }
        return st.ok(); // Stop list if delete failed
    }));
    ret.update(iter_st);
    ret.update(deleter.finish());

    auto t1 = butil::gettimeofday_s();
    g_vacuum_txnlog_latency << (t1 - t0);

    return ret;
}

Status vacuum_impl(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response) {
    if (UNLIKELY(tablet_mgr == nullptr)) {
        return Status::InvalidArgument("tablet_mgr is null");
    }
    if (UNLIKELY(request.tablet_ids_size() == 0 && request.tablet_infos_size() == 0)) {
        return Status::InvalidArgument("both tablet_ids and tablet_infos are empty");
    }
    if (UNLIKELY(request.min_retain_version() <= 0)) {
        return Status::InvalidArgument("value of min_retain_version is zero or negative");
    }
    if (UNLIKELY(request.grace_timestamp() <= 0)) {
        return Status::InvalidArgument("value of grace_timestamp is zero or nagative");
    }

    auto tablet_infos = std::vector<TabletInfoPB>();
    if (request.tablet_infos_size() > 0) {
        tablet_infos.reserve(request.tablet_infos_size());
        tablet_infos.insert(tablet_infos.begin(), request.tablet_infos().begin(), request.tablet_infos().end());
    } else { // This is a request from older version FE
        tablet_infos.reserve(request.tablet_ids_size());
        for (const auto& tablet_id : request.tablet_ids()) {
            auto& tablet_info = tablet_infos.emplace_back();
            tablet_info.set_tablet_id(tablet_id);
            tablet_info.set_min_version(0);
        }
    }
    auto root_loc = tablet_mgr->tablet_root_location(tablet_infos[0].tablet_id());
    auto min_retain_version = request.min_retain_version();
    auto grace_timestamp = request.grace_timestamp();
    auto min_active_txn_id = request.min_active_txn_id();
    std::unordered_set<int64_t> retain_versions;
    if (request.retain_versions_size() > 0) {
        retain_versions.insert(request.retain_versions().begin(), request.retain_versions().end());
    }

    int64_t vacuumed_files = 0;
    int64_t vacuumed_file_size = 0;
    int64_t vacuumed_version = 0;
    int64_t extra_file_size = 0;

    std::sort(tablet_infos.begin(), tablet_infos.end(),
              [](const auto& a, const auto& b) { return a.tablet_id() < b.tablet_id(); });

    RETURN_IF_ERROR(vacuum_tablet_metadata(tablet_mgr, root_loc, tablet_infos, min_retain_version, grace_timestamp,
                                           request.enable_file_bundling(), &vacuumed_files, &vacuumed_file_size,
                                           &vacuumed_version, &extra_file_size, retain_versions));
    extra_file_size -= vacuumed_file_size;
    if (request.delete_txn_log()) {
        RETURN_IF_ERROR(vacuum_txn_log(root_loc, min_active_txn_id, &vacuumed_files, &vacuumed_file_size));
    }
    response->set_vacuumed_files(vacuumed_files);
    response->set_vacuumed_file_size(vacuumed_file_size);
    response->set_vacuumed_version(vacuumed_version);
    response->set_extra_file_size(extra_file_size);
    for (const auto& tablet_info : tablet_infos) {
        response->add_tablet_infos()->CopyFrom(tablet_info);
    }
    return Status::OK();
}

void vacuum(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response) {
    auto st = vacuum_impl(tablet_mgr, request, response);
    LOG_IF(ERROR, !st.ok()) << st;
    st.to_protobuf(response->mutable_status());
}

// The state of the bundle tablet meta, used to determine whether the bundle file can be deleted.
// ALL_TABLETS_TO_BE_DELETED means all tablets in this bundle tablet meta are going to be deleted,
// SOME_TABLETS_NOT_TO_BE_DELETED means some tablets in this bundle tablet meta aren't going to be deleted,
// NO_TABLETS_TO_BE_DELETED means no tablets in this bundle tablet meta are going to be deleted,
// NOT_BUNDLE_TABLET_META means this tablet meta is not the bundle tablet meta.
enum class BundleTabletMetaState {
    ALL_TABLETS_TO_BE_DELETED = 0,
    SOME_TABLETS_NOT_TO_BE_DELETED = 1,
    NO_TABLETS_TO_BE_DELETED = 2,
    NOT_BUNDLE_TABLET_META = 3,
};

static bool can_bundle_meta_file_to_be_deleted(const BundleTabletMetaState& state) {
    // if there are some tablets in this bundle meta that are not to be deleted,
    // we can not delete the bundle file.
    return state == BundleTabletMetaState::ALL_TABLETS_TO_BE_DELETED;
}

static StatusOr<BundleTabletMetaState> check_bundle_tablet_meta_state(
        const std::string& meta_path, const std::vector<int64_t>& to_delete_tablet_ids) {
    RandomAccessFileOptions opts{.skip_fill_local_cache = true};
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(meta_path));
    ASSIGN_OR_RETURN(auto input_file, fs->new_random_access_file(opts, meta_path));
    // Read the entire file content into a string.
    ASSIGN_OR_RETURN(auto serialized_string, input_file->read_all());
    // Parse the bundle tablet metadata from the serialized string.
    ASSIGN_OR_RETURN(auto bundle_metadata, TabletManager::parse_bundle_tablet_metadata(meta_path, serialized_string));
    bool shared_meta_contains_deleted_tablet = false;
    bool shared_meta_contains_alive_tablet = false;
    // Check if the shared metadata contains tablets that are not to be deleted.
    std::unordered_set<int64_t> to_delete_tablet_ids_set(to_delete_tablet_ids.begin(), to_delete_tablet_ids.end());
    for (const auto& tablet_id : bundle_metadata->tablet_meta_pages()) {
        if (to_delete_tablet_ids_set.find(tablet_id.first) == to_delete_tablet_ids_set.end()) {
            shared_meta_contains_alive_tablet = true;
        } else {
            shared_meta_contains_deleted_tablet = true;
        }
    }
    // Determine the state of the bundle tablet meta based on the presence of deleted and alive tablets.
    if (shared_meta_contains_deleted_tablet && shared_meta_contains_alive_tablet) {
        // Some tablets in this bundle meta are not to be deleted,
        return BundleTabletMetaState::SOME_TABLETS_NOT_TO_BE_DELETED;
    } else if (shared_meta_contains_deleted_tablet) {
        // All tablets in this bundle meta are going to be deleted,
        return BundleTabletMetaState::ALL_TABLETS_TO_BE_DELETED;
    } else {
        // No tablets in this bundle meta are going to be deleted,
        return BundleTabletMetaState::NO_TABLETS_TO_BE_DELETED;
    }
}

static Status delete_files_under_txnlog(const std::string& data_dir, const TxnLogPB& log, bool contains_alive_tablets,
                                        bool is_combined_log, AsyncFileDeleter& deleter) {
    if (log.has_op_write()) {
        const auto& op = log.op_write();
        // Check if the rowset is bundled and if it's safe to delete
        // We can delete segments if either:
        // 1. This rowset doesn't use bundle files (bundle_file_offsets_size == 0), or
        // 2. This is a combined log and all tablets in the combined log are being deleted
        //
        // We do not allow delete bundle segment file under not combined log, because the bundle file may be shared by
        // multiple tablets, and we can't determine whether the bundle file is shared by other alive tablets.
        // This case can only happen when alter table and set bundle_file = false.
        if (op.rowset().bundle_file_offsets_size() == 0 || (is_combined_log && !contains_alive_tablets)) {
            // Iterate through all segments in the rowset and delete them
            for (const auto& segment : op.rowset().segments()) {
                RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, segment)));
            }
        }
        // delete del files
        for (const auto& f : op.dels()) {
            RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, f)));
        }
    }
    if (log.has_op_compaction()) {
        const auto& op = log.op_compaction();
        for (const auto& segment : op.output_rowset().segments()) {
            RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, segment)));
        }
    }
    if (log.has_op_schema_change()) {
        const auto& op = log.op_schema_change();
        for (const auto& rowset : op.rowsets()) {
            for (const auto& segment : rowset.segments()) {
                RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, segment)));
            }
        }
    }
    return Status::OK();
}

// TODO: remote list objects requests
Status delete_tablets_impl(TabletManager* tablet_mgr, const std::string& root_dir,
                           const std::vector<int64_t>& tablet_ids) {
    DCHECK(tablet_mgr != nullptr);
    DCHECK(std::is_sorted(tablet_ids.begin(), tablet_ids.end()));

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_dir));

    std::unordered_set<int64_t> bundle_tablet_versions;
    std::unordered_map<int64_t, std::map<int64_t, BundleTabletMetaState>> tablet_versions;
    //                 ^^^^^^^ tablet id
    //                                  ^^^^^^^^^ version number -> state of this tablet meta

    auto meta_dir = join_path(root_dir, kMetadataDirectoryName);
    auto data_dir = join_path(root_dir, kSegmentDirectoryName);
    auto log_dir = join_path(root_dir, kTxnLogDirectoryName);

    std::set<std::string> txn_logs;
    std::set<std::string> combine_txn_logs;
    RETURN_IF_ERROR(ignore_not_found(fs->iterate_dir(log_dir, [&](std::string_view name) {
        if (is_txn_log(name)) {
            auto [tablet_id, txn_id] = parse_txn_log_filename(name);
            if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
                return true;
            }
        } else if (is_txn_slog(name)) {
            auto [tablet_id, txn_id] = parse_txn_slog_filename(name);
            if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
                return true;
            }
        } else if (is_txn_vlog(name)) {
            auto [tablet_id, version] = parse_txn_vlog_filename(name);
            if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
                return true;
            }
        } else if (is_combined_txn_log(name)) {
            // should be deleted
            combine_txn_logs.emplace(name);
            return true;
        } else {
            return true;
        }

        auto [_, inserted] = txn_logs.emplace(name);
        LOG_IF(FATAL, !inserted) << kDuplicateFilesError << " duplicate file:" << join_path(log_dir, name);

        return true;
    })));

    AsyncFileDeleter deleter(config::lake_vacuum_min_batch_delete_size);
    // Used to avoid deleting bundle files that are shared by multiple tablets.
    AsyncBundleFileDeleter dummy_bundle_file_deleter(config::lake_vacuum_min_batch_delete_size);

    for (const auto& log_name : txn_logs) {
        auto res = tablet_mgr->get_txn_log(join_path(log_dir, log_name), false);
        if (res.status().is_not_found()) {
            continue;
        } else if (!res.ok()) {
            return res.status();
        } else {
            auto log_ptr = std::move(res).value();
            // delete files under txnlog
            RETURN_IF_ERROR(delete_files_under_txnlog(data_dir, *log_ptr, false, false, deleter));
            // delete txnlog
            RETURN_IF_ERROR(deleter.delete_file(join_path(log_dir, log_name)));
        }
    }

    for (const auto& log_name : combine_txn_logs) {
        auto res = tablet_mgr->get_combined_txn_log(join_path(log_dir, log_name), false);
        if (res.status().is_not_found()) {
            continue;
        } else if (!res.ok()) {
            return res.status();
        } else {
            auto combine_log_ptr = std::move(res).value();
            // check whether every txn_log is contained in tablet_ids.
            // If not, it means this combine txn log is also shared by other alive tablets.
            bool contains_alive_tablets = false;
            for (const auto& log : combine_log_ptr->txn_logs()) {
                if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), log.tablet_id())) {
                    contains_alive_tablets = true;
                    break;
                }
            }
            // delete files under txnlog
            for (const auto& log : combine_log_ptr->txn_logs()) {
                if (std::binary_search(tablet_ids.begin(), tablet_ids.end(), log.tablet_id())) {
                    RETURN_IF_ERROR(delete_files_under_txnlog(data_dir, log, contains_alive_tablets, true, deleter));
                }
            }
            // delete txnlog
            if (!contains_alive_tablets) {
                RETURN_IF_ERROR(deleter.delete_file(join_path(log_dir, log_name)));
            }
        }
    }

    RETURN_IF_ERROR(ignore_not_found(fs->iterate_dir(meta_dir, [&](std::string_view name) {
        if (!is_tablet_metadata(name)) {
            return true;
        }
        auto [tablet_id, version] = parse_tablet_metadata_filename(name);
        // if the tablet is the bundle tablet, we need to record the version.
        // And if the version is equal to kInitialVersion, it means this is the initial tablet meta,
        // not bundle tablet meta.
        if (tablet_id == 0 && version != kInitialVersion) {
            bundle_tablet_versions.insert(version);
            return true;
        }
        // if the tablet is not in tablet_ids, we need to skip it.
        if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
            return true;
        }
        tablet_versions[tablet_id][version] = BundleTabletMetaState::NOT_BUNDLE_TABLET_META;
        return true;
    })));

    // For tablet meta, there are three cases:
    // 1. This tablet meta is the bundle tablet meta, and all tablets in this bundle tablet meta are going to be deleted.
    // 2. This tablet meta is the bundle tablet meta, and some tablets in this bundle tablet aren't going to be deleted.
    // 3. This tablet meta is not the bundle tablet meta, and it is going to be deleted.
    //
    // We will check the bundle tablet meta state for each version in bundle_tablet_versions.
    // ALL_TABLETS_TO_BE_DELETED means all tablets in this bundle tablet meta are going to be deleted,
    // SOME_TABLETS_NOT_TO_BE_DELETED means some tablets in this bundle tablet meta aren't going to be deleted,
    // NO_TABLETS_TO_BE_DELETED means no tablets in this bundle tablet meta are going to be deleted,
    // NOT_BUNDLE_TABLET_META means this tablet meta is not the bundle tablet meta.
    //
    // we will only delete the bundle segment files and bundle tablet meta when the state is ALL_TABLETS_TO_BE_DELETED
    // and NOT_BUNDLE_TABLET_META.
    for (auto version : bundle_tablet_versions) {
        // Get the path of the bundle tablet metadata file for this version
        // We use the first tablet ID from tablet_ids as a reference to get the location
        auto path = tablet_mgr->bundle_tablet_metadata_location(*tablet_ids.begin(), version);

        // Check the state of the bundle tablet metadata for this version
        // This tells us whether all tablets in this bundle are being deleted or not
        ASSIGN_OR_RETURN(auto bundle_meta_state, check_bundle_tablet_meta_state(path, tablet_ids));

        // If there are any tablets to be deleted in this bundle (not NO_TABLETS_TO_BE_DELETED),
        // we need to record this state for each tablet in tablet_ids
        if (bundle_meta_state != BundleTabletMetaState::NO_TABLETS_TO_BE_DELETED) {
            // Record the bundle meta state for all tablets that are being deleted
            // This information will be used later to determine if bundle files can be deleted
            for (auto tablet_id : tablet_ids) {
                tablet_versions[tablet_id][version] = bundle_meta_state;
            }
        }
    }

    for (auto& [tablet_id, versions_and_states] : tablet_versions) {
        DCHECK(!versions_and_states.empty());
        std::set<int64_t> versions_to_delete;
        for (auto& [version, state] : versions_and_states) {
            versions_to_delete.insert(version);
        }

        TabletMetadataPtr latest_metadata = nullptr;

        // Find metadata files that has garbage data files and delete all those files
        for (int64_t garbage_version = *versions_to_delete.rbegin(), min_v = *versions_to_delete.begin();
             garbage_version >= min_v;
             /**/) {
            auto res = tablet_mgr->get_tablet_metadata(tablet_id, garbage_version, false);
            if (res.status().is_not_found()) {
                break;
            } else if (!res.ok()) {
                LOG(ERROR) << "Fail to read tablet_id=" << tablet_id << ", version=" << garbage_version << ": "
                           << res.status();
                return res.status();
            } else {
                auto metadata = std::move(res).value();
                if (latest_metadata == nullptr) {
                    latest_metadata = metadata;
                }
                int64_t dummy_file_size = 0;
                RETURN_IF_ERROR(
                        collect_garbage_files(*metadata, data_dir, &deleter,
                                              can_bundle_meta_file_to_be_deleted(versions_and_states[garbage_version])
                                                      ? nullptr
                                                      : &dummy_bundle_file_deleter,
                                              &dummy_file_size, TabletRetainInfo()));
                if (metadata->has_prev_garbage_version()) {
                    garbage_version = metadata->prev_garbage_version();
                } else {
                    break;
                }
            }
        }

        if (latest_metadata != nullptr) {
            for (const auto& rowset : latest_metadata->rowsets()) {
                for (int i = 0; i < rowset.segments_size(); ++i) {
                    if (rowset.shared_segments_size() > 0 && rowset.shared_segments(i)) {
                        continue;
                    }
                    // if the segment file is not shared by other alive tablets, we can delete the segments directly.
                    if (rowset.bundle_file_offsets_size() == 0 ||
                        can_bundle_meta_file_to_be_deleted(versions_and_states[latest_metadata->version()])) {
                        RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, rowset.segments(i))));
                    }
                }
            }
            if (latest_metadata->has_delvec_meta()) {
                for (const auto& [v, f] : latest_metadata->delvec_meta().version_to_file()) {
                    if (f.shared()) {
                        continue;
                    }
                    RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, f.name())));
                }
            }
            if (latest_metadata->sstable_meta().sstables_size() > 0) {
                for (const auto& sst : latest_metadata->sstable_meta().sstables()) {
                    if (sst.shared()) {
                        continue;
                    }
                    RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, sst.filename())));
                }
            }
        }

        for (auto& [version, state] : versions_and_states) {
            if (state == BundleTabletMetaState::NOT_BUNDLE_TABLET_META) {
                // delete the individual tablet metadata file
                auto path = join_path(meta_dir, tablet_metadata_filename(tablet_id, version));
                RETURN_IF_ERROR(deleter.delete_file(std::move(path)));
            } else if (state == BundleTabletMetaState::ALL_TABLETS_TO_BE_DELETED) {
                // delete the bundle tablet metadata file
                auto path = join_path(meta_dir, tablet_metadata_filename(0, version));
                RETURN_IF_ERROR(deleter.delete_file(std::move(path)));
            } else if (state == BundleTabletMetaState::SOME_TABLETS_NOT_TO_BE_DELETED) {
                // we can not delete the bundle file, so we just skip it.
            } else {
                // do nothing for NO_TABLETS_TO_BE_DELETED
            }
        }
    }

    return deleter.finish();
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

void delete_txn_log(TabletManager* tablet_mgr, const DeleteTxnLogRequest& request, DeleteTxnLogResponse* response) {
    DCHECK(tablet_mgr != nullptr);
    DCHECK(request.tablet_ids_size() > 0);
    DCHECK(response != nullptr);

    std::vector<std::string> files_to_delete;
    files_to_delete.reserve(request.tablet_ids_size() * (request.txn_ids_size() + request.txn_infos_size()));

    for (auto tablet_id : request.tablet_ids()) {
        // For each DeleteTxnLogRequest, FE will only set one of txn_ids and txn_infos, here we don't want
        // to bother with determining which one is set, just iterate through both.
        for (auto txn_id : request.txn_ids()) {
            auto log_path = tablet_mgr->txn_log_location(tablet_id, txn_id);
            files_to_delete.emplace_back(log_path);
            tablet_mgr->metacache()->erase(log_path);
        }
        for (auto&& info : request.txn_infos()) {
            auto log_path = info.combined_txn_log() ? tablet_mgr->combined_txn_log_location(tablet_id, info.txn_id())
                                                    : tablet_mgr->txn_log_location(tablet_id, info.txn_id());
            files_to_delete.emplace_back(log_path);
        }
    }

    delete_files_async(files_to_delete);
}

static std::string proto_to_json(const google::protobuf::Message& message) {
    json2pb::Pb2JsonOptions options;
    options.pretty_json = true;
    std::string json;
    std::string error;
    if (!json2pb::ProtoMessageToJson(message, &json, options, &error)) {
        LOG(WARNING) << "Failed to convert proto to json, " << error;
    }
    return json;
}

static StatusOr<TabletMetadataPtr> get_tablet_metadata(const string& metadata_location, bool fill_cache) {
    auto metadata = std::make_shared<TabletMetadataPB>();
    ProtobufFile file(metadata_location);
    RETURN_IF_ERROR_WITH_WARN(file.load(metadata.get(), fill_cache), "Failed to load " + metadata_location);
    return metadata;
}

StatusOr<std::pair<std::list<std::string>, std::list<std::string>>> list_meta_files(
        FileSystem* fs, const std::string& metadata_root_location) {
    LOG(INFO) << "Start to list " << metadata_root_location;
    std::list<std::string> meta_files;
    std::list<std::string> bundle_meta_files;
    RETURN_IF_ERROR_WITH_WARN(
            ignore_not_found(fs->iterate_dir(metadata_root_location,
                                             [&](std::string_view name) {
                                                 if (!is_tablet_metadata(name)) {
                                                     return true;
                                                 }
                                                 auto [tablet_id, version] =
                                                         parse_tablet_metadata_filename(basename(name));
                                                 if (tablet_id == 0 && version != kInitialVersion) {
                                                     // This is a bundle tablet metadata file
                                                     bundle_meta_files.emplace_back(name);
                                                 } else {
                                                     meta_files.emplace_back(name);
                                                 }
                                                 return true;
                                             })),
            "Failed to list " + metadata_root_location);
    LOG(INFO) << "Found " << meta_files.size() << " meta files, " << bundle_meta_files.size() << " bundle meta files";
    return std::make_pair(std::move(meta_files), std::move(bundle_meta_files));
}

static StatusOr<std::map<std::string, DirEntry>> list_data_files(FileSystem* fs,
                                                                 const std::string& segment_root_location,
                                                                 int64_t expired_seconds) {
    LOG(INFO) << "Start to list " << segment_root_location;
    std::map<std::string, DirEntry> data_files;
    int64_t total_files = 0;
    int64_t total_bytes = 0;
    const auto now = std::time(nullptr);
    RETURN_IF_ERROR_WITH_WARN(
            ignore_not_found(fs->iterate_dir2(segment_root_location,
                                              [&](DirEntry entry) {
                                                  total_files++;
                                                  total_bytes += entry.size.value_or(0);

                                                  // should consider segment files, sst, del file, delvector
                                                  if (!is_segment(entry.name) && !is_sst(entry.name) &&
                                                      !is_delvec(entry.name) && !is_del(entry.name)) {
                                                      return true;
                                                  }
                                                  if (!entry.mtime.has_value()) {
                                                      LOG(WARNING) << "Fail to get modified time of " << entry.name;
                                                      return true;
                                                  }

                                                  if (now >= entry.mtime.value() + expired_seconds) {
                                                      data_files.emplace(entry.name, entry);
                                                  }
                                                  return true;
                                              })),
            "Failed to list " + segment_root_location);
    LOG(INFO) << segment_root_location << ": Listed all data files, total files: " << total_files
              << ", total bytes: " << total_bytes << ", candidate files: " << data_files.size();
    return data_files;
}

StatusOr<std::map<std::string, DirEntry>> find_orphan_data_files(FileSystem* fs, std::string_view root_location,
                                                                 int64_t expired_seconds,
                                                                 const std::list<std::string>& meta_files,
                                                                 const std::list<std::string>& bundle_meta_files,
                                                                 std::ostream* audit_ostream) {
    const auto metadata_root_location = join_path(root_location, kMetadataDirectoryName);
    const auto segment_root_location = join_path(root_location, kSegmentDirectoryName);

    ASSIGN_OR_RETURN(auto data_files, list_data_files(fs, segment_root_location, expired_seconds));

    if (data_files.empty()) {
        return data_files;
    }

    std::set<std::string> data_files_in_metadatas;
    auto check_reference_files = [&](const TabletMetadataPtr& check_meta) {
        for (const auto& rowset : check_meta->rowsets()) {
            for (const auto& segment : rowset.segments()) {
                data_files.erase(segment);
                data_files_in_metadatas.emplace(segment);
            }
            for (const auto& del_file : rowset.del_files()) {
                data_files.erase(del_file.name());
                data_files_in_metadatas.emplace(del_file.name());
            }
        }

        const auto& delvector_meta = check_meta->delvec_meta();
        for (const auto& [_, file_meta_pb] : delvector_meta.version_to_file()) {
            data_files.erase(file_meta_pb.name());
            data_files_in_metadatas.emplace(file_meta_pb.name());
        }

        const auto& sstable_meta = check_meta->sstable_meta();
        for (const auto& sst : sstable_meta.sstables()) {
            data_files.erase(sst.filename());
            data_files_in_metadatas.emplace(sst.filename());
        }
    };

    if (audit_ostream) {
        (*audit_ostream) << "Total meta files: " << meta_files.size() << " bundle meta files"
                         << bundle_meta_files.size() << std::endl;
    }
    LOG(INFO) << "Start to filter with metadatas, count: " << meta_files.size()
              << " bundle meta files: " << bundle_meta_files.size();

    int64_t progress = 0;
    // Iterate through all metadata files and check if the data files are referenced in them.
    for (const auto& name : meta_files) {
        auto location = join_path(metadata_root_location, name);
        auto res = get_tablet_metadata(location, false);
        if (res.status().is_not_found()) { // This metadata file was deleted by another node
            LOG(INFO) << location << " is deleted by other node";
            continue;
        } else if (!res.ok()) {
            LOG(WARNING) << "Failed to read metadata file: " << location << ", error: " << res.status();
            return res.status();
        }
        const auto& metadata = res.value();
        check_reference_files(metadata);
        ++progress;
        if (audit_ostream) {
            (*audit_ostream) << '(' << progress << '/' << meta_files.size() << ") " << name << '\n'
                             << proto_to_json(*metadata) << std::endl;
        }
        LOG(INFO) << "Filtered with meta file: " << name << " (" << progress << '/' << meta_files.size() << ')';
    }

    // Iterate through all bundle metadata files and check if the data files are referenced in them.
    progress = 0;
    for (const auto& name : bundle_meta_files) {
        auto location = join_path(metadata_root_location, name);
        ASSIGN_OR_RETURN(auto metadatas, TabletManager::get_metas_from_bundle_tablet_metadata(location, fs));
        for (const auto& metadata : metadatas) {
            check_reference_files(metadata);
        }
        ++progress;
        if (audit_ostream) {
            (*audit_ostream) << '(' << progress << '/' << bundle_meta_files.size() << ") " << name << std::endl;
        }
        LOG(INFO) << "Filtered with bundle meta file: " << name << " (" << progress << '/' << bundle_meta_files.size()
                  << ')';
    }

    LOG(INFO) << "Start to double checking";

    for (const auto& [name, entry] : data_files) {
        if (data_files_in_metadatas.contains(name)) {
            LOG(WARNING) << "Failed to do double checking, file: " << name;
            return Status::InternalError("Failed to do double checking");
        }
    }

    LOG(INFO) << "Succeed to do double checking";
    LOG(INFO) << "Found " << data_files.size() << " orphan files";

    return data_files;
}

static StatusOr<std::map<std::string, DirEntry>> find_orphan_data_files(FileSystem* fs, std::string_view root_location,
                                                                        int64_t expired_seconds,
                                                                        std::ostream& audit_ostream) {
    ASSIGN_OR_RETURN(auto meta_files_and_bundle_files,
                     list_meta_files(fs, join_path(root_location, kMetadataDirectoryName)));
    const auto& meta_files = std::move(meta_files_and_bundle_files.first);
    const auto& bundle_meta_files = std::move(meta_files_and_bundle_files.second);
    return find_orphan_data_files(fs, root_location, expired_seconds, meta_files, bundle_meta_files, &audit_ostream);
}

// root_location is a partition dir in s3
static StatusOr<std::pair<int64_t, int64_t>> partition_datafile_gc(std::string_view root_location,
                                                                   std::string_view audit_file_path,
                                                                   int64_t expired_seconds, bool do_delete) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    std::ofstream audit_ostream(std::string(audit_file_path), std::ofstream::app);

    if (audit_ostream) {
        audit_ostream << "Start to clean partition root location: " << root_location << std::endl;
    }
    LOG(INFO) << "Start to clean partition root location: " << root_location << std::endl;
    ASSIGN_OR_RETURN(auto orphan_data_files,
                     find_orphan_data_files(fs.get(), root_location, expired_seconds, audit_ostream));

    if (audit_ostream) {
        audit_ostream << "Total orphan data files: " << orphan_data_files.size() << std::endl;
    }
    LOG(INFO) << "Total orphan data files: " << orphan_data_files.size();

    std::vector<std::string> files_to_delete;
    std::set<int64_t> transaction_ids;
    int64_t bytes_to_delete = 0;
    int64_t progress = 0;
    const auto segment_root_location = join_path(root_location, kSegmentDirectoryName);
    for (const auto& [name, entry] : orphan_data_files) {
        files_to_delete.push_back(join_path(segment_root_location, name));
        transaction_ids.insert(extract_txn_id_prefix(name).value_or(0));
        bytes_to_delete += entry.size.value_or(0);
        auto time = entry.mtime.value_or(0);
        auto outtime = std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S");
        ++progress;
        if (audit_ostream) {
            audit_ostream << '(' << progress << '/' << orphan_data_files.size() << ") " << name
                          << ", size: " << entry.size.value_or(0) << ", time: " << outtime << std::endl;
        }
        LOG(INFO) << '(' << progress << '/' << orphan_data_files.size() << ") " << name
                  << ", size: " << entry.size.value_or(0) << ", time: " << outtime;
    }

    if (audit_ostream) {
        audit_ostream << "Total orphan data files: " << orphan_data_files.size() << ", total size: " << bytes_to_delete
                      << std::endl;
    }
    LOG(INFO) << "Total orphan data files: " << orphan_data_files.size() << ", total size: " << bytes_to_delete;

    if (audit_ostream) {
        audit_ostream << "Total transaction ids: " << transaction_ids.size() << std::endl;
    }
    LOG(INFO) << "Total transaction ids: " << transaction_ids.size();

    progress = 0;
    for (auto txn_id : transaction_ids) {
        ++progress;
        if (audit_ostream) {
            audit_ostream << '(' << progress << '/' << transaction_ids.size() << ") "
                          << "transaction id: " << txn_id << std::endl;
        }
        LOG(INFO) << '(' << progress << '/' << transaction_ids.size() << ") "
                  << "transaction id: " << txn_id;
    }

    if (audit_ostream) {
        audit_ostream << "Total orphan data files: " << orphan_data_files.size() << ", total size: " << bytes_to_delete
                      << ", total transaction ids: " << transaction_ids.size() << std::endl;
    }
    LOG(INFO) << "Total orphan data files: " << orphan_data_files.size() << ", total size: " << bytes_to_delete
              << ", total transaction ids: " << transaction_ids.size();

    if (!do_delete) {
        return std::pair<int64_t, int64_t>(orphan_data_files.size(), bytes_to_delete);
    }

    if (audit_ostream) {
        audit_ostream << "Start to delete orphan data files: " << orphan_data_files.size()
                      << ", total size: " << bytes_to_delete << ", total transaction ids: " << transaction_ids.size()
                      << std::endl;
    }
    LOG(INFO) << "Start to delete orphan data files: " << orphan_data_files.size()
              << ", total size: " << bytes_to_delete << ", total transaction ids: " << transaction_ids.size();

    RETURN_IF_ERROR(do_delete_files(fs.get(), files_to_delete));

    return std::pair<int64_t, int64_t>(orphan_data_files.size(), bytes_to_delete);
}

static StatusOr<std::pair<int64_t, int64_t>> path_datafile_gc(std::string_view root_location,
                                                              std::string_view audit_file_path, int64_t expired_seconds,
                                                              bool do_delete) {
    Status status;
    std::pair<int64_t, int64_t> total(0, 0);

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    RETURN_IF_ERROR_WITH_WARN(
            ignore_not_found(fs->iterate_dir2(
                    std::string(root_location),
                    [&](DirEntry entry) {
                        if (!entry.is_dir.value_or(false)) {
                            return true;
                        }

                        if (entry.name == kSegmentDirectoryName || entry.name == kMetadataDirectoryName ||
                            entry.name == kTxnLogDirectoryName) {
                            auto pair_or =
                                    partition_datafile_gc(root_location, audit_file_path, expired_seconds, do_delete);
                            if (!pair_or.ok()) {
                                status = pair_or.status();
                                LOG(WARNING) << "Failed to gc: " << root_location << ", status: " << pair_or.status();
                                return false;
                            }
                            total.first += pair_or.value().first;
                            total.second += pair_or.value().second;
                            return false;
                        }

                        auto pair_or = path_datafile_gc(join_path(root_location, entry.name), audit_file_path,
                                                        expired_seconds, do_delete);

                        if (!pair_or.ok()) {
                            status = pair_or.status();
                            LOG(WARNING) << "Failed to gc: " << root_location << ", status: " << pair_or.status();
                            return false;
                        }

                        total.first += pair_or.value().first;
                        total.second += pair_or.value().second;
                        return true;
                    })),
            "Failed to list " + std::string(root_location));

    if (!status.ok()) {
        return status;
    }
    return total;
}

StatusOr<int64_t> datafile_gc(std::string_view root_location, std::string_view audit_file_path, int64_t expired_seconds,
                              bool do_delete) {
    auto pair_or = path_datafile_gc(root_location, audit_file_path, expired_seconds, do_delete);
    if (!pair_or.ok()) {
        LOG(WARNING) << "Failed to gc: " << root_location << ", status: " << pair_or.status();
        return pair_or.status();
    }

    LOG(INFO) << "Finished to gc: " << root_location << ", total orphan data files: " << pair_or.value().first
              << ", total size: " << pair_or.value().second;

    return pair_or.value().first;
}

StatusOr<int64_t> garbage_file_check(std::string_view root_location) {
    return datafile_gc(root_location, "", 0, false);
}

} // namespace starrocks::lake
