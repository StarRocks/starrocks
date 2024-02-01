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

#include "common/config.h"
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
#include "storage/lake/update_manager.h"
#include "storage/protobuf_file.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"
#include "util/raw_container.h"

namespace starrocks::lake {

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
    return MatchPattern(message, config::lake_vacuum_retry_pattern);
}

int64_t calculate_retry_delay(int64_t attempted_retries) {
    int64_t min_delay = config::lake_vacuum_retry_min_delay_ms;
    return min_delay * (1 << attempted_retries);
}

Status delete_files_with_retry(FileSystem* fs, const std::vector<std::string>& paths) {
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

// Batch delete files with specified FileSystem object |fs|
Status do_delete_files(FileSystem* fs, const std::vector<std::string>& paths) {
    if (UNLIKELY(paths.empty())) {
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
    auto st = delete_files_with_retry(fs, paths);
    if (st.ok()) {
        auto t1 = butil::gettimeofday_us();
        g_del_file_latency << (t1 - t0);
        g_deleted_files << paths.size();
        VLOG(5) << "Deleted " << paths.size() << " files cost " << (t1 - t0) << "us";
    } else {
        g_del_fails << 1;
        LOG(WARNING) << "Fail to delete: " << st;
    }
    return st;
}

// AsyncFileDeleter
// A class for asynchronously deleting files in batches.
//
// The AsyncFileDeleter class provides a mechanism to delete files in batches in an asynchronous manner.
// It allows specifying the batch size, which determines the number of files to be deleted in each batch.
class AsyncFileDeleter {
public:
    using DeleteCallback = std::function<void(const std::vector<std::string>&)>;

    explicit AsyncFileDeleter(int64_t batch_size) : _batch_size(batch_size) {}
    explicit AsyncFileDeleter(int64_t batch_size, DeleteCallback cb) : _batch_size(batch_size), _cb(std::move(cb)) {}

    Status delete_file(std::string path) {
        _batch.emplace_back(std::move(path));
        if (_batch.size() < _batch_size) {
            return Status::OK();
        }
        return submit(&_batch);
    }

    Status finish() {
        if (!_batch.empty()) {
            RETURN_IF_ERROR(submit(&_batch));
        }
        return wait();
    }

    int64_t delete_count() const { return _delete_count; }

private:
    // Wait for all submitted deletion tasks to finish and return task execution results.
    Status wait() {
        if (_prev_task_status.valid()) {
            return _prev_task_status.get();
        } else {
            return Status::OK();
        }
    }

    Status submit(std::vector<std::string>* files_to_delete) {
        // Await previous task completion before submitting a new deletion.
        RETURN_IF_ERROR(wait());
        _delete_count += files_to_delete->size();
        if (_cb) {
            _cb(*files_to_delete);
        }
        _prev_task_status = delete_files_callable(std::move(*files_to_delete));
        files_to_delete->clear();
        DCHECK(_prev_task_status.valid());
        return Status::OK();
    }

    int64_t _batch_size;
    int64_t _delete_count = 0;
    std::vector<std::string> _batch;
    std::future<Status> _prev_task_status;
    DeleteCallback _cb;
};

} // namespace

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
                                    AsyncFileDeleter* deleter, int64_t* garbage_data_size) {
    for (const auto& rowset : metadata.compaction_inputs()) {
        for (const auto& segment : rowset.segments()) {
            RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, segment)));
        }
        *garbage_data_size += rowset.data_size();
    }
    for (const auto& file : metadata.orphan_files()) {
        RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, file.name())));
        *garbage_data_size += file.size();
    }
    return Status::OK();
}

static Status collect_files_to_vacuum(TabletManager* tablet_mgr, std::string_view root_dir, int64_t tablet_id,
                                      int64_t grace_timestamp, int64_t min_retain_version,
                                      AsyncFileDeleter* datafile_deleter, AsyncFileDeleter* metafile_deleter,
                                      int64_t* total_datafile_size) {
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
                RETURN_IF_ERROR(collect_garbage_files(*metadata, data_dir, datafile_deleter, total_datafile_size));
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
                    RETURN_IF_ERROR(collect_garbage_files(*metadata, data_dir, datafile_deleter, total_datafile_size));
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
        RETURN_IF_ERROR(metafile_deleter->delete_file(join_path(meta_dir, tablet_metadata_filename(tablet_id, v))));
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

    auto metafile_delete_cb = [=](const std::vector<std::string>& files) {
        erase_tablet_metadata_from_metacache(tablet_mgr, files);
    };

    for (auto tablet_id : tablet_ids) {
        AsyncFileDeleter datafile_deleter(config::lake_vacuum_min_batch_delete_size);
        AsyncFileDeleter metafile_deleter(INT64_MAX, metafile_delete_cb);
        RETURN_IF_ERROR(collect_files_to_vacuum(tablet_mgr, root_dir, tablet_id, grace_timestamp, min_retain_version,
                                                &datafile_deleter, &metafile_deleter, vacuumed_file_size));
        RETURN_IF_ERROR(datafile_deleter.finish());
        RETURN_IF_ERROR(metafile_deleter.finish());
        (*vacuumed_files) += datafile_deleter.delete_count();
        (*vacuumed_files) += metafile_deleter.delete_count();
    }
    return Status::OK();
}

static Status vacuum_txn_log(std::string_view root_location, int64_t min_active_txn_id, int64_t* vacuumed_files,
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
        } else {
            return true;
        }

        *vacuumed_files += 1;
        *vacuumed_file_size += entry.size.value_or(0);

        ret.update(deleter.delete_file(join_path(log_dir, entry.name)));
        return ret.ok(); // Stop list if delete failed
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

    std::unordered_map<int64_t, std::set<int64_t>> tablet_versions;
    //                 ^^^^^^^ tablet id
    //                                  ^^^^^^^^^ version number

    auto meta_dir = join_path(root_dir, kMetadataDirectoryName);
    auto data_dir = join_path(root_dir, kSegmentDirectoryName);
    auto log_dir = join_path(root_dir, kTxnLogDirectoryName);

    std::set<std::string> txn_logs;
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
        } else {
            return true;
        }

        auto [_, inserted] = txn_logs.emplace(name);
        LOG_IF(FATAL, !inserted) << kDuplicateFilesError << " duplicate file:" << join_path(log_dir, name);

        return true;
    })));

    AsyncFileDeleter deleter(config::lake_vacuum_min_batch_delete_size);
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
                    RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, segment)));
                }
                for (const auto& f : op.dels()) {
                    RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, f)));
                }
            }
            if (log->has_op_compaction()) {
                const auto& op = log->op_compaction();
                for (const auto& segment : op.output_rowset().segments()) {
                    RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, segment)));
                }
            }
            if (log->has_op_schema_change()) {
                const auto& op = log->op_schema_change();
                for (const auto& rowset : op.rowsets()) {
                    for (const auto& segment : rowset.segments()) {
                        RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, segment)));
                    }
                }
            }
            RETURN_IF_ERROR(deleter.delete_file(join_path(log_dir, log_name)));
        }
    }

    RETURN_IF_ERROR(ignore_not_found(fs->iterate_dir(meta_dir, [&](std::string_view name) {
        if (!is_tablet_metadata(name)) {
            return true;
        }
        auto [tablet_id, version] = parse_tablet_metadata_filename(name);
        if (!std::binary_search(tablet_ids.begin(), tablet_ids.end(), tablet_id)) {
            return true;
        }
        auto [_, inserted] = tablet_versions[tablet_id].insert(version);
        LOG_IF(FATAL, !inserted) << kDuplicateFilesError << " duplicate file: " << join_path(meta_dir, name);
        return true;
    })));

    for (auto& [tablet_id, versions] : tablet_versions) {
        DCHECK(!versions.empty());

        TabletMetadataPtr latest_metadata = nullptr;

        // Find metadata files that has garbage data files and delete all those files
        for (int64_t garbage_version = *versions.rbegin(), min_v = *versions.begin(); garbage_version >= min_v; /**/) {
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
                RETURN_IF_ERROR(collect_garbage_files(*metadata, data_dir, &deleter, &dummy_file_size));
                if (metadata->has_prev_garbage_version()) {
                    garbage_version = metadata->prev_garbage_version();
                } else {
                    break;
                }
            }
        }

        if (latest_metadata != nullptr) {
            for (const auto& rowset : latest_metadata->rowsets()) {
                for (const auto& segment : rowset.segments()) {
                    RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, segment)));
                }
            }
            if (latest_metadata->has_delvec_meta()) {
                for (const auto& [v, f] : latest_metadata->delvec_meta().version_to_file()) {
                    RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, f.name())));
                }
            }
        }

        for (auto version : versions) {
            auto path = join_path(meta_dir, tablet_metadata_filename(tablet_id, version));
            RETURN_IF_ERROR(deleter.delete_file(std::move(path)));
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
    files_to_delete.reserve(request.tablet_ids_size() * request.txn_ids_size());

    for (auto tablet_id : request.tablet_ids()) {
        for (auto txn_id : request.txn_ids()) {
            auto log_path = tablet_mgr->txn_log_location(tablet_id, txn_id);
            files_to_delete.emplace_back(log_path);

            tablet_mgr->metacache()->erase(log_path);
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

static StatusOr<std::list<std::string>> list_meta_files(FileSystem* fs, const std::string& metadata_root_location) {
    LOG(INFO) << "Start to list " << metadata_root_location;
    std::list<std::string> meta_files;
    RETURN_IF_ERROR_WITH_WARN(ignore_not_found(fs->iterate_dir(metadata_root_location,
                                                               [&](std::string_view name) {
                                                                   if (is_tablet_metadata(name)) {
                                                                       return true;
                                                                   }
                                                                   meta_files.emplace_back(name);
                                                                   return true;
                                                               })),
                              "Failed to list " + metadata_root_location);
    LOG(INFO) << "Found " << meta_files.size() << " meta files";
    return meta_files;
}

static StatusOr<std::map<std::string, DirEntry>> list_data_files(FileSystem* fs,
                                                                 const std::string& segment_root_location,
                                                                 int64_t expired_seconds) {
    LOG(INFO) << "Start to list " << segment_root_location;
    std::map<std::string, DirEntry> data_files;
    int64_t total_files = 0;
    int64_t total_bytes = 0;
    const auto now = std::time(nullptr);
    RETURN_IF_ERROR_WITH_WARN(ignore_not_found(fs->iterate_dir2(segment_root_location,
                                                                [&](DirEntry entry) {
                                                                    total_files++;
                                                                    total_bytes += entry.size.value_or(0);

                                                                    if (!is_segment(entry.name)) { // Only segment files
                                                                        return true;
                                                                    }
                                                                    if (!entry.mtime.has_value()) {
                                                                        LOG(WARNING) << "Fail to get modified time of "
                                                                                     << entry.name;
                                                                        return true;
                                                                    }

                                                                    if (now >= entry.mtime.value() + expired_seconds) {
                                                                        data_files.emplace(entry.name, entry);
                                                                    }
                                                                    return true;
                                                                })),
                              "Failed to list " + segment_root_location);
    LOG(INFO) << "Listed all data files, total files: " << total_files << ", total bytes: " << total_bytes
              << ", candidate files: " << data_files.size();
    return data_files;
}

static StatusOr<std::map<std::string, DirEntry>> find_orphan_data_files(FileSystem* fs, std::string_view root_location,
                                                                        int64_t expired_seconds,
                                                                        std::ostream& audit_ostream) {
    const auto metadata_root_location = join_path(root_location, kMetadataDirectoryName);
    const auto segment_root_location = join_path(root_location, kSegmentDirectoryName);

    ASSIGN_OR_RETURN(auto data_files, list_data_files(fs, segment_root_location, expired_seconds));

    if (data_files.empty()) {
        return data_files;
    }

    ASSIGN_OR_RETURN(auto meta_files, list_meta_files(fs, metadata_root_location));

    std::set<std::string> data_files_in_metadatas;
    auto check_rowset = [&](const RowsetMetadata& rowset) {
        for (const auto& segment : rowset.segments()) {
            data_files.erase(segment);
            data_files_in_metadatas.emplace(segment);
        }
    };

    audit_ostream << "Total meta files: " << meta_files.size() << std::endl;
    LOG(INFO) << "Start to filter with metadatas, count: " << meta_files.size();

    int64_t progress = 0;
    for (const auto& name : meta_files) {
        auto location = join_path(metadata_root_location, name);
        auto res = get_tablet_metadata(location, false);
        if (res.status().is_not_found()) { // This metadata file was deleted by another node
            LOG(INFO) << location << " is deleted by other node";
            continue;
        } else if (!res.ok()) {
            LOG(WARNING) << "Failed to get meta file: " << location << ", status: " << res.status();
            continue;
        }
        const auto& metadata = res.value();
        for (const auto& rowset : metadata->rowsets()) {
            check_rowset(rowset);
        }
        ++progress;
        audit_ostream << '(' << progress << '/' << meta_files.size() << ") " << name << '\n'
                      << proto_to_json(*metadata) << std::endl;
        LOG(INFO) << "Filtered with meta file: " << name << " (" << progress << '/' << meta_files.size() << ')';
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

// root_location is a partition dir in s3
Status datafile_gc(std::string_view root_location, std::string_view audit_file_path, int64_t expired_seconds,
                   bool do_delete) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    std::ofstream audit_ostream(std::string(audit_file_path), std::ofstream::app);
    if (!audit_ostream) {
        LOG(WARNING) << "Cannot open " << audit_file_path;
        return Status::InternalError("Cannot open audit file");
    }

    audit_ostream << "Audit root location: " << root_location << std::endl;
    ASSIGN_OR_RETURN(auto orphan_data_files,
                     find_orphan_data_files(fs.get(), root_location, expired_seconds, audit_ostream));

    audit_ostream << "Total orphan data files: " << orphan_data_files.size() << std::endl;
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
        audit_ostream << '(' << progress << '/' << orphan_data_files.size() << ") " << name
                      << ", size: " << entry.size.value_or(0) << ", time: " << outtime << std::endl;
        LOG(INFO) << '(' << progress << '/' << orphan_data_files.size() << ") " << name
                  << ", size: " << entry.size.value_or(0) << ", time: " << outtime;
    }

    audit_ostream << "Total orphan data files: " << orphan_data_files.size() << ", total size: " << bytes_to_delete
                  << std::endl;
    LOG(INFO) << "Total orphan data files: " << orphan_data_files.size() << ", total size: " << bytes_to_delete;

    audit_ostream << "Total transaction ids: " << transaction_ids.size() << std::endl;
    LOG(INFO) << "Total transaction ids: " << transaction_ids.size();

    progress = 0;
    for (auto txn_id : transaction_ids) {
        ++progress;
        audit_ostream << '(' << progress << '/' << transaction_ids.size() << ") "
                      << "transaction id: " << txn_id << std::endl;
        LOG(INFO) << '(' << progress << '/' << transaction_ids.size() << ") "
                  << "transaction id: " << txn_id;
    }

    audit_ostream << "Total orphan data files: " << orphan_data_files.size() << ", total size: " << bytes_to_delete
                  << ", total transaction ids: " << transaction_ids.size() << std::endl;
    LOG(INFO) << "Total orphan data files: " << orphan_data_files.size() << ", total size: " << bytes_to_delete
              << ", total transaction ids: " << transaction_ids.size();

    if (!do_delete) {
        audit_ostream.close();
        return Status::OK();
    }

    audit_ostream << "Start to delete orphan data files: " << orphan_data_files.size()
                  << ", total size: " << bytes_to_delete << ", total transaction ids: " << transaction_ids.size()
                  << std::endl;
    LOG(INFO) << "Start to delete orphan data files: " << orphan_data_files.size()
              << ", total size: " << bytes_to_delete << ", total transaction ids: " << transaction_ids.size();

    audit_ostream.close();

    return do_delete_files(fs.get(), files_to_delete);
}

} // namespace starrocks::lake
