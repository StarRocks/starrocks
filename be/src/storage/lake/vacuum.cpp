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

#include <butil/fast_rand.h>
#include <butil/time.h>
#include <bvar/bvar.h>
#include <fmt/format.h>

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

// Recent (60s) mean of files-per-batch across DeleteObjects calls. The IntRecorder is wrapped
// in a Window so the exposed value tracks current batch size after
// lake_vacuum_min_batch_delete_size tuning instead of the lifetime average dominated by
// historical samples. Recording happens once per logical batch (before retries) so that retried
// batches do not contribute duplicate samples that would bias the mean during throttling -- the
// very condition operators are trying to read.
static bvar::IntRecorder g_del_file_batch_size;
static bvar::Window<bvar::IntRecorder> g_del_file_batch_size_minute("lake_vacuum", "del_file_batch_size_minute",
                                                                    &g_del_file_batch_size, 60);

// Number of delete retries triggered in the last 60s. Surfaces transient throttling
// pressure (S3 RequestRate / try-again responses) and validates that jitter / backoff
// tuning actually reduces retry frequency.
static bvar::Adder<uint64_t> g_del_file_retries;
static bvar::Window<bvar::Adder<uint64_t>> g_del_file_retries_minute("lake_vacuum", "del_file_retries_minute",
                                                                     &g_del_file_retries, 60);

// Decorrelated jitter (AWS Architecture Blog "Exponential Backoff And Jitter"):
//   next_delay = min(cap, rand([base, last_delay * 3]))
// where cap = base * 2^max_retries.
//
// Pure helper: all knobs (base, max_retries) are passed in by the caller so the function has no
// hidden dependencies on global config and is trivial to unit-test. Caller passes
// last_delay (= base on the first attempt) and feeds the returned value back on the next call.
//
// Compared to deterministic backoff, retry timestamps of independent CNs grow decorrelated over
// successive attempts even when they all start throttled at the same moment, keeping the
// per-prefix request rate below the remote storage's limit. The returned value is always
// in [base, cap], so the caller-configured minimum delay is preserved as a floor.
int64_t calculate_retry_delay(int64_t last_delay, int64_t base, int64_t max_retries) {
    int64_t cap = base * (1L << max_retries);
    int64_t upper = std::min(cap, last_delay * 3);
    if (upper <= base) {
        return base;
    }
    int64_t range = upper - base + 1;
    return base + static_cast<int64_t>(butil::fast_rand_less_than(range));
}

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

// Returns Status::TimedOut once |deadline_ms| (milliseconds since the Epoch) has passed.
// deadline_ms <= 0 means no deadline. The deadline is anchored at the time the BE received
// the vacuum request, so it also expires for tasks that waited too long in the thread pool
// queue: by then the FE caller has given up waiting and would re-dispatch the partition,
// continuing would only keep a vacuum worker occupied for a response nobody reads.
Status check_vacuum_deadline(int64_t deadline_ms) {
    if (deadline_ms <= 0) {
        return Status::OK();
    }
    int64_t now_ms = butil::gettimeofday_ms();
    TEST_SYNC_POINT_CALLBACK("vacuum:check_deadline", &now_ms);
    if (now_ms >= deadline_ms) {
        return Status::TimedOut(fmt::format("vacuum task deadline exceeded, now={}, deadline={}", now_ms, deadline_ms));
    }
    return Status::OK();
}

Status delete_files_with_retry(FileSystem* fs, std::span<const std::string> paths) {
    const int64_t base = config::lake_vacuum_retry_min_delay_ms;
    const int64_t max_retries = config::lake_vacuum_retry_max_attempts;
    int64_t last_delay = base;
    for (int64_t attempted_retries = 0; /**/; attempted_retries++) {
        auto st = fs->delete_files(paths);
        if (!st.ok() && should_retry(st, attempted_retries)) {
            g_del_file_retries << 1;
            last_delay = calculate_retry_delay(last_delay, base, max_retries);
            LOG(WARNING) << "Fail to delete: " << st << " will retry after " << last_delay << "ms";
            std::this_thread::sleep_for(std::chrono::milliseconds(last_delay));
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

    auto delete_single_batch = [fs](std::span<const std::string> batch) -> Status {
        g_del_file_batch_size << batch.size();
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
            try {
                return _prev_task_status.get();
            } catch (const std::exception& e) {
                return Status::InternalError(e.what());
            }
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
        if (rowset.bundle_file_offsets_size() == 0) {
            // skip delete rowset with bundle file offsets
            for (const auto& segment : rowset.segments()) {
                RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, segment)));
            }
        }
        for (const auto& del_file : rowset.del_files()) {
            RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, del_file.name())));
        }
        *garbage_data_size += rowset.data_size();
    }
    for (const auto& file : metadata.orphan_files()) {
        RETURN_IF_ERROR(deleter->delete_file(join_path(base_dir, file.name())));
        *garbage_data_size += file.size();
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
                                      AsyncFileDeleter* datafile_deleter, AsyncFileDeleter* metafile_deleter,
<<<<<<< HEAD
                                      int64_t* total_datafile_size, int64_t* vacuumed_version,
                                      int64_t* extra_datafile_size) {
=======
                                      AsyncSharedFileDeleter* shared_file_deleter, int64_t* total_datafile_size,
                                      int64_t* vacuumed_version, int64_t* extra_datafile_size,
                                      const TabletRetainInfo& retain_info, int64_t deadline_ms) {
>>>>>>> 4e25291a18 ([BugFix] Abort BE vacuum tasks once the FE caller's timeout elapses (#74694))
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
<<<<<<< HEAD
        auto res = tablet_mgr->get_tablet_metadata(tablet_id, version, false);
=======
        if (auto st = check_vacuum_deadline(deadline_ms); !st.ok()) {
            return Status::TimedOut(fmt::format("{} tablet_id={}", st.message(), tablet_id));
        }
        // fill data cache to avoid read bundle meta file from remote storage repeatedly.
        auto res = tablet_mgr->get_tablet_metadata(
                tablet_id, version, false /* Not need to fill meta cache */,
                vacuum_version_range != nullptr /* fill data cache when enable file bundle */);
>>>>>>> 4e25291a18 ([BugFix] Abort BE vacuum tasks once the FE caller's timeout elapses (#74694))
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
                RETURN_IF_ERROR(
                        collect_garbage_files(*metadata, data_dir, datafile_deleter, &prepare_vacuum_file_size));
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
        // The final_retain_version is set to min_retain_version or minmum exist version which has garbage files.
        // So we set vacuumed_version to `final_retain_version - 1` to avoid the garbage files of final_retain_version can
        // not be deleted
        *vacuumed_version = final_retain_version - 1;
        return Status::OK();
    }
    *vacuumed_version = final_retain_version;
    DCHECK_LE(version, final_retain_version);
    for (auto v = version + 1; v < final_retain_version; v++) {
        RETURN_IF_ERROR(metafile_deleter->delete_file(join_path(meta_dir, tablet_metadata_filename(tablet_id, v))));
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
<<<<<<< HEAD
                                     int64_t grace_timestamp, int64_t* vacuumed_files, int64_t* vacuumed_file_size,
                                     int64_t* vacuumed_version, int64_t* extra_file_size) {
=======
                                     int64_t grace_timestamp, bool enable_file_bundling,
                                     bool enable_shared_file_cleanup, int64_t* vacuumed_files,
                                     int64_t* vacuumed_file_size, int64_t* vacuumed_version, int64_t* extra_file_size,
                                     const std::unordered_set<int64_t>& retain_versions, int64_t deadline_ms) {
>>>>>>> 4e25291a18 ([BugFix] Abort BE vacuum tasks once the FE caller's timeout elapses (#74694))
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
    int64_t final_vacuum_version = std::numeric_limits<int64_t>::max();
    for (auto& tablet_info : tablet_infos) {
        int64_t tablet_vacuumed_version = 0;
        AsyncFileDeleter datafile_deleter(config::lake_vacuum_min_batch_delete_size);
        AsyncFileDeleter metafile_deleter(INT64_MAX, metafile_delete_cb);
        RETURN_IF_ERROR(collect_files_to_vacuum(tablet_mgr, root_dir, tablet_info, grace_timestamp, min_retain_version,
<<<<<<< HEAD
                                                &datafile_deleter, &metafile_deleter, vacuumed_file_size,
                                                &tablet_vacuumed_version, extra_file_size));
=======
                                                vacuum_version_range.get(), &datafile_deleter, &metafile_deleter,
                                                &shared_file_deleter, vacuumed_file_size, &tablet_vacuumed_version,
                                                extra_file_size, tablet_retain_info, deadline_ms));
>>>>>>> 4e25291a18 ([BugFix] Abort BE vacuum tasks once the FE caller's timeout elapses (#74694))
        RETURN_IF_ERROR(datafile_deleter.finish());
        RETURN_IF_ERROR(metafile_deleter.finish());
        if (final_vacuum_version > tablet_vacuumed_version) {
            // set partition vacuumed_version to min tablet vacuumed version
            final_vacuum_version = tablet_vacuumed_version;
        }
        (*vacuumed_files) += datafile_deleter.delete_count();
        (*vacuumed_files) += metafile_deleter.delete_count();
    }
    *vacuumed_version = final_vacuum_version;
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

<<<<<<< HEAD
Status vacuum_impl(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response) {
=======
// Reclaim load_spill subtrees that no longer correspond to any active load.
Status vacuum_load_spill(std::string_view root_location, int64_t min_active_txn_id, bool cleanup_legacy_load_spill,
                         int64_t* deleted_files) {
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(root_location));
    auto t0 = butil::gettimeofday_s();
    auto ret = Status::OK();
    int64_t local_deleted = 0;

    // Object storage typically surfaces CommonPrefixes with a trailing '/'. Normalize
    // so the name-matching logic below works on both POSIX-style and S3-style listings.
    auto normalize_entry_name = [](DirEntry entry) -> std::string_view {
        std::string_view name = entry.name;
        if (!name.empty() && name.back() == '/') {
            name.remove_suffix(1);
        }
        return name;
    };

    // ---- (1) Active flat layout: <root>/load_spill_txns/<txn_id_hex>_..._<seq> ----
    // One paginated list over the flat dir; reclaim by parsing the leading hex segment.
    auto load_spill_txns_dir = join_path(root_location, kLoadSpillTxnsDirectoryName);

    // Parse the leading hex segment from "<hex>_...". Returns nullopt unless the name
    // starts with "<1..16 hex digits>_" decoding to a positive int64.
    auto parse_hex_txn_id_prefix = [](std::string_view name) -> std::optional<int64_t> {
        auto sep = name.find('_');
        if (sep == std::string_view::npos || sep == 0 || sep > 16) return std::nullopt;
        StringParser::ParseResult res = StringParser::PARSE_FAILURE;
        int64_t txn_id = StringParser::string_to_int<int64_t>(name.data(), sep, 16, &res);
        if (res != StringParser::PARSE_SUCCESS || txn_id <= 0) return std::nullopt;
        return txn_id;
    };

    std::vector<std::string> to_delete;

    auto txns_iter_st = ignore_not_found(fs->iterate_dir2(load_spill_txns_dir, [&](DirEntry entry) {
        std::string_view name = normalize_entry_name(entry);
        if (name.empty()) return true;

        // Defensive: residual sub-directory from any abandoned nested layout. Warn but do
        // not auto-delete to avoid wiping active data on a misconfigured deployment.
        if (entry.is_dir.has_value() && entry.is_dir.value()) {
            LOG_EVERY_N(WARNING, 100) << "Unexpected sub-directory under flat load_spill_txns: "
                                      << join_path(load_spill_txns_dir, std::string(name));
            return true;
        }

        auto parsed = parse_hex_txn_id_prefix(name);
        if (!parsed.has_value()) {
            LOG_EVERY_N(WARNING, 100) << "Skip unrecognized file under " << load_spill_txns_dir << ": " << name;
            return true;
        }
        if (*parsed >= min_active_txn_id) {
            return true; // still potentially in use
        }

        to_delete.emplace_back(join_path(load_spill_txns_dir, std::string(name)));
        return true;
    }));
    ret.update(txns_iter_st);

    if (!to_delete.empty()) {
        local_deleted += to_delete.size();
        delete_files_async(std::move(to_delete));
    }

    // ---- (2) Legacy layout: <root>/load_spill/<load_id>/ ----
    //
    // Safe to wipe in one shot when the caller opts in:
    //   - Post-upgrade Lake writers all use the flat layout above, so no in-flight writer
    //     produces new entries here.
    //   - Non-Lake callers (connector / SpillPartitionChunkWriter) write under a different
    //     LocationProvider root, never reachable via |root_location| of this function.
    auto legacy_dir = join_path(root_location, kLoadSpillDirectoryName);
    if (!cleanup_legacy_load_spill) {
        LOG_EVERY_N(INFO, 1000) << "Skip legacy load_spill tree (caller did not opt in): " << legacy_dir;
    } else {
        // Probe first: |delete_dir_recursive| swallows NotFound on some FS impls (e.g.
        // PosixFileSystem returns OK), so we cannot distinguish "really deleted a tree"
        // from "tree never existed" by looking at its return status alone. Avoid the
        // false +1 by skipping the call entirely when the legacy root is absent.
        auto exists_st = fs->path_exists(legacy_dir);
        if (exists_st.is_not_found()) {
            // Legacy tree never materialized on this root — common path, not a deletion.
        } else if (!exists_st.ok()) {
            LOG(WARNING) << "Fail to stat legacy load_spill tree " << legacy_dir << ": " << exists_st;
            ret.update(exists_st);
        } else {
            auto legacy_st = fs->delete_dir_recursive(legacy_dir);
            if (!legacy_st.ok()) {
                LOG(WARNING) << "Fail to delete legacy load_spill tree " << legacy_dir << ": " << legacy_st;
                ret.update(legacy_st);
            } else {
                // Recursive delete reclaims the whole subtree in one FS call; account it
                // as a single logical reclamation unit (per-file count is not surfaced).
                ++local_deleted;
            }
        }
    }

    auto t1 = butil::gettimeofday_s();
    g_vacuum_load_spill_latency << (t1 - t0);
    g_vacuum_load_spill_deleted_files << local_deleted;
    if (deleted_files != nullptr) {
        *deleted_files += local_deleted;
    }

    return ret;
}

Status vacuum_impl(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response,
                   int64_t deadline_ms) {
>>>>>>> 4e25291a18 ([BugFix] Abort BE vacuum tasks once the FE caller's timeout elapses (#74694))
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
    // The task may have stayed in the thread pool queue long enough that the FE caller already
    // timed out and gave up, or so long that only a sliver of the deadline window remains. Walking
    // the whole version chain only to abort mid-way would waste a worker and FS list QPS without
    // advancing any metadata, so refuse to start unless a minimum useful window is still left. The
    // window is min(5min, 1/10 of the FE timeout): 5min is roughly enough to make progress on a
    // round, while the 1/10 cap keeps it below the timeout so a freshly dispatched (full-window)
    // task is never rejected even when the timeout is configured very small. Bringing the effective
    // deadline that much earlier expresses exactly this, and a task whose deadline already passed
    // while queued is caught by the same check.
    static constexpr int64_t kMaxStartWindowMs = 5 * 60 * 1000;
    int64_t start_deadline_ms = deadline_ms;
    if (deadline_ms > 0 && request.has_timeout_ms() && request.timeout_ms() > 0) {
        start_deadline_ms -= std::min<int64_t>(kMaxStartWindowMs, request.timeout_ms() / 10);
    }
    RETURN_IF_ERROR(check_vacuum_deadline(start_deadline_ms));

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

    int64_t vacuumed_files = 0;
    int64_t vacuumed_file_size = 0;
    int64_t vacuumed_version = 0;
    int64_t extra_file_size = 0;

    std::sort(tablet_infos.begin(), tablet_infos.end(),
              [](const auto& a, const auto& b) { return a.tablet_id() < b.tablet_id(); });

    RETURN_IF_ERROR(vacuum_tablet_metadata(tablet_mgr, root_loc, tablet_infos, min_retain_version, grace_timestamp,
<<<<<<< HEAD
                                           &vacuumed_files, &vacuumed_file_size, &vacuumed_version, &extra_file_size));
=======
                                           enable_file_bundling, enable_shared_file_cleanup, &vacuumed_files,
                                           &vacuumed_file_size, &vacuumed_version, &extra_file_size, retain_versions,
                                           deadline_ms));
>>>>>>> 4e25291a18 ([BugFix] Abort BE vacuum tasks once the FE caller's timeout elapses (#74694))
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

void vacuum(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response, int64_t deadline_ms) {
    auto st = vacuum_impl(tablet_mgr, request, response, deadline_ms);
    LOG_IF(ERROR, !st.ok()) << "Fail to vacuum partition " << request.partition_id() << ": " << st;
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
                if (op.rowset().bundle_file_offsets_size() == 0) {
                    for (const auto& segment : op.rowset().segments()) {
                        RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, segment)));
                    }
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
                if (rowset.bundle_file_offsets_size() == 0) {
                    // skip delete segments in rowset with bundle file offsets
                    for (const auto& segment : rowset.segments()) {
                        RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, segment)));
                    }
                }
            }
            if (latest_metadata->has_delvec_meta()) {
                for (const auto& [v, f] : latest_metadata->delvec_meta().version_to_file()) {
                    RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, f.name())));
                }
            }
            if (latest_metadata->sstable_meta().sstables_size() > 0) {
                for (const auto& sst : latest_metadata->sstable_meta().sstables()) {
                    RETURN_IF_ERROR(deleter.delete_file(join_path(data_dir, sst.filename())));
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

static StatusOr<std::list<std::string>> list_meta_files(FileSystem* fs, const std::string& metadata_root_location) {
    LOG(INFO) << "Start to list " << metadata_root_location;
    std::list<std::string> meta_files;
    RETURN_IF_ERROR_WITH_WARN(ignore_not_found(fs->iterate_dir(metadata_root_location,
                                                               [&](std::string_view name) {
                                                                   if (!is_tablet_metadata(name)) {
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
    RETURN_IF_ERROR_WITH_WARN(
            ignore_not_found(fs->iterate_dir2(segment_root_location,
                                              [&](DirEntry entry) {
                                                  total_files++;
                                                  total_bytes += entry.size.value_or(0);

                                                  if (!is_segment(entry.name) &&
                                                      !is_sst(entry.name)) { // Only segment files and sst
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
    auto check_sst_meta = [&](const PersistentIndexSstableMetaPB& sst_meta) {
        for (const auto& sst : sst_meta.sstables()) {
            data_files.erase(sst.filename());
            data_files_in_metadatas.emplace(sst.filename());
        }
    };

    if (audit_ostream) {
        audit_ostream << "Total meta files: " << meta_files.size() << std::endl;
    }
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
        check_sst_meta(metadata->sstable_meta());
        ++progress;
        if (audit_ostream) {
            audit_ostream << '(' << progress << '/' << meta_files.size() << ") " << name << '\n'
                          << proto_to_json(*metadata) << std::endl;
        }
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
        std::time_t time = static_cast<std::time_t>(entry.mtime.value_or(0));
        std::tm tm_buf{};
        localtime_r(&time, &tm_buf);
        auto outtime = std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
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

Status datafile_gc(std::string_view root_location, std::string_view audit_file_path, int64_t expired_seconds,
                   bool do_delete) {
    auto pair_or = path_datafile_gc(root_location, audit_file_path, expired_seconds, do_delete);
    if (!pair_or.ok()) {
        LOG(WARNING) << "Failed to gc: " << root_location << ", status: " << pair_or.status();
        return pair_or.status();
    }

    LOG(INFO) << "Finished to gc: " << root_location << ", total orphan data files: " << pair_or.value().first
              << ", total size: " << pair_or.value().second;

    return Status::OK();
}

} // namespace starrocks::lake
