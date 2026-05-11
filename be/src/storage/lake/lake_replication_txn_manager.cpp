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

#include "storage/lake/lake_replication_txn_manager.h"

#include <atomic>
#include <mutex>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config_lake_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/system/master_info.h"
#include "common/thread/threadpool.h"
#include "fs/fs_factory.h"
#include "fs/fs_starlet.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gen_cpp/lake_types.pb.h"
#include "persistent_index_sstable.h"
#include "replication_txn_manager.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/segment_stream_converter.h"
#include "util/dynamic_cache.h"
#include "vacuum.h"

namespace starrocks::lake {
namespace {
// Backlog guard for shared REPLICATE_SNAPSHOT thread pool.
// Parallel copy is disabled when queue depth exceeds num_threads * this factor
// to avoid adding more pressure to an already saturated pool.
constexpr int kParallelCopyMaxQueuePerThread = 8;
} // namespace

#ifdef USE_STAROS
std::string convert_s3_path_to_starlet_uri(std::string_view s3_path, int64_t shard_id) {
    // S3 URI format: s3://bucket/path...
    // Starlet URI format: staros://shard_id/path...
    // We need to replace "s3://bucket/" with "staros://shard_id/"
    std::string_view path = s3_path;
    if (path.find("s3://") == 0) {
        // Remove "s3://" prefix
        path.remove_prefix(5);
        // Find the first "/" after bucket name and skip the bucket part
        size_t first_slash_pos = path.find('/');
        if (first_slash_pos != std::string_view::npos) {
            path.remove_prefix(first_slash_pos + 1);
        } else {
            path = std::string_view();
        }
        // Build starlet URI: staros://shard_id/path...
        return build_starlet_uri(shard_id, path);
    }

    // Not a valid S3 path - log warning
    LOG(WARNING) << "S3 path does not start with 's3://': " << s3_path;
    return build_starlet_uri(shard_id, path);
}
#endif // USE_STAROS

std::string remove_last_path_component(const std::string& path) {
    // Find the last "/" which separates the directory name (meta or data)
    size_t last_slash = path.find_last_of('/');
    if (last_slash == std::string::npos) {
        return path;
    }

    // Get the directory name (meta or data)
    std::string dir_name = path.substr(last_slash + 1);

    // Get the path before the directory name
    std::string base_path = path.substr(0, last_slash);

    // Find the second-to-last "/"
    size_t second_last_slash = base_path.find_last_of('/');
    if (second_last_slash == std::string::npos) {
        return path;
    }

    // Remove the component between second_last_slash and last_slash
    return base_path.substr(0, second_last_slash + 1) + dir_name;
}

std::string remove_db_id_component(const std::string& path, int64_t db_id) {
    std::string db_pattern = "/db" + std::to_string(db_id) + "/";
    size_t pos = path.find(db_pattern);
    if (pos == std::string::npos) {
        return path;
    }
    // Remove "db{db_id}/" but keep the "/" before it
    return path.substr(0, pos + 1) + path.substr(pos + db_pattern.length());
}

Status LakeReplicationTxnManager::replicate_lake_remote_storage(const TReplicateSnapshotRequest& request,
                                                                ThreadPool* replicate_file_thread_pool) {
    auto src_tablet_id = request.src_tablet_id;
    auto src_visible_version = request.src_visible_version;
    auto src_db_id = request.src_db_id;
    auto src_table_id = request.src_table_id;
    auto src_partition_id = request.src_partition_id;

    auto data_version = request.data_version;
    auto target_visible_version = request.visible_version;
    auto target_tablet_id = request.tablet_id;

    auto txn_id = request.transaction_id;
    auto virtual_tablet_id = request.virtual_tablet_id;

    // Check if FE provides full path for S3 storage type
    // - has_full_path=true: S3 storage type, FE provides full S3 path (supports partitioned prefix)
    // - has_full_path=false: Non-S3 storage type (OSS/Azure/HDFS/GFS), use RemoteStarletLocationProvider
    bool has_full_path = request.__isset.src_partition_full_path && !request.src_partition_full_path.empty();
    std::string src_partition_full_path = has_full_path ? request.src_partition_full_path : "";

    LOG(INFO) << "Start to replicate lake remote storage, txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
              << ", src_tablet_id: " << src_tablet_id << ", src_db_id: " << src_db_id
              << ", src_table_id: " << src_table_id << ", src_partition_id: " << src_partition_id
              << ", visible_version: " << target_visible_version << ", data_version: " << data_version
              << ", virtual_tablet_id: " << virtual_tablet_id << ", src_visible_version: " << src_visible_version
              << ", has_full_path: " << has_full_path
              << (has_full_path ? ", src_partition_full_path: " + src_partition_full_path : "");

    // step 1: validate request and locate source tablet metadata/files.
    std::vector<Version> missed_versions;
    for (auto v = data_version + 1; v <= src_visible_version; ++v) {
        missed_versions.emplace_back(v, v);
    }
    if (UNLIKELY(missed_versions.empty())) {
        LOG(WARNING) << "Replicate lake remote storage skipped, no missing version"
                     << ", txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
                     << ", src_tablet_id: " << src_tablet_id << ", visible_version: " << target_visible_version
                     << ", data_version: " << data_version << ", src_visible_version: " << src_visible_version;
        return Status::Corruption("No missing version");
    }

    std::string src_meta_dir;
    std::string src_data_dir;
    std::shared_ptr<FileSystem> shared_src_fs;
    TabletMetadataPtr src_tablet_meta;

#ifdef USE_STAROS
    if (has_full_path) {
        // S3 storage type: FE provides full S3 path (supports partitioned prefix feature)
        // Use S3 raw path mode - starlet will use the path as-is without normalize_path
        if (src_partition_full_path.find("s3://") != 0) {
            return Status::InvalidArgument(
                    fmt::format("Full path must be S3 type (start with 's3://'), got: {}", src_partition_full_path));
        }
        std::string src_partition_starlet_uri = convert_s3_path_to_starlet_uri(src_partition_full_path, src_tablet_id);

        // Append metadata and segment directory names
        src_meta_dir = join_path(src_partition_starlet_uri, kMetadataDirectoryName);
        src_data_dir = join_path(src_partition_starlet_uri, kSegmentDirectoryName);

        VLOG(3) << "S3 storage: converted S3 full path to starlet URI, original: " << src_partition_full_path
                << ", starlet_uri: " << src_partition_starlet_uri << ", meta_dir: " << src_meta_dir
                << ", data_dir: " << src_data_dir;

        // Create filesystem with S3 raw path mode enabled
        shared_src_fs = new_fs_starlet(virtual_tablet_id, true /* use_raw_path */);
        if (shared_src_fs == nullptr) {
            return Status::Corruption("Failed to create virtual starlet filesystem");
        }

        ASSIGN_OR_RETURN(src_tablet_meta,
                         try_build_source_tablet_meta_with_fallback(src_tablet_id, src_visible_version, src_db_id,
                                                                    txn_id, src_meta_dir, src_data_dir, shared_src_fs));
    } else {
        // Non-S3 storage type (OSS/Azure/HDFS/GFS): use RemoteStarletLocationProvider
        // Use normal mode - starlet will use normalize_path to combine sys.root with relative path
        src_meta_dir = _remote_location_provider->metadata_root_location(src_tablet_id, src_db_id, src_table_id,
                                                                         src_partition_id);
        src_data_dir = _remote_location_provider->segment_root_location(src_tablet_id, src_db_id, src_table_id,
                                                                        src_partition_id);

        LOG(INFO) << "Non-S3 storage: using RemoteStarletLocationProvider, meta_dir: " << src_meta_dir
                  << ", data_dir: " << src_data_dir;

        // Create filesystem with normal mode (no S3 raw path)
        shared_src_fs = new_fs_starlet(virtual_tablet_id, false /* use_raw_path */);
        if (shared_src_fs == nullptr) {
            return Status::Corruption("Failed to create virtual starlet filesystem");
        }
        ASSIGN_OR_RETURN(src_tablet_meta,
                         build_source_tablet_meta(src_tablet_id, src_visible_version, src_meta_dir, shared_src_fs));
    }
#else
    return Status::NotSupported("Lake replication remote storage requires build with shared-data support!");
#endif

    VLOG(3) << "Lake replicate storage task, built source meta and data dir, meta dir: " << src_meta_dir
            << ", data dir: " << src_data_dir << ", txn_id: " << txn_id << ", src_tablet_id: " << src_tablet_id
            << ", tablet_id: " << target_tablet_id;

    // step 2: build target metadata and file mappings.

    // `file_locations` is the mapping between source and target file locations,
    // it contains all files that need to replicate from source to target storage
    std::map<std::string, std::string> file_locations;
    // `filename_map` is another mapping between source and target file name,
    // and it's borrowed from lake::ReplicationTxnManager
    std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>> filename_map;
    // `segment_name_to_size_map` is the mapping between segment file name to its file size
    // we use the `segment_size` field in rowset metadata to get the file size.
    // for history reasons, the `segment_size` field is not always present, so the resulting map is not guaranteed to
    // have all segment file sizes.
    std::unordered_map<std::string, size_t> segment_name_to_size_map;

    auto txn_log = std::make_shared<TxnLog>();

    ASSIGN_OR_RETURN(auto target_tablet, _tablet_manager->get_tablet(target_tablet_id));
    ASSIGN_OR_RETURN(auto target_tablet_meta, target_tablet.get_metadata(target_visible_version));
    // Copy the rowsets, sstables etc. into tablet metadata on target cluster,
    // then replace file names and return `copied_target_tablet_meta` as the final target tablet metadata
    ASSIGN_OR_RETURN(auto copied_target_tablet_meta,
                     convert_and_build_new_tablet_meta(src_tablet_meta, target_tablet_meta, src_tablet_id,
                                                       target_tablet_id, txn_id, data_version, src_data_dir,
                                                       segment_name_to_size_map, file_locations, filename_map));
    // calc column unique id to adapt for fast schema change
    if (!src_tablet_meta->has_schema()) {
        LOG(WARNING) << "Failed to get source schema, source tablet: " << src_tablet_id
                     << ", target tablet: " << target_tablet_id;
        return Status::Corruption("Failed to get source schema");
    }
    const TabletSchemaPB& source_schema_pb = src_tablet_meta->schema();
    std::unordered_map<uint32_t, uint32_t> column_unique_id_map;
    ReplicationUtils::calc_column_unique_id_map(source_schema_pb.column(), target_tablet_meta->schema().column(),
                                                &column_unique_id_map);

    if (column_unique_id_map.size() > 0) {
        LOG(INFO) << "Lake replicate storage task, need rebuild column unique id, txn_id: " << txn_id
                  << ", tablet_id: " << target_tablet_id << ", unique_id_map size: " << column_unique_id_map.size();
    }
    std::vector<std::string> files_to_delete;
    CancelableDefer clean_files([&files_to_delete]() { lake::delete_files_async(std::move(files_to_delete)); });

    auto file_converters = lake::ReplicationTxnManager::build_file_converters(_tablet_manager, request, filename_map,
                                                                              column_unique_id_map, files_to_delete);

    // Track which segments have size changes
    std::unordered_map<std::string, size_t> segment_size_changes;

    // step 3: prepare copy mode and build per-file copy tasks.
    MonotonicStopWatch watch;
    watch.start();
    std::atomic<size_t> total_file_size{0};

    ThreadPool* repl_pool = replicate_file_thread_pool;
    bool use_parallel = should_use_parallel_copy(filename_map.size(), repl_pool);
    std::mutex mu;
    std::mutex* shared_mutex = nullptr;
    FileConverterCreatorFunc active_file_converters = file_converters;
    if (use_parallel) {
        LOG(INFO) << "Start parallel file copy, file_count: " << filename_map.size() << ", txn_id: " << txn_id
                  << ", tablet_id: " << target_tablet_id << ", pool num_threads: " << repl_pool->num_threads()
                  << ", pool active_threads: " << repl_pool->active_threads()
                  << ", pool queued_tasks: " << repl_pool->num_queued_tasks();
        shared_mutex = &mu;
        active_file_converters = [&file_converters, &mu](
                                         const std::string& file_name,
                                         uint64_t file_size) -> StatusOr<std::unique_ptr<FileStreamConverter>> {
            std::lock_guard lock(mu);
            return file_converters(file_name, file_size);
        };
    }

    std::vector<ReplicationTask> tasks;
    tasks.reserve(filename_map.size());
    for (const auto& pair : filename_map) {
        const auto& src_file_name = pair.first;
        auto src_file_location = join_path(src_data_dir, src_file_name);
        auto it = file_locations.find(src_file_location);
        if (it == file_locations.end()) {
            return Status::Corruption("Found invalid file location, src file location: " + src_file_location);
        }
        const auto& target_file_location = it->second;
        size_t src_file_size = 0;
        auto size_it = segment_name_to_size_map.find(src_file_name);
        if (size_it != segment_name_to_size_map.end()) {
            src_file_size = size_it->second;
        }
        bool is_seg = is_segment(src_file_name);
        const auto& target_file_name = pair.second.first;
        FileEncryptionInfo encryption_info;
        if (config::enable_transparent_data_encryption) {
            encryption_info = pair.second.second.info;
        }

        tasks.emplace_back([&, src_file_name, src_file_location, target_file_location, target_file_name, src_file_size,
                            is_seg, encryption_info]() -> Status {
            // Fast cancel: check right before each file copy starts.
            if (txn_id < get_master_info().min_active_txn_id) {
                LOG(WARNING) << "Lake replication task cancelled before file copy, transaction is aborted"
                             << ", txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
                             << ", min_active_txn_id: " << get_master_info().min_active_txn_id
                             << ", src_file: " << src_file_name;
                return Status::Aborted("Lake replication cancelled, transaction is aborted");
            }
            TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::replicate_lake_remote_storage::before_copy", nullptr);

            LOG(INFO) << "Start replicate src file: " << src_file_location << ", target: " << target_file_location
                      << ", txn_id: " << txn_id << ", tablet_id: " << target_tablet_id;

            size_t final_file_size = 0;
            auto start_ts = butil::gettimeofday_us();
            if (is_seg) {
                TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::replicate_task::download_segment",
                                         &final_file_size);
                if (final_file_size == 0) {
                    RETURN_IF_ERROR(ReplicationUtils::download_lake_segment_file(
                            src_file_location, src_file_name, src_file_size, shared_src_fs, active_file_converters,
                            &final_file_size));
                }
                if (final_file_size > 0 && final_file_size != src_file_size) {
                    if (shared_mutex != nullptr) {
                        std::lock_guard lock(*shared_mutex);
                        segment_size_changes[target_file_name] = final_file_size;
                    } else {
                        segment_size_changes[target_file_name] = final_file_size;
                    }
                    LOG(INFO) << "Segment file size changed after conversion, src_file: " << src_file_name
                              << ", target_file: " << target_file_name << ", original size: " << src_file_size
                              << ", final size: " << final_file_size;
                }
            } else {
                WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
                if (config::enable_transparent_data_encryption) {
                    opts.encryption_info = encryption_info;
                }
                int max_retry = std::max(1, config::lake_replication_max_file_copy_retry);
                TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::replicate_task::copy_non_segment",
                                         &final_file_size);
                if (final_file_size == 0) {
                    ASSIGN_OR_RETURN(final_file_size,
                                     copy_non_segment_file_with_retry(src_file_location, shared_src_fs,
                                                                      target_file_location, opts, max_retry));
                }
                if (shared_mutex != nullptr) {
                    std::lock_guard lock(*shared_mutex);
                    files_to_delete.push_back(target_file_location);
                } else {
                    files_to_delete.push_back(target_file_location);
                }
            }

            total_file_size.fetch_add(final_file_size, std::memory_order_relaxed);
            auto cost = butil::gettimeofday_us() - start_ts;
            auto is_slow = cost >= config::lake_replication_slow_log_ms * 1000;
            if (is_slow) {
                LOG(INFO) << "Finished replicate src file: " << src_file_location
                          << ", target: " << target_file_location << ", txn_id: " << txn_id
                          << ", tablet_id: " << target_tablet_id << ", size: " << final_file_size
                          << ", cost(s): " << cost / 1000. / 1000.;
            }
            return Status::OK();
        });
    }
    // step 4: execute tasks and collect copy metrics.
    // Follow the ThreadPoolToken(CONCURRENT) + wait() pattern used in txn_manager.cpp.
    if (use_parallel) {
        auto token = repl_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
        std::vector<Status> task_results(tasks.size());
        for (size_t i = 0; i < tasks.size(); i++) {
            auto st =
                    token->submit_func([&task_results, i, task = std::move(tasks[i])]() { task_results[i] = task(); });
            if (!st.ok()) {
                task_results[i] = std::move(st);
            }
        }
        token->wait();
        for (const auto& r : task_results) {
            if (!r.ok()) {
                LOG(WARNING) << "Parallel file copy failed, txn_id: " << txn_id << ", tablet_id: " << target_tablet_id
                             << ", pool num_threads: " << repl_pool->num_threads()
                             << ", pool active_threads: " << repl_pool->active_threads()
                             << ", pool queued_tasks: " << repl_pool->num_queued_tasks() << ", error: " << r;
                return r;
            }
        }
    } else {
        for (const auto& task : tasks) {
            RETURN_IF_ERROR(task());
        }
    }
    double total_time_sec = watch.elapsed_time() / 1000. / 1000. / 1000.;
    double copy_rate = 0.0;
    if (total_time_sec > 0) {
        copy_rate = (total_file_size / 1024. / 1024.) / total_time_sec;
    }
    LOG(INFO) << "Replicated tablet file count: " << filename_map.size() << ", total bytes: " << total_file_size
              << ", cost: " << total_time_sec << "s, rate: " << copy_rate
              << "MB/s, parallel: " << (use_parallel ? "true" : "false") << ", txn_id: " << txn_id
              << ", tablet_id: " << target_tablet_id;

    // step 5: update metadata and write txn log.
    // Update segment sizes in tablet_metadata if there are any changes
    if (!segment_size_changes.empty()) {
        RETURN_IF_ERROR(update_tablet_metadata_segment_sizes(copied_target_tablet_meta, segment_size_changes));
    }
    txn_log->mutable_op_replication()->mutable_tablet_metadata()->CopyFrom(*copied_target_tablet_meta);

    // write txn log
    txn_log->set_tablet_id(target_tablet_id);
    txn_log->set_txn_id(txn_id);

    auto* txn_meta = txn_log->mutable_op_replication()->mutable_txn_meta();
    txn_meta->set_tablet_id(target_tablet_id);
    txn_meta->set_txn_id(txn_id);
    txn_meta->set_txn_state(ReplicationTxnStatePB::TXN_REPLICATED);
    txn_meta->set_visible_version(target_visible_version);
    txn_meta->set_data_version(data_version);
    txn_meta->set_snapshot_version(src_visible_version);
    // mark full replication for shared-data cluster migration
    txn_meta->set_incremental_snapshot(false);

    RETURN_IF_ERROR(_tablet_manager->put_txn_log(txn_log));

    VLOG(3) << "Replicate lake remote files finished, txn_id: " << txn_id << ", tablet_id: " << target_tablet_id;

    clean_files.cancel();
    return Status::OK();
}

bool LakeReplicationTxnManager::should_use_parallel_copy(size_t file_count, const ThreadPool* thread_pool) {
    if (thread_pool == nullptr) {
        return false;
    }
    const int min_file_count = std::max(0, config::lake_replication_parallel_copy_min_file_count);
    if (min_file_count == 0) {
        return false;
    }
    if (file_count < static_cast<size_t>(min_file_count)) {
        return false;
    }
    const int num_threads = thread_pool->num_threads();
    if (num_threads <= 0) {
        return false;
    }
    return thread_pool->num_queued_tasks() <= num_threads * kParallelCopyMaxQueuePerThread;
}

StatusOr<size_t> LakeReplicationTxnManager::copy_non_segment_file_with_retry(
        const std::string& src_file_location, const std::shared_ptr<FileSystem>& shared_src_fs,
        const std::string& target_file_location, const WritableFileOptions& opts, int max_retry) {
    ASSIGN_OR_RETURN(auto expected_size, shared_src_fs->get_file_size(src_file_location));

    const size_t buff_size = std::max<size_t>(
            std::min<size_t>(expected_size, config::lake_replication_read_buffer_size), 1 * 1024 * 1024);

    max_retry = std::max(1, max_retry);
    Status copy_status;
    size_t final_file_size = 0;
    for (int retry = 0; retry < max_retry; ++retry) {
        auto res = fs::copy_file(src_file_location, shared_src_fs, target_file_location, nullptr, opts, buff_size);
        if (!res.ok()) {
            copy_status = res.status();
            LOG(WARNING) << "Failed to copy file " << src_file_location << " to " << target_file_location
                         << ", retry=" << retry << ", error: " << copy_status;
            continue;
        }
        final_file_size = *res;
        TEST_SYNC_POINT_CALLBACK("lake_replication_non_segment_copy_size", &final_file_size);
        if (static_cast<int64_t>(final_file_size) == static_cast<int64_t>(expected_size)) {
            return final_file_size;
        }
        copy_status = Status::Corruption(fmt::format("File size mismatch after copy: expected={}, actual={}, src={}",
                                                     expected_size, final_file_size, src_file_location));
        LOG(WARNING) << copy_status.message() << ", retry=" << retry;
    }
    return copy_status;
}

StatusOr<TabletMetadataPtr> LakeReplicationTxnManager::build_source_tablet_meta(
        int64_t src_tablet_id, int64_t version, const std::string& meta_dir,
        const std::shared_ptr<FileSystem>& shared_src_fs) {
    LOG(INFO) << "Lake replicate storage task, building source tablet meta for tablet: " << src_tablet_id
              << ", version: " << version << ", meta_dir: " << meta_dir;

#ifdef BE_TEST
    TabletMetadataPtr injected_meta = nullptr;
    TEST_SYNC_POINT_CALLBACK("LakeReplicationTxnManager::build_source_tablet_meta::inject",
                             static_cast<void*>(&injected_meta));
    if (injected_meta != nullptr) {
        return injected_meta;
    }
#endif

    auto src_metadata_file_name = tablet_metadata_filename(src_tablet_id, version);
    auto src_tablet_meta_path = join_path(meta_dir, src_metadata_file_name);
    auto src_tablet_meta_or = _tablet_manager->get_tablet_metadata(src_tablet_meta_path, false, 0, shared_src_fs);
    if (!src_tablet_meta_or.ok()) {
        VLOG(3) << "Lake replicate storage task, failed to build source tablet meta for version: " << version
                << ", src_tablet_id: " << src_tablet_id << ", error: " << src_tablet_meta_or.status();
        return src_tablet_meta_or;
    }
    return src_tablet_meta_or.value();
}

StatusOr<TabletMetadataPtr> LakeReplicationTxnManager::try_build_source_tablet_meta_with_fallback(
        int64_t src_tablet_id, int64_t version, int64_t src_db_id, TTransactionId txn_id, std::string& src_meta_dir,
        std::string& src_data_dir, const std::shared_ptr<FileSystem>& shared_src_fs) {
    // Strategy: Try current format first, then fallback to legacy formats on NotFound error.
    const std::string original_meta_dir = src_meta_dir;
    const std::string original_data_dir = src_data_dir;

    // Attempt 1: Try current path format
    auto result = build_source_tablet_meta(src_tablet_id, version, src_meta_dir, shared_src_fs);
    if (result.ok()) {
        return result;
    }

    // If error is not NotFound, return immediately
    if (!result.status().is_not_found()) {
        LOG(WARNING) << "Lake replicate storage task, failed to build source tablet meta for version: " << version
                     << ", src_tablet_id: " << src_tablet_id << ", error: " << result.status();
        return result;
    }

    LOG(INFO) << "Source tablet meta not found with current path format, trying legacy format without db_id"
              << ", src_meta_dir: " << src_meta_dir << ", txn_id: " << txn_id;

    // Attempt 2: Try legacy format without db_id (keep partition_id)
    // Example: db56764/56970/63453/meta -> 56970/63453/meta
    std::string legacy_meta_dir = remove_db_id_component(original_meta_dir, src_db_id);
    std::string legacy_data_dir = remove_db_id_component(original_data_dir, src_db_id);

    result = build_source_tablet_meta(src_tablet_id, version, legacy_meta_dir, shared_src_fs);
    if (result.ok()) {
        LOG(INFO) << "Source tablet meta found with legacy format (without db_id)"
                  << ", updated meta_dir: " << legacy_meta_dir << ", data_dir: " << legacy_data_dir
                  << ", txn_id: " << txn_id;
        src_meta_dir = legacy_meta_dir;
        src_data_dir = legacy_data_dir;
        return result;
    }

    // If error is not NotFound, return immediately
    if (!result.status().is_not_found()) {
        LOG(WARNING) << "Lake replicate storage task, failed to build source tablet meta for version: " << version
                     << ", src_tablet_id: " << src_tablet_id << ", legacy_meta_dir: " << legacy_meta_dir
                     << ", error: " << result.status();
        return result;
    }

    LOG(INFO) << "Source tablet meta not found with legacy format without db_id, "
              << "trying very old format without db_id and partition_id"
              << ", legacy_meta_dir: " << legacy_meta_dir << ", txn_id: " << txn_id;

    // Attempt 3: Try very old format without db_id and partition_id
    // Remove partition_id from legacy1 path
    // Example: 56970/63453/meta -> 56970/meta
    std::string very_old_meta_dir = remove_last_path_component(legacy_meta_dir);
    std::string very_old_data_dir = remove_last_path_component(legacy_data_dir);

    result = build_source_tablet_meta(src_tablet_id, version, very_old_meta_dir, shared_src_fs);
    if (result.ok()) {
        LOG(INFO) << "Source tablet meta found with very old format (without db_id and partition_id)"
                  << ", updated meta_dir: " << very_old_meta_dir << ", data_dir: " << very_old_data_dir
                  << ", txn_id: " << txn_id;
        src_meta_dir = very_old_meta_dir;
        src_data_dir = very_old_data_dir;
        return result;
    }

    // All attempts failed, return the last error
    LOG(WARNING) << "Lake replicate storage task, failed to build source tablet meta after all fallback attempts"
                 << ", version: " << version << ", src_tablet_id: " << src_tablet_id << ", txn_id: " << txn_id
                 << ", error: " << result.status();
    return result;
}

Status LakeReplicationTxnManager::build_existed_filename_uuids_map(
        const TabletMetadataPtr& target_data_version_tablet_meta,
        std::unordered_map<std::string, std::pair<std::string, std::string>>& existed_filename_uuids) {
    // Collect UUIDs from rowsets (segments and del files)
    for (const auto& rowset : target_data_version_tablet_meta->rowsets()) {
        // the condition is very strict, because currently encryption meta for each segment is not bind to the segment
        // we can only find the encryption meta by index, so the precondition is that the size of segment files
        // is strictly equal to the size of encryption metas
        bool has_encryption_meta = rowset.segments_size() == rowset.segment_encryption_metas_size();
        for (size_t i = 0; i < rowset.segments_size(); ++i) {
            const auto& segment_name = rowset.segments(i);
            if (has_encryption_meta) {
                existed_filename_uuids.emplace(extract_uuid_from(segment_name),
                                               std::make_pair(segment_name, rowset.segment_encryption_metas(i)));
            } else {
                existed_filename_uuids.emplace(extract_uuid_from(segment_name), std::make_pair(segment_name, ""));
            }
        }
        for (const auto& del : rowset.del_files()) {
            const auto& del_filename = del.name();
            existed_filename_uuids.emplace(extract_uuid_from(del_filename),
                                           std::make_pair(del_filename, del.encryption_meta()));
        }
    }

    // Collect UUIDs from SST files
    if (target_data_version_tablet_meta->has_sstable_meta()) {
        const auto& dest_meta = target_data_version_tablet_meta->sstable_meta();
        for (const auto& sst : dest_meta.sstables()) {
            const auto& sst_filename = sst.filename();
            existed_filename_uuids.emplace(extract_uuid_from(sst_filename),
                                           std::make_pair(sst_filename, sst.encryption_meta()));
        }
    }

    // Collect UUIDs from delvec files
    if (target_data_version_tablet_meta->has_delvec_meta()) {
        const auto& dest_meta = target_data_version_tablet_meta->delvec_meta();
        for (const auto& [_, file_meta_pb] : dest_meta.version_to_file()) {
            const auto& delvec_filename = file_meta_pb.name();
            // Note: delvec files don't have separate encryption metas in current implementation
            existed_filename_uuids.emplace(extract_uuid_from(delvec_filename), std::make_pair(delvec_filename, ""));
        }
    }

    // Collect UUIDs from dcg files
    if (target_data_version_tablet_meta->has_dcg_meta()) {
        const auto& dcg_meta = target_data_version_tablet_meta->dcg_meta();
        for (const auto& [_, dcg_ver_pb] : dcg_meta.dcgs()) {
            bool has_encryption_meta = dcg_ver_pb.column_files_size() == dcg_ver_pb.encryption_metas_size();
            for (int i = 0; i < dcg_ver_pb.column_files_size(); ++i) {
                const auto& dcg_filename = dcg_ver_pb.column_files(i);
                if (has_encryption_meta) {
                    existed_filename_uuids.emplace(extract_uuid_from(dcg_filename),
                                                   std::make_pair(dcg_filename, dcg_ver_pb.encryption_metas(i)));
                } else {
                    existed_filename_uuids.emplace(extract_uuid_from(dcg_filename), std::make_pair(dcg_filename, ""));
                }
            }
        }
    }

    return Status::OK();
}

StatusOr<std::shared_ptr<TabletMetadataPB>> LakeReplicationTxnManager::convert_and_build_new_tablet_meta(
        const TabletMetadataPtr& src_tablet_meta, const TabletMetadataPtr& target_tablet_meta, int64_t src_tablet_id,
        int64_t target_tablet_id, TTransactionId txn_id, int64_t data_version, const std::string& src_data_dir,
        std::unordered_map<std::string, size_t>& segment_name_to_size_map,
        std::map<std::string, std::string>& file_locations,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map) {
    VLOG(3) << "Lake replicate storage task, building new tablet meta for tablet: " << target_tablet_id
            << ", src_tablet_id: " << src_tablet_id << ", txn_id: " << txn_id << ", data_version: " << data_version;
    // find all files that already replicated to target storage in previous txns
    ASSIGN_OR_RETURN(auto target_data_version_tablet_meta,
                     _tablet_manager->get_tablet_metadata(target_tablet_id, data_version, false, 0, nullptr));
    // `existed_filename_uuids` represented files that already replicated to target storage in previous txns
    // <uuid, pair<existed_filename, encryption_meta>>
    std::unordered_map<std::string, std::pair<std::string, std::string>> existed_filename_uuids;
    RETURN_IF_ERROR(build_existed_filename_uuids_map(target_data_version_tablet_meta, existed_filename_uuids));

    VLOG(3) << "Lake replicate storage task, found " << existed_filename_uuids.size() << " existed files";
    // make new metadata
    std::shared_ptr<TabletMetadataPB> new_metadata = std::make_shared<TabletMetadataPB>(*target_tablet_meta);
    // Replace the tablet id with target tablet id
    new_metadata->mutable_rowsets()->Clear();
    new_metadata->mutable_dcg_meta()->mutable_dcgs()->clear();
    new_metadata->mutable_sstable_meta()->Clear();
    new_metadata->mutable_delvec_meta()->Clear();

    // deal with segments and dels
    for (const auto& src_rowset_meta : src_tablet_meta->rowsets()) {
        auto new_rowset_meta = new_metadata->add_rowsets();
        new_rowset_meta->CopyFrom(src_rowset_meta);
        new_rowset_meta->mutable_segments()->Clear();
        new_rowset_meta->mutable_segment_encryption_metas()->Clear();
        new_rowset_meta->mutable_segment_size()->Clear();
        new_rowset_meta->mutable_del_files()->Clear();
        // check if segment size is valid
        auto segment_size_size = src_rowset_meta.segment_size_size();
        if (segment_size_size > 0) {
            auto segment_file_size = src_rowset_meta.segments_size();
            // `segment_size_size` and `segment_file_size` should always be equal
            if (UNLIKELY(segment_size_size > 0 && segment_size_size != segment_file_size)) {
                return Status::Corruption(
                        fmt::format("found invalid segment_size, src_tablet_id: {}, "
                                    "rowset_id: {}, segment_size_size: {}, segment_file_size: {}",
                                    src_tablet_id, src_rowset_meta.id(), segment_size_size, segment_file_size));
            }
        }

        // Convert rowset metadata
        for (int i = 0; i < src_rowset_meta.segments_size(); ++i) {
            const auto& src_segment_filename = src_rowset_meta.segments(i);
            std::string final_segment_filename;
            ASSIGN_OR_RETURN(auto is_existed,
                             determine_final_filename(src_segment_filename, txn_id, existed_filename_uuids,
                                                      final_segment_filename, target_tablet_id, src_data_dir,
                                                      file_locations, filename_map));
            new_rowset_meta->add_segments(final_segment_filename);

            // Copy segment_size from source rowset metadata as inital value for the target rowset metadata
            if (segment_size_size > 0) {
                new_rowset_meta->add_segment_size(src_rowset_meta.segment_size(i));
            }

            // Add encryption metadata for files
            if (config::enable_transparent_data_encryption) {
                if (!is_existed) {
                    // segment file doesn't exist, use the newly generated encryption metadata
                    std::pair<std::string, FileEncryptionPair> pair = filename_map[src_segment_filename];
                    new_rowset_meta->add_segment_encryption_metas(pair.second.encryption_meta);
                } else {
                    // segment file already exists, use the existing encryption metadata from target tablet
                    auto uuid = extract_uuid_from(src_segment_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        const std::string& existing_encryption_meta = it->second.second;
                        new_rowset_meta->add_segment_encryption_metas(existing_encryption_meta);
                    } else {
                        // should never happend
                        return Status::Corruption(fmt::format("no existing encryption metadata found for file: {}",
                                                              src_segment_filename));
                    }
                }
            }

            // build segment_name_to_size_map, record the size of source segment file
            if (segment_size_size > 0) {
                auto segment_size = src_rowset_meta.segment_size(i);
                segment_name_to_size_map.emplace(src_segment_filename, segment_size);
            }
        }
        // update next_rowset_id
        new_metadata->set_next_rowset_id(src_tablet_meta->next_rowset_id());

        // Convert dels
        for (int i = 0; i < src_rowset_meta.del_files_size(); ++i) {
            const DelfileWithRowsetId& src_del = src_rowset_meta.del_files(i);
            const auto& src_del_filename = src_del.name();
            std::string final_del_filename;
            ASSIGN_OR_RETURN(auto is_existed, determine_final_filename(src_del_filename, txn_id, existed_filename_uuids,
                                                                       final_del_filename, target_tablet_id,
                                                                       src_data_dir, file_locations, filename_map));
            auto* new_del = new_rowset_meta->add_del_files();
            new_del->CopyFrom(src_del);
            new_del->set_name(final_del_filename);

            if (config::enable_transparent_data_encryption) {
                if (!is_existed) {
                    // del doesn't exist, use the newly generated encryption metadata
                    std::pair<std::string, FileEncryptionPair> pair = filename_map[src_del_filename];
                    new_del->set_encryption_meta(pair.second.encryption_meta);
                } else {
                    // del already exists, use the existing encryption metadata from target tablet
                    auto uuid = extract_uuid_from(src_del_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        const std::string& existing_encryption_meta = it->second.second;
                        new_del->set_encryption_meta(existing_encryption_meta);
                    }
                }
            }
        }
    }

    // deal with sstable
    if (src_tablet_meta->has_sstable_meta()) {
        PersistentIndexSstableMetaPB* dest_meta = new_metadata->mutable_sstable_meta();
        dest_meta->CopyFrom(src_tablet_meta->sstable_meta());
        for (int i = 0; i < dest_meta->sstables_size(); ++i) {
            PersistentIndexSstablePB* sst = dest_meta->mutable_sstables(i);
            const auto& src_sst_filename = sst->filename();
            std::string final_sst_filename;
            ASSIGN_OR_RETURN(auto is_existed, determine_final_filename(src_sst_filename, txn_id, existed_filename_uuids,
                                                                       final_sst_filename, target_tablet_id,
                                                                       src_data_dir, file_locations, filename_map));
            sst->set_filename(final_sst_filename);

            if (config::enable_transparent_data_encryption) {
                if (!is_existed) {
                    // sst doesn't exist, use the newly generated encryption metadata
                    std::pair<std::string, FileEncryptionPair> pair = filename_map[src_sst_filename];
                    sst->set_encryption_meta(pair.second.encryption_meta);
                } else {
                    // sst already exists, use the existing encryption metadata from target tablet
                    auto uuid = extract_uuid_from(src_sst_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        const std::string& existing_encryption_meta = it->second.second;
                        sst->set_encryption_meta(existing_encryption_meta);
                    }
                }
            }
        }
    }

    // deal with delvec
    if (src_tablet_meta->has_delvec_meta()) {
        DelvecMetadataPB* dest_meta = new_metadata->mutable_delvec_meta();
        dest_meta->CopyFrom(src_tablet_meta->delvec_meta());
        for (const auto& [version, file_meta_pb] : dest_meta->version_to_file()) {
            auto src_delvec_filename = file_meta_pb.name();
            std::string final_delvec_filename;
            ASSIGN_OR_RETURN(
                    auto is_existed,
                    determine_final_filename(src_delvec_filename, txn_id, existed_filename_uuids, final_delvec_filename,
                                             target_tablet_id, src_data_dir, file_locations, filename_map));
            auto& item = (*dest_meta->mutable_version_to_file())[version];
            item.set_name(final_delvec_filename);

            if (config::enable_transparent_data_encryption) {
                if (!is_existed) {
                    // del file doesn't exist, use the newly generated encryption metadata
                    std::pair<std::string, FileEncryptionPair> pair = filename_map[src_delvec_filename];
                    item.set_encryption_meta(pair.second.encryption_meta);
                } else {
                    // del file already exists, use the existing encryption metadata from target tablet
                    auto uuid = extract_uuid_from(src_delvec_filename);
                    auto it = existed_filename_uuids.find(uuid);
                    if (it != existed_filename_uuids.end()) {
                        const std::string& existing_encryption_meta = it->second.second;
                        item.set_encryption_meta(existing_encryption_meta);
                    }
                }
            }
        }
    }

    // deal with dcg
    if (src_tablet_meta->has_dcg_meta()) {
        DeltaColumnGroupMetadataPB* dest_meta = new_metadata->mutable_dcg_meta();
        dest_meta->CopyFrom(src_tablet_meta->dcg_meta());
        for (auto& [segment_id, dcg_ver_pb] : *dest_meta->mutable_dcgs()) {
            for (int i = 0; i < dcg_ver_pb.column_files_size(); ++i) {
                auto src_dcg_filename = dcg_ver_pb.column_files(i);
                std::string final_dcg_filename;
                ASSIGN_OR_RETURN(
                        auto is_existed,
                        determine_final_filename(src_dcg_filename, txn_id, existed_filename_uuids, final_dcg_filename,
                                                 target_tablet_id, src_data_dir, file_locations, filename_map));
                dcg_ver_pb.set_column_files(i, final_dcg_filename);

                if (config::enable_transparent_data_encryption) {
                    if (!is_existed) {
                        // dcg file doesn't exist, use the newly generated encryption metadata
                        std::pair<std::string, FileEncryptionPair> pair = filename_map[src_dcg_filename];
                        if (dcg_ver_pb.encryption_metas_size() > i) {
                            dcg_ver_pb.set_encryption_metas(i, pair.second.encryption_meta);
                        } else {
                            dcg_ver_pb.add_encryption_metas(pair.second.encryption_meta);
                        }
                    } else {
                        // dcg file already exists, use the existing encryption metadata from target tablet
                        auto uuid = extract_uuid_from(src_dcg_filename);
                        auto it = existed_filename_uuids.find(uuid);
                        if (it != existed_filename_uuids.end()) {
                            const std::string& existing_encryption_meta = it->second.second;
                            if (dcg_ver_pb.encryption_metas_size() > i) {
                                dcg_ver_pb.set_encryption_metas(i, existing_encryption_meta);
                            } else {
                                dcg_ver_pb.add_encryption_metas(existing_encryption_meta);
                            }
                        }
                    }
                }
            }
        }
    }

    return new_metadata;
}

StatusOr<bool> LakeReplicationTxnManager::determine_final_filename(
        const std::string& src_filename, TTransactionId txn_id,
        const std::unordered_map<std::string, std::pair<std::string, std::string>>& existed_filename_uuids,
        std::string& final_filename, const int64_t target_tablet_id, const std::string& src_data_dir,
        std::map<std::string, std::string>& file_locations,
        std::unordered_map<std::string, std::pair<std::string, FileEncryptionPair>>& filename_map) {
    auto uuid = extract_uuid_from(src_filename);
    auto it = existed_filename_uuids.find(uuid);
    if (it != existed_filename_uuids.end()) {
        // UUID exists, use the existing target filename
        final_filename = it->second.first; // pair.first is the filename
        LOG(INFO) << "File: " << src_filename
                  << " already exists on target cluster, use existing target filename: " << final_filename;
        return true;
    }

    // UUID not exists, generate new filename
    final_filename = gen_filename_from(txn_id, src_filename);
    if (UNLIKELY(final_filename.empty())) {
        return Status::Corruption("Failed to generate new filename from: " + src_filename);
    }

    // Build file_locations map
    auto target_file_path = _tablet_manager->segment_location(target_tablet_id, final_filename);
    file_locations.emplace(join_path(src_data_dir, src_filename), target_file_path);

    // Build filename_map
    FileEncryptionPair encryption_pair;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(encryption_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    }
    auto pair = filename_map.emplace(src_filename, std::pair(final_filename, std::move(encryption_pair)));
    if (!pair.second) {
        return Status::Corruption("Duplicated file: " + pair.first->first);
    }
    return false;
}

Status LakeReplicationTxnManager::update_tablet_metadata_segment_sizes(
        const std::shared_ptr<TabletMetadataPB>& tablet_metadata,
        const std::unordered_map<std::string, size_t>& segment_size_changes) {
    if (segment_size_changes.empty()) {
        return Status::OK();
    }

    int updated_count = 0;

    // Iterate through all rowsets in the tablet metadata
    for (int rowset_idx = 0; rowset_idx < tablet_metadata->rowsets_size(); ++rowset_idx) {
        auto* rowset = tablet_metadata->mutable_rowsets(rowset_idx);

        // Check if this rowset has segment_size field
        if (rowset->segment_size_size() == 0) {
            // No segment_size recorded, skip
            continue;
        }

        // Verify segment_size array matches segments array
        if (rowset->segment_size_size() != rowset->segments_size()) {
            LOG(WARNING) << "Rowset segment_size count mismatch, rowset_id: " << rowset->id()
                         << ", segments: " << rowset->segments_size()
                         << ", segment_sizes: " << rowset->segment_size_size();
            continue;
        }

        // Update segment sizes if they changed
        for (int seg_idx = 0; seg_idx < rowset->segments_size(); ++seg_idx) {
            const auto& segment_name = rowset->segments(seg_idx);
            auto it = segment_size_changes.find(segment_name);
            if (it != segment_size_changes.end()) {
                uint64_t old_size = rowset->segment_size(seg_idx);
                uint64_t new_size = it->second;

                if (old_size != new_size) {
                    rowset->set_segment_size(seg_idx, new_size);
                    updated_count++;

                    LOG(INFO) << "Updated segment size in tablet_metadata, rowset_id: " << rowset->id()
                              << ", segment: " << segment_name << ", old_size: " << old_size
                              << ", new_size: " << new_size;
                }
            }
        }
    }

    if (updated_count > 0) {
        LOG(INFO) << "Updated " << updated_count << " segment sizes in tablet_metadata";
    }

    return Status::OK();
}

} // namespace starrocks::lake
