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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/storage_engine.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/storage_engine.h"

#include <fmt/format.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <new>
#include <queue>
#include <random>
#include <set>

#include "agent/master_info.h"
#include "common/status.h"
#include "cumulative_compaction.h"
#include "fs/fd_cache.h"
#include "fs/fs_util.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/client_cache.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/base_compaction.h"
#include "storage/compaction_manager.h"
#include "storage/data_dir.h"
#include "storage/memtable_flush_executor.h"
#include "storage/publish_version_manager.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/rowset/unique_rowset_id_generator.h"
#include "storage/segment_flush_executor.h"
#include "storage/segment_replicate_executor.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/task/engine_task.h"
#include "storage/update_manager.h"
#include "testutil/sync_point.h"
#include "util/bthreads/executor.h"
#include "util/lru_cache.h"
#include "util/scoped_cleanup.h"
#include "util/starrocks_metrics.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

StorageEngine* StorageEngine::_s_instance = nullptr;
StorageEngine* StorageEngine::_p_instance = nullptr;

static Status _validate_options(const EngineOptions& options) {
    if (options.store_paths.empty()) {
        return Status::InternalError("store paths is empty");
    }
    return Status::OK();
}

Status StorageEngine::open(const EngineOptions& options, StorageEngine** engine_ptr) {
    if (options.need_write_cluster_id) {
        RETURN_IF_ERROR(_validate_options(options));
    }

    std::unique_ptr<StorageEngine> engine = std::make_unique<StorageEngine>(options);
    RETURN_IF_ERROR_WITH_WARN(engine->_open(options), "open engine failed");
    *engine_ptr = engine.release();
    return Status::OK();
}

StorageEngine::StorageEngine(const EngineOptions& options)
        : _effective_cluster_id(-1),
          _options(options),
          _available_storage_medium_type_count(0),
          _is_all_cluster_id_exist(true),
          _tablet_manager(new TabletManager(config::tablet_map_shard_size)),
          _txn_manager(new TxnManager(config::txn_map_shard_size, config::txn_shard_size, options.store_paths.size())),
          _rowset_id_generator(new UniqueRowsetIdGenerator(options.backend_uid)),
          _memtable_flush_executor(nullptr),
          _update_manager(new UpdateManager(options.update_mem_tracker)),
          _compaction_manager(new CompactionManager()),
          _publish_version_manager(new PublishVersionManager()) {
#ifdef BE_TEST
    _p_instance = _s_instance;
    _s_instance = this;
#endif
    if (_s_instance == nullptr) {
        _s_instance = this;
    }
    REGISTER_GAUGE_STARROCKS_METRIC(unused_rowsets_count, [this]() {
        std::lock_guard lock(_gc_mutex);
        return _unused_rowsets.size();
    })
    _delta_column_group_cache_mem_tracker = std::make_unique<MemTracker>(-1, "delta_column_group_non_pk_cache");
}

StorageEngine::~StorageEngine() {
#ifdef BE_TEST
    if (_s_instance == this) {
        _s_instance = _p_instance;
    }
#else
    if (_s_instance != nullptr) {
        _s_instance = nullptr;
    }
#endif
}

void StorageEngine::load_data_dirs(const std::vector<DataDir*>& data_dirs) {
    std::vector<std::thread> threads;
    threads.reserve(data_dirs.size());
    for (auto data_dir : data_dirs) {
        threads.emplace_back([data_dir] {
            auto res = data_dir->load();
            if (!res.ok()) {
                LOG(WARNING) << "Fail to load data dir=" << data_dir->path() << ", res=" << res.to_string();
            }
        });
        Thread::set_thread_name(threads.back(), "load_data_dir");

        threads.emplace_back([data_dir] {
            if (config::manual_compact_before_data_dir_load) {
                uint64_t live_sst_files_size_before = 0;
                if (!data_dir->get_meta()->get_live_sst_files_size(&live_sst_files_size_before)) {
                    LOG(WARNING) << "data dir " << data_dir->path() << " get_live_sst_files_size failed";
                }
                Status s = data_dir->get_meta()->compact();
                if (!s.ok()) {
                    LOG(WARNING) << "data dir " << data_dir->path() << " manual compact meta failed: " << s;
                } else {
                    uint64_t live_sst_files_size_after = 0;
                    if (!data_dir->get_meta()->get_live_sst_files_size(&live_sst_files_size_after)) {
                        LOG(WARNING) << "data dir " << data_dir->path() << " get_live_sst_files_size failed";
                    }
                    LOG(INFO) << "data dir " << data_dir->path() << " manual compact meta successfully, "
                              << "live_sst_files_size_before: " << live_sst_files_size_before
                              << " live_sst_files_size_after: " << live_sst_files_size_after
                              << data_dir->get_meta()->get_stats();
                }
            }
        });
        Thread::set_thread_name(threads.back(), "compact_data_dir");
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

Status StorageEngine::_open(const EngineOptions& options) {
    // init store_map
    RETURN_IF_ERROR_WITH_WARN(_init_store_map(), "_init_store_map failed");

    _effective_cluster_id = config::cluster_id;
    _need_write_cluster_id = options.need_write_cluster_id;
    RETURN_IF_ERROR_WITH_WARN(_check_all_root_path_cluster_id(), "fail to check cluster id");

    _update_storage_medium_type_count();

    RETURN_IF_ERROR_WITH_WARN(_check_file_descriptor_number(), "check fd number failed");

    RETURN_IF_ERROR_WITH_WARN(_update_manager->init(), "init update_manager failed");

    RETURN_IF_ERROR_WITH_WARN(_publish_version_manager->init(), "init publish_version_manager failed");

    auto dirs = get_stores<false>();

    // `load_data_dirs` depend on |_update_manager|.
    load_data_dirs(dirs);

    std::unique_ptr<ThreadPool> thread_pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("delta_writer")
                            .set_min_threads(config::number_tablet_writer_threads / 2)
                            .set_max_threads(std::max<int>(1, config::number_tablet_writer_threads))
                            .set_max_queue_size(40960 /*a random chosen number that should big enough*/)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(/*5 minutes=*/5 * 60 * 1000))
                            .build(&thread_pool));

    _async_delta_writer_executor =
            std::make_unique<bthreads::ThreadPoolExecutor>(thread_pool.release(), kTakesOwnership);
    REGISTER_GAUGE_STARROCKS_METRIC(async_delta_writer_queue_count, [this]() {
        return static_cast<bthreads::ThreadPoolExecutor*>(_async_delta_writer_executor.get())
                ->get_thread_pool()
                ->num_queued_tasks();
    })

    _memtable_flush_executor = std::make_unique<MemTableFlushExecutor>();
    RETURN_IF_ERROR_WITH_WARN(_memtable_flush_executor->init(dirs), "init MemTableFlushExecutor failed");
    REGISTER_GAUGE_STARROCKS_METRIC(memtable_flush_queue_count, [this]() {
        return _memtable_flush_executor->get_thread_pool()->num_queued_tasks();
    })

    _segment_flush_executor = std::make_unique<SegmentFlushExecutor>();
    RETURN_IF_ERROR_WITH_WARN(_segment_flush_executor->init(dirs), "init SegmentFlushExecutor failed");
    REGISTER_GAUGE_STARROCKS_METRIC(segment_flush_queue_count,
                                    [this]() { return _segment_flush_executor->get_thread_pool()->num_queued_tasks(); })

    _segment_replicate_executor = std::make_unique<SegmentReplicateExecutor>();
    RETURN_IF_ERROR_WITH_WARN(_segment_replicate_executor->init(dirs), "init SegmentReplicateExecutor failed");
    REGISTER_GAUGE_STARROCKS_METRIC(segment_replicate_queue_count, [this]() {
        return _segment_replicate_executor->get_thread_pool()->num_queued_tasks();
    })

    return Status::OK();
}

Status StorageEngine::_init_store_map() {
    std::vector<std::pair<bool, DataDir*>> tmp_stores;
    ScopedCleanup release_guard([&] {
        for (const auto& item : tmp_stores) {
            if (item.first) {
                delete item.second;
            }
        }
    });
    std::vector<std::thread> threads;
    SpinLock error_msg_lock;
    std::string error_msg;
    for (auto& path : _options.store_paths) {
        auto* store = new DataDir(path.path, path.storage_medium, _tablet_manager.get(), _txn_manager.get());
        ScopedCleanup store_release_guard([&]() { delete store; });
        tmp_stores.emplace_back(true, store);
        store_release_guard.cancel();
        threads.emplace_back([store, &error_msg_lock, &error_msg]() {
            auto st = store->init();
            if (!st.ok()) {
                {
                    std::lock_guard<SpinLock> l(error_msg_lock);
                    error_msg.append(st.to_string() + ";");
                }
                LOG(WARNING) << "Store load failed, status=" << st.to_string() << ", path=" << store->path();
            }
        });
        Thread::set_thread_name(threads.back(), "init_store_path");
    }
    for (auto& thread : threads) {
        thread.join();
    }

    if (!error_msg.empty()) {
        return Status::InternalError(strings::Substitute("init path failed, error=$0", error_msg));
    }

    for (auto& store : tmp_stores) {
        _store_map.emplace(store.second->path(), store.second);
        store.first = false;
    }

    release_guard.cancel();
    return Status::OK();
}

void StorageEngine::_update_storage_medium_type_count() {
    std::set<TStorageMedium::type> available_storage_medium_types;

    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        if (it.second->is_used()) {
            available_storage_medium_types.insert(it.second->storage_medium());
        }
    }

    _available_storage_medium_type_count = available_storage_medium_types.size();
}

Status StorageEngine::_judge_and_update_effective_cluster_id(int32_t cluster_id) {
    if (cluster_id == -1 && _effective_cluster_id == -1) {
        // maybe this is a new cluster, cluster id will get from heartbeat message
        return Status::OK();
    } else if (cluster_id != -1 && _effective_cluster_id == -1) {
        _effective_cluster_id = cluster_id;
        return Status::OK();
    } else if (cluster_id == -1 && _effective_cluster_id != -1) {
        // _effective_cluster_id is the right effective cluster id
        return Status::OK();
    } else {
        if (cluster_id != _effective_cluster_id) {
            RETURN_IF_ERROR_WITH_WARN(
                    Status::Corruption(strings::Substitute(
                            "multiple cluster ids is not equal. one=$0, other=", _effective_cluster_id, cluster_id)),
                    "cluster id not equal");
        }
    }

    return Status::OK();
}

template <bool include_unused>
std::vector<DataDir*> StorageEngine::get_stores() {
    std::vector<DataDir*> stores;
    stores.reserve(_store_map.size());

    std::lock_guard<std::mutex> l(_store_lock);
    if (include_unused) {
        for (auto& it : _store_map) {
            stores.push_back(it.second);
        }
    } else {
        for (auto& it : _store_map) {
            if (it.second->is_used()) {
                stores.push_back(it.second);
            }
        }
    }
    return stores;
}

template std::vector<DataDir*> StorageEngine::get_stores<false>();
template std::vector<DataDir*> StorageEngine::get_stores<true>();

void StorageEngine::get_all_data_dir_info(vector<DataDirInfo>* data_dir_infos, bool need_update) {
    data_dir_infos->clear();

    // 1. update available capacity of each data dir
    // get all root path info and construct a path map.
    // path -> DataDirInfo
    std::map<std::string, DataDirInfo> path_map;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {
            if (need_update) {
                it.second->update_capacity();
            }
            path_map.emplace(it.first, it.second->get_dir_info());
        }
    }

    // 2. get total tablets' size of each data dir
    size_t tablet_count = 0;
    _tablet_manager->update_root_path_info(&path_map, &tablet_count);

    // add path info to data_dir_infos
    for (auto& entry : path_map) {
        data_dir_infos->emplace_back(entry.second);
    }
}

void StorageEngine::_start_disk_stat_monitor() {
    for (auto& it : _store_map) {
        it.second->health_check();
    }

    _update_storage_medium_type_count();
    bool some_tablets_were_dropped = _delete_tablets_on_unused_root_path();
    // If some tablets were dropped, we should notify disk_state_worker_thread and
    // tablet_worker_thread (see TaskWorkerPool) to make them report to FE ASAP.
    if (some_tablets_were_dropped) {
        trigger_report();
    }

    // Once sweep operation can lower the disk water level by removing data, so it doesn't make sense
    // to wake up the sweeper thread multiple times in a short period of time. To avoid multiple
    // disk scans, set an valid disk scan interval.
    static time_t last_sweep_time = 0;
    static const int32_t valid_sweep_interval = 30;
    for (auto& it : _store_map) {
        if (difftime(time(nullptr), last_sweep_time) > valid_sweep_interval && it.second->capacity_limit_reached(0)) {
            std::unique_lock<std::mutex> lk(_trash_sweeper_mutex);
            _trash_sweeper_cv.notify_one();
            last_sweep_time = time(nullptr);
        }
    }
}

// TODO(lingbin): Should be in EnvPosix?
Status StorageEngine::_check_file_descriptor_number() {
    struct rlimit l;
    int ret = getrlimit(RLIMIT_NOFILE, &l);
    if (ret != 0) {
        LOG(WARNING) << "Fail to getrlimit() with errno=" << strerror(errno) << ", use default configuration instead.";
        return Status::OK();
    }
    if (l.rlim_cur < config::min_file_descriptor_number) {
        LOG(ERROR) << "File descriptor number is less than " << config::min_file_descriptor_number
                   << ". Please use (ulimit -n) to set a value equal or greater than "
                   << config::min_file_descriptor_number;
        return Status::InternalError("file descriptors limit is too small");
    }
    return Status::OK();
}

Status StorageEngine::_check_all_root_path_cluster_id() {
    int32_t cluster_id = -1;
    for (auto& it : _store_map) {
        int32_t tmp_cluster_id = it.second->cluster_id();
        if (tmp_cluster_id == -1) {
            _is_all_cluster_id_exist = false;
        } else if (tmp_cluster_id == cluster_id) {
            // both hava right cluster id, do nothing
        } else if (cluster_id == -1) {
            cluster_id = tmp_cluster_id;
        } else {
            RETURN_IF_ERROR_WITH_WARN(
                    Status::Corruption(strings::Substitute(
                            "multiple cluster ids is not equal. one=$0, other=", cluster_id, tmp_cluster_id)),
                    "cluster id not equal");
        }
    }

    // judge and get effective cluster id
    RETURN_IF_ERROR(_judge_and_update_effective_cluster_id(cluster_id));

    // write cluster id into cluster_id_path if get effective cluster id success
    if (_effective_cluster_id != -1 && !_is_all_cluster_id_exist && _need_write_cluster_id) {
        set_cluster_id(_effective_cluster_id);
    }

    return Status::OK();
}

Status StorageEngine::set_cluster_id(int32_t cluster_id) {
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        RETURN_IF_ERROR(it.second->set_cluster_id(cluster_id));
    }
    _effective_cluster_id = cluster_id;
    _is_all_cluster_id_exist = true;
    return Status::OK();
}

std::vector<string> StorageEngine::get_store_paths() {
    std::vector<string> paths;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {
            paths.push_back(it.first);
        }
    }
    return paths;
}

std::vector<DataDir*> StorageEngine::get_stores_for_create_tablet(TStorageMedium::type storage_medium) {
    std::vector<DataDir*> stores;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {
            if (it.second->is_used()) {
                if (_available_storage_medium_type_count == 1 || it.second->storage_medium() == storage_medium) {
                    if (!it.second->capacity_limit_reached(0)) {
                        stores.push_back(it.second);
                    }
                }
            }
        }
    }

    // sort by disk usage in asc order
    std::sort(stores.begin(), stores.end(),
              [](const auto& a, const auto& b) { return a->disk_usage(0) < b->disk_usage(0); });

    // compute average usage of all disks
    double avg_disk_usage = 0.0;
    double usage_sum = 0.0;
    for (const auto& v : stores) {
        usage_sum += v->disk_usage(0);
    }
    avg_disk_usage = usage_sum / stores.size();

    // find the last root path which will participate in vector shuffle so that all the paths
    // before and included can be chosen to create tablet on preferentially
    size_t last_candidate_idx = 0;
    for (const auto v : stores) {
        if (v->disk_usage(0) > avg_disk_usage + config::storage_high_usage_disk_protect_ratio) {
            break;
        }
        last_candidate_idx++;
    }

    // randomize the preferential paths to balance number of tablets each disk has
    std::srand(std::random_device()());
    std::shuffle(stores.begin(), stores.begin() + last_candidate_idx, std::mt19937(std::random_device()()));
    return stores;
}

DataDir* StorageEngine::get_store(const std::string& path) {
    // _store_map is unchanged, no need to lock
    auto it = _store_map.find(path);
    if (it == std::end(_store_map)) {
        return nullptr;
    }
    return it->second;
}

DataDir* StorageEngine::get_store(int64_t path_hash) {
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        if (it.second->path_hash() == path_hash) {
            return it.second;
        }
    }
    return nullptr;
}

// maybe nullptr if as cn
DataDir* StorageEngine::get_persistent_index_store(int64_t tablet_id) {
    auto stores = get_stores<false>();
    if (stores.empty()) {
        return nullptr;
    } else {
        return stores[tablet_id % stores.size()];
    }
}

static bool too_many_disks_are_failed(uint32_t unused_num, uint32_t total_num) {
    return total_num == 0 || unused_num * 100 / total_num > config::max_percentage_of_error_disk;
}

bool StorageEngine::_delete_tablets_on_unused_root_path() {
    std::vector<TabletInfo> tablet_info_vec;
    uint32_t unused_root_path_num = 0;
    uint32_t total_root_path_num = 0;

    {
        std::lock_guard<std::mutex> l(_store_lock);
        if (_store_map.size() == 0) {
            return false;
        }

        for (auto& it : _store_map) {
            ++total_root_path_num;
            if (it.second->is_used()) {
                continue;
            }
            it.second->clear_tablets(&tablet_info_vec);
            ++unused_root_path_num;
        }
    }

    if (too_many_disks_are_failed(unused_root_path_num, total_root_path_num)) {
        LOG(FATAL) << "meet too many error disks, process exit. "
                   << "max_ratio_allowed=" << config::max_percentage_of_error_disk << "%"
                   << ", error_disk_count=" << unused_root_path_num << ", total_disk_count=" << total_root_path_num;
        exit(0);
    }

    auto st = _tablet_manager->drop_tablets_on_error_root_path(tablet_info_vec);
    st.permit_unchecked_error();
    // If tablet_info_vec is not empty, means we have dropped some tablets.
    return !tablet_info_vec.empty();
}

void StorageEngine::stop() {
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& store_pair : _store_map) {
            // store_pair.second will be delete later
            store_pair.second->stop_bg_worker();
        }
        // notify the cv in case anyone is waiting for.
        _report_cv.notify_all();
    }

    _bg_worker_stopped.store(true, std::memory_order_release);

#define JOIN_THREAD(THREAD)  \
    if (THREAD.joinable()) { \
        THREAD.join();       \
    }

#define JOIN_THREADS(THREADS)      \
    for (auto& thread : THREADS) { \
        JOIN_THREAD(thread);       \
    }

    JOIN_THREAD(_update_cache_expire_thread)
    JOIN_THREAD(_update_cache_evict_thread)
    JOIN_THREAD(_unused_rowset_monitor_thread)
    JOIN_THREAD(_garbage_sweeper_thread)
    JOIN_THREAD(_disk_stat_monitor_thread)
    wake_finish_publish_vesion_thread();
    JOIN_THREAD(_finish_publish_version_thread)

    JOIN_THREADS(_base_compaction_threads)
    JOIN_THREADS(_cumulative_compaction_threads)
    JOIN_THREADS(_update_compaction_threads)

    JOIN_THREAD(_repair_compaction_thread)

    JOIN_THREADS(_manual_compaction_threads)
    JOIN_THREADS(_tablet_checkpoint_threads)

    JOIN_THREAD(_pk_index_major_compaction_thread)

#ifndef USE_STAROS
    JOIN_THREAD(_local_pk_index_shard_data_gc_thread)
#endif

    JOIN_THREAD(_fd_cache_clean_thread)
    JOIN_THREAD(_adjust_cache_thread)

    if (config::path_gc_check) {
        JOIN_THREADS(_path_scan_threads)
        JOIN_THREADS(_path_gc_threads)
    }

#undef JOIN_THREADS
#undef JOIN_THREAD

    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& store_pair : _store_map) {
            delete store_pair.second;
            store_pair.second = nullptr;
        }
        _store_map.clear();
    }

    _checker_cv.notify_all();
    if (_compaction_checker_thread.joinable()) {
        _compaction_checker_thread.join();
    }

    if (_update_manager) {
        _update_manager->stop();
    }

    if (_compaction_manager) {
        _compaction_manager->stop();
    }
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id) {
    // clear transaction task may not contains partitions ids, we should get partition id from txn manager.
    std::vector<int64_t> partition_ids;
    StorageEngine::instance()->txn_manager()->get_partition_ids(transaction_id, &partition_ids);
    clear_transaction_task(transaction_id, partition_ids);
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id,
                                           const std::vector<TPartitionId>& partition_ids) {
    LOG(INFO) << "Clearing transaction task txn_id: " << transaction_id;

    for (const TPartitionId& partition_id : partition_ids) {
        std::map<TabletInfo, RowsetSharedPtr> tablet_infos;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(transaction_id, partition_id, &tablet_infos);

        // each tablet
        for (auto& tablet_info : tablet_infos) {
            // should use tablet uid to ensure clean txn correctly
            TabletSharedPtr tablet =
                    _tablet_manager->get_tablet(tablet_info.first.tablet_id, tablet_info.first.tablet_uid);
            // The tablet may be dropped or altered, leave a INFO log and go on process other tablet
            if (tablet == nullptr) {
                LOG(INFO) << "tablet is no longer exist, tablet_id=" << tablet_info.first.tablet_id
                          << " schema_hash=" << tablet_info.first.schema_hash
                          << " tablet_uid=" << tablet_info.first.tablet_uid;
                continue;
            }
            auto st = StorageEngine::instance()->txn_manager()->delete_txn(partition_id, tablet, transaction_id);
            st.permit_unchecked_error();
        }
    }
    LOG(INFO) << "Cleared transaction task txn_id: " << transaction_id;
}

void StorageEngine::_start_clean_fd_cache() {
    VLOG(10) << "Cleaning file descriptor cache";
    FdCache::Instance()->prune();
    VLOG(10) << "Cleaned file descriptor cache";
}

void StorageEngine::compaction_check() {
    int checker_one_round_sleep_time_s = 1800;
    while (!bg_worker_stopped()) {
        MonotonicStopWatch stop_watch;
        stop_watch.start();
        LOG(INFO) << "start to check compaction";
        size_t num = _compaction_check_one_round();
        stop_watch.stop();
        LOG(INFO) << num << " tablets checked. time elapse:" << stop_watch.elapsed_time() / 1000000000 << " seconds."
                  << " compaction checker will be scheduled again in " << checker_one_round_sleep_time_s << " seconds";
        std::unique_lock<std::mutex> lk(_checker_mutex);
        _checker_cv.wait_for(lk, std::chrono::seconds(checker_one_round_sleep_time_s),
                             [this] { return bg_worker_stopped(); });
    }
}

// Base compaction may be started by time(once every day now)
// Compaction checker will check whether to schedule base compaction for tablets
size_t StorageEngine::_compaction_check_one_round() {
    size_t batch_size = _compaction_manager->max_task_num();
    int batch_sleep_time_ms = 1000;
    std::vector<TabletSharedPtr> tablets;
    tablets.reserve(batch_size);
    size_t tablets_num_checked = 0;
    while (!bg_worker_stopped()) {
        // if compaction task is too many, skip this round
        if (_compaction_manager->candidates_size() <= batch_size) {
            bool finished = _tablet_manager->get_next_batch_tablets(batch_size, &tablets);
            for (auto& tablet : tablets) {
                _compaction_manager->update_tablet(tablet);
            }
            tablets_num_checked += tablets.size();
            tablets.clear();
            if (finished) {
                break;
            }
        }
        std::unique_lock<std::mutex> lk(_checker_mutex);
        _checker_cv.wait_for(lk, std::chrono::milliseconds(batch_sleep_time_ms),
                             [this] { return bg_worker_stopped(); });
    }
    return tablets_num_checked;
}

struct ManualCompactionTask {
    int64_t tablet_id{0};
    int64_t rowset_size_threshold{0};
    vector<uint32_t> input_rowset_ids;
    size_t total_bytes{0};
    int64_t start_time{0};
    int64_t end_time{0};
    std::string status;
};

static std::mutex manual_compaction_mutex;
static size_t total_manual_compaction_tasks_submitted = 0;
static size_t total_manual_compaction_tasks_finished = 0;
static size_t total_manual_compaction_bytes = 0;
static std::deque<ManualCompactionTask> manual_compaction_tasks;
static const int64_t MAX_MANUAL_COMPACTION_HISTORY = 1000;
static std::deque<ManualCompactionTask> manual_compaction_history;

void StorageEngine::submit_manual_compaction_task(int64_t tablet_id, int64_t rowset_size_threshold) {
    std::lock_guard lg(manual_compaction_mutex);
    auto& task = manual_compaction_tasks.emplace_back();
    task.tablet_id = tablet_id;
    task.rowset_size_threshold = rowset_size_threshold;
    total_manual_compaction_tasks_submitted++;
    LOG(INFO) << "submit manual compaction task tablet:" << tablet_id
              << " rowset_size_threshold:" << rowset_size_threshold;
}

std::string StorageEngine::get_manual_compaction_status() {
    std::ostringstream os;
    std::lock_guard lg(manual_compaction_mutex);
    os << "current task:" << manual_compaction_tasks.size() << "\ntablet_ids: ";
    for (auto& task : manual_compaction_tasks) {
        os << task.tablet_id << ",";
    }
    os << "\n";
    for (auto& t : manual_compaction_history) {
        os << "  tablet:" << t.tablet_id << " #rowset:" << t.input_rowset_ids.size() << " bytes:" << t.total_bytes
           << " start:" << t.start_time << " duration:" << t.end_time - t.start_time << " " << t.status << std::endl;
    }
    os << "history: " << manual_compaction_history.size() << " submitted:" << total_manual_compaction_tasks_submitted
       << " finished:" << total_manual_compaction_tasks_finished << " bytes:" << total_manual_compaction_bytes
       << std::endl;
    return os.str();
}

static void do_manual_compaction(TabletManager* tablet_manager, ManualCompactionTask& t) {
    auto tablet = tablet_manager->get_tablet(t.tablet_id);
    if (!tablet) {
        LOG(WARNING) << "repair compaction failed, tablet not found: " << t.tablet_id;
        return;
    }
    if (tablet->updates() == nullptr) {
        LOG(ERROR) << "manual compaction failed, tablet not primary key tablet found: " << t.tablet_id;
        return;
    }
    auto* mem_tracker = GlobalEnv::GetInstance()->compaction_mem_tracker();
    auto st = tablet->updates()->get_rowsets_for_compaction(t.rowset_size_threshold, t.input_rowset_ids, t.total_bytes);
    if (!st.ok()) {
        t.status = st.to_string();
        LOG(WARNING) << "get_rowsets_for_compaction failed: " << st.get_error_msg();
        return;
    }
    st = tablet->updates()->compaction(mem_tracker, t.input_rowset_ids);
    t.status = st.to_string();
    if (!st.ok()) {
        LOG(WARNING) << "manual compaction failed: " << st.get_error_msg() << " tablet:" << t.tablet_id;
        return;
    }
}

bool StorageEngine::_check_and_run_manual_compaction_task() {
    ManualCompactionTask t;
    {
        std::lock_guard lg(manual_compaction_mutex);
        if (manual_compaction_tasks.empty()) {
            return false;
        }
        t = manual_compaction_tasks.front();
        manual_compaction_tasks.pop_front();
    }
    t.start_time = UnixSeconds();
    do_manual_compaction(_tablet_manager.get(), t);
    t.end_time = UnixSeconds();
    {
        std::lock_guard lg(manual_compaction_mutex);
        total_manual_compaction_tasks_finished++;
        total_manual_compaction_bytes += t.total_bytes;
        manual_compaction_history.push_back(t);
        while (manual_compaction_history.size() > MAX_MANUAL_COMPACTION_HISTORY) {
            manual_compaction_history.pop_front();
        }
    }
    return true;
}

void* StorageEngine::_manual_compaction_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    Status status = Status::OK();
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        _check_and_run_manual_compaction_task();
        int64_t left_seconds = 5;
        while (!_bg_worker_stopped.load(std::memory_order_consume) && left_seconds > 0) {
            sleep(1);
            --left_seconds;
        }
    }
    return nullptr;
}

Status StorageEngine::_perform_cumulative_compaction(DataDir* data_dir,
                                                     std::pair<int32_t, int32_t> tablet_shards_range) {
    scoped_refptr<Trace> trace(new Trace);
    MonotonicStopWatch watch;
    watch.start();
    SCOPED_CLEANUP({
        if (watch.elapsed_time() / 1000000000 > config::compaction_trace_threshold) {
            LOG(INFO) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    })
    ADOPT_TRACE(trace.get())
    TRACE("start to perform cumulative compaction");
    TabletSharedPtr best_tablet = _tablet_manager->find_best_tablet_to_compaction(CompactionType::CUMULATIVE_COMPACTION,
                                                                                  data_dir, tablet_shards_range);
    if (best_tablet == nullptr) {
        return Status::NotFound("there are no suitable tablets");
    }
    TRACE("found best tablet $0", best_tablet->get_tablet_info().tablet_id);

    StarRocksMetrics::instance()->cumulative_compaction_request_total.increment(1);

    std::unique_ptr<MemTracker> mem_tracker =
            std::make_unique<MemTracker>(MemTracker::COMPACTION, -1, "", _options.compaction_mem_tracker);
    CumulativeCompaction cumulative_compaction(mem_tracker.get(), best_tablet);

    Status res = cumulative_compaction.compact();
    if (!res.ok()) {
        if (!res.is_mem_limit_exceeded()) {
            best_tablet->set_last_cumu_compaction_failure_time(UnixMillis());
            best_tablet->set_last_cumu_compaction_failure_status(res.code());
        }
        if (!res.is_not_found()) {
            StarRocksMetrics::instance()->cumulative_compaction_request_failed.increment(1);
            LOG(WARNING) << "Fail to vectorized compact table=" << best_tablet->full_name()
                         << ", err=" << res.to_string();
        }
        return res;
    }

    best_tablet->set_last_cumu_compaction_failure_time(0);
    best_tablet->set_last_cumu_compaction_failure_status(TStatusCode::OK);
    return Status::OK();
}

Status StorageEngine::_perform_base_compaction(DataDir* data_dir, std::pair<int32_t, int32_t> tablet_shards_range) {
    scoped_refptr<Trace> trace(new Trace);
    MonotonicStopWatch watch;
    watch.start();
    SCOPED_CLEANUP({
        if (watch.elapsed_time() / 1000000000 > config::compaction_trace_threshold) {
            LOG(INFO) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    })
    ADOPT_TRACE(trace.get())
    TRACE("start to perform base compaction");
    TabletSharedPtr best_tablet = _tablet_manager->find_best_tablet_to_compaction(CompactionType::BASE_COMPACTION,
                                                                                  data_dir, tablet_shards_range);
    if (best_tablet == nullptr) {
        return Status::NotFound("there are no suitable tablets");
    }
    TRACE("found best tablet $0", best_tablet->get_tablet_info().tablet_id);

    StarRocksMetrics::instance()->base_compaction_request_total.increment(1);

    std::unique_ptr<MemTracker> mem_tracker =
            std::make_unique<MemTracker>(MemTracker::COMPACTION, -1, "", _options.compaction_mem_tracker);
    BaseCompaction base_compaction(mem_tracker.get(), best_tablet);

    Status res = base_compaction.compact();
    if (!res.ok()) {
        best_tablet->set_last_base_compaction_failure_time(UnixMillis());
        if (!res.is_not_found()) {
            StarRocksMetrics::instance()->base_compaction_request_failed.increment(1);
            LOG(WARNING) << "failed to init vectorized base compaction. res=" << res.to_string()
                         << ", table=" << best_tablet->full_name();
        }
        return res;
    }

    best_tablet->set_last_base_compaction_failure_time(0);
    return Status::OK();
}

Status StorageEngine::_perform_update_compaction(DataDir* data_dir) {
    scoped_refptr<Trace> trace(new Trace);
    MonotonicStopWatch watch;
    watch.start();
    SCOPED_CLEANUP({
        if (watch.elapsed_time() / 1000000000 > config::compaction_trace_threshold) {
            LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    })
    ADOPT_TRACE(trace.get())
    TRACE("start to perform update compaction");
    TabletSharedPtr best_tablet = _tablet_manager->find_best_tablet_to_do_update_compaction(data_dir);
    if (best_tablet == nullptr) {
        return Status::NotFound("there are no suitable tablets");
    }
    if (best_tablet->updates() == nullptr) {
        return Status::InternalError("not an updatable tablet");
    }
    TRACE("found best tablet $0", best_tablet->tablet_id());

    // The concurrency of migration and compaction will lead to inconsistency between the meta and
    // primary index cache of the new tablet. So we should abort the compaction for the old tablet
    // when executing migration.
    std::shared_lock rlock(best_tablet->get_migration_lock(), std::try_to_lock);
    if (!rlock.owns_lock()) {
        return Status::InternalError("Fail to get migration lock, tablet_id: {}", best_tablet->tablet_id());
    }
    if (Tablet::check_migrate(best_tablet)) {
        return Status::InternalError("Fail to check migrate tablet, tablet_id: {}", best_tablet->tablet_id());
    }

    Status res;
    int64_t duration_ns = 0;
    {
        StarRocksMetrics::instance()->update_compaction_request_total.increment(1);
        SCOPED_RAW_TIMER(&duration_ns);

        std::unique_ptr<MemTracker> mem_tracker =
                std::make_unique<MemTracker>(MemTracker::COMPACTION, -1, "", _options.compaction_mem_tracker);
        res = best_tablet->updates()->compaction(mem_tracker.get());
    }
    StarRocksMetrics::instance()->update_compaction_duration_us.increment(duration_ns / 1000);
    if (!res.ok()) {
        StarRocksMetrics::instance()->update_compaction_request_failed.increment(1);
        LOG(WARNING) << "failed to perform update compaction. res=" << res.to_string()
                     << ", tablet=" << best_tablet->full_name();
        return res;
    }
    return Status::OK();
}

Status StorageEngine::_start_trash_sweep(double* usage) {
    LOG(INFO) << "start to sweep trash";
    Status res = Status::OK();

    const int32_t snapshot_expire = config::snapshot_expire_time_sec;
    const int32_t trash_expire = config::trash_file_expire_time_sec;
    const double guard_space = config::storage_flood_stage_usage_percent / 100.0;
    std::vector<DataDirInfo> data_dir_infos;
    get_all_data_dir_info(&data_dir_infos, false);

    time_t now = time(nullptr);
    tm local_tm_now;
    memset(&local_tm_now, 0, sizeof(tm));

    if (localtime_r(&now, &local_tm_now) == nullptr) {
        return Status::InternalError(fmt::format("Fail to localtime_r time: {}", now));
    }
    const time_t local_now = mktime(&local_tm_now);

    double max_usage = 0.0;
    for (DataDirInfo& info : data_dir_infos) {
        if (!info.is_used) {
            continue;
        }

        double curr_usage = (double)(info.disk_capacity - info.available) / info.disk_capacity;
        max_usage = std::max(max_usage, curr_usage);

        Status curr_res = Status::OK();
        std::string snapshot_path = info.path + SNAPSHOT_PREFIX;
        curr_res = _do_sweep(snapshot_path, local_now, snapshot_expire);
        if (!curr_res.ok()) {
            LOG(WARNING) << "failed to sweep snapshot. path=" << snapshot_path << ", err_code=" << curr_res;
            res = curr_res;
        }

        std::string trash_path = info.path + TRASH_PREFIX;
        curr_res = _do_sweep(trash_path, local_now, curr_usage > guard_space ? 0 : trash_expire);
        if (!curr_res.ok()) {
            LOG(WARNING) << "failed to sweep trash. [path=%s" << trash_path << ", err_code=" << curr_res;
            res = curr_res;
        }
    }
    if (usage != nullptr) {
        *usage = max_usage;
    }

    // clear expire incremental rowset, move deleted tablet to trash
    CHECK(_tablet_manager->start_trash_sweep().ok());

    // clean rubbish transactions
    _clean_unused_txns();

    // clean unused rowset metas in KVStore
    _clean_unused_rowset_metas();

    do_manual_compact(false);

    return res;
}

void StorageEngine::do_manual_compact(bool force_compact) {
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        uint64_t live_sst_files_size_before = 0;
        if (!data_dir->get_meta()->get_live_sst_files_size(&live_sst_files_size_before)) {
            LOG(WARNING) << "data dir " << data_dir->path() << " get_live_sst_files_size failed";
            continue;
        }
        if (force_compact || live_sst_files_size_before > config::meta_threshold_to_manual_compact) {
            Status s = data_dir->get_meta()->compact();
            if (!s.ok()) {
                LOG(WARNING) << "data dir " << data_dir->path() << " manual compact meta failed: " << s;
            } else {
                uint64_t live_sst_files_size_after = 0;
                if (!data_dir->get_meta()->get_live_sst_files_size(&live_sst_files_size_after)) {
                    LOG(WARNING) << "data dir " << data_dir->path() << " get_live_sst_files_size failed";
                }
                LOG(INFO) << "data dir " << data_dir->path() << " manual compact meta successfully, "
                          << "live_sst_files_size_before: " << live_sst_files_size_before
                          << " live_sst_files_size_after: " << live_sst_files_size_after
                          << data_dir->get_meta()->get_stats();
            }
        }
    }
}

void StorageEngine::_clean_unused_rowset_metas() {
    size_t total_rowset_meta_count = 0;
    std::vector<std::pair<TabletUid, RowsetId>> invalid_rowsets;
    auto clean_rowset_func = [&](const TabletUid& tablet_uid, RowsetId rowset_id, std::string_view meta_str) -> bool {
        total_rowset_meta_count++;
        bool parsed = false;
        auto rowset_meta = std::make_shared<RowsetMeta>(meta_str, &parsed);
        if (!parsed) {
            LOG(WARNING) << "parse rowset meta string failed, remove rowset meta, rowset_id:" << rowset_id
                         << " tablet_uid: " << tablet_uid;
            invalid_rowsets.emplace_back(rowset_meta->tablet_uid(), rowset_meta->rowset_id());
            return true;
        }
        if (rowset_meta->tablet_uid() != tablet_uid) {
            LOG(WARNING) << "tablet uid is not match, remove rowset meta, rowset_id=" << rowset_meta->rowset_id()
                         << " tablet: " << rowset_meta->tablet_id() << ", tablet_uid_in_key=" << tablet_uid
                         << ", tablet_uid in rowset meta=" << rowset_meta->tablet_uid();
            invalid_rowsets.emplace_back(rowset_meta->tablet_uid(), rowset_meta->rowset_id());
            return true;
        }

        TabletSharedPtr tablet = _tablet_manager->get_tablet(rowset_meta->tablet_id(), tablet_uid);
        if (tablet == nullptr) {
            // maybe too many due to historical bug, limit logging
            LOG_EVERY_SECOND(WARNING) << "tablet not found, remove rowset meta, rowset_id=" << rowset_meta->rowset_id()
                                      << " tablet: " << rowset_meta->tablet_id();
            invalid_rowsets.emplace_back(rowset_meta->tablet_uid(), rowset_meta->rowset_id());
            return true;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE && (!tablet->rowset_meta_is_useful(rowset_meta))) {
            LOG(INFO) << "rowset meta is useless, remove rowset meta, rowset_id=" << rowset_meta->rowset_id()
                      << " tablet:" << tablet->tablet_id();
            invalid_rowsets.emplace_back(rowset_meta->tablet_uid(), rowset_meta->rowset_id());
        }
        return true;
    };
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        int64_t start_ts = MonotonicMillis();
        (void)RowsetMetaManager::traverse_rowset_metas(data_dir->get_meta(), clean_rowset_func);
        if (!invalid_rowsets.empty()) {
            for (auto& rowset : invalid_rowsets) {
                (void)RowsetMetaManager::remove(data_dir->get_meta(), rowset.first, rowset.second);
            }
            LOG(WARNING) << "traverse_rowset_meta and remove " << invalid_rowsets.size() << "/"
                         << total_rowset_meta_count << " invalid rowset metas, path:" << data_dir->path()
                         << " duration:" << (MonotonicMillis() - start_ts) << "ms";
            invalid_rowsets.clear();
        }
    }
}

void StorageEngine::_clean_unused_txns() {
    std::set<TabletInfo> tablet_infos;
    _txn_manager->get_all_related_tablets(&tablet_infos);
    for (auto& tablet_info : tablet_infos) {
        TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.tablet_id, tablet_info.tablet_uid, true);
        if (tablet == nullptr) {
            // TODO(ygl) :  should check if tablet still in meta, it's a improvement
            // case 1: tablet still in meta, just remove from memory
            // case 2: tablet not in meta store, remove rowset from meta
            // currently just remove them from memory
            // nullptr to indicate not remove them from meta store
            _txn_manager->force_rollback_tablet_related_txns(nullptr, tablet_info.tablet_id, tablet_info.schema_hash,
                                                             tablet_info.tablet_uid);
        }
    }
}

Status StorageEngine::_do_sweep(const std::string& scan_root, const time_t& local_now, const int32_t expire) {
    Status res = Status::OK();
    if (!fs::path_exist(scan_root)) {
        // dir not existed. no need to sweep trash.
        return res;
    }

    try {
        for (const auto& item : std::filesystem::directory_iterator(scan_root)) {
            std::string path_name = item.path().string();
            std::string dir_name = item.path().filename().string();
            std::string str_time = dir_name.substr(0, dir_name.find('.'));
            tm local_tm_create;
            memset(&local_tm_create, 0, sizeof(tm));

            if (strptime(str_time.c_str(), "%Y%m%d%H%M%S", &local_tm_create) == nullptr) {
                LOG(WARNING) << "Fail to strptime time:" << str_time;
                res = Status::InternalError(fmt::format("Fail to strptime time: {}", str_time));
                continue;
            }

            int32_t actual_expire = expire;
            // try get timeout in dir name, the old snapshot dir does not contain timeout
            // eg: 20190818221123.3.86400, the 86400 is timeout, in second
            size_t pos = dir_name.find('.', str_time.size() + 1);
            if (pos != std::string::npos) {
                actual_expire = std::stoi(dir_name.substr(pos + 1));
            }
            VLOG(10) << "get actual expire time " << actual_expire << " of dir: " << dir_name;

            if (difftime(local_now, mktime(&local_tm_create)) >= actual_expire) {
                Status ret = fs::remove_all(path_name);
                if (!ret.ok()) {
                    LOG(WARNING) << "fail to remove file. path: " << path_name << ", error: " << ret.to_string();
                    res = Status::IOError(
                            fmt::format("Fail remove file. path: {}, error: {}", path_name, ret.to_string()));
                    continue;
                }
            }
        }
    } catch (...) {
        LOG(WARNING) << "Exception occur when scan directory. path=" << scan_root;
        res = Status::IOError(fmt::format("Exception occur when scan directory. path: {}", scan_root));
    }

    return res;
}

Status StorageEngine::_get_remote_next_increment_id_interval(const TAllocateAutoIncrementIdParam& request,
                                                             TAllocateAutoIncrementIdResult* result) {
    TNetworkAddress master_addr = get_master_address();
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->allocAutoIncrementId(*result, request); });
}

double StorageEngine::delete_unused_rowset() {
    MonotonicStopWatch timer;
    timer.start();
    std::vector<RowsetSharedPtr> delete_rowsets;
    constexpr int64_t batch_size = 4096;
    double deleted_pct = 1;
    delete_rowsets.reserve(batch_size);
    {
        std::lock_guard lock(_gc_mutex);
        for (auto it = _unused_rowsets.begin(); it != _unused_rowsets.end();) {
            if (it->second.use_count() != 1) {
                ++it;
            } else if (it->second->need_delete_file()) {
                delete_rowsets.emplace_back(it->second);
                it = _unused_rowsets.erase(it);
            }
            if (delete_rowsets.size() >= batch_size) {
                deleted_pct = static_cast<double>(batch_size) / (_unused_rowsets.size() + batch_size);
                break;
            }
        }
    }

    if (delete_rowsets.empty()) {
        return deleted_pct;
    }
    auto collect_time = timer.elapsed_time() / (1000 * 1000);
    for (auto& rowset : delete_rowsets) {
        VLOG(3) << "start to remove rowset:" << rowset->rowset_id() << ", version:" << rowset->version().first << "-"
                << rowset->version().second;
        Status status = rowset->remove();
        LOG_IF(WARNING, !status.ok()) << "remove rowset:" << rowset->rowset_id() << " finished. status:" << status;
        if (rowset->keys_type() != PRIMARY_KEYS) {
            clear_rowset_delta_column_group_cache(*rowset.get());
            status = rowset->remove_delta_column_group();
        }
        LOG_IF(WARNING, !status.ok()) << "remove delta column group error rowset:" << rowset->rowset_id()
                                      << " finished. status:" << status;
    }
    LOG(INFO) << "remove " << delete_rowsets.size() << " rowsets collect cost " << collect_time << "ms total "
              << timer.elapsed_time() / (1000 * 1000) << "ms";

    return deleted_pct;
}

void StorageEngine::add_unused_rowset(const RowsetSharedPtr& rowset) {
    if (rowset == nullptr) {
        return;
    }

    VLOG(3) << "add unused rowset, rowset id:" << rowset->rowset_id() << ", version:" << rowset->version()
            << ", unique id:" << rowset->unique_id();

    auto rowset_id = rowset->rowset_id().to_string();

    std::lock_guard lock(_gc_mutex);
    if (_unused_rowsets.find(rowset_id) == _unused_rowsets.end()) {
        rowset->set_need_delete_file();
        rowset->close();
        _unused_rowsets[rowset_id] = rowset;
        release_rowset_id(rowset->rowset_id());
    }
}

Status StorageEngine::create_tablet(const TCreateTabletReq& request) {
    // Get all available stores, use ref_root_path if the caller specified
    std::vector<DataDir*> stores;
    stores = get_stores_for_create_tablet(request.storage_medium);
    if (stores.empty()) {
        LOG(WARNING) << "there is no available disk that can be used to create tablet.";
        return Status::InvalidArgument("empty store limit in request");
    }
    return _tablet_manager->create_tablet(request, stores);
}

Status StorageEngine::obtain_shard_path(TStorageMedium::type storage_medium, int64_t path_hash, std::string* shard_path,
                                        DataDir** store) {
    DCHECK(shard_path != nullptr) << "Null pointer";

    if (path_hash != -1) {
        // get store by path hash
        *store = StorageEngine::instance()->get_store(path_hash);
        if (*store == nullptr) {
            LOG(WARNING) << "Fail to get store. path_hash=" << path_hash;
            return Status::InternalError(fmt::format("Fail to get store. path_hash: {}", path_hash));
        }
    } else {
        // get store randomly by the specified medium
        auto stores = get_stores_for_create_tablet(storage_medium);
        if (stores.empty()) {
            LOG(WARNING) << "There is no suitable DataDir";
            return Status::InternalError("There is no suitable DataDir");
        }
        *store = stores[0];
    }

    uint64_t shard = 0;
    RETURN_IF_ERROR((*store)->get_shard(&shard));

    std::stringstream root_path_stream;
    root_path_stream << (*store)->path() << DATA_PREFIX << "/" << shard;
    *shard_path = root_path_stream.str();

    LOG(INFO) << "Obtained shard path=" << *shard_path;
    return Status::OK();
}

Status StorageEngine::load_header(const std::string& shard_path, const TCloneReq& request, bool restore,
                                  bool is_primary_key) {
    DataDir* store = nullptr;
    {
        try {
            auto store_path = std::filesystem::path(shard_path).parent_path().parent_path().string();
            store = get_store(store_path);
            if (store == nullptr) {
                LOG(WARNING) << "Invalid shard path=" << shard_path << "tablet_id=" << request.tablet_id;
                return Status::InternalError(
                        fmt::format("Invalid shard_path: {}, tablet_id: {}", shard_path, request.tablet_id));
            }
        } catch (...) {
            return Status::InternalError(
                    fmt::format("Invalid shard_path: {}, tablet_id: {}", shard_path, request.tablet_id));
        }
    }

    std::stringstream schema_hash_path_stream;
    schema_hash_path_stream << shard_path << "/" << request.tablet_id << "/" << request.schema_hash;
    // not surely, reload and restore tablet action call this api
    Status st;
    if (!is_primary_key) {
        st = _tablet_manager->load_tablet_from_dir(store, request.tablet_id, request.schema_hash,
                                                   schema_hash_path_stream.str(), false, restore);
    } else {
        st = _tablet_manager->create_tablet_from_meta_snapshot(store, request.tablet_id, request.schema_hash,
                                                               schema_hash_path_stream.str(), true);
    }

    if (!st.ok()) {
        LOG(WARNING) << "Fail to load headers, "
                     << "tablet_id=" << request.tablet_id << " status=" << st;
        return st;
    }
    LOG(INFO) << "Loaded headers tablet_id=" << request.tablet_id << " schema_hash=" << request.schema_hash;
    return Status::OK();
}

Status StorageEngine::execute_task(EngineTask* task) {
    return task->execute();
}

// check whether any unused rowsets's id equal to rowset_id
bool StorageEngine::check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id) {
    std::lock_guard lock(_gc_mutex);
    auto search = _unused_rowsets.find(rowset_id.to_string());
    return search != _unused_rowsets.end();
}

void StorageEngine::increase_update_compaction_thread(const int num_threads_per_disk) {
    // convert store map to vector
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
    }
    const auto data_dir_num = static_cast<int32_t>(data_dirs.size());
    const int32_t cur_threads_per_disk = _update_compaction_threads.size() / data_dir_num;
    if (num_threads_per_disk <= cur_threads_per_disk) {
        LOG(WARNING) << fmt::format("not support decrease update compaction thread, from {} to {}",
                                    cur_threads_per_disk, num_threads_per_disk);
        return;
    }
    for (uint32_t i = 0; i < data_dir_num * (num_threads_per_disk - cur_threads_per_disk); ++i) {
        _update_compaction_threads.emplace_back([this, data_dir_num, data_dirs, i] {
            _update_compaction_thread_callback(nullptr, data_dirs[i % data_dir_num]);
        });
        Thread::set_thread_name(_update_compaction_threads.back(), "update_compact");
    }
}

void StorageEngine::remove_increment_map_by_table_id(int64_t table_id) {
    std::unique_lock<std::mutex> l(_auto_increment_mutex);
    if (_auto_increment_meta_map.find(table_id) != _auto_increment_meta_map.end()) {
        _auto_increment_meta_map.erase(_auto_increment_meta_map.find(table_id));
    }
}

Status StorageEngine::get_next_increment_id_interval(int64_t tableid, size_t num_row, std::vector<int64_t>& ids) {
    if (num_row == 0) {
        return Status::OK();
    }
    DCHECK_EQ(num_row, ids.size());

    std::shared_ptr<AutoIncrementMeta> meta;

    {
        std::unique_lock<std::mutex> l(_auto_increment_mutex);

        if (UNLIKELY(_auto_increment_meta_map.find(tableid) == _auto_increment_meta_map.end())) {
            _auto_increment_meta_map.insert({tableid, std::make_shared<AutoIncrementMeta>()});
        }
        meta = _auto_increment_meta_map[tableid];
        TEST_SYNC_POINT_CALLBACK("StorageEngine::get_next_increment_id_interval.1", &meta);
    }

    // lock for different tableid
    std::unique_lock<std::mutex> l(meta->mutex);

    // avaliable id interval: [cur_avaliable_min_id, cur_max_id)
    int64_t& cur_avaliable_min_id = meta->min;
    int64_t& cur_max_id = meta->max;
    CHECK_GE(cur_max_id, cur_avaliable_min_id);

    size_t cur_avaliable_rows = cur_max_id - cur_avaliable_min_id;
    auto ids_iter = ids.begin();
    if (UNLIKELY(num_row > cur_avaliable_rows)) {
        size_t alloc_rows = num_row - cur_avaliable_rows;
        // rpc request for the new available interval from fe
        TAllocateAutoIncrementIdParam alloc_params;
        TAllocateAutoIncrementIdResult alloc_result;

        alloc_params.__set_table_id(tableid);
        alloc_params.__set_rows(alloc_rows);

        auto st = _get_remote_next_increment_id_interval(alloc_params, &alloc_result);

        if (!st.ok() || alloc_result.status.status_code != TStatusCode::OK) {
            std::stringstream err_msg;
            for (auto& msg : alloc_result.status.error_msgs) {
                err_msg << msg;
            }

            return Status::InternalError("auto increment allocate failed, err msg: " + err_msg.str());
        }

        if (cur_avaliable_rows > 0) {
            std::iota(ids_iter, ids_iter + cur_avaliable_rows, cur_avaliable_min_id);
            ids_iter += cur_avaliable_rows;
        }

        cur_avaliable_min_id = alloc_result.auto_increment_id;
        cur_max_id = alloc_result.auto_increment_id + alloc_result.allocated_rows;

        num_row -= cur_avaliable_rows;
    }

    std::iota(ids_iter, ids.end(), cur_avaliable_min_id);
    cur_avaliable_min_id += num_row;

    return Status::OK();
}

// calc the total memory usage of delta column group list, can be optimazed later if need.
size_t StorageEngine::delta_column_group_list_memory_usage(const DeltaColumnGroupList& dcgs) {
    size_t res = 0;
    for (const auto& dcg : dcgs) {
        res += dcg->memory_usage();
    }
    return res;
}

// Search delta column group from `all_dcgs` which version isn't larger than `version`.
// Using it because we record all version dcgs in cache
void StorageEngine::search_delta_column_groups_by_version(const DeltaColumnGroupList& all_dcgs, int64_t version,
                                                          DeltaColumnGroupList* dcgs) {
    for (const auto& dcg : all_dcgs) {
        if (dcg->version() <= version) {
            dcgs->push_back(dcg);
        }
    }
}

Status StorageEngine::get_delta_column_group(KVStore* meta, int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id,
                                             int64_t version, DeltaColumnGroupList* dcgs) {
    StarRocksMetrics::instance()->delta_column_group_get_non_pk_total.increment(1);
    // Currently non-Primary Key tablet can generate cols file only through
    // schema change which will change tablet_id. Every time we do schema change
    // we will get cache miss and guarantee that we always can get the newest
    // cols file.
    DeltaColumnGroupKey dcg_key;
    dcg_key.tablet_id = tablet_id;
    dcg_key.rowsetid = rowsetid;
    dcg_key.segment_id = segment_id;
    {
        // find in delta column group cache
        std::lock_guard<std::mutex> lg(_delta_column_group_cache_lock);
        auto itr = _delta_column_group_cache.find(dcg_key);
        if (itr != _delta_column_group_cache.end()) {
            search_delta_column_groups_by_version(itr->second, version, dcgs);
            StarRocksMetrics::instance()->delta_column_group_get_non_pk_hit_cache.increment(1);
            return Status::OK();
        }
    }
    // find from rocksdb
    DeltaColumnGroupList new_dcgs;
    RETURN_IF_ERROR(
            TabletMetaManager::get_delta_column_group(meta, tablet_id, rowsetid, segment_id, INT64_MAX, &new_dcgs));
    search_delta_column_groups_by_version(new_dcgs, version, dcgs);
    {
        // fill delta column group cache
        std::lock_guard<std::mutex> lg(_delta_column_group_cache_lock);
        bool ok = _delta_column_group_cache.insert({dcg_key, new_dcgs}).second;
        if (ok) {
            // insert success
            _delta_column_group_cache_mem_tracker->consume(delta_column_group_list_memory_usage(new_dcgs));
        }
    }
    return Status::OK();
}

Status StorageEngine::_clear_persistent_index(DataDir* data_dir, int64_t tablet_id, const std::string& dir) {
    // remove meta in RocksDB
    WriteBatch wb;
    auto status = TabletMetaManager::clear_persistent_index(data_dir, &wb, tablet_id);
    if (status.ok()) {
        status = data_dir->get_meta()->write_batch(&wb);
        if (!status.ok()) {
            LOG(WARNING) << "fail to remove persistent index meta, tablet_id=[" + std::to_string(tablet_id)
                         << "] error[" << status.to_string() << "]";
        } else {
            // remove tablet persistent_index dir
            status = fs::remove_all(dir);
            if (!status.ok()) {
                LOG(WARNING) << "fail to remove local persistent index dir=[" + dir << "] error[" << status.to_string()
                             << "]";
            }
        }
    }

    return status;
}

void StorageEngine::clear_cached_delta_column_group(const std::vector<DeltaColumnGroupKey>& dcg_keys) {
    std::lock_guard<std::mutex> lg(_delta_column_group_cache_lock);
    for (const auto& dcg_key : dcg_keys) {
        auto itr = _delta_column_group_cache.find(dcg_key);
        if (itr != _delta_column_group_cache.end()) {
            _delta_column_group_cache_mem_tracker->release(
                    StorageEngine::instance()->delta_column_group_list_memory_usage(itr->second));
            _delta_column_group_cache.erase(itr);
        }
    }
}

void StorageEngine::clear_rowset_delta_column_group_cache(const Rowset& rowset) {
    StorageEngine::instance()->clear_cached_delta_column_group([&]() {
        std::vector<DeltaColumnGroupKey> dcg_keys;
        dcg_keys.reserve(rowset.num_segments());
        for (auto i = 0; i < rowset.num_segments(); i++) {
            dcg_keys.emplace_back(
                    DeltaColumnGroupKey(rowset.rowset_meta()->tablet_id(), rowset.rowset_meta()->rowset_id(), i));
        }
        return dcg_keys;
    }());
}

} // namespace starrocks
