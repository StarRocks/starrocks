// This file is made available under Elastic License 2.0.
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

#include <rapidjson/document.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <new>
#include <queue>
#include <random>
#include <set>

#include "common/status.h"
#include "env/env.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/async_delta_writer_executor.h"
#include "storage/data_dir.h"
#include "storage/fs/file_block_manager.h"
#include "storage/lru_cache.h"
#include "storage/memtable_flush_executor.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/rowset/unique_rowset_id_generator.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "storage/utils.h"
#include "storage/vectorized/base_compaction.h"
#include "storage/vectorized/cumulative_compaction.h"
#include "util/defer_op.h"
#include "util/file_utils.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

StorageEngine* StorageEngine::_s_instance = nullptr;

static Status _validate_options(const EngineOptions& options) {
    if (options.store_paths.empty()) {
        return Status::InternalError("store paths is empty");
    }
    return Status::OK();
}

Status StorageEngine::open(const EngineOptions& options, StorageEngine** engine_ptr) {
    RETURN_IF_ERROR(_validate_options(options));
    LOG(INFO) << "Opening storage engine using uid=" << options.backend_uid.to_string();
    std::unique_ptr<StorageEngine> engine = std::make_unique<StorageEngine>(options);
    RETURN_IF_ERROR_WITH_WARN(engine->_open(), "open engine failed");
    *engine_ptr = engine.release();
    LOG(INFO) << "Opened storage engine";
    return Status::OK();
}

StorageEngine::StorageEngine(const EngineOptions& options)
        : _options(options),
          _available_storage_medium_type_count(0),
          _effective_cluster_id(-1),
          _is_all_cluster_id_exist(true),
          _file_cache(nullptr),
          _tablet_manager(new TabletManager(options.metadata_mem_tracker, config::tablet_map_shard_size)),
          _txn_manager(new TxnManager(config::txn_map_shard_size, config::txn_shard_size, options.store_paths.size())),
          _rowset_id_generator(new UniqueRowsetIdGenerator(options.backend_uid)),
          _memtable_flush_executor(nullptr),
          _block_manager(nullptr),
          _update_manager(new UpdateManager(options.update_mem_tracker)) {
    if (_s_instance == nullptr) {
        _s_instance = this;
    }
    REGISTER_GAUGE_STARROCKS_METRIC(unused_rowsets_count, [this]() {
        std::lock_guard lock(_gc_mutex);
        return _unused_rowsets.size();
    });
}

StorageEngine::~StorageEngine() {
#ifdef BE_TEST
    if (_s_instance == this) {
        _s_instance = nullptr;
    }
#endif
}

void StorageEngine::load_data_dirs(const std::vector<DataDir*>& data_dirs) {
    std::vector<std::thread> threads;
    threads.reserve(data_dirs.size());
    for (auto data_dir : data_dirs) {
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
            auto res = data_dir->load();
            if (!res.ok()) {
                LOG(WARNING) << "Fail to load data dir=" << data_dir->path() << ", res=" << res.to_string();
            }
        });
        Thread::set_thread_name(threads.back(), "load_data_dir");
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

Status StorageEngine::_open() {
    // init store_map
    RETURN_IF_ERROR_WITH_WARN(_init_store_map(), "_init_store_map failed");

    _effective_cluster_id = config::cluster_id;
    RETURN_IF_ERROR_WITH_WARN(_check_all_root_path_cluster_id(), "fail to check cluster id");

    _update_storage_medium_type_count();

    RETURN_IF_ERROR_WITH_WARN(_check_file_descriptor_number(), "check fd number failed");

    _index_stream_lru_cache = new_lru_cache(config::index_stream_cache_capacity);

    _file_cache.reset(new_lru_cache(config::file_descriptor_cache_capacity));

    fs::BlockManagerOptions bm_opts;
    bm_opts.read_only = false;
    // |_block_manager| depend on |_file_cache|.
    _block_manager = std::make_unique<fs::FileBlockManager>(Env::Default(), std::move(bm_opts));

    // |_update_manager| depend on |_block_manager|.
    // |fs::fs_util::block_manager| depends on ExecEnv's storage engine
    starrocks::ExecEnv::GetInstance()->set_storage_engine(this);
    RETURN_IF_ERROR_WITH_WARN(_update_manager->init(), "init update_manager failed");

    auto dirs = get_stores<false>();

    // `load_data_dirs` depend on |_update_manager|.
    load_data_dirs(dirs);

    _async_delta_writer_executor = std::make_unique<AsyncDeltaWriterExecutor>();
    RETURN_IF_ERROR_WITH_WARN(_async_delta_writer_executor->init(), "init AsyncDeltaWriterExecutor failed");

    _memtable_flush_executor = std::make_unique<MemTableFlushExecutor>();
    RETURN_IF_ERROR_WITH_WARN(_memtable_flush_executor->init(dirs), "init MemTableFlushExecutor failed");

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
        DataDir* store = new DataDir(path.path, path.storage_medium, _tablet_manager.get(), _txn_manager.get());
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

Status StorageEngine::get_all_data_dir_info(vector<DataDirInfo>* data_dir_infos, bool need_update) {
    data_dir_infos->clear();

    MonotonicStopWatch timer;
    timer.start();

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

    timer.stop();
    return Status::OK();
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
        if (difftime(time(NULL), last_sweep_time) > valid_sweep_interval && it.second->reach_capacity_limit(0)) {
            std::unique_lock<std::mutex> lk(_trash_sweeper_mutex);
            _trash_sweeper_cv.notify_one();
            last_sweep_time = time(NULL);
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
    if (_effective_cluster_id != -1 && !_is_all_cluster_id_exist) {
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

std::vector<DataDir*> StorageEngine::get_stores_for_create_tablet(TStorageMedium::type storage_medium) {
    std::vector<DataDir*> stores;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {
            if (it.second->is_used()) {
                if (_available_storage_medium_type_count == 1 || it.second->storage_medium() == storage_medium) {
                    stores.push_back(it.second);
                }
            }
        }
    }
    //  TODO(lingbin): should it be a global util func?
    std::srand(std::random_device()());
    std::shuffle(stores.begin(), stores.end(), std::mt19937(std::random_device()()));
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

    (void)_tablet_manager->drop_tablets_on_error_root_path(tablet_info_vec);
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
    }

    _bg_worker_stopped.store(true, std::memory_order_release);

    if (_update_cache_expire_thread.joinable()) {
        _update_cache_expire_thread.join();
    }
    if (_unused_rowset_monitor_thread.joinable()) {
        _unused_rowset_monitor_thread.join();
    }
    if (_garbage_sweeper_thread.joinable()) {
        _garbage_sweeper_thread.join();
    }
    if (_disk_stat_monitor_thread.joinable()) {
        _disk_stat_monitor_thread.join();
    }
    for (auto& thread : _base_compaction_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    for (auto& thread : _cumulative_compaction_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    for (auto& thread : _update_compaction_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    for (auto& thread : _tablet_checkpoint_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    if (_fd_cache_clean_thread.joinable()) {
        _fd_cache_clean_thread.join();
    }
    if (config::path_gc_check) {
        for (auto& thread : _path_scan_threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        for (auto& thread : _path_gc_threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& store_pair : _store_map) {
            delete store_pair.second;
            store_pair.second = nullptr;
        }
        _store_map.clear();
    }

    SAFE_DELETE(_index_stream_lru_cache);
    _file_cache.reset();
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
            StorageEngine::instance()->txn_manager()->delete_txn(partition_id, tablet, transaction_id);
        }
    }
    LOG(INFO) << "Cleared transaction task txn_id: " << transaction_id;
}

void StorageEngine::_start_clean_fd_cache() {
    VLOG(10) << "Cleaning file descriptor cache";
    _file_cache->prune();
    VLOG(10) << "Cleaned file descriptor cache";
}

Status StorageEngine::_perform_cumulative_compaction(DataDir* data_dir,
                                                     std::pair<int32_t, int32_t> tablet_shards_range) {
    scoped_refptr<Trace> trace(new Trace);
    MonotonicStopWatch watch;
    watch.start();
    SCOPED_CLEANUP({
        if (watch.elapsed_time() / 1e9 > config::cumulative_compaction_trace_threshold) {
            LOG(INFO) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    });
    ADOPT_TRACE(trace.get());
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
    vectorized::CumulativeCompaction cumulative_compaction(mem_tracker.get(), best_tablet);

    Status res = cumulative_compaction.compact();
    if (!res.ok()) {
        if (!res.is_mem_limit_exceeded()) {
            best_tablet->set_last_cumu_compaction_failure_time(UnixMillis());
        }
        if (!res.is_not_found()) {
            StarRocksMetrics::instance()->cumulative_compaction_request_failed.increment(1);
            LOG(WARNING) << "Fail to vectorized compact table=" << best_tablet->full_name()
                         << ", err=" << res.to_string();
        }
        return res;
    }

    best_tablet->set_last_cumu_compaction_failure_time(0);
    return Status::OK();
}

Status StorageEngine::_perform_base_compaction(DataDir* data_dir, std::pair<int32_t, int32_t> tablet_shards_range) {
    scoped_refptr<Trace> trace(new Trace);
    MonotonicStopWatch watch;
    watch.start();
    SCOPED_CLEANUP({
        if (watch.elapsed_time() / 1e9 > config::base_compaction_trace_threshold) {
            LOG(INFO) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    });
    ADOPT_TRACE(trace.get());
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
    vectorized::BaseCompaction base_compaction(mem_tracker.get(), best_tablet);

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
        if (watch.elapsed_time() / 1e9 > config::update_compaction_trace_threshold) {
            LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    });
    ADOPT_TRACE(trace.get());
    TRACE("start to perform update compaction");
    TabletSharedPtr best_tablet = _tablet_manager->find_best_tablet_to_do_update_compaction(data_dir);
    if (best_tablet == nullptr) {
        return Status::NotFound("there are no suitable tablets");
    }
    if (best_tablet->updates() == nullptr) {
        return Status::InternalError("not an updatable tablet");
    }
    TRACE("found best tablet $0", best_tablet->tablet_id());

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
    RETURN_IF_ERROR(get_all_data_dir_info(&data_dir_infos, false));

    time_t now = time(nullptr);
    tm local_tm_now;
    memset(&local_tm_now, 0, sizeof(tm));

    if (localtime_r(&now, &local_tm_now) == nullptr) {
        return Status::InternalError(fmt::format("Fail to localtime_r time: {}", now));
    }
    const time_t local_now = mktime(&local_tm_now);

    for (DataDirInfo& info : data_dir_infos) {
        if (!info.is_used) {
            continue;
        }

        double curr_usage = (double)(info.disk_capacity - info.available) / info.disk_capacity;
        *usage = *usage > curr_usage ? *usage : curr_usage;

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

    // clear expire incremental rowset, move deleted tablet to trash
    (void)_tablet_manager->start_trash_sweep();

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
    std::vector<RowsetMetaSharedPtr> invalid_rowset_metas;
    auto clean_rowset_func = [this, &invalid_rowset_metas](const TabletUid& tablet_uid, RowsetId rowset_id,
                                                           const std::string& meta_str) -> bool {
        RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
        bool parsed = rowset_meta->init(meta_str);
        if (!parsed) {
            LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
            // return false will break meta iterator, return true to skip this error
            return true;
        }
        if (rowset_meta->tablet_uid() != tablet_uid) {
            LOG(WARNING) << "tablet uid is not equal, skip the rowset"
                         << ", rowset_id=" << rowset_meta->rowset_id() << ", in_put_tablet_uid=" << tablet_uid
                         << ", tablet_uid in rowset meta=" << rowset_meta->tablet_uid();
            return true;
        }

        TabletSharedPtr tablet = _tablet_manager->get_tablet(rowset_meta->tablet_id(), tablet_uid);
        if (tablet == nullptr) {
            return true;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE && (!tablet->rowset_meta_is_useful(rowset_meta))) {
            LOG(INFO) << "rowset meta is useless any more, remote it. rowset_id=" << rowset_meta->rowset_id();
            invalid_rowset_metas.push_back(rowset_meta);
        }
        return true;
    };
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        (void)RowsetMetaManager::traverse_rowset_metas(data_dir->get_meta(), clean_rowset_func);
        for (auto& rowset_meta : invalid_rowset_metas) {
            (void)RowsetMetaManager::remove(data_dir->get_meta(), rowset_meta->tablet_uid(), rowset_meta->rowset_id());
        }
        invalid_rowset_metas.clear();
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
    if (!FileUtils::check_exist(scan_root)) {
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
                Status ret = FileUtils::remove_all(path_name);
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

void StorageEngine::start_delete_unused_rowset() {
    std::lock_guard lock(_gc_mutex);
    for (auto it = _unused_rowsets.begin(); it != _unused_rowsets.end();) {
        if (it->second.use_count() != 1) {
            ++it;
        } else if (it->second->need_delete_file()) {
            VLOG(3) << "start to remove rowset:" << it->second->rowset_id()
                    << ", version:" << it->second->version().first << "-" << it->second->version().second;
            Status status = it->second->remove();
            VLOG(3) << "remove rowset:" << it->second->rowset_id() << " finished. status:" << status;
            it = _unused_rowsets.erase(it);
        }
    }
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
    // 1. add wlock to related tablets
    // 2. do prepare work
    // 3. release wlock
    {
        std::vector<TabletInfo> tablet_infos;
        task->get_related_tablets(&tablet_infos);
        sort(tablet_infos.begin(), tablet_infos.end());
        std::vector<TabletSharedPtr> related_tablets;
        DeferOp release_lock([&]() {
            for (TabletSharedPtr& tablet : related_tablets) {
                tablet->release_header_lock();
            }
        });
        for (TabletInfo& tablet_info : tablet_infos) {
            TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.tablet_id);
            if (tablet != nullptr) {
                tablet->obtain_header_wrlock();
                ScopedCleanup release_guard([&]() { tablet->release_header_lock(); });
                related_tablets.push_back(tablet);
                release_guard.cancel();
            } else {
                LOG(WARNING) << "could not get tablet before prepare tabletid: " << tablet_info.tablet_id;
            }
        }
        // add write lock to all related tablets
        RETURN_IF_ERROR(task->prepare());
    }

    // do execute work without lock
    RETURN_IF_ERROR(task->execute());

    // 1. add wlock to related tablets
    // 2. do finish work
    // 3. release wlock
    {
        std::vector<TabletInfo> tablet_infos;
        // related tablets may be changed after execute task, so that get them here again
        task->get_related_tablets(&tablet_infos);
        sort(tablet_infos.begin(), tablet_infos.end());
        std::vector<TabletSharedPtr> related_tablets;
        DeferOp release_lock([&]() {
            for (TabletSharedPtr& tablet : related_tablets) {
                tablet->release_header_lock();
            }
        });
        for (TabletInfo& tablet_info : tablet_infos) {
            TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.tablet_id);
            if (tablet != nullptr) {
                tablet->obtain_header_wrlock();
                ScopedCleanup release_guard([&]() { tablet->release_header_lock(); });
                related_tablets.push_back(tablet);
                release_guard.cancel();
            } else {
                LOG(WARNING) << "Fail to get tablet before finish tablet_id=" << tablet_info.tablet_id;
            }
        }
        // add write lock to all related tablets
        return task->finish();
    }
}

// check whether any unused rowsets's id equal to rowset_id
bool StorageEngine::check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id) {
    std::lock_guard lock(_gc_mutex);
    auto search = _unused_rowsets.find(rowset_id.to_string());
    return search != _unused_rowsets.end();
}

} // namespace starrocks
