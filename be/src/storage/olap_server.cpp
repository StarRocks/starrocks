// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/olap_server.cpp

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

#include <gperftools/profiler.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cmath>
#include <ctime>
#include <string>

#include "common/status.h"
#include "storage/compaction.h"
#include "storage/compaction_scheduler.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "util/thread.h"
#include "util/time.h"

using std::string;

namespace starrocks {

// TODO(yingchun): should be more graceful in the future refactor.
#define SLEEP_IN_BG_WORKER(seconds)                                                   \
    int64_t left_seconds = (seconds);                                                 \
    while (!_bg_worker_stopped.load(std::memory_order_consume) && left_seconds > 0) { \
        sleep(1);                                                                     \
        --left_seconds;                                                               \
    }                                                                                 \
    if (_bg_worker_stopped.load(std::memory_order_consume)) {                         \
        break;                                                                        \
    }

// number of running SCHEMA-CHANGE threads
volatile uint32_t g_schema_change_active_threads = 0;

Status StorageEngine::start_bg_threads() {
    _update_cache_expire_thread = std::thread([this] { _update_cache_expire_thread_callback(nullptr); });
    Thread::set_thread_name(_update_cache_expire_thread, "cache_expire");
    LOG(INFO) << "update cache expire thread started";

    _unused_rowset_monitor_thread = std::thread([this] { _unused_rowset_monitor_thread_callback(nullptr); });
    Thread::set_thread_name(_unused_rowset_monitor_thread, "rowset_monitor");
    LOG(INFO) << "unused rowset monitor thread started";

    // start thread for monitoring the snapshot and trash folder
    _garbage_sweeper_thread = std::thread([this] { _garbage_sweeper_thread_callback(nullptr); });
    Thread::set_thread_name(_garbage_sweeper_thread, "garbage_sweeper");
    LOG(INFO) << "garbage sweeper thread started";

    // start thread for monitoring the tablet with io error
    _disk_stat_monitor_thread = std::thread([this] { _disk_stat_monitor_thread_callback(nullptr); });
    Thread::set_thread_name(_disk_stat_monitor_thread, "disk_monitor");
    LOG(INFO) << "disk stat monitor thread started";

    // convert store map to vector
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
    }
    int32_t data_dir_num = data_dirs.size();

    if (!config::enable_event_based_compaction_framework) {
        // base and cumulative compaction threads
        int32_t base_compaction_num_threads_per_disk =
                std::max<int32_t>(1, config::base_compaction_num_threads_per_disk);
        int32_t cumulative_compaction_num_threads_per_disk =
                std::max<int32_t>(1, config::cumulative_compaction_num_threads_per_disk);
        int32_t base_compaction_num_threads = base_compaction_num_threads_per_disk * data_dir_num;
        int32_t cumulative_compaction_num_threads = cumulative_compaction_num_threads_per_disk * data_dir_num;

        // calc the max concurrency of compaction tasks
        int32_t max_compaction_concurrency = config::max_compaction_concurrency;
        if (max_compaction_concurrency < 0 ||
            max_compaction_concurrency > base_compaction_num_threads + cumulative_compaction_num_threads) {
            max_compaction_concurrency = base_compaction_num_threads + cumulative_compaction_num_threads;
        }
        vectorized::Compaction::init(max_compaction_concurrency);

        _base_compaction_threads.reserve(base_compaction_num_threads);
        for (uint32_t i = 0; i < base_compaction_num_threads; ++i) {
            _base_compaction_threads.emplace_back([this, data_dir_num, data_dirs, i] {
                _base_compaction_thread_callback(nullptr, data_dirs[i % data_dir_num]);
            });
            Thread::set_thread_name(_base_compaction_threads.back(), "base_compact");
        }
        LOG(INFO) << "base compaction threads started. number: " << base_compaction_num_threads;

        _cumulative_compaction_threads.reserve(cumulative_compaction_num_threads);
        for (uint32_t i = 0; i < cumulative_compaction_num_threads; ++i) {
            _cumulative_compaction_threads.emplace_back([this, data_dir_num, data_dirs, i] {
                _cumulative_compaction_thread_callback(nullptr, data_dirs[i % data_dir_num]);
            });
            Thread::set_thread_name(_cumulative_compaction_threads.back(), "cumulat_compact");
        }
        LOG(INFO) << "cumulative compaction threads started. number: " << cumulative_compaction_num_threads;
    } else {
        // new compaction framework

        // compaction_manager must init_max_task_num() before any comapction_scheduler starts
        _compaction_manager->init_max_task_num();
        _compaction_scheduler = std::thread([] {
            CompactionScheduler compaction_scheduler;
            compaction_scheduler.schedule();
        });
        Thread::set_thread_name(_compaction_scheduler, "compact_sched");
        LOG(INFO) << "compaction scheduler started";

        _compaction_checker_thread = std::thread([this] { compaction_check(); });
        Thread::set_thread_name(_compaction_checker_thread, "compact_check");
        LOG(INFO) << "compaction checker started";
    }

    int32_t update_compaction_num_threads_per_disk =
            config::update_compaction_num_threads_per_disk >= 0 ? config::update_compaction_num_threads_per_disk : 1;
    int32_t update_compaction_num_threads = update_compaction_num_threads_per_disk * data_dir_num;
    _update_compaction_threads.reserve(update_compaction_num_threads);
    for (uint32_t i = 0; i < update_compaction_num_threads; ++i) {
        _update_compaction_threads.emplace_back([this, data_dir_num, data_dirs, i] {
            _update_compaction_thread_callback(nullptr, data_dirs[i % data_dir_num]);
        });
        Thread::set_thread_name(_update_compaction_threads.back(), "update_compact");
    }
    LOG(INFO) << "update compaction threads started. number: " << update_compaction_num_threads;

    // tablet checkpoint thread
    for (auto data_dir : data_dirs) {
        _tablet_checkpoint_threads.emplace_back([this, data_dir] { _tablet_checkpoint_callback((void*)data_dir); });
        Thread::set_thread_name(_tablet_checkpoint_threads.back(), "tablet_check_pt");
    }
    LOG(INFO) << "tablet checkpoint thread started";

    // fd cache clean thread
    _fd_cache_clean_thread = std::thread([this] { _fd_cache_clean_callback(nullptr); });
    Thread::set_thread_name(_fd_cache_clean_thread, "fd_cache_clean");
    LOG(INFO) << "fd cache clean thread started";

    // path scan and gc thread
    if (config::path_gc_check) {
        for (auto data_dir : get_stores()) {
            _path_scan_threads.emplace_back([this, data_dir] { _path_scan_thread_callback((void*)data_dir); });
            _path_gc_threads.emplace_back([this, data_dir] { _path_gc_thread_callback((void*)data_dir); });
            Thread::set_thread_name(_path_scan_threads.back(), "path_scan");
            Thread::set_thread_name(_path_gc_threads.back(), "path_gc");
        }
        LOG(INFO) << "path scan/gc threads started. number:" << get_stores().size();
    }

    LOG(INFO) << "all storage engine's background threads are started.";
    return Status::OK();
}

void* StorageEngine::_fd_cache_clean_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        int32_t interval = config::file_descriptor_cache_clean_interval;
        if (interval <= 0) {
            LOG(WARNING) << "config of file descriptor clean interval is illegal: " << interval << "force set to 3600";
            interval = 3600;
        }
        SLEEP_IN_BG_WORKER(interval);

        _start_clean_fd_cache();
    }

    return nullptr;
}

void* StorageEngine::_base_compaction_thread_callback(void* arg, DataDir* data_dir) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    //string last_base_compaction_fs;
    //TTabletId last_base_compaction_tablet_id = -1;
    Status status = Status::OK();
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        // must be here, because this thread is start on start and
        if (!data_dir->reach_capacity_limit(0)) {
            status = _perform_base_compaction(data_dir);
        } else {
            status = Status::InternalError("data dir out of capacity");
        }
        if (status.ok()) {
            continue;
        }

        int32_t interval = config::base_compaction_check_interval_seconds;
        if (interval <= 0) {
            LOG(WARNING) << "base compaction check interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        do {
            SLEEP_IN_BG_WORKER(interval);
            if (!_options.compaction_mem_tracker->any_limit_exceeded()) {
                break;
            }
        } while (true);
    }

    return nullptr;
}

void* StorageEngine::_update_compaction_thread_callback(void* arg, DataDir* data_dir) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    Status status = Status::OK();
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        // must be here, because this thread is start on start and
        if (!data_dir->reach_capacity_limit(0)) {
            status = _perform_update_compaction(data_dir);
        } else {
            status = Status::InternalError("data dir out of capacity");
        }
        if (status.ok()) {
            continue;
        }

        int32_t interval = config::update_compaction_check_interval_seconds;
        if (interval <= 0) {
            LOG(WARNING) << "base compaction check interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        do {
            SLEEP_IN_BG_WORKER(interval);
            if (!_options.compaction_mem_tracker->any_limit_exceeded()) {
                break;
            }
        } while (true);
    }

    return nullptr;
}

void* StorageEngine::_garbage_sweeper_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    uint32_t max_interval = config::max_garbage_sweep_interval;
    uint32_t min_interval = config::min_garbage_sweep_interval;

    if (!(max_interval >= min_interval && min_interval > 0)) {
        LOG(WARNING) << "garbage sweep interval config is illegal: max=" << max_interval << " min=" << min_interval;
        min_interval = 1;
        max_interval = max_interval >= min_interval ? max_interval : min_interval;
        LOG(INFO) << "force reset garbage sweep interval. "
                  << "max_interval=" << max_interval << ", min_interval=" << min_interval;
    }

    const double pi = 4 * std::atan(1);
    double usage = 1.0;
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        usage *= 100.0;
        // when disk usage is less than 60%, ratio is about 1;
        // when disk usage is between [60%, 75%], ratio drops from 0.87 to 0.27;
        // when disk usage is greater than 75%, ratio drops slowly.
        // when disk usage =90%, ratio is about 0.0057
        double ratio = (1.1 * (pi / 2 - std::atan(usage / 5 - 14)) - 0.28) / pi;
        ratio = ratio > 0 ? ratio : 0;
        uint32_t curr_interval = max_interval * ratio;
        // when usage < 60%,curr_interval is about max_interval,
        // when usage > 80%, curr_interval is close to min_interval
        curr_interval = curr_interval > min_interval ? curr_interval : min_interval;

        // For shutdown gracefully
        std::cv_status cv_status = std::cv_status::no_timeout;
        int64_t left_seconds = curr_interval;
        while (!_bg_worker_stopped.load(std::memory_order_consume) && left_seconds > 0) {
            std::unique_lock<std::mutex> lk(_trash_sweeper_mutex);
            cv_status = _trash_sweeper_cv.wait_for(lk, std::chrono::seconds(1));
            if (cv_status == std::cv_status::no_timeout) {
                LOG(INFO) << "trash sweeper has been notified";
                break;
            }
            --left_seconds;
        }
        if (_bg_worker_stopped.load(std::memory_order_consume)) {
            break;
        }
        // start sweep, and get usage after sweep
        Status res = _start_trash_sweep(&usage);
        if (!res.ok()) {
            LOG(WARNING) << "one or more errors occur when sweep trash."
                            "see previous message for detail. err code="
                         << res;
        }
    }

    return nullptr;
}

void* StorageEngine::_disk_stat_monitor_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        _start_disk_stat_monitor();

        int32_t interval = config::disk_stat_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "disk_stat_monitor_interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_cumulative_compaction_thread_callback(void* arg, DataDir* data_dir) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start cumulative compaction process!";

    Status status = Status::OK();
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        // must be here, because this thread is start on start and
        if (!data_dir->reach_capacity_limit(0)) {
            status = _perform_cumulative_compaction(data_dir);
        } else {
            status = Status::InternalError("data dir out of capacity");
        }
        if (status.ok()) {
            continue;
        }

        int32_t interval = config::cumulative_compaction_check_interval_seconds;
        if (interval <= 0) {
            LOG(WARNING) << "cumulative compaction check interval config is illegal:" << interval
                         << "will be forced set to one";
            interval = 1;
        }
        do {
            SLEEP_IN_BG_WORKER(interval);
            if (!_options.compaction_mem_tracker->any_limit_exceeded()) {
                break;
            }
        } while (true);
    }

    return nullptr;
}

void* StorageEngine::_update_cache_expire_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        int32_t expire_sec = config::update_cache_expire_sec;
        if (expire_sec <= 0) {
            LOG(WARNING) << "update_cache_expire_sec config is illegal: " << expire_sec << ", force set to 360";
            expire_sec = 360;
        }
        _update_manager->set_cache_expire_ms(expire_sec * 1000);
        int32_t sleep_sec = std::max(1, expire_sec / 2);
        SLEEP_IN_BG_WORKER(sleep_sec);
        _update_manager->expire_cache();
    }

    return nullptr;
}

void* StorageEngine::_unused_rowset_monitor_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        double deleted_pct = delete_unused_rowset();
        // delete 20% means we nead speedup 5x which make interval 1/5 before
        int32_t interval = config::unused_rowset_monitor_interval * deleted_pct;
        if (interval <= 0) {
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_path_gc_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    LOG(INFO) << "try to start path gc thread!";

    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        LOG(INFO) << "try to perform path gc by tablet!";
        ((DataDir*)arg)->perform_path_gc_by_tablet();

        LOG(INFO) << "try to perform path gc by rowsetid!";
        // perform path gc by rowset id
        ((DataDir*)arg)->perform_path_gc_by_rowsetid();

        int32_t interval = config::path_gc_check_interval_second;
        if (interval <= 0) {
            LOG(WARNING) << "path gc thread check interval config is illegal:" << interval
                         << "will be forced set to half hour";
            interval = 1800; // 0.5 hour
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_path_scan_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    LOG(INFO) << "wait 10min to start path scan thread";
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        SLEEP_IN_BG_WORKER(600);
        break;
    }
    LOG(INFO) << "try to start path scan thread!";

    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        LOG(INFO) << "try to perform path scan!";
        ((DataDir*)arg)->perform_path_scan();

        int32_t interval = config::path_scan_interval_second;
        if (interval <= 0) {
            LOG(WARNING) << "path gc thread check interval config is illegal:" << interval
                         << "will be forced set to one day";
            interval = 24 * 3600; // one day
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_tablet_checkpoint_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start tablet meta checkpoint thread!";
    while (!_bg_worker_stopped.load(std::memory_order_consume)) {
        LOG(INFO) << "begin to do tablet meta checkpoint:" << ((DataDir*)arg)->path();
        int64_t start_time = UnixMillis();
        _tablet_manager->do_tablet_meta_checkpoint((DataDir*)arg);
        int64_t used_time = (UnixMillis() - start_time) / 1000;
        if (used_time < config::tablet_meta_checkpoint_min_interval_secs) {
            int64_t interval = config::tablet_meta_checkpoint_min_interval_secs - used_time;
            SLEEP_IN_BG_WORKER(interval);
        } else {
            sleep(1);
        }
    }

    return nullptr;
}

} // namespace starrocks
