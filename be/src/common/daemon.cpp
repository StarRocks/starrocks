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
//   https://github.com/apache/incubator-doris/blob/master/be/src/common/daemon.cpp

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

#include "common/daemon.h"

#include <gflags/gflags.h>

#include "column/column_helper.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "common/minidump.h"
#include "exec/workgroup/work_group.h"
#include "gutil/cpu.h"
#include "jemalloc/jemalloc.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "runtime/time_types.h"
#include "runtime/user_function_cache.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/gc_helper.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/misc.h"
#include "util/monotime.h"
#include "util/network_util.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"
#include "util/thrift_util.h"
#include "util/time.h"
#include "util/timezone_utils.h"

namespace starrocks {
DEFINE_bool(cn, false, "start as compute node");

// NOTE: when BE receiving SIGTERM, this flag will be set to true. Then BE will reject
// all ExecPlanFragments call by returning a fail status(brpc::EINTERNAL).
// After all existing fragments executed, BE will exit.
std::atomic<bool> k_starrocks_exit = false;

// NOTE: when call `/api/_stop_be` http interface, this flag will be set to true. Then BE will reject
// all ExecPlanFragments call by returning a fail status(brpc::EINTERNAL).
// After all existing fragments executed, BE will exit.
// The difference between k_starrocks_exit and the flag is that
// k_starrocks_exit not only require waiting for all existing fragment to complete,
// but also waiting for all threads to exit gracefully.
std::atomic<bool> k_starrocks_exit_quick = false;

class ReleaseColumnPool {
public:
    explicit ReleaseColumnPool(double ratio) : _ratio(ratio) {}

    template <typename Pool>
    void operator()() {
        _freed_bytes += Pool::singleton()->release_free_columns(_ratio);
    }

    size_t freed_bytes() const { return _freed_bytes; }

private:
    double _ratio;
    size_t _freed_bytes = 0;
};

void gc_memory(void* arg_this) {
    using namespace starrocks;
    const static float kFreeRatio = 0.5;

    auto* daemon = static_cast<Daemon*>(arg_this);
    while (!daemon->stopped()) {
        nap_sleep(config::memory_maintenance_sleep_time_s, [daemon] { return daemon->stopped(); });

        ReleaseColumnPool releaser(kFreeRatio);
        ForEach<ColumnPoolList>(releaser);
        LOG_IF(INFO, releaser.freed_bytes() > 0) << "Released " << releaser.freed_bytes() << " bytes from column pool";
    }
}

/*
 * This thread will calculate some metrics at a fix interval(15 sec)
 * 1. push bytes per second
 * 2. scan bytes per second
 * 3. max io util of all disks
 * 4. max network send bytes rate
 * 5. max network receive bytes rate
 */
void calculate_metrics(void* arg_this) {
    int64_t last_ts = -1L;
    int64_t lst_push_bytes = -1;
    int64_t lst_query_bytes = -1;

    std::map<std::string, int64_t> lst_disks_io_time;
    std::map<std::string, int64_t> lst_net_send_bytes;
    std::map<std::string, int64_t> lst_net_receive_bytes;

    auto* daemon = static_cast<Daemon*>(arg_this);
    while (!daemon->stopped()) {
        StarRocksMetrics::instance()->metrics()->trigger_hook();

        if (last_ts == -1L) {
            last_ts = MonotonicSeconds();
            lst_push_bytes = StarRocksMetrics::instance()->push_request_write_bytes.value();
            lst_query_bytes = StarRocksMetrics::instance()->query_scan_bytes.value();
            StarRocksMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);
            StarRocksMetrics::instance()->system_metrics()->get_network_traffic(&lst_net_send_bytes,
                                                                                &lst_net_receive_bytes);
        } else {
            int64_t current_ts = MonotonicSeconds();
            long interval = (current_ts - last_ts);
            last_ts = current_ts;

            // 1. push bytes per second.
            int64_t current_push_bytes = StarRocksMetrics::instance()->push_request_write_bytes.value();
            int64_t pps = (current_push_bytes - lst_push_bytes) / (interval == 0 ? 1 : interval);
            StarRocksMetrics::instance()->push_request_write_bytes_per_second.set_value(pps < 0 ? 0 : pps);
            lst_push_bytes = current_push_bytes;

            // 2. query bytes per second.
            int64_t current_query_bytes = StarRocksMetrics::instance()->query_scan_bytes.value();
            int64_t qps = (current_query_bytes - lst_query_bytes) / (interval == 0 ? 1 : interval);
            StarRocksMetrics::instance()->query_scan_bytes_per_second.set_value(qps < 0 ? 0 : qps);
            lst_query_bytes = current_query_bytes;

            // 3. max disk io util.
            StarRocksMetrics::instance()->max_disk_io_util_percent.set_value(
                    StarRocksMetrics::instance()->system_metrics()->get_max_io_util(lst_disks_io_time, 15));
            // Update lst map.
            StarRocksMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);

            // 4. max network traffic.
            int64_t max_send = 0;
            int64_t max_receive = 0;
            StarRocksMetrics::instance()->system_metrics()->get_max_net_traffic(
                    lst_net_send_bytes, lst_net_receive_bytes, 15, &max_send, &max_receive);
            StarRocksMetrics::instance()->max_network_send_bytes_rate.set_value(max_send);
            StarRocksMetrics::instance()->max_network_receive_bytes_rate.set_value(max_receive);
            // update lst map
            StarRocksMetrics::instance()->system_metrics()->get_network_traffic(&lst_net_send_bytes,
                                                                                &lst_net_receive_bytes);
        }

        auto* mem_metrics = StarRocksMetrics::instance()->system_metrics()->memory_metrics();

        LOG(INFO) << fmt::format(
                "Current memory statistics: process({}), query_pool({}), load({}), "
                "metadata({}), compaction({}), schema_change({}), column_pool({}), "
                "page_cache({}), update({}), chunk_allocator({}), clone({}), consistency({})",
                mem_metrics->process_mem_bytes.value(), mem_metrics->query_mem_bytes.value(),
                mem_metrics->load_mem_bytes.value(), mem_metrics->metadata_mem_bytes.value(),
                mem_metrics->compaction_mem_bytes.value(), mem_metrics->schema_change_mem_bytes.value(),
                mem_metrics->column_pool_mem_bytes.value(), mem_metrics->storage_page_cache_mem_bytes.value(),
                mem_metrics->update_mem_bytes.value(), mem_metrics->chunk_allocator_mem_bytes.value(),
                mem_metrics->clone_mem_bytes.value(), mem_metrics->consistency_mem_bytes.value());

        nap_sleep(15, [daemon] { return daemon->stopped(); });
    }
}

static void init_starrocks_metrics(const std::vector<StorePath>& store_paths) {
    bool init_system_metrics = config::enable_system_metrics;
    std::set<std::string> disk_devices;
    std::vector<std::string> network_interfaces;
    std::vector<std::string> paths;
    paths.reserve(store_paths.size());
    for (auto& store_path : store_paths) {
        paths.emplace_back(store_path.path);
    }
    if (init_system_metrics) {
        auto st = DiskInfo::get_disk_devices(paths, &disk_devices);
        if (!st.ok()) {
            LOG(WARNING) << "get disk devices failed, status=" << st.message();
            return;
        }
        st = get_inet_interfaces(&network_interfaces);
        if (!st.ok()) {
            LOG(WARNING) << "get inet interfaces failed, status=" << st.message();
            return;
        }
    }
    StarRocksMetrics::instance()->initialize(paths, init_system_metrics, disk_devices, network_interfaces);
}

void sigterm_handler(int signo, siginfo_t* info, void* context) {
    if (info == nullptr) {
        LOG(ERROR) << "got signal: " << strsignal(signo) << "from unknown pid, is going to exit";
    } else {
        LOG(ERROR) << "got signal: " << strsignal(signo) << " from pid: " << info->si_pid << ", is going to exit";
    }
    k_starrocks_exit.store(true);
}

int install_signal(int signo, void (*handler)(int sig, siginfo_t* info, void* context)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_sigaction = handler;
    sigemptyset(&sa.sa_mask);
    auto ret = sigaction(signo, &sa, nullptr);
    if (ret != 0) {
        PLOG(ERROR) << "install signal failed, signo=" << signo;
    }
    return ret;
}

void init_signals() {
    auto ret = install_signal(SIGINT, sigterm_handler);
    if (ret < 0) {
        exit(-1);
    }
    ret = install_signal(SIGTERM, sigterm_handler);
    if (ret < 0) {
        exit(-1);
    }
}

void init_minidump() {
#ifdef __x86_64__
    if (config::sys_minidump_enable) {
        LOG(INFO) << "Minidump is enabled";
        Minidump::init();
    } else {
        LOG(INFO) << "Minidump is disabled";
    }
#else
    LOG(INFO) << "Minidump is disabled on non-x86_64 arch";
#endif
}

void Daemon::init(bool as_cn, const std::vector<StorePath>& paths) {
    if (as_cn) {
        init_glog("cn", true);
    } else {
        init_glog("be", true);
    }

    LOG(INFO) << get_version_string(false);

    init_thrift_logging();
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    LOG(INFO) << CpuInfo::debug_string();
    LOG(INFO) << DiskInfo::debug_string();
    LOG(INFO) << MemInfo::debug_string();
    LOG(INFO) << base::CPU::instance()->debug_string();

    CHECK(UserFunctionCache::instance()->init(config::user_function_dir).ok());

    date::init_date_cache();

    TimezoneUtils::init_time_zones();

    std::thread gc_thread(gc_memory, this);
    Thread::set_thread_name(gc_thread, "gc_daemon");
    _daemon_threads.emplace_back(std::move(gc_thread));

    init_starrocks_metrics(paths);

    if (config::enable_metric_calculator) {
        std::thread calculate_metrics_thread(calculate_metrics, this);
        Thread::set_thread_name(calculate_metrics_thread, "metrics_daemon");
        _daemon_threads.emplace_back(std::move(calculate_metrics_thread));
    }

    init_signals();
    init_minidump();
}

void Daemon::stop() {
    _stopped.store(true, std::memory_order_release);
    size_t thread_size = _daemon_threads.size();
    for (size_t i = 0; i < thread_size; ++i) {
        if (_daemon_threads[i].joinable()) {
            _daemon_threads[i].join();
        }
    }
}

bool Daemon::stopped() {
    return _stopped.load(std::memory_order_consume);
}

} // namespace starrocks
