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
#include "common/config.h"
#include "common/minidump.h"
#include "common/process_exit.h"
#include "exec/workgroup/work_group.h"
#ifdef USE_STAROS
#include "fslib/star_cache_handler.h"
#endif
#include "fs/encrypt_file.h"
#include "gutil/cpu.h"
#include "jemalloc/jemalloc.h"
#include "runtime/time_types.h"
#include "runtime/user_function_cache.h"
#include "service/backend_options.h"
#include "service/mem_hook.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
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

/*
 * This thread will calculate some metrics at a fix interval(15 sec)
 * 1. push bytes per second
 * 2. scan bytes per second
 * 3. max io util of all disks
 * 4. max network send bytes rate
 * 5. max network receive bytes rate
 * 6. datacache memory usage
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
                "metadata({}), compaction({}), schema_change({}), "
                "page_cache({}), update({}), chunk_allocator({}), passthrough({}), clone({}), consistency({}), "
                "datacache({}), jit({})",
                mem_metrics->process_mem_bytes.value(), mem_metrics->query_mem_bytes.value(),
                mem_metrics->load_mem_bytes.value(), mem_metrics->metadata_mem_bytes.value(),
                mem_metrics->compaction_mem_bytes.value(), mem_metrics->schema_change_mem_bytes.value(),
                mem_metrics->storage_page_cache_mem_bytes.value(), mem_metrics->update_mem_bytes.value(),
                mem_metrics->chunk_allocator_mem_bytes.value(), mem_metrics->passthrough_mem_bytes.value(),
                mem_metrics->clone_mem_bytes.value(), mem_metrics->consistency_mem_bytes.value(),
                mem_metrics->datacache_mem_bytes.value(), mem_metrics->jit_cache_mem_bytes.value());

        StarRocksMetrics::instance()->table_metrics_mgr()->cleanup();
        nap_sleep(15, [daemon] { return daemon->stopped(); });
    }
}

struct JemallocStats {
    int64_t allocated = 0;
    int64_t active = 0;
    int64_t metadata = 0;
    int64_t resident = 0;
    int64_t mapped = 0;
    int64_t retained = 0;
};

static void retrieve_jemalloc_stats(JemallocStats* stats) {
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    int64_t value = 0;
    sz = sizeof(value);
    if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
        stats->allocated = value;
    }
    if (je_mallctl("stats.active", &value, &sz, nullptr, 0) == 0) {
        stats->active = value;
    }
    if (je_mallctl("stats.metadata", &value, &sz, nullptr, 0) == 0) {
        stats->metadata = value;
    }
    if (je_mallctl("stats.resident", &value, &sz, nullptr, 0) == 0) {
        stats->resident = value;
    }
    if (je_mallctl("stats.mapped", &value, &sz, nullptr, 0) == 0) {
        stats->mapped = value;
    }
    if (je_mallctl("stats.retained", &value, &sz, nullptr, 0) == 0) {
        stats->retained = value;
    }
}

// Tracker the memory usage of jemalloc
void jemalloc_tracker_daemon(void* arg_this) {
    auto* daemon = static_cast<Daemon*>(arg_this);
    while (!daemon->stopped()) {
        JemallocStats stats;
        retrieve_jemalloc_stats(&stats);

        // Jemalloc metadata
        if (GlobalEnv::GetInstance()->jemalloc_metadata_traker() && stats.metadata > 0) {
            auto tracker = GlobalEnv::GetInstance()->jemalloc_metadata_traker();
            int64_t delta = stats.metadata - tracker->consumption();
            tracker->consume(delta);
        }

        nap_sleep(1, [daemon] { return daemon->stopped(); });
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
        st = get_inet_interfaces(&network_interfaces, BackendOptions::is_bind_ipv6());
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
    set_process_exit();
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
    LOG(INFO) << "openssl aesni support: " << openssl_supports_aesni();
    auto unsupported_flags = CpuInfo::unsupported_cpu_flags_from_current_env();
    if (!unsupported_flags.empty()) {
        LOG(FATAL) << fmt::format(
                "CPU flags check failed! The following instruction sets are enabled during compiling but not supported "
                "in current running env: {}!",
                fmt::join(unsupported_flags, ","));
        std::abort();
    }

    CHECK(UserFunctionCache::instance()->init(config::user_function_dir).ok());

    date::init_date_cache();

    TimezoneUtils::init_time_zones();

    init_starrocks_metrics(paths);

    if (config::enable_metric_calculator) {
        std::thread calculate_metrics_thread(calculate_metrics, this);
        Thread::set_thread_name(calculate_metrics_thread, "metrics_daemon");
        _daemon_threads.emplace_back(std::move(calculate_metrics_thread));
    }

    if (config::enable_jemalloc_memory_tracker) {
        std::thread jemalloc_tracker_thread(jemalloc_tracker_daemon, this);
        Thread::set_thread_name(jemalloc_tracker_thread, "jemalloc_tracker_daemon");
        _daemon_threads.emplace_back(std::move(jemalloc_tracker_thread));
    }

    init_signals();
    init_minidump();

    // Don't bother set the limit if the process is running with very limited memory capacity
    if (MemInfo::physical_mem() > 1024 * 1024 * 1024) {
        // set mem hook to reject the memory allocation if large than available physical memory detected.
        set_large_memory_alloc_failure_threshold(MemInfo::physical_mem());
    }
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
