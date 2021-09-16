// This file is made available under Elastic License 2.0.
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
#include <gperftools/malloc_extension.h>

#include "column/column_helper.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "common/minidump.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/user_function_cache.h"
#include "runtime/vectorized/time_types.h"
#include "storage/options.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/network_util.h"
#include "util/starrocks_metrics.h"
#include "util/system_metrics.h"
#include "util/thrift_util.h"
#include "util/time.h"

namespace starrocks {

bool k_starrocks_exit = false;

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

void* tcmalloc_gc_thread(void* dummy) {
    using namespace starrocks::vectorized;
    const static float kFreeRatio = 0.5;
    while (1) {
        sleep(10);
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
        MallocExtension::instance()->MarkThreadBusy();
#endif
        ReleaseColumnPool releaser(kFreeRatio);
        ForEach<ColumnPoolList>(releaser);
        LOG_IF(INFO, releaser.freed_bytes() > 0) << "Released " << releaser.freed_bytes() << " bytes from column pool";
        auto* local_column_pool_mem_tracker = ExecEnv::GetInstance()->local_column_pool_mem_tracker();
        if (local_column_pool_mem_tracker != nullptr) {
            // Frequent update MemTracker where allocate or release column may affect performance,
            // so here update MemTracker regularly
            local_column_pool_mem_tracker->consume(g_column_pool_total_local_bytes.get_value() -
                                                   local_column_pool_mem_tracker->consumption());
        }
        auto* central_column_pool_mem_tracker = ExecEnv::GetInstance()->central_column_pool_mem_tracker();
        if (central_column_pool_mem_tracker != nullptr) {
            // Frequent update MemTracker where allocate or release column may affect performance,
            // so here update MemTracker regularly
            central_column_pool_mem_tracker->consume(g_column_pool_total_central_bytes.get_value() -
                                                     central_column_pool_mem_tracker->consumption());
        }

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
        size_t used_size = 0;
        size_t free_size = 0;
        MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &used_size);
        MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes", &free_size);
        size_t phy_size = used_size + free_size; // physical memory usage
        if (phy_size > config::tc_use_memory_min) {
            size_t max_free_size = phy_size * config::tc_free_memory_rate / 100;
            if (free_size > max_free_size) {
                MallocExtension::instance()->ReleaseToSystem(free_size - max_free_size);
            }
        }
        MallocExtension::instance()->MarkThreadIdle();
#endif
    }

    return NULL;
}

void* memory_maintenance_thread(void* dummy) {
    while (true) {
        sleep(config::memory_maintenance_sleep_time_s);
        ExecEnv* env = ExecEnv::GetInstance();
        // ExecEnv may not have been created yet or this may be the catalogd or statestored,
        // which don't have ExecEnvs.
        if (env != nullptr) {
            BufferPool* buffer_pool = env->buffer_pool();
            if (buffer_pool != nullptr) buffer_pool->Maintenance();

            // The process limit as measured by our trackers may get out of sync with the
            // process usage if memory is allocated or freed without updating a MemTracker.
            // The metric is refreshed whenever memory is consumed or released via a MemTracker,
            // so on a system with queries executing it will be refreshed frequently. However
            // if the system is idle, we need to refresh the tracker occasionally since
            // untracked memory may be allocated or freed, e.g. by background threads.
            if (env->process_mem_tracker() != nullptr && !env->process_mem_tracker()->is_consumption_metric_null()) {
                env->process_mem_tracker()->RefreshConsumptionFromMetric();
            }
        }
    }

    return NULL;
}

/*
 * this thread will calculate some metrics at a fix interval(15 sec)
 * 1. push bytes per second
 * 2. scan bytes per second
 * 3. max io util of all disks
 * 4. max network send bytes rate
 * 5. max network receive bytes rate
 */
void* calculate_metrics(void* dummy) {
    int64_t last_ts = -1L;
    int64_t lst_push_bytes = -1;
    int64_t lst_query_bytes = -1;

    std::map<std::string, int64_t> lst_disks_io_time;
    std::map<std::string, int64_t> lst_net_send_bytes;
    std::map<std::string, int64_t> lst_net_receive_bytes;

    while (true) {
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

            // 1. push bytes per second
            int64_t current_push_bytes = StarRocksMetrics::instance()->push_request_write_bytes.value();
            int64_t pps = (current_push_bytes - lst_push_bytes) / (interval == 0 ? 1 : interval);
            StarRocksMetrics::instance()->push_request_write_bytes_per_second.set_value(pps < 0 ? 0 : pps);
            lst_push_bytes = current_push_bytes;

            // 2. query bytes per second
            int64_t current_query_bytes = StarRocksMetrics::instance()->query_scan_bytes.value();
            int64_t qps = (current_query_bytes - lst_query_bytes) / (interval == 0 ? 1 : interval);
            StarRocksMetrics::instance()->query_scan_bytes_per_second.set_value(qps < 0 ? 0 : qps);
            lst_query_bytes = current_query_bytes;

            // 3. max disk io util
            StarRocksMetrics::instance()->max_disk_io_util_percent.set_value(
                    StarRocksMetrics::instance()->system_metrics()->get_max_io_util(lst_disks_io_time, 15));
            // update lst map
            StarRocksMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);

            // 4. max network traffic
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

        sleep(15); // 15 seconds
    }

    return NULL;
}

static void init_starrocks_metrics(const std::vector<StorePath>& store_paths) {
    bool init_system_metrics = config::enable_system_metrics;
    std::set<std::string> disk_devices;
    std::vector<std::string> network_interfaces;
    std::vector<std::string> paths;
    for (auto& store_path : store_paths) {
        paths.emplace_back(store_path.path);
    }
    if (init_system_metrics) {
        auto st = DiskInfo::get_disk_devices(paths, &disk_devices);
        if (!st.ok()) {
            LOG(WARNING) << "get disk devices failed, stauts=" << st.get_error_msg();
            return;
        }
        st = get_inet_interfaces(&network_interfaces);
        if (!st.ok()) {
            LOG(WARNING) << "get inet interfaces failed, stauts=" << st.get_error_msg();
            return;
        }
    }
    StarRocksMetrics::instance()->initialize(paths, init_system_metrics, disk_devices, network_interfaces);

    if (config::enable_metric_calculator) {
        pthread_t calculator_pid;
        pthread_create(&calculator_pid, NULL, calculate_metrics, NULL);
    }
}

void sigterm_handler(int signo) {
    k_starrocks_exit = true;
}

int install_signal(int signo, void (*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = handler;
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
    if (config::sys_minidump_enable) {
        LOG(INFO) << "Minidump is enable";
        Minidump::init();
    } else {
        LOG(INFO) << "Minidump is disable";
    }
}

void init_daemon(int argc, char** argv, const std::vector<StorePath>& paths) {
    // google::SetVersionString(get_build_version(false));
    // google::ParseCommandLineFlags(&argc, &argv, true);
    google::ParseCommandLineFlags(&argc, &argv, true);
    init_glog("be", true);

    LOG(INFO) << get_version_string(false);

    init_thrift_logging();
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    UserFunctionCache::instance()->init(config::user_function_dir);

    vectorized::ColumnHelper::init_static_variable();
    vectorized::date::init_date_cache();

    pthread_t tc_malloc_pid;
    pthread_create(&tc_malloc_pid, NULL, tcmalloc_gc_thread, NULL);

    pthread_t buffer_pool_pid;
    pthread_create(&buffer_pool_pid, NULL, memory_maintenance_thread, NULL);

    LOG(INFO) << CpuInfo::debug_string();
    LOG(INFO) << DiskInfo::debug_string();
    LOG(INFO) << MemInfo::debug_string();
    init_starrocks_metrics(paths);
    init_signals();
    init_minidump();

    ChunkAllocator::init_instance(config::chunk_reserved_bytes_limit);
}

} // namespace starrocks
