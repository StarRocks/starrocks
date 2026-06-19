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

#include "service/failure_handler.h"

#include <glog/logging.h>
#include <jemalloc/jemalloc.h>
#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach/mach_init.h>
#include <mach/mach_port.h>
#include <mach/thread_act.h>
#endif

#include <fmt/format.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <mutex>
#include <string>
#include <tuple>

#include "base/uid_util.h"
#include "cache/datacache.h"
#include "common/config_diagnostic_fwd.h"
#include "common/process_exit.h"
#include "common/util/debug_util.h"
#include "gutil/endian.h"
#include "gutil/sysinfo.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"

namespace starrocks {

// avoid to allocate extra memory
static int print_unique_id(char* buffer, const TUniqueId& uid) {
    char buff[32];
    struct {
        int64_t hi;
        int64_t lo;
    } data;
    data.hi = gbswap_64(uid.hi);
    data.lo = gbswap_64(uid.lo);
    to_hex(data.hi, buff + 16);
    to_hex(data.lo, buff);

    auto* raw = reinterpret_cast<int16_t*>(buff);
    std::reverse(raw, raw + 16);

    memset(buffer, '-', 36);
    memcpy(buffer, buff, 8);
    memcpy(buffer + 8 + 1, buff + 8, 4);
    memcpy(buffer + 8 + 4 + 2, buff + 8 + 4, 4);
    memcpy(buffer + 8 + 4 + 4 + 3, buff + 8 + 4 + 4, 4);
    memcpy(buffer + 8 + 4 + 4 + 4 + 4, buff + 8 + 4 + 4 + 4, 12);
    return 36;
}

// heap may broken when call dump trace info.
// so we shouldn't allocate any memory allocate function here
static void dump_trace_info() {
    static bool start_dump = false;
    if (!start_dump) {
        // dump query_id and fragment id
        auto query_id = CurrentThread::current().query_id();
        auto fragment_instance_id = CurrentThread::current().fragment_instance_id();
        const std::string& custom_coredump_msg = CurrentThread::current().get_custom_coredump_msg();
        int32_t plan_node_id = CurrentThread::current().plan_node_id();
        const uint32_t MAX_BUFFER_SIZE = 512;
        char buffer[MAX_BUFFER_SIZE] = {};

        // write build version
        int res = get_build_version(buffer, sizeof(buffer));
        std::ignore = write(STDERR_FILENO, buffer, res);

        res = sprintf(buffer, "query_id:");
        res = print_unique_id(buffer + res, query_id) + res;
        res = sprintf(buffer + res, ", ") + res;
        res = sprintf(buffer + res, "fragment_instance:") + res;
        res = print_unique_id(buffer + res, fragment_instance_id) + res;
        res = sprintf(buffer + res, ", plan_node_id:%d", plan_node_id) + res;
        res = sprintf(buffer + res, "\n") + res;

        // print for lake filename
        if (!custom_coredump_msg.empty()) {
            // Avoid buffer overflow, because custom coredump msg's length in not fixed
            res = snprintf(buffer + res, MAX_BUFFER_SIZE - res, "%s\n", custom_coredump_msg.c_str()) + res;
        }

        std::ignore = write(STDERR_FILENO, buffer, res);
    }
    start_dump = true;
}

#define FMT_LOG(msg, ...)                                                                                      \
    fmt::format_to(std::back_inserter(mbuffer), "[{}.{}][thread: {}] " msg "\n", tv.tv_sec, tv.tv_usec / 1000, \
                   tid __VA_OPT__(, ) __VA_ARGS__);                                                            \
    DCHECK(mbuffer.size() < 500);                                                                              \
    std::ignore = write(STDERR_FILENO, mbuffer.data(), mbuffer.size());                                        \
    mbuffer.clear();

#ifndef __APPLE__
#define JEMALLOC_CTL je_mallctl

static int jemalloc_purge() {
    char buffer[100];
    int res = snprintf(buffer, sizeof(buffer), "arena.%d.purge", MALLCTL_ARENAS_ALL);
    buffer[res] = '\0';
    return JEMALLOC_CTL(buffer, nullptr, nullptr, nullptr, 0);
}

static int jemalloc_dontdump() {
    char buffer[100];
    int res = snprintf(buffer, sizeof(buffer), "arena.%d.dontdump", MALLCTL_ARENAS_ALL);
    buffer[res] = '\0';
    return JEMALLOC_CTL(buffer, nullptr, nullptr, nullptr, 0);
}

static void dontdump_unused_pages() {
    size_t prev_allocate_size = CurrentThread::current().get_consumed_bytes();
    static bool start_dump = false;
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    // On macOS, pthread_t is an opaque pointer; convert to a numeric id for fmt
#ifdef __APPLE__
    uint64_t tid = static_cast<uint64_t>(pthread_mach_thread_np(pthread_self()));
#else
    pthread_t tid = pthread_self();
#endif
    // memory_buffer allocate 500 bytes from stack
    fmt::memory_buffer mbuffer;
    if (!start_dump) {
        int ret = jemalloc_purge();

        if (ret != 0) {
            FMT_LOG("je_mallctl execute purge failed, errno:{}", ret);
        } else {
            FMT_LOG("je_mallctl execute purge success");
        }

        ret = jemalloc_dontdump();

        if (ret != 0) {
            FMT_LOG("je_mallctl execute dontdump failed, errno:{}", ret);
        } else {
            FMT_LOG("je_mallctl execute dontdump success");
        }
    }
    DCHECK_EQ(prev_allocate_size, CurrentThread::current().get_consumed_bytes());
    start_dump = true;
}
#endif

static void failure_handler_after_output_log() {
    static bool start_dump = false;
    if (!start_dump && config::enable_core_file_size_optimization && base::get_cur_core_file_limit() != 0) {
        set_process_is_crashing();

        ExecEnv::GetInstance()->try_release_resource_before_core_dump();
#ifndef __APPLE__
        DataCache::GetInstance()->try_release_resource_before_core_dump();
#endif
#ifndef __APPLE__
        dontdump_unused_pages();
#endif
    }
    start_dump = true;
}

static void failure_writer(const char* data, size_t size) {
    dump_trace_info();
    std::ignore = write(STDERR_FILENO, data, size);
}

// MUST not add LOG(XXX) in this function, may cause deadlock.
static void failure_function() {
    dump_trace_info();
    failure_handler_after_output_log();
    std::abort();
}

void init_runtime_logging_hooks() {
    // dump trace info may access some runtime stats
    // if runtime stats broken we won't dump stack
    // These function should be called after InitGoogleLogging.
    static std::mutex logging_hooks_mutex;
    static bool logging_hooks_initialized = false;
    std::lock_guard<std::mutex> lock(logging_hooks_mutex);
    if (logging_hooks_initialized) {
        return;
    }
    if (config::dump_trace_info) {
        google::InstallFailureWriter(failure_writer);
        google::InstallFailureFunction((google::logging_fail_func_t)failure_function);
#ifndef __APPLE__
        // This symbol may be unavailable on macOS builds using system glog.
        google::InstallFailureHandlerAfterOutputLog(failure_handler_after_output_log);
#endif
    }
    logging_hooks_initialized = true;
}

} // namespace starrocks
