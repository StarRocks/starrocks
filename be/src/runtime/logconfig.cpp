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

#include <glog/logging.h>
#include <glog/vlog_is_on.h>
#include <jemalloc/jemalloc.h>
#include <unistd.h>

#include <limits>

#include "common/process_exit.h"
#include "util/uid_util.h"
#ifdef __APPLE__
#include <mach/mach_init.h>
#include <mach/mach_port.h>
#include <mach/thread_act.h>
#include <pthread.h>
#endif

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>

#include "cache/datacache.h"
#include "cache/mem_cache/page_cache.h"
#include "common/config.h"
#include "gutil/endian.h"
#include "gutil/stringprintf.h"
#include "gutil/sysinfo.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/logconfig.h"
#include "util/logging.h"
#include "util/stack_util.h"

namespace starrocks {

static bool logging_initialized = false;

static std::mutex logging_mutex;

static bool iequals(const std::string& a, const std::string& b) {
    size_t sz = a.size();
    if (b.size() != sz) {
        return false;
    }
    for (size_t i = 0; i < sz; ++i) {
        if (tolower(a[i]) != tolower(b[i])) {
            return false;
        }
    }
    return true;
}

size_t get_build_version(char* buffer, size_t max_size);

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

#ifdef __APPLE__
#define JEMALLOC_CTL mallctl
#else
#define JEMALLOC_CTL je_mallctl
#endif

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

std::string lite_exec(const std::vector<std::string>& argv_vec, int timeout_ms);
static std::mutex gcore_mutex;
static bool gcore_done = false;
void hook_on_query_timeout(const TUniqueId& query_id, size_t timeout_seconds) {
    if (config::pipeline_gcore_timeout_threshold_sec > 0 &&
        timeout_seconds > static_cast<size_t>(config::pipeline_gcore_timeout_threshold_sec)) {
        std::unique_lock<std::mutex> lock(gcore_mutex);
        if (gcore_done) {
            return;
        }

        if (config::enable_core_file_size_optimization) {
            jemalloc_purge();
            jemalloc_dontdump();
        }

        std::string core_file = config::pipeline_gcore_output_dir + "/core-" + print_id(query_id);
        LOG(WARNING) << "dump gcore via query timeout:" << timeout_seconds;
        pid_t pid = getpid();
        // gcore have too many verbose output. skip them
        (void)lite_exec({"gcore", "-o", core_file, std::to_string(pid)}, std::numeric_limits<int>::max());
        LOG(WARNING) << "gcore finished ";
        gcore_done = true;
    }
}

// Calculate timezone offset string dynamically with caching (thread-safe)
static std::string get_timezone_offset_string(const google::LogMessageTime& time) {
    // Cache the last offset and its string representation
    static std::mutex cache_mutex;
    static std::atomic<long> cached_offset_seconds(-1);
    static std::shared_ptr<std::string> cached_tz_str_ptr(std::make_shared<std::string>(""));

    // Get timezone offset from LogMessageTime
    long offset_seconds = time.gmtoffset().count();

    // Fast path: check atomic variables without lock
    long cached_offset = cached_offset_seconds.load(std::memory_order_acquire);
    if (offset_seconds == cached_offset) {
        std::shared_ptr<std::string> tz_ptr = std::atomic_load_explicit(&cached_tz_str_ptr, std::memory_order_acquire);
        return *tz_ptr;
    }

    // Slow path: recalculate and update cache
    char tz_str[16] = "+0000";
    int offset_hours = static_cast<int>(offset_seconds / 3600);
    int offset_mins = static_cast<int>((offset_seconds % 3600) / 60);
    if (offset_mins < 0) offset_mins = -offset_mins;

    if (offset_seconds < 0) {
        snprintf(tz_str, sizeof(tz_str), "-%02d%02d", -offset_hours, offset_mins);
    } else {
        snprintf(tz_str, sizeof(tz_str), "+%02d%02d", offset_hours, offset_mins);
    }

    std::string result(tz_str);
    auto new_tz_ptr = std::make_shared<std::string>(result);

    // Update cache with lock
    std::lock_guard<std::mutex> lock(cache_mutex);
    long expected_offset = cached_offset_seconds.load(std::memory_order_relaxed);
    if (offset_seconds != expected_offset) {
        // Publish the cached_str before updating the offset
        std::atomic_store_explicit(&cached_tz_str_ptr, new_tz_ptr, std::memory_order_release);
        cached_offset_seconds.store(offset_seconds, std::memory_order_release);
    } else {
        // Another thread already updated, use the cached string
        std::shared_ptr<std::string> current_ptr =
                std::atomic_load_explicit(&cached_tz_str_ptr, std::memory_order_acquire);
        result = *current_ptr;
    }

    return result;
}

// Custom prefix formatter that includes timezone information
static void custom_prefix_formatter(std::ostream& s, const google::LogMessage& message, void*) {
    const google::LogMessageTime& time = message.time();

    // Severity level (single character)
    s << google::GetLogSeverityName(message.severity())[0];

    // Date and time
    if (FLAGS_log_year_in_prefix) {
        // Format: YYYYMMDD HH:MM:SS.uuuuuu +HHMM
        s << std::setfill('0') << std::setw(4) << (1900 + time.year()) << std::setw(2) << (1 + time.month())
          << std::setw(2) << time.day() << ' ' << std::setw(2) << time.hour() << ':' << std::setw(2) << time.min()
          << ':' << std::setw(2) << time.sec() << '.' << std::setw(6) << time.usec();
    } else {
        // Format: MMDD HH:MM:SS.uuuuuu +HHMM
        s << std::setfill('0') << std::setw(2) << (1 + time.month()) << std::setw(2) << time.day() << ' '
          << std::setw(2) << time.hour() << ':' << std::setw(2) << time.min() << ':' << std::setw(2) << time.sec()
          << '.' << std::setw(6) << time.usec();
    }

    // Timezone offset (calculated dynamically)
    s << ' ' << get_timezone_offset_string(time);

    // Thread ID
    s << ' ' << message.thread_id() << ' ';

    // File and line
    s << message.basename() << ':' << message.line() << "] ";
}

bool init_glog(const char* basename, bool install_signal_handler) {
    std::lock_guard<std::mutex> logging_lock(logging_mutex);

    if (logging_initialized) {
        return true;
    }

    if (install_signal_handler) {
#ifndef __APPLE__
        google::InstallFailureSignalHandler();
#endif
    }

    // only write fatal log to stderr
    FLAGS_stderrthreshold = 3;
    // Set glog log dir.
    FLAGS_log_dir = config::sys_log_dir;
    // 0 means buffer INFO only.
    FLAGS_logbuflevel = 0;
    // Buffer log messages for at most this many seconds.
    FLAGS_logbufsecs = 30;
    // Set roll num. Not available with Homebrew glog on macOS.
#ifndef __APPLE__
    FLAGS_log_filenum_quota = config::sys_log_roll_num;
#endif

    // Set log level.
    std::string loglevel = config::sys_log_level;
    if (iequals(loglevel, "INFO")) {
        FLAGS_minloglevel = 0;
    } else if (iequals(loglevel, "WARNING")) {
        FLAGS_minloglevel = 1;
    } else if (iequals(loglevel, "ERROR")) {
        FLAGS_minloglevel = 2;
    } else if (iequals(loglevel, "FATAL")) {
        FLAGS_minloglevel = 3;
    } else {
        std::cerr << "sys_log_level needs to be INFO, WARNING, ERROR, FATAL" << std::endl;
        return false;
    }

    // Set log buffer level, defalut is 0.
    std::string& logbuflevel = config::log_buffer_level;
    if (iequals(logbuflevel, "-1")) {
        FLAGS_logbuflevel = -1;
    } else if (iequals(logbuflevel, "0")) {
        FLAGS_logbuflevel = 0;
    }

    // Set log roll mode.
    std::string& rollmode = config::sys_log_roll_mode;
    std::string sizeflag = "SIZE-MB-";
    bool ok = false;
    if (rollmode.compare("TIME-DAY") == 0) {
#ifndef __APPLE__
        FLAGS_log_split_method = "day";
#endif
        ok = true;
    } else if (rollmode.compare("TIME-HOUR") == 0) {
#ifndef __APPLE__
        FLAGS_log_split_method = "hour";
#endif
        ok = true;
    } else if (rollmode.substr(0, sizeflag.length()).compare(sizeflag) == 0) {
#ifndef __APPLE__
        FLAGS_log_split_method = "size";
#endif
        std::string sizestr = rollmode.substr(sizeflag.size(), rollmode.size() - sizeflag.size());
        if (sizestr.size() != 0) {
            char* end = nullptr;
            errno = 0;
            const char* sizecstr = sizestr.c_str();
            int64_t ret64 = strtoll(sizecstr, &end, 10);
            if ((errno == 0) && (end == sizecstr + strlen(sizecstr))) {
                auto retval = static_cast<int32_t>(ret64);
                if (retval == ret64) {
                    FLAGS_max_log_size = retval;
                    ok = true;
                }
            }
        }
    } else {
        ok = false;
    }
    if (!ok) {
        std::cerr << "sys_log_roll_mode needs to be TIME-DAY, TIME-HOUR, SIZE-MB-nnn" << std::endl;
        return false;
    }

    // Set verbose modules.
    FLAGS_v = -1;
    std::vector<std::string>& verbose_modules = config::sys_log_verbose_modules;
    int32_t vlog_level = config::sys_log_verbose_level;
    for (auto& verbose_module : verbose_modules) {
        if (verbose_module.size() != 0) {
            google::SetVLOGLevel(verbose_module.c_str(), vlog_level);
        }
    }

    google::InitGoogleLogging(basename);

    // Install custom prefix formatter to include timezone information if enabled
    if (config::sys_log_timezone) {
        google::InstallPrefixFormatter(&custom_prefix_formatter, nullptr);
    }

    // dump trace info may access some runtime stats
    // if runtime stats broken we won't dump stack
    // These function should be called after InitGoogleLogging.

    if (config::dump_trace_info) {
        google::InstallFailureWriter(failure_writer);
        google::InstallFailureFunction((google::logging_fail_func_t)failure_function);
#ifndef __APPLE__
        // This symbol may be unavailable on macOS builds using system glog.
        google::InstallFailureHandlerAfterOutputLog(failure_handler_after_output_log);
#endif
    }

    logging_initialized = true;

    return true;
}

void shutdown_logging() {
    std::lock_guard<std::mutex> logging_lock(logging_mutex);
    google::ShutdownGoogleLogging();
}

std::string FormatTimestampForLog(MicrosecondsInt64 micros_since_epoch) {
    time_t secs_since_epoch = micros_since_epoch / 1000000;
    int64_t usecs = micros_since_epoch % 1000000;
    struct tm tm_time;
    localtime_r(&secs_since_epoch, &tm_time);

    return StringPrintf("%02d%02d %02d:%02d:%02d.%06ld", 1 + tm_time.tm_mon, tm_time.tm_mday, tm_time.tm_hour,
                        tm_time.tm_min, tm_time.tm_sec, usecs);
}

void update_logging() {
    if (iequals(config::sys_log_level, "INFO")) {
        FLAGS_minloglevel = 0;
    } else if (iequals(config::sys_log_level, "WARNING")) {
        FLAGS_minloglevel = 1;
    } else if (iequals(config::sys_log_level, "ERROR")) {
        FLAGS_minloglevel = 2;
    } else if (iequals(config::sys_log_level, "FATAL")) {
        FLAGS_minloglevel = 3;
    } else {
        LOG(WARNING) << "update sys_log_level failed, need to be INFO, WARNING, ERROR, FATAL";
    }
}

} // namespace starrocks
