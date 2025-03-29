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

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>

#include "common/config.h"
#include "gutil/endian.h"
#include "gutil/stringprintf.h"
#include "gutil/sysinfo.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/page_cache.h"
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
        const uint32_t MAX_BUFFER_SIZE = 512;
        char buffer[MAX_BUFFER_SIZE] = {};

        // write build version
        int res = get_build_version(buffer, sizeof(buffer));
        [[maybe_unused]] auto wt = write(STDERR_FILENO, buffer, res);

        res = sprintf(buffer, "query_id:");
        res = print_unique_id(buffer + res, query_id) + res;
        res = sprintf(buffer + res, ", ") + res;
        res = sprintf(buffer + res, "fragment_instance:") + res;
        res = print_unique_id(buffer + res, fragment_instance_id) + res;
        res = sprintf(buffer + res, "\n") + res;

        // print for lake filename
        if (!custom_coredump_msg.empty()) {
            // Avoid buffer overflow, because custom coredump msg's length in not fixed
            res = snprintf(buffer + res, MAX_BUFFER_SIZE - res, "%s\n", custom_coredump_msg.c_str()) + res;
        }

        wt = write(STDERR_FILENO, buffer, res);
        // dump memory usage
        // copy trackers
        auto trackers = GlobalEnv::GetInstance()->mem_trackers();
        for (const auto& tracker : trackers) {
            if (tracker) {
                size_t len = tracker->debug_string(buffer, sizeof(buffer));
                wt = write(STDERR_FILENO, buffer, len);
            }
        }
    }
    start_dump = true;
}

static void dontdump_unused_pages() {
    static bool start_dump = false;
    if (!start_dump) {
        std::string purge_msg = "arena." + std::to_string(MALLCTL_ARENAS_ALL) + ".purge";
        int ret = je_mallctl(purge_msg.c_str(), nullptr, nullptr, nullptr, 0);
        if (ret != 0) {
            LOG(ERROR) << "je_mallctl execute purge failed: " << strerror(ret);
        } else {
            LOG(INFO) << "je_mallctl execute purge success";
        }

        std::string dontdump_msg = "arena." + std::to_string(MALLCTL_ARENAS_ALL) + ".dontdump";
        ret = je_mallctl(dontdump_msg.c_str(), nullptr, nullptr, nullptr, 0);
        if (ret != 0) {
            LOG(ERROR) << "je_mallctl execute dontdump failed: " << strerror(ret);
        } else {
            LOG(INFO) << "je_mallctl execute dontdump success";
        }
    }
    start_dump = true;
}

static void failure_handler_after_output_log() {
    static bool start_dump = false;
    if (!start_dump && config::enable_core_file_size_optimization && base::get_cur_core_file_limit() != 0) {
        ExecEnv::GetInstance()->try_release_resource_before_core_dump();
        CacheEnv::GetInstance()->try_release_resource_before_core_dump();
        dontdump_unused_pages();
    }
    start_dump = true;
}

static void failure_writer(const char* data, size_t size) {
    dump_trace_info();
    [[maybe_unused]] auto wt = write(STDERR_FILENO, data, size);
}

// MUST not add LOG(XXX) in this function, may cause deadlock.
static void failure_function() {
    dump_trace_info();
    failure_handler_after_output_log();
    std::abort();
}

bool init_glog(const char* basename, bool install_signal_handler) {
    std::lock_guard<std::mutex> logging_lock(logging_mutex);

    if (logging_initialized) {
        return true;
    }

    if (install_signal_handler) {
        google::InstallFailureSignalHandler();
    }

    // only write fatal log to stderr
    FLAGS_stderrthreshold = 3;
    // Set glog log dir.
    FLAGS_log_dir = config::sys_log_dir;
    // 0 means buffer INFO only.
    FLAGS_logbuflevel = 0;
    // Buffer log messages for at most this many seconds.
    FLAGS_logbufsecs = 30;
    // Set roll num.
    FLAGS_log_filenum_quota = config::sys_log_roll_num;

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
        FLAGS_log_split_method = "day";
        ok = true;
    } else if (rollmode.compare("TIME-HOUR") == 0) {
        FLAGS_log_split_method = "hour";
        ok = true;
    } else if (rollmode.substr(0, sizeflag.length()).compare(sizeflag) == 0) {
        FLAGS_log_split_method = "size";
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

    // dump trace info may access some runtime stats
    // if runtime stats broken we won't dump stack
    // These function should be called after InitGoogleLogging.

    if (config::dump_trace_info) {
        google::InstallFailureWriter(failure_writer);
        google::InstallFailureFunction((google::logging_fail_func_t)failure_function);
        google::InstallFailureHandlerAfterOutputLog(failure_handler_after_output_log);
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
