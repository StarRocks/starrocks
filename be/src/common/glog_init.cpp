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

#include "common/glog_init.h"

#include <glog/logging.h>
#include <glog/vlog_is_on.h>

#include <atomic>
#include <cctype>
#include <cerrno>
#include <cinttypes>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

#include "common/config_diagnostic_fwd.h"
#include "common/config_path_fwd.h"
#include "common/configbase.h"
#include "fmt/format.h"
#include "gutil/stringprintf.h"

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

// Kept in sync with the values sys_log_level is declared with in config.h.
static bool apply_log_level(const std::string& loglevel) {
    if (iequals(loglevel, "INFO")) {
        FLAGS_minloglevel = 0;
    } else if (iequals(loglevel, "WARNING")) {
        FLAGS_minloglevel = 1;
    } else if (iequals(loglevel, "ERROR")) {
        FLAGS_minloglevel = 2;
    } else if (iequals(loglevel, "FATAL")) {
        FLAGS_minloglevel = 3;
    } else {
        return false;
    }
    return true;
}

static bool apply_roll_mode(const std::string& rollmode) {
    const std::string sizeflag = "SIZE-MB-";
    if (rollmode.compare("TIME-DAY") == 0) {
#ifndef __APPLE__
        FLAGS_log_split_method = "day";
#endif
        return true;
    }
    if (rollmode.compare("TIME-HOUR") == 0) {
#ifndef __APPLE__
        FLAGS_log_split_method = "hour";
#endif
        return true;
    }
    if (rollmode.substr(0, sizeflag.length()).compare(sizeflag) != 0) {
        return false;
    }
    std::string sizestr = rollmode.substr(sizeflag.size(), rollmode.size() - sizeflag.size());
    if (sizestr.size() == 0) {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const char* sizecstr = sizestr.c_str();
    int64_t ret64 = strtoll(sizecstr, &end, 10);
    if ((errno != 0) || (end != sizecstr + strlen(sizecstr))) {
        return false;
    }
    auto retval = static_cast<int32_t>(ret64);
    if (retval != ret64) {
        return false;
    }
#ifndef __APPLE__
    FLAGS_log_split_method = "size";
#endif
    FLAGS_max_log_size = retval;
    return true;
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

    // Set log level. sys_log_level is a validated enum config, so this only fails if the two lists
    // drift apart.
    if (!apply_log_level(config::sys_log_level)) {
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

    // Set log roll mode. A roll mode that cannot be parsed falls back to the declared default rather
    // than failing: unusable logging is a worse outcome than logging that rolls differently than
    // asked, and the fallback is reported once logging is up.
    if (!apply_roll_mode(config::sys_log_roll_mode)) {
        std::string rejected = config::sys_log_roll_mode;
        if (!config::fall_back_to_default("sys_log_roll_mode", rejected, "TIME-DAY,TIME-HOUR,SIZE-MB-nnn") ||
            !apply_roll_mode(config::sys_log_roll_mode)) {
            std::cerr << "sys_log_roll_mode needs to be TIME-DAY, TIME-HOUR, SIZE-MB-nnn" << std::endl;
            return false;
        }
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

    logging_initialized = true;

    // Config parsing runs before logging exists, so values it had to reject are reported here, where
    // every caller of init_glog gets it rather than only the ones that remember to ask.
    report_config_fallbacks();

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

    return StringPrintf("%02d%02d %02d:%02d:%02d.%06" PRId64, 1 + tm_time.tm_mon, tm_time.tm_mday, tm_time.tm_hour,
                        tm_time.tm_min, tm_time.tm_sec, usecs);
}

void report_config_fallbacks() {
    for (const auto& fallback : config::take_config_fallbacks()) {
        std::string message =
                fmt::format("invalid config '{}'='{}', using '{}' instead; valid values: {}", fallback.name,
                            fallback.rejected_value, fallback.effective_value, fallback.allowed_values);
        // On stderr as well as through glog: the log below is subject to FLAGS_minloglevel, which a
        // sys_log_level of FATAL sets high enough to drop it, and stderr is where the message used
        // to appear before these values were given a fallback.
        std::cerr << message << std::endl;
        LOG(ERROR) << message;
    }
}

void update_logging() {
    if (!apply_log_level(config::sys_log_level)) {
        LOG(WARNING) << "update sys_log_level failed, need to be INFO, WARNING, ERROR, FATAL";
    }
}

} // namespace starrocks
