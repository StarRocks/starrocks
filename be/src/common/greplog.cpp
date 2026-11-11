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

#include "common/greplog.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <sstream>
#include <string_view>
#include <vector>

#include "base/utility/defer_op.h"
#include "common/config_path_fwd.h"
#include "common/system/backend_options.h"
#include "gutil/strings/substitute.h"
#include "hs/hs_compile.h"
#include "hs/hs_runtime.h"

using namespace std;

namespace starrocks {

static std::vector<string> list_log_files_in_dir(const string& log_dir, char level) {
    std::vector<string> files;
    // if level in WARNING, ERROR, FATAL, use logging logs, else use info logs
    const std::string process = BackendOptions::is_cn() ? "cn" : "be";
    const std::string pattern = process + (string("WEF").find(level) == string::npos ? ".INFO.log." : ".WARNING.log.");
    for (const auto& entry : filesystem::directory_iterator(log_dir)) {
        if (entry.is_regular_file()) {
            auto name = entry.path().filename().string();
            if (name.length() > pattern.length() && name.find(pattern) != string::npos) {
                files.push_back(entry.path().string());
            }
        }
    }
    std::sort(files.begin(), files.end(), std::greater<string>());
    LOG_IF(WARNING, files.empty()) << "list_log_files_in_dir failed, no log files in " << log_dir;
    return files;
}

std::vector<string> list_log_files(char level) {
    return list_log_files_in_dir(config::sys_log_dir, level);
}

struct GrepLogContext {
    int64_t start_ts{0};
    int64_t end_ts{0};
    char level{'I'};
    int64_t limit{0};
    int64_t cur_ts{0};
    int32_t cur_year{0};
    char* line_start{nullptr};
    size_t line_len{0};
    bool stop_scan{false};
    GrepLogEntry current_entry;
    std::deque<GrepLogEntry>* entries;
};

inline bool is_log_prefix(std::string_view log) {
    if (log.length() < 30) {
        return false;
    }
    if (log[0] != 'I' && log[0] != 'W' && log[0] != 'E' && log[0] != 'F') {
        return false;
    }
    // glog prefixes are either LMMdd or LyyyyMMdd, followed by a space.
    size_t date_end = log[5] == ' ' ? 5 : 9;
    if (log[date_end] != ' ') {
        return false;
    }
    for (size_t i = 1; i < date_end; ++i) {
        if (!std::isdigit(static_cast<unsigned char>(log[i]))) {
            return false;
        }
    }
    return true;
}

static void parse_log(std::string_view log, GrepLogContext* context) {
    if (!is_log_prefix(log)) {
        return;
    }
    auto& entry = context->current_entry;
    entry = GrepLogEntry();
    entry.level = log[0];
    bool has_year = log[9] == ' ';
    // convert time string to timestamp
    tm local_tm;
    memset(&local_tm, 0, sizeof(tm));
    if (strptime(log.data() + 1, has_year ? "%Y%m%d %H:%M:%S" : "%m%d %H:%M:%S", &local_tm) != nullptr) {
        local_tm.tm_isdst = -1;
        if (has_year) {
            entry.timestamp = mktime(&local_tm);
        } else {
            // handle the case log file was created on 2020-12-31 23:59:59, but the log was written on 2021-01-01 00:00:03
            local_tm.tm_year = context->cur_year - 1900;
            // mktime mutates tm_isdst; let each candidate infer it independently.
            tm next_year_tm = local_tm;
            next_year_tm.tm_year++;
            int64_t candi1 = mktime(&local_tm);
            int64_t candi2 = mktime(&next_year_tm);
            if (std::abs(candi1 - context->cur_ts) < std::abs(candi2 - context->cur_ts)) {
                entry.timestamp = candi1;
            } else {
                entry.timestamp = candi2;
            }
        }
    }
    auto tid_start = log.find(' ', has_year ? 10 : 6);
    if (tid_start != string::npos) {
        tid_start = log.find_first_not_of(' ', tid_start);
        if (tid_start != string::npos) {
            auto tid_end = log.find(' ', tid_start);
            // sys_log_timezone inserts a signed HHMM offset before the thread ID.
            if (tid_end != string::npos && tid_end - tid_start == 5 &&
                (log[tid_start] == '+' || log[tid_start] == '-') &&
                std::all_of(log.begin() + tid_start + 1, log.begin() + tid_end,
                            [](unsigned char c) { return std::isdigit(c); })) {
                tid_start = log.find_first_not_of(' ', tid_end);
            }
            if (tid_start != string::npos) {
                entry.thread_id = ::strtoll(log.data() + tid_start, nullptr, 10);
            }
        }
    }
}

static int scan_by_line_handler(unsigned int id, unsigned long long from, unsigned long long to, unsigned int flags,
                                void* ctx) {
    auto* context = (GrepLogContext*)ctx;
    auto& new_log = context->entries->emplace_back(context->current_entry);
    new_log.log.assign(context->line_start, context->line_len);
    if (context->start_ts > 0 && new_log.timestamp < context->start_ts) {
        context->entries->pop_back();
        return 0;
    }
    if (context->end_ts > 0 && new_log.timestamp >= context->end_ts) {
        context->entries->pop_back();
        context->stop_scan = true;
        return 0;
    }
    if (context->limit > 0) {
        while (context->entries->size() > context->limit) {
            context->entries->pop_front();
        }
    }
    return 0;
}

static void parse_ts_from_filename(const string& path, int64_t& ts, int32_t& year) {
    auto default_time = [&ts, &year]() {
        time_t rawtime;
        struct tm timeinfo;
        time(&rawtime);
        localtime_r(&rawtime, &timeinfo);
        ts = (int64_t)rawtime;
        year = timeinfo.tm_year + 1900;
    };
    auto pos = path.find_last_of('.');
    if (pos == string::npos) {
        default_time();
        return;
    }
    tm local_tm_create;
    memset(&local_tm_create, 0, sizeof(tm));
    if (strptime(path.c_str() + pos + 1, "%Y%m%d-%H%M%S", &local_tm_create) == nullptr) {
        default_time();
        return;
    }
    local_tm_create.tm_isdst = -1;
    ts = mktime(&local_tm_create);
    year = local_tm_create.tm_year + 1900;
}

Status grep_log_single_file(const string& path, int64_t start_ts, int64_t end_ts, char level, hs_database_t* database,
                            hs_scratch_t* scratch, size_t limit, std::deque<GrepLogEntry>& entries) {
    FILE* fp = fopen(path.c_str(), "r");
    if (fp == nullptr) {
        return Status::InternalError(strings::Substitute("grep log failed open $0 failed", path));
    }
    DeferOp fclose_defer([&fp]() { fclose(fp); });
    char* line = (char*)malloc(1024 * 10);
    DeferOp free_line_defer([&line]() { free(line); });
    size_t line_len = 1024 * 10;
    GrepLogContext ctx;
    ctx.start_ts = start_ts;
    ctx.end_ts = end_ts;
    parse_ts_from_filename(path, ctx.cur_ts, ctx.cur_year);
    ctx.level = level;
    ctx.limit = limit;
    ctx.entries = &entries;
    while (true) {
        ssize_t read = getline(&line, &line_len, fp);
        if (read == -1) {
            break;
        }
        ctx.line_start = line;
        if (read > 0 && line[read - 1] == '\n') {
            read--;
        }
        if (read > 0 && line[read - 1] == '\r') {
            read--;
        }
        ctx.line_len = read;
        // Parse every prefix, even when it does not match the pattern, so continuation
        // lines inherit the timestamp, severity and thread ID of their record.
        parse_log(std::string_view(line, read), &ctx);
        if (database == nullptr) {
            // no pattern, add all lines
            scan_by_line_handler(0, 0, read, 0, &ctx);
        } else {
            if (hs_scan(database, line, read, 0, scratch, scan_by_line_handler, &ctx) != HS_SUCCESS) {
                break;
            }
        }
        if (ctx.stop_scan) {
            break;
        }
    }
    return Status::OK();
}

Status grep_log(int64_t start_ts, int64_t end_ts, char level, const std::string& pattern, size_t limit,
                std::deque<GrepLogEntry>& entries) {
    level = std::toupper(level);
    const string log_dir = config::sys_log_dir;
    if (log_dir.empty()) {
        return Status::InternalError(strings::Substitute("grep log failed $0 is empty", log_dir));
    }
    // check log_dir is directory
    if (!filesystem::is_directory(log_dir)) {
        return Status::InternalError(strings::Substitute("grep log failed $0 is not dir", log_dir));
    }
    hs_database_t* database = nullptr;
    if (!pattern.empty()) {
        hs_compile_error_t* compile_err;
        if (hs_compile(pattern.c_str(), HS_FLAG_SINGLEMATCH, HS_MODE_BLOCK, nullptr, &database, &compile_err) !=
            HS_SUCCESS) {
            std::string error_msg = compile_err != nullptr ? compile_err->message : "";
            hs_free_compile_error(compile_err);
            return Status::InternalError(
                    strings::Substitute("grep log failed compile pattern $0 failed $1", pattern, error_msg));
        }
    }
    DeferOp free_database_defer([&database]() {
        if (database != nullptr) hs_free_database(database);
    });

    hs_scratch_t* scratch = nullptr;
    if (database != nullptr) {
        if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
            return Status::InternalError(
                    strings::Substitute("grep log failed alloc scratch failed pattern:$0", pattern));
        }
    }
    DeferOp free_scratch_defer([&scratch]() {
        if (scratch != nullptr) hs_free_scratch(scratch);
    });

    for (const auto& path : list_log_files_in_dir(log_dir, level)) {
        if (limit > 0 && entries.size() >= limit) {
            break;
        }
        size_t cur_limit = 0;
        if (limit > 0) {
            cur_limit = limit - entries.size();
        }
        std::deque<GrepLogEntry> tmp_entries;
        auto st = grep_log_single_file(path, start_ts, end_ts, level, database, scratch, cur_limit, tmp_entries);
        if (!st.ok()) {
            LOG(WARNING) << "grep_log_single_file failed: " << st.to_string() << " file:" << path;
            continue;
        }
        for (auto itr = tmp_entries.rbegin(); itr != tmp_entries.rend(); ++itr) {
            auto& added = entries.emplace_front();
            added.log.swap(itr->log);
            added.level = itr->level;
            added.timestamp = itr->timestamp;
            added.thread_id = itr->thread_id;
        }
    }
    return Status::OK();
}

std::string grep_log_as_string(int64_t start_ts, int64_t end_ts, const std::string& level, const std::string& pattern,
                               size_t limit) {
    std::ostringstream ss;
    std::deque<GrepLogEntry> entries;
    auto st = grep_log(start_ts, end_ts, level[0], pattern, limit, entries);
    if (!st.ok()) {
        ss << strings::Substitute("grep log failed $0 start_ts:$1 end_ts:$2 level:$3 pattern:$4 limit:$5\n",
                                  st.to_string(), start_ts, end_ts, level, pattern, limit);
        return ss.str();
    }
    for (auto& entry : entries) {
        ss << entry.log << "\n";
    }
    ss << "Lines: " << entries.size() << "\n";
    return ss.str();
}

} // namespace starrocks
