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

#include <deque>
#include <filesystem>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "hs/hs_compile.h"
#include "hs/hs_runtime.h"
#include "util/defer_op.h"

using namespace std;

namespace starrocks {

static std::vector<string> list_log_files_in_dir(const string& log_dir, char level) {
    std::vector<string> files;
    // if level in WARNING, ERROR, FATAL, use logging logs, else use info logs
    const std::string pattern = string("WEF").find(level) == string::npos ? "be.INFO.log." : "be.WARNING.log.";
    for (const auto& entry : filesystem::directory_iterator(log_dir)) {
        if (entry.is_regular_file()) {
            auto name = entry.path().filename().string();
            if (name.length() > pattern.length() && name.substr(0, pattern.length()) == pattern) {
                files.push_back(entry.path().string());
            }
        }
    }
    std::sort(files.begin(), files.end(), std::greater<string>());
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
    std::deque<GrepLogEntry>* entries;
};

inline bool is_log_prefix(const string& log) {
    if (log.length() < 30) {
        return false;
    }
    if (log[5] == ' ') {
        if (log[0] == 'I' || log[0] == 'W' || log[0] == 'E' || log[0] == 'F') {
            if (std::isdigit(log[1]) && std::isdigit(log[2]) && std::isdigit(log[3]) && std::isdigit(log[4])) {
                return true;
            }
        }
    }
    return false;
}

static void parse_log(GrepLogEntry& entry, GrepLogContext* context) {
    if (!is_log_prefix(entry.log)) {
        return;
    }
    entry.level = entry.log[0];
    // convert time string to timestamp
    tm local_tm;
    memset(&local_tm, 0, sizeof(tm));
    if (strptime(entry.log.c_str() + 1, "%m%d %H:%M:%S", &local_tm) != nullptr) {
        // handle the case log file was created on 2020-12-31 23:59:59, but the log was written on 2021-01-01 00:00:03
        local_tm.tm_year = context->cur_year - 1900;
        int64_t candi1 = mktime(&local_tm);
        local_tm.tm_year++;
        int64_t candi2 = mktime(&local_tm);
        if (std::abs(candi1 - context->cur_ts) < std::abs(candi2 - context->cur_ts)) {
            entry.timestamp = candi1;
        } else {
            entry.timestamp = candi2;
        }
    }
    auto tid_start = entry.log.find(' ', 6);
    if (tid_start != string::npos) {
        entry.thread_id = ::strtoll(entry.log.c_str() + tid_start + 1, nullptr, 10);
    }
}

static int scan_by_line_handler(unsigned int id, unsigned long long from, unsigned long long to, unsigned int flags,
                                void* ctx) {
    auto* context = (GrepLogContext*)ctx;
    auto& new_log = context->entries->emplace_back();
    new_log.log.assign(context->line_start, context->line_len);
    parse_log(new_log, context);
    if (context->start_ts > 0 && new_log.timestamp < context->start_ts) {
        context->entries->pop_back();
    }
    if (context->end_ts > 0 && new_log.timestamp >= context->end_ts) {
        context->entries->pop_back();
        context->stop_scan = true;
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
        if (database == nullptr) {
            // no pattern, add all lines
            scan_by_line_handler(0, 0, 0, 0, &ctx);
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
        if (hs_compile(pattern.c_str(), 0, HS_MODE_BLOCK, nullptr, &database, &compile_err) != HS_SUCCESS) {
            hs_free_compile_error(compile_err);
            return Status::InternalError(
                    strings::Substitute("grep log failed compile pattern $0 failed $1", pattern, compile_err->message));
        }
    }
    DeferOp free_database_defer([&database]() {
        if (database != nullptr) hs_free_database(database);
    });

    hs_scratch_t* scratch = nullptr;
    if (database != nullptr) {
        if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
            hs_free_database(database);
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

std::string grep_log_as_string(int64_t start_ts, int64_t end_ts, char level, const std::string& pattern, size_t limit) {
    std::ostringstream ss;
    std::deque<GrepLogEntry> entries;
    auto st = grep_log(start_ts, end_ts, level, pattern, limit, entries);
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