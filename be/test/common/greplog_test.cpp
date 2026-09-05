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

#include "common/greplog.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <ctime>
#include <deque>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <string>
#include <vector>

#include "common/config_path_fwd.h"
#include "common/system/backend_options.h"

namespace starrocks {

namespace {

using ::testing::ElementsAre;
using ::testing::HasSubstr;

int64_t unix_seconds(int year, int month, int day, int hour, int minute, int second) {
    tm local_tm;
    memset(&local_tm, 0, sizeof(tm));
    local_tm.tm_year = year - 1900;
    local_tm.tm_mon = month - 1;
    local_tm.tm_mday = day;
    local_tm.tm_hour = hour;
    local_tm.tm_min = minute;
    local_tm.tm_sec = second;
    local_tm.tm_isdst = -1;
    return mktime(&local_tm);
}

std::vector<std::string> filenames(const std::vector<std::string>& paths) {
    std::vector<std::string> names;
    names.reserve(paths.size());
    for (const auto& path : paths) {
        names.emplace_back(std::filesystem::path(path).filename().string());
    }
    return names;
}

std::vector<std::string> logs(const std::deque<GrepLogEntry>& entries) {
    std::vector<std::string> result;
    result.reserve(entries.size());
    for (const auto& entry : entries) {
        result.emplace_back(entry.log);
    }
    return result;
}

class GrepLogTest : public testing::Test {
protected:
    void SetUp() override {
        _old_sys_log_dir = config::sys_log_dir;
        _old_is_cn = BackendOptions::_is_cn;
        BackendOptions::_is_cn = false;

        const auto suffix = std::to_string(getpid()) + "_" +
                            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        _log_dir = std::filesystem::temp_directory_path() / ("greplog_test_" + suffix);
        ASSERT_TRUE(std::filesystem::create_directories(_log_dir));
        config::sys_log_dir = _log_dir.string();
    }

    void TearDown() override {
        config::sys_log_dir = _old_sys_log_dir;
        BackendOptions::_is_cn = _old_is_cn;
        std::error_code ec;
        std::filesystem::remove_all(_log_dir, ec);
    }

    void write_log_file(const std::string& filename, std::initializer_list<std::string> lines) {
        const auto path = _log_dir / filename;
        std::ofstream out(path);
        ASSERT_TRUE(out.is_open()) << path;
        for (const auto& line : lines) {
            out << line << "\n";
        }
    }

private:
    std::string _old_sys_log_dir;
    bool _old_is_cn = false;
    std::filesystem::path _log_dir;
};

TEST_F(GrepLogTest, ListLogFilesSelectsFilesByLevel) {
    write_log_file("be.INFO.log.20240101-000000", {});
    write_log_file("be.INFO.log.20240102-000000", {});
    write_log_file("be.WARNING.log.20240101-000000", {});
    write_log_file("cn.INFO.log.20240103-000000", {});
    write_log_file("be.INFO.out", {});

    EXPECT_THAT(filenames(list_log_files('I')),
                ElementsAre("be.INFO.log.20240102-000000", "be.INFO.log.20240101-000000"));
    EXPECT_THAT(filenames(list_log_files('W')), ElementsAre("be.WARNING.log.20240101-000000"));
    EXPECT_THAT(filenames(list_log_files('E')), ElementsAre("be.WARNING.log.20240101-000000"));
}

TEST_F(GrepLogTest, GrepLogFiltersByPatternTimestampAndLimit) {
    write_log_file("be.INFO.log.20240101-000000", {
                                                          "I0101 00:00:01.000000 101 old target line",
                                                          "I0101 00:00:02.000000 102 old skipped line",
                                                  });
    write_log_file("be.INFO.log.20240102-000000", {
                                                          "I0102 00:00:01.000000 201 new target line",
                                                          "I0102 00:00:02.000000 202 final target line",
                                                  });

    std::deque<GrepLogEntry> entries;
    auto status = grep_log(0, 0, 'i', "target", 0, entries);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_THAT(logs(entries),
                ElementsAre("I0101 00:00:01.000000 101 old target line", "I0102 00:00:01.000000 201 new target line",
                            "I0102 00:00:02.000000 202 final target line"));
    ASSERT_EQ(3, entries.size());
    EXPECT_EQ('I', entries[0].level);
    EXPECT_EQ(101, entries[0].thread_id);

    entries.clear();
    status = grep_log(unix_seconds(2024, 1, 2, 0, 0, 0), unix_seconds(2024, 1, 2, 0, 0, 2), 'I', "target", 0, entries);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_THAT(logs(entries), ElementsAre("I0102 00:00:01.000000 201 new target line"));

    entries.clear();
    status = grep_log(0, 0, 'I', "target", 2, entries);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_THAT(logs(entries), ElementsAre("I0102 00:00:01.000000 201 new target line",
                                           "I0102 00:00:02.000000 202 final target line"));
}

TEST_F(GrepLogTest, GrepLogAsStringReturnsLinesAndCount) {
    write_log_file("be.INFO.log.20240101-000000", {
                                                          "I0101 00:00:01.000000 101 alpha target line",
                                                          "I0101 00:00:02.000000 102 beta skipped line",
                                                  });

    const auto output = grep_log_as_string(0, 0, "I", "alpha", 10);
    EXPECT_THAT(output, HasSubstr("I0101 00:00:01.000000 101 alpha target line\n"));
    EXPECT_THAT(output, HasSubstr("Lines: 1\n"));
}

TEST_F(GrepLogTest, GrepLogReturnsErrorForInvalidPattern) {
    std::deque<GrepLogEntry> entries;
    const auto status = grep_log(0, 0, 'I', "[", 10, entries);

    EXPECT_FALSE(status.ok());
    EXPECT_THAT(status.to_string(), HasSubstr("grep log failed compile pattern [ failed"));
    EXPECT_TRUE(entries.empty());
}

} // namespace

} // namespace starrocks
