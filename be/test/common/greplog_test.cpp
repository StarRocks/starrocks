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
#include <cstdlib>
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
        const auto* timezone = std::getenv("TZ");
        _had_timezone = timezone != nullptr;
        _old_timezone = timezone != nullptr ? timezone : "";
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
        if (_had_timezone) {
            EXPECT_EQ(0, setenv("TZ", _old_timezone.c_str(), 1));
        } else {
            EXPECT_EQ(0, unsetenv("TZ"));
        }
        tzset();
        config::sys_log_dir = _old_sys_log_dir;
        BackendOptions::_is_cn = _old_is_cn;
        std::error_code ec;
        std::filesystem::remove_all(_log_dir, ec);
    }

    void set_timezone(const char* timezone) {
        ASSERT_EQ(0, setenv("TZ", timezone, 1));
        tzset();
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
    bool _had_timezone = false;
    std::string _old_timezone;
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

class GrepLogTimeFilterTest : public GrepLogTest, public testing::WithParamInterface<bool> {
protected:
    std::string prefix(const std::string& date) const { return "I" + (GetParam() ? date : date.substr(4)); }
};

TEST_P(GrepLogTimeFilterTest, TimestampBoundsAndRequestOutput) {
    // Exercise both BE and CN file selection with the same query parameters used by /greplog.
    for (bool is_cn : {false, true}) {
        BackendOptions::_is_cn = is_cn;
        const auto p = prefix("20261017");
        const auto target = p + " 09:45:54.448190 21807688042176 lake_service.cpp:594] Published txns=48";
        write_log_file(std::string(is_cn ? "cn" : "be") + ".INFO.log.20261017-090000",
                       {p + " 09:45:53.999999 101 Published before", target,
                        p + " 09:45:54.999999 103 Published same second", p + " 09:45:55.000000 104 Published end"});
        const auto start = unix_seconds(2026, 10, 17, 9, 45, 54);
        const auto end = start + 1;

        std::deque<GrepLogEntry> entries;
        ASSERT_TRUE(grep_log(0, 0, 'I', "Published", 0, entries).ok());
        ASSERT_EQ(4, entries.size());
        EXPECT_EQ(start, entries[1].timestamp);
        EXPECT_EQ(start, entries[2].timestamp);
        EXPECT_EQ(21807688042176ULL, entries[1].thread_id);
        EXPECT_EQ('I', entries[1].level);

        entries.clear();
        ASSERT_TRUE(grep_log(start, end, 'I', "Published", 0, entries).ok());
        EXPECT_THAT(logs(entries), ElementsAre(target, p + " 09:45:54.999999 103 Published same second"));

        entries.clear();
        ASSERT_TRUE(grep_log(start, 0, 'I', "Published", 0, entries).ok());
        EXPECT_THAT(logs(entries), ElementsAre(target, p + " 09:45:54.999999 103 Published same second",
                                               p + " 09:45:55.000000 104 Published end"));
        entries.clear();
        ASSERT_TRUE(grep_log(0, end, 'I', "Published", 0, entries).ok());
        EXPECT_THAT(logs(entries), ElementsAre(p + " 09:45:53.999999 101 Published before", target,
                                               p + " 09:45:54.999999 103 Published same second"));

        const auto unbounded = grep_log_as_string(0, 0, "I", "Published txns=48", 10);
        EXPECT_EQ(target + "\nLines: 1\n", unbounded);
        EXPECT_EQ(unbounded, grep_log_as_string(start, end, "I", "Published txns=48", 10));
    }
}

TEST_P(GrepLogTimeFilterTest, YearRollover) {
    const auto december = prefix("20251231") + " 23:59:59.999999 101 Published before rollover";
    const auto january = prefix("20260101") + " 00:00:00.000001 102 Published after rollover";
    write_log_file("be.INFO.log.20251231-235958", {december, january});
    const auto start = unix_seconds(2026, 1, 1, 0, 0, 0);

    std::deque<GrepLogEntry> entries;
    ASSERT_TRUE(grep_log(0, 0, 'I', "Published", 0, entries).ok());
    ASSERT_EQ(2, entries.size());
    EXPECT_EQ(start - 1, entries[0].timestamp);
    EXPECT_EQ(start, entries[1].timestamp);

    entries.clear();
    ASSERT_TRUE(grep_log(start, start + 1, 'I', "Published", 0, entries).ok());
    EXPECT_THAT(logs(entries), ElementsAre(january));
    entries.clear();
    ASSERT_TRUE(grep_log(start - 1, start, 'I', "Published", 0, entries).ok());
    EXPECT_THAT(logs(entries), ElementsAre(december));
}

TEST_P(GrepLogTimeFilterTest, ContinuationLinesInheritUnmatchedPrefix) {
    const auto p = prefix("20260101");
    // Neither record prefix matches the regex. Include a malformed prefix-like continuation.
    write_log_file("be.INFO.log.20260101-000000",
                   {p + " 00:00:00.000000 101 record before window", "Published before continuation",
                    "W" + p.substr(1) + " 00:00:01.000000 202 record in window", "Published in-window continuation",
                    "I2026x101 00:00:00.000000 999 Published in-window malformed prefix",
                    p + " 00:00:02.000000 303 record at end", "Published end continuation"});
    const auto start = unix_seconds(2026, 1, 1, 0, 0, 1);
    for (const auto& pattern : {std::string("Published"), std::string()}) {
        std::deque<GrepLogEntry> entries;
        ASSERT_TRUE(grep_log(start, start + 1, 'I', pattern, 0, entries).ok());
        ASSERT_EQ(pattern.empty() ? 3 : 2, entries.size());
        EXPECT_EQ("Published in-window continuation", entries[entries.size() - 2].log);
        EXPECT_EQ("I2026x101 00:00:00.000000 999 Published in-window malformed prefix", entries.back().log);
        for (const auto& entry : entries) {
            EXPECT_EQ(start, entry.timestamp);
            EXPECT_EQ('W', entry.level);
            EXPECT_EQ(202, entry.thread_id);
        }
    }
    EXPECT_EQ(grep_log_as_string(0, 0, "I", "Published in-window", 10),
              grep_log_as_string(start, start + 1, "I", "Published in-window", 10));
}

TEST_P(GrepLogTimeFilterTest, LocalTimeRespectsDaylightSaving) {
    set_timezone("America/New_York");
    const auto winter = prefix("20260115") + " 12:00:00.123456 101 Published winter";
    const auto summer = prefix("20260715") + " 12:00:00.654321 102 Published summer";
    write_log_file("be.INFO.log.20260101-000000", {winter, summer});
    const auto winter_ts = unix_seconds(2026, 1, 15, 12, 0, 0);
    const auto summer_ts = unix_seconds(2026, 7, 15, 12, 0, 0);

    std::deque<GrepLogEntry> entries;
    ASSERT_TRUE(grep_log(0, 0, 'I', "Published", 0, entries).ok());
    ASSERT_EQ(2, entries.size());
    EXPECT_EQ(winter_ts, entries[0].timestamp);
    EXPECT_EQ(summer_ts, entries[1].timestamp);
    EXPECT_EQ(grep_log_as_string(0, 0, "I", "Published winter", 10),
              grep_log_as_string(winter_ts, winter_ts + 1, "I", "Published winter", 10));
    EXPECT_EQ(grep_log_as_string(0, 0, "I", "Published summer", 10),
              grep_log_as_string(summer_ts, summer_ts + 1, "I", "Published summer", 10));
}

TEST_P(GrepLogTimeFilterTest, InferredYearUsesItsOwnDaylightSavingRules) {
    set_timezone("America/New_York");
    // March 9 is standard time in 2024 but daylight time in 2025. Do not reuse
    // the DST decision made while evaluating the file's year for the next year.
    const auto target = prefix("20250309") + " 12:00:00.123456 101 Published next year";
    write_log_file("be.INFO.log.20241231-235959", {target});
    const auto start = unix_seconds(2025, 3, 9, 12, 0, 0);
    std::deque<GrepLogEntry> entries;
    ASSERT_TRUE(grep_log(start, start + 1, 'I', "Published", 0, entries).ok());
    ASSERT_EQ(1, entries.size());
    EXPECT_EQ(start, entries[0].timestamp);
    EXPECT_EQ(target + "\nLines: 1\n", grep_log_as_string(start, start + 1, "I", "Published", 10));
}

TEST_P(GrepLogTimeFilterTest, OptionalTimezoneOffsetPrecedesThreadId) {
    const struct {
        const char* timezone;
        const char* offset;
    } cases[] = {{"Asia/Shanghai", "+0800"},
                 {"America/New_York", "-0400"},
                 {"Asia/Kolkata", "+0530"},
                 {"America/St_Johns", "-0230"},
                 {"UTC", "+0000"}};
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.timezone);
        set_timezone(test_case.timezone);
        const auto record = prefix("20260715") + " 12:00:00.123456 " + test_case.offset +
                            "   21807688042176 record without a regex match";
        write_log_file("be.INFO.log.20260715-000000", {record, "Published continuation"});
        const auto start = unix_seconds(2026, 7, 15, 12, 0, 0);
        for (const auto& pattern : {std::string(), std::string("Published")}) {
            std::deque<GrepLogEntry> entries;
            ASSERT_TRUE(grep_log(start, start + 1, 'I', pattern, 0, entries).ok());
            ASSERT_EQ(pattern.empty() ? 2 : 1, entries.size());
            for (const auto& entry : entries) {
                EXPECT_EQ(start, entry.timestamp);
                EXPECT_EQ(21807688042176ULL, entry.thread_id);
            }
            EXPECT_EQ("Published continuation", entries.back().log);
        }
        EXPECT_EQ(grep_log_as_string(0, 0, "I", "Published", 10),
                  grep_log_as_string(start, start + 1, "I", "Published", 10));
    }
}

INSTANTIATE_TEST_SUITE_P(LogFormats, GrepLogTimeFilterTest, testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) { return info.param ? "WithYear" : "Legacy"; });

TEST_F(GrepLogTest, ExplicitYearOverridesFilenameYear) {
    write_log_file("be.INFO.log.20240101-000000", {"E20261017 09:45:54.448190 123 Published explicit year"});
    const auto start = unix_seconds(2026, 10, 17, 9, 45, 54);
    std::deque<GrepLogEntry> entries;
    ASSERT_TRUE(grep_log(start, start + 1, 'I', "Published", 0, entries).ok());
    ASSERT_EQ(1, entries.size());
    EXPECT_EQ(start, entries[0].timestamp);
    EXPECT_EQ('E', entries[0].level);
    EXPECT_EQ(123, entries[0].thread_id);
}

} // namespace

} // namespace starrocks
