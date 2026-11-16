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

#include <glog/logging.h>
#include <gtest/gtest.h>

#define private public

#include <limits>
#include <random>
#include <string>
#include <vector>

#include "base/time/timezone_utils.h"
#include "types/date_value.h"
#include "types/datetime_value.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"

namespace starrocks {

TEST(TimestampValueTest, normal) {
    LOG(INFO) << "MAX: " << timestamp::from_julian_and_time(date::MAX_DATE, 86400 * USECS_PER_SEC - 1);
    LOG(INFO) << "MIN: " << timestamp::from_julian_and_time(date::MIN_DATE, 0);

    {
        auto v = TimestampValue::create(2004, 1, 1, 18, 30, 30);

        LOG(INFO) << "UNIX SECONDS: " << v.to_unix_second();

        TimestampValue a;
        a.from_unix_second(v.to_unix_second());
        LOG(INFO) << "UNIX TIMESTAMP: " << a;
        ASSERT_EQ(20040101183030, v.to_timestamp_literal());
        ASSERT_EQ("2004-01-01 18:30:30", v.to_string());
    }

    {
        auto v = TimestampValue::create(1004, 1, 1, 18, 30, 30);

        LOG(INFO) << "UNIX SECONDS: " << v.to_unix_second();

        TimestampValue a;
        a.from_unix_second(v.to_unix_second());
        LOG(INFO) << "UNIX TIMESTAMP: " << a;
    }
}

TEST(TimestampValueTest, toString) {
    {
        auto v = TimestampValue::create(2004, 1, 1, 18, 30, 30);
        ASSERT_EQ("2004-01-01 18:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30);
        ASSERT_EQ("2004-02-29 23:30:30", v.to_string());
    }
}

TEST(TimestampValueTest, calculate) {
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::SECOND>(30);
        ASSERT_EQ("2004-02-29 23:31:00", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::MINUTE>(30);
        ASSERT_EQ("2004-03-01 00:00:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::HOUR>(1);
        ASSERT_EQ("2004-03-01 00:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::DAY>(30);
        ASSERT_EQ("2004-03-30 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::DAY>(365);
        ASSERT_EQ("2005-02-28 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 3, 29, 23, 30, 30).add<TimeUnit::DAY>(365);
        ASSERT_EQ("2005-03-29 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::DAY>(-365);
        ASSERT_EQ("2003-03-01 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::YEAR>(1);
        ASSERT_EQ("2005-02-28 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::YEAR>(8);
        ASSERT_EQ("2012-02-29 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::MONTH>(8);
        ASSERT_EQ("2004-10-29 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::MONTH>(13);
        ASSERT_EQ("2005-03-29 23:30:30", v.to_string());
    }
}

TEST(TimestampValueTest, cast) {
    auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30);
    DateValue date = (DateValue)v;
    ASSERT_EQ("2004-02-29", date.to_string());
    ASSERT_EQ(1078012800000, date.to_unixtime());
}

TEST(TimestampValueTest, unixTime) {
    auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30);
    ASSERT_EQ(1078097430000, v.to_unixtime());
    ASSERT_EQ(1078097430000, v.to_unixtime(cctz::utc_time_zone()));
}

TEST(TimestampValueTest, from_uncommon_format_str_microsecond) {
    // Test that from_uncommon_format_str preserves microseconds parsed from %f
    {
        TimestampValue ts;
        std::string format = "%Y-%m-%dT%H:%i:%s.%f";
        std::string value = "2026-02-09T00:15:01.535569";
        bool result = ts.from_uncommon_format_str(format.c_str(), format.size(), value.c_str(), value.size());
        ASSERT_TRUE(result);
        EXPECT_EQ("2026-02-09 00:15:01.535569", ts.to_string());
    }
    // Test with fewer than 6 fractional digits
    {
        TimestampValue ts;
        std::string format = "%Y-%m-%d %H:%i:%s.%f";
        std::string value = "2026-02-09 12:30:45.123";
        bool result = ts.from_uncommon_format_str(format.c_str(), format.size(), value.c_str(), value.size());
        ASSERT_TRUE(result);
        EXPECT_EQ("2026-02-09 12:30:45.123000", ts.to_string());
    }
    // Test with zero microseconds
    {
        TimestampValue ts;
        std::string format = "%Y-%m-%d %H:%i:%s.%f";
        std::string value = "2026-02-09 12:30:45.000000";
        bool result = ts.from_uncommon_format_str(format.c_str(), format.size(), value.c_str(), value.size());
        ASSERT_TRUE(result);
        EXPECT_EQ("2026-02-09 12:30:45", ts.to_string());
    }
}

TEST(TimestampValueTest, from_date_format_str_rejects_zero_day_or_month) {
    TimestampValue ts;
    const char* fmt = "%Y-%m-%d";
    ASSERT_FALSE(ts.from_date_format_str("0000-01-00", 10, fmt));
    ASSERT_FALSE(ts.from_date_format_str("0000-00-01", 10, fmt));
    ASSERT_TRUE(ts.from_date_format_str("2020-01-01", 10, fmt));

    TimestampValue ts2;
    const char* dt_fmt = "%Y-%m-%d %H:%i:%s";
    ASSERT_FALSE(ts2.from_datetime_format_str("0000-01-00 00:00:00", 19, dt_fmt));
    ASSERT_TRUE(ts2.from_datetime_format_str("2020-01-01 00:00:00", 19, dt_fmt));
}

// from_unixtime_with_offset, given the zone's offset at the instant, builds exactly what from_unixtime builds with a
// cctz conversion, for TimestampValue and DateTimeValue: random instants over years 1..9999 and beyond, the edges of
// that range, negative and carried microseconds, and DST and fixed zones.
TEST(TimestampValueTest, from_unixtime_with_offset_matches_cctz) {
    std::mt19937_64 rng(5);
    std::vector<int64_t> seconds = {0,
                                    -1,
                                    1,
                                    timestamp::MIN_CIVIL_UNIX_SECOND,
                                    timestamp::MIN_CIVIL_UNIX_SECOND + 2 * SECS_PER_DAY,
                                    timestamp::MIN_CIVIL_UNIX_SECOND + 2 * SECS_PER_DAY - 1,
                                    timestamp::MAX_CIVIL_UNIX_SECOND,
                                    timestamp::MAX_CIVIL_UNIX_SECOND - 2 * SECS_PER_DAY,
                                    timestamp::MAX_CIVIL_UNIX_SECOND - 2 * SECS_PER_DAY + 1,
                                    timestamp::MIN_CIVIL_UNIX_SECOND - 86400 * 400,
                                    timestamp::MAX_CIVIL_UNIX_SECOND + 86400 * 400,
                                    std::numeric_limits<int64_t>::min() / 1000000,
                                    std::numeric_limits<int64_t>::max() / 1000000};
    for (int i = 0; i < 20000; i++) {
        seconds.push_back(timestamp::MIN_CIVIL_UNIX_SECOND +
                          static_cast<int64_t>(rng() % static_cast<uint64_t>(timestamp::MAX_CIVIL_UNIX_SECOND -
                                                                             timestamp::MIN_CIVIL_UNIX_SECOND)));
        seconds.push_back(static_cast<int64_t>(rng() % 4000000000ULL) - 1000000000LL);
    }
    for (const std::string name : {"America/Los_Angeles", "Europe/London", "Asia/Kolkata", "UTC", "-03:30"}) {
        cctz::time_zone tz;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(name, tz)) << name;
        for (size_t i = 0; i < seconds.size(); i++) {
            const int64_t second = seconds[i];
            const int64_t microsecond = static_cast<int64_t>(i % 5) * 250000 - 500000;
            // The offset a TzOffsetCache gives: cctz's at the instant.
            const cctz::time_point<cctz::seconds> tp =
                    std::chrono::time_point_cast<cctz::seconds>(std::chrono::system_clock::from_time_t(0)) +
                    cctz::seconds(second);
            const int64_t exact_offset = tz.lookup_offset(tp).offset;

            TimestampValue want;
            want.from_unixtime(second, microsecond, tz);
            TimestampValue got;
            got.from_unixtime_with_offset(second, microsecond, exact_offset);
            ASSERT_EQ(want.timestamp(), got.timestamp()) << name << " second=" << second << " us=" << microsecond;

            DateTimeValue want_dtv;
            ASSERT_TRUE(want_dtv.from_unixtime(second, microsecond, tz));
            DateTimeValue got_dtv;
            got_dtv.from_unixtime_with_offset(second, microsecond, exact_offset);
            ASSERT_EQ(want_dtv.year(), got_dtv.year()) << name << " second=" << second;
            ASSERT_EQ(want_dtv.month(), got_dtv.month()) << name << " second=" << second;
            ASSERT_EQ(want_dtv.day(), got_dtv.day()) << name << " second=" << second;
            ASSERT_EQ(want_dtv.hour(), got_dtv.hour()) << name << " second=" << second;
            ASSERT_EQ(want_dtv.minute(), got_dtv.minute()) << name << " second=" << second;
            ASSERT_EQ(want_dtv.second(), got_dtv.second()) << name << " second=" << second;
            ASSERT_EQ(want_dtv.microsecond(), got_dtv.microsecond()) << name << " second=" << second;
            ASSERT_EQ(want_dtv.type(), got_dtv.type()) << name << " second=" << second;
            char want_buf[64];
            char got_buf[64];
            want_dtv.to_string(want_buf);
            got_dtv.to_string(got_buf);
            ASSERT_STREQ(want_buf, got_buf) << name << " second=" << second;
        }
    }
}

} // namespace starrocks
