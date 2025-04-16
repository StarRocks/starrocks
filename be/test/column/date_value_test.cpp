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

#include <type_traits>

#include "butil/time.h"
#include "column/fixed_length_column.h"
#include "runtime/time_types.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

TEST(DateValueTest, normalDate) {
    ASSERT_EQ(1, std::is_pod_v<starrocks::DateValue>);
    ASSERT_EQ(1, std::is_pod_v<starrocks::TimestampValue>);

    LOG(WARNING) << "0000-1-1: " << date::from_date(0000, 1, 1);
    LOG(WARNING) << "2000-1-1: " << date::from_date(2000, 1, 1);
    LOG(WARNING) << "3560-1-1: " << date::from_date(3560, 1, 1);
    LOG(WARNING) << "1970-1-1: " << date::from_date(1970, 1, 1);
    LOG(WARNING) << "1971-1-1: " << date::from_date(1971, 1, 1);
    LOG(WARNING) << "1971-2-1: " << date::from_date(1971, 2, 1);
    LOG(WARNING) << "1971-2-2: " << date::from_date(1971, 2, 2);
    LOG(WARNING) << "2001-2-2: " << date::from_date(2001, 2, 2);
    LOG(WARNING) << "2004-2-2: " << date::from_date(2004, 2, 2);
    LOG(WARNING) << "2004-1-1: " << date::from_date(2004, 1, 1);
    LOG(WARNING) << "2005-1-1: " << date::from_date(2005, 1, 1);
    LOG(WARNING) << "2006-1-1: " << date::from_date(2006, 1, 1);
    LOG(WARNING) << "2007-1-1: " << date::from_date(2007, 1, 1);
    LOG(WARNING) << "2008-1-1: " << date::from_date(2008, 1, 1);
    LOG(WARNING) << "2009-1-1: " << date::from_date(2009, 1, 1);
    LOG(WARNING) << "BC 0000: " << date::from_date(0000, 1, 1);
    LOG(WARNING) << "UNIX: " << date::from_date(1970, 1, 1);
    LOG(WARNING) << "MIN: " << date::from_date(-2000, 1, 1);
    LOG(WARNING) << "MAX: " << date::from_date(9999, 12, 31);

    {
        int year, month, day;
        date::to_date(0, &year, &month, &day);
        LOG(WARNING) << year << "-" << month << "-" << day;
    }

    {
        // BCE
        int year, month, day;
        auto julian = date::from_date(-200, 1, 1);
        date::to_date(julian, &year, &month, &day);

        ASSERT_EQ(-200, year);
        ASSERT_EQ(1, month);
        ASSERT_EQ(1, day);
    }

    {
        int year, month, day;
        auto julian = date::from_date(1971, 1, 1);
        date::to_date(julian, &year, &month, &day);

        ASSERT_EQ(1971, year);
        ASSERT_EQ(1, month);
        ASSERT_EQ(1, day);
    }
    {
        int year, month, day;
        auto julian = date::from_date(2003, 2, 28);
        date::to_date(julian, &year, &month, &day);

        LOG(INFO) << "2003-2-28: " << julian;
        ASSERT_EQ(2003, year);
        ASSERT_EQ(2, month);
        ASSERT_EQ(28, day);

        date::to_date(julian + 1, &year, &month, &day);
        LOG(WARNING) << year << "-" << month << "-" << day;

        date::to_date(julian + 2, &year, &month, &day);
        LOG(WARNING) << year << "-" << month << "-" << day;
    }

    {
        int year, month, day;
        auto julian = date::from_date(2004, 2, 29);
        date::to_date(julian, &year, &month, &day);

        LOG(INFO) << "2004-2-29: " << julian;
        ASSERT_EQ(2004, year);
        ASSERT_EQ(2, month);
        ASSERT_EQ(29, day);

        date::to_date(julian + 1, &year, &month, &day);
        LOG(WARNING) << year << "-" << month << "-" << day;

        date::to_date(julian + 2, &year, &month, &day);
        LOG(WARNING) << year << "-" << month << "-" << day;
    }
}

TEST(DateValueTest, normalTimestamp) {
    {
        int year, month, day, hour, minute, sec, usec;
        auto timestamp = timestamp::from_datetime(2004, 2, 29, 12, 1, 2, 1);

        timestamp::to_datetime(timestamp, &year, &month, &day, &hour, &minute, &sec, &usec);

        ASSERT_EQ(2004, year);
        ASSERT_EQ(2, month);
        ASSERT_EQ(29, day);
        ASSERT_EQ(12, hour);
        ASSERT_EQ(1, minute);
        ASSERT_EQ(1, usec);
    }

    {
        int year, month, day, hour, minute, sec, usec;
        auto timestamp = timestamp::from_datetime(1993, 2, 1, 12, 1, 2, 1);

        timestamp::to_datetime(timestamp, &year, &month, &day, &hour, &minute, &sec, &usec);

        ASSERT_EQ(1993, year);
        ASSERT_EQ(2, month);
        ASSERT_EQ(1, day);
        ASSERT_EQ(12, hour);
        ASSERT_EQ(1, minute);
        ASSERT_EQ(1, usec);
    }

    {
        int year, month, day, hour, minute, sec, usec;
        auto timestamp = timestamp::from_datetime(-2000, 2, 1, 20, 1, 2, 1);

        timestamp::to_datetime(timestamp, &year, &month, &day, &hour, &minute, &sec, &usec);

        ASSERT_EQ(-2000, year);
        ASSERT_EQ(2, month);
        ASSERT_EQ(1, day);
        ASSERT_EQ(20, hour);
        ASSERT_EQ(1, minute);
        ASSERT_EQ(1, usec);
    }

    {
        // MIN
        int year, month, day, hour, minute, sec, usec;
        timestamp::to_datetime(INT64_MIN, &year, &month, &day, &hour, &minute, &sec, &usec);

        LOG(WARNING) << year << "-" << month << "-" << day << " " << hour << ":" << minute << ":" << sec;
    }

    {
        // MAX
        int year, month, day, hour, minute, sec, usec;
        timestamp::to_datetime(INT64_MAX, &year, &month, &day, &hour, &minute, &sec, &usec);

        LOG(WARNING) << year << "-" << month << "-" << day << " " << hour << ":" << minute << ":" << sec;
    }
}

TEST(DateValueTest, getOffsetByTimezone) {
    {
        auto timestampInDST = timestamp::from_datetime(1986, 8, 25, 0, 0, 0, 0);
        auto timezone = "Asia/Shanghai";
        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(timezone, ctz);

        auto offset = timestamp::get_timezone_offset_by_timestamp(timestampInDST, ctz);

        ASSERT_EQ(32400, offset);
    }

    {
        auto timestampOutOfDST = timestamp::from_datetime(2004, 8, 25, 0, 0, 0, 0);
        auto timezone = "Asia/Shanghai";
        cctz::time_zone ctz;
        TimezoneUtils::find_cctz_time_zone(timezone, ctz);

        auto offset = timestamp::get_timezone_offset_by_timestamp(timestampOutOfDST, ctz);

        ASSERT_EQ(28800, offset);
    }
}

TEST(DateValueTest, calculate) {
    DateValue dv;
    {
        dv.from_date(2004, 2, 20);
        auto v = dv.add<TimeUnit::DAY>(10);
        ASSERT_EQ("2004-03-01", v.to_string());
    }
    {
        dv.from_date(2004, 2, 29);
        auto v = dv.add<TimeUnit::YEAR>(1);
        ASSERT_EQ("2005-02-28", v.to_string());
    }
    {
        dv.from_date(2004, 2, 29);
        auto v = dv.add<TimeUnit::DAY>(365);
        ASSERT_EQ("2005-02-28", v.to_string());
    }
    {
        dv.from_date(2004, 2, 29);
        auto v = dv.add<TimeUnit::YEAR>(-1);
        ASSERT_EQ("2003-02-28", v.to_string());
    }

    {
        dv.from_date(2004, 2, 29);
        auto v = dv.add<TimeUnit::YEAR>(-1);
        ASSERT_EQ("2003-02-28", v.to_string());
    }
    {
        dv.from_date(2004, 2, 29);
        auto v = dv.add<TimeUnit::YEAR>(-1);
        ASSERT_EQ("2003-02-28", v.to_string());
    }
    {
        dv.from_date(2003, 12, 29);
        auto v = dv.add<TimeUnit::MONTH>(2);
        ASSERT_EQ("2004-02-29", v.to_string());
    }
    {
        dv.from_date(2003, 12, 29);
        auto v = dv.add<TimeUnit::MONTH>(12);
        ASSERT_EQ("2004-12-29", v.to_string());
    }
    {
        dv.from_date(2003, 12, 29);
        auto v = dv.add<TimeUnit::MONTH>(13);
        ASSERT_EQ("2005-01-29", v.to_string());
    }
    {
        dv.from_date(2003, 12, 31);
        auto v = dv.add<TimeUnit::MONTH>(1);
        ASSERT_EQ("2004-01-31", v.to_string());
    }
    {
        dv.from_date(2004, 3, 31);
        auto v = dv.add<TimeUnit::MONTH>(-3);
        ASSERT_EQ("2003-12-31", v.to_string());
    }
}

TEST(DateValueTest, cast) {
    DateValue dv;
    dv.from_date(2004, 3, 31);
    auto v = (TimestampValue)dv;
    ASSERT_EQ("2004-03-31 00:00:00", v.to_string());
}

TEST(DateValueTest, weekday) {
    DateValue dv;
    dv.from_date(2020, 5, 31);
    ASSERT_EQ(0, dv.weekday()); // Sunday
    dv.from_date(2020, 6, 1);
    ASSERT_EQ(1, dv.weekday()); // Monday
    dv.from_date(2020, 6, 2);
    ASSERT_EQ(2, dv.weekday()); // Tuesday
    dv.from_date(2020, 6, 3);
    ASSERT_EQ(3, dv.weekday()); // Wednesday
    dv.from_date(2020, 6, 4);
    ASSERT_EQ(4, dv.weekday()); // Thursday
    dv.from_date(2020, 6, 5);
    ASSERT_EQ(5, dv.weekday()); // Friday
    dv.from_date(2020, 6, 6);
    ASSERT_EQ(6, dv.weekday()); // Saturday
    dv.from_date(2020, 6, 7);
    ASSERT_EQ(0, dv.weekday()); // Sunday
}

} // namespace starrocks
