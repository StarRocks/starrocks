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

#include "butil/time.h"
#include "column/fixed_length_column.h"
#include "runtime/time_types.h"
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
    ASSERT_EQ("2004-02-29", ((DateValue)v).to_string());
}

} // namespace starrocks
