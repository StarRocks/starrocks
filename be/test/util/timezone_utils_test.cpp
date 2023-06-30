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

#include "util/timezone_utils.h"

#include <gtest/gtest.h>

#include <string>

#include "common/logging.h"
#include "runtime/datetime_value.h"
#include "testutil/parallel_test.h"
#include "util/logging.h"

namespace starrocks {

class TimezoneUtilTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

PARALLEL_TEST(TimezoneUtilTest, utc_to_offset) {
    cctz::time_zone ctz;
    bool ok = TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", ctz);
    EXPECT_TRUE(ok);
    int64_t offset = TimezoneUtils::to_utc_offset(ctz);
    EXPECT_EQ(offset, 28800);

    ok = TimezoneUtils::find_cctz_time_zone("+08:00", ctz);
    EXPECT_TRUE(ok);
    offset = TimezoneUtils::to_utc_offset(ctz);
    EXPECT_EQ(offset, 28800);

    ok = TimezoneUtils::find_cctz_time_zone("-08:00", ctz);
    EXPECT_TRUE(ok);
    offset = TimezoneUtils::to_utc_offset(ctz);
    EXPECT_EQ(offset, -28800);
}

PARALLEL_TEST(TimezoneUtilTest, find_time_zone) {
    std::vector<std::pair<std::string, bool>> test_cases{
            {"Asia/Shanghai", true},   {"-08:00", true}, {"GMT+08:00", true}, {"GMT-08:00", true},
            {"Asia/Shanghaia", false}, {"-8:00", false}, {"GMT-8:00", false}};
    cctz::time_zone ctz;
    for (const auto& test_case : test_cases) {
        EXPECT_EQ(TimezoneUtils::find_cctz_time_zone(test_case.first, ctz), test_case.second) << test_case.first;
    }
}

} // namespace starrocks
