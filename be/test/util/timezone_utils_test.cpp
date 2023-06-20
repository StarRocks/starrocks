

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
    std::vector<std::pair<std::string, bool>> test_cases{{"Asia/Shanghai", true},
                                                         {"-8:00", true},
                                                         {"GMT+8:00", true},
                                                         {"GMT-8:00", true},
                                                         {"Asia/Shanghaia", false}};
    cctz::time_zone ctz;
    for (const auto& test_case : test_cases) {
        EXPECT_EQ(TimezoneUtils::find_cctz_time_zone(test_case.first, ctz), test_case.second);
    }
}

} // namespace starrocks