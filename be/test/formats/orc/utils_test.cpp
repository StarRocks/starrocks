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

#include "formats/orc/utils.h"

#include <gtest/gtest.h>

#include "base/time/timezone_utils.h"
#include "column/column_helper.h"
#include "formats/orc/orc_min_max_decoder.h"
#include "runtime/descriptors.h"

namespace orc {

TEST(Int128CompareTest, Simple) {
    Int128 x = 123;
    EXPECT_EQ(Int128(123), x);
    EXPECT_EQ(true, x == 123);
    EXPECT_EQ(true, !(x == 124));
    EXPECT_EQ(true, !(x == -124));
    EXPECT_EQ(true, !(x == Int128(2, 123)));
    EXPECT_EQ(true, !(x != 123));
    EXPECT_EQ(true, x != -123);
    EXPECT_EQ(true, x != 124);
    EXPECT_EQ(true, x != Int128(-1, 123));
    x = Int128(0x123, 0x456);
    EXPECT_EQ(true, !(x < Int128(0x123, 0x455)));
    EXPECT_EQ(true, !(x < Int128(0x123, 0x456)));
    EXPECT_EQ(true, x < Int128(0x123, 0x457));
    EXPECT_EQ(true, !(x < Int128(0x122, 0x456)));
    EXPECT_EQ(true, x < Int128(0x124, 0x456));

    EXPECT_EQ(true, !(x <= Int128(0x123, 0x455)));
    EXPECT_EQ(true, x <= Int128(0x123, 0x456));
    EXPECT_EQ(true, x <= Int128(0x123, 0x457));
    EXPECT_EQ(true, !(x <= Int128(0x122, 0x456)));
    EXPECT_EQ(true, x <= Int128(0x124, 0x456));

    EXPECT_EQ(true, x > Int128(0x123, 0x455));
    EXPECT_EQ(true, !(x > Int128(0x123, 0x456)));
    EXPECT_EQ(true, !(x > Int128(0x123, 0x457)));
    EXPECT_EQ(true, x > Int128(0x122, 0x456));
    EXPECT_EQ(true, !(x > Int128(0x124, 0x456)));

    EXPECT_EQ(true, x >= Int128(0x123, 0x455));
    EXPECT_EQ(true, x >= Int128(0x123, 0x456));
    EXPECT_EQ(true, !(x >= Int128(0x123, 0x457)));
    EXPECT_EQ(true, x >= Int128(0x122, 0x456));
    EXPECT_EQ(true, !(x >= Int128(0x124, 0x456)));

    EXPECT_EQ(true, Int128(-3) < Int128(-2));
    EXPECT_EQ(true, Int128(-3) < Int128(0));
    EXPECT_EQ(true, Int128(-3) < Int128(3));
    EXPECT_EQ(true, Int128(0) < Int128(5));
    EXPECT_EQ(true, Int128::minimumValue() < 0);
    EXPECT_EQ(true, Int128(0) < Int128::maximumValue());
    EXPECT_EQ(true, Int128::minimumValue() < Int128::maximumValue());
}

TEST(Decimal, testDecimalComparison) {
    // same scales
    EXPECT_TRUE(compare(Decimal(Int128(99), 0), Decimal(Int128(100), 0)));
    EXPECT_TRUE(compare(Decimal(Int128(34543), 5), Decimal(Int128(4324324), 5)));
    EXPECT_TRUE(compare(Decimal(Int128(345345435432l), 15), Decimal(Int128(345344425435432l), 15)));
    EXPECT_TRUE(compare(Decimal(Int128(5), 20), Decimal(Int128(50), 20)));

    // different scales
    EXPECT_TRUE(compare(Decimal(Int128(10000), 4), Decimal(Int128(10000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(1111), 3), Decimal(Int128(111), 2)));
    EXPECT_TRUE(compare(Decimal(Int128(999999), 5), Decimal(Int128(9999999), 5)));
    EXPECT_FALSE(compare(Decimal(Int128(99), 0), Decimal(Int128(100), 1)));

    // same integral parts
    EXPECT_TRUE(compare(Decimal(Int128(99999), 0), Decimal(Int128(999999), 1)));
    EXPECT_TRUE(compare(Decimal(Int128(12345123), 3), Decimal(Int128(1234553432), 5)));

    // equal numbers
    EXPECT_FALSE(compare(Decimal(Int128(100000), 3), Decimal(Int128(100), 0)));
    EXPECT_FALSE(compare(Decimal(Int128(100), 0), Decimal(Int128(100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(100000), 3), Decimal(Int128(100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(100000), 3), Decimal(Int128(100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(1), 10), Decimal(Int128(10), 11)));
    EXPECT_FALSE(compare(Decimal(Int128(10), 11), Decimal(Int128(1), 10)));

    // large scales (>18)
    EXPECT_TRUE(compare(Decimal(Int128(99), 35), Decimal(Int128(100), 35)));
    EXPECT_TRUE(compare(Decimal(Int128("12345678999999999999999999999999999998"), 29),
                        Decimal(Int128("123456789999999999999999999999999999999"), 30)));
    EXPECT_FALSE(compare(Decimal(Int128("123456789999999999999999999999999999900"), 30),
                         Decimal(Int128("12345678999999999999999999999999999990"), 29)));
    EXPECT_FALSE(compare(Decimal(Int128("12345678999999999999999999999999999990"), 29),
                         Decimal(Int128("123456789999999999999999999999999999900"), 30)));

    // fractional overflow
    EXPECT_TRUE(compare(Decimal(Int128::maximumValue(), 39),
                        Decimal(Int128("99999999999999999999999999999999999999"), 38)));

    // negative numbers
    EXPECT_TRUE(compare(Decimal(Int128(-99), 0), Decimal(Int128(100), 0)));
    EXPECT_TRUE(compare(Decimal(Int128(-4324324), 5), Decimal(Int128(-34543), 5)));
    EXPECT_TRUE(compare(Decimal(Int128(-345344425435432l), 15), Decimal(Int128(-345345435432l), 15)));
    EXPECT_TRUE(compare(Decimal(Int128(-50), 20), Decimal(Int128(-5), 20)));
    EXPECT_TRUE(compare(Decimal(Int128(-10000), 3), Decimal(Int128(-10000), 4)));
    EXPECT_TRUE(compare(Decimal(Int128(-1111), 3), Decimal(Int128(-111), 2)));
    EXPECT_TRUE(compare(Decimal(Int128(-9999999), 5), Decimal(Int128(-999999), 5)));
    EXPECT_TRUE(compare(Decimal(Int128(-99), 0), Decimal(Int128(-100), 1)));
    EXPECT_TRUE(compare(Decimal(Int128(-999999), 1), Decimal(Int128(-99999), 0)));
    EXPECT_TRUE(compare(Decimal(Int128(-1234553432), 5), Decimal(Int128(-12345123), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(-100000), 3), Decimal(Int128(-100), 0)));
    EXPECT_FALSE(compare(Decimal(Int128(-100), 0), Decimal(Int128(-100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(-100000), 3), Decimal(Int128(-100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(-100000), 3), Decimal(Int128(-100000), 3)));
    EXPECT_FALSE(compare(Decimal(Int128(-1), 10), Decimal(Int128(-10), 11)));
    EXPECT_FALSE(compare(Decimal(Int128(-10), 11), Decimal(Int128(-1), 10)));
    EXPECT_TRUE(compare(Decimal(Int128(-100), 35), Decimal(Int128(-99), 35)));
    EXPECT_TRUE(compare(Decimal(Int128("-123456789999999999999999999999999999999"), 30),
                        Decimal(Int128("-12345678999999999999999999999999999998"), 29)));
    EXPECT_FALSE(compare(Decimal(Int128("-123456789999999999999999999999999999900"), 30),
                         Decimal(Int128("-12345678999999999999999999999999999990"), 29)));
    EXPECT_FALSE(compare(Decimal(Int128("-12345678999999999999999999999999999990"), 29),
                         Decimal(Int128("-123456789999999999999999999999999999900"), 30)));
    EXPECT_TRUE(compare(Decimal(Int128("-99999999999999999999999999999999999999"), 38),
                        Decimal(Int128::minimumValue(), 39)));

    // more tests
    EXPECT_TRUE(compare(Decimal(Int128("-99999999999999999999999999999999999999"), 38), Decimal(Int128(100), 0)));
    EXPECT_FALSE(compare(Decimal(Int128(0), 38), Decimal(Int128(Int128::minimumValue()), 37)));
    EXPECT_FALSE(compare(Decimal(Int128(Int128::maximumValue()), 38), Decimal(Int128("-1"), 37)));
}

} // namespace orc

namespace starrocks {

static TimestampValue orc_instant(int64_t seconds, const cctz::time_zone& tz) {
    TimestampValue tv;
    OrcTimestampHelper::orc_ts_to_native_ts(&tv, tz, seconds, 0, /*is_instant=*/true);
    return tv;
}

// An instant is shown with the offset in effect at that instant, as the parquet reader does, not with the zone's
// offset at the epoch (Europe/London was UTC+1 all year in 1970).
TEST(OrcTimestampHelperTest, InstantUsesOffsetAtInstant) {
    cctz::time_zone london;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Europe/London", london));
    EXPECT_EQ("2024-07-01 13:00:00", orc_instant(1719835200, london).to_string()); // 2024-07-01 12:00 UTC, BST
    EXPECT_EQ("2024-01-01 12:00:00", orc_instant(1704110400, london).to_string()); // 2024-01-01 12:00 UTC, GMT
    // the reader's batch conversion gives the same values across the BST start / end, in and out of order, for a
    // batch within one offset window and before the epoch, with null rows skipped
    TzOffsetCache cache;
    std::vector<int64_t> offsets;
    std::vector<std::vector<int64_t>> batches = {
            {1711846800L - 1, 1711846800L, 1729990800L - 1, 1729990800L, 1719835200L, 1704110400L},
            {1719835200L, 1719835200L + 60, 1719835200L - 86400},
            {-86400L * 365 + 3600, 1719835200L, -1L}};
    for (const auto& seconds : batches) {
        std::vector<int64_t> nanos(seconds.size(), 500000000L);
        std::vector<uint8_t> is_null(seconds.size(), 0);
        is_null[1] = 1;
        std::vector<TimestampValue> out(seconds.size());
        OrcTimestampHelper::orc_instants_to_native_ts(out.data(), london, seconds.data(), nanos.data(), is_null.data(),
                                                      seconds.size(), &cache, &offsets);
        for (size_t i = 0; i < seconds.size(); i++) {
            if (is_null[i]) {
                continue;
            }
            TimestampValue want;
            OrcTimestampHelper::orc_ts_to_native_ts(&want, london, seconds[i], nanos[i], /*is_instant=*/true);
            EXPECT_EQ(want.to_string(), out[i].to_string()) << seconds[i];
        }
    }
    // the first hours after the epoch fall before 1970 west of UTC
    cctz::time_zone los_angeles;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("America/Los_Angeles", los_angeles));
    EXPECT_EQ("1969-12-31 16:00:00", orc_instant(0, los_angeles).to_string());
    EXPECT_EQ("1969-12-31 23:59:59", orc_instant(8 * 3600 - 1, los_angeles).to_string());
    EXPECT_EQ("1970-01-01 00:00:00", orc_instant(8 * 3600, los_angeles).to_string());
    // a plain TIMESTAMP is not shifted
    TimestampValue tv;
    OrcTimestampHelper::orc_ts_to_native_ts(&tv, london, 1704110400, 0, /*is_instant=*/false);
    EXPECT_EQ("2024-01-01 12:00:00", tv.to_string());
}

static std::pair<std::string, std::string> decode_instant_min_max(int64_t min_ms, int64_t max_ms,
                                                                  const cctz::time_zone& tz) {
    std::unique_ptr<orc::Type> orc_type = orc::createPrimitiveType(orc::TypeKind::TIMESTAMP_INSTANT);
    TypeDescriptor type(TYPE_DATETIME);
    SlotDescriptor slot(0, "c0", type);
    orc::proto::ColumnStatistics stats;
    stats.mutable_timestampstatistics()->set_minimumutc(min_ms);
    stats.mutable_timestampstatistics()->set_maximumutc(max_ms);
    auto min_col = ColumnHelper::create_column(type, slot.is_nullable());
    auto max_col = ColumnHelper::create_column(type, slot.is_nullable());
    // tz_offset_in_seconds (the epoch-offset model) is ignored when the reader zone is given.
    CHECK(OrcMinMaxDecoder::decode(&slot, orc_type.get(), stats, min_col.get(), max_col.get(), 12345, &tz).ok());
    return {min_col->debug_item(0), max_col->debug_item(0)};
}

// Row-group bounds of an instant column are converted like its values, and stay a superset of the loaded values
// across a daylight saving shift just inside the range.
TEST(OrcTimestampHelperTest, InstantMinMaxUsesOffsetAtInstant) {
    cctz::time_zone london;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Europe/London", london));
    auto [min, max] = decode_instant_min_max(1719835200000L, 1719835200000L, london);
    EXPECT_EQ("2024-07-01 13:00:00", min);
    EXPECT_EQ("2024-07-01 13:00:00.001000", max); // an absent max nanos is ceiled

    // BST ends at 2024-10-27 01:00 UTC: 00:30 UTC loads as 01:30, 01:10 UTC loads as 01:10, below 00:30 + 1h.
    int64_t before_shift = 1729989000; // 2024-10-27 00:30:00 UTC
    int64_t after_shift = 1729991400;  // 2024-10-27 01:10:00 UTC
    auto [lo, hi] = decode_instant_min_max(before_shift * 1000, after_shift * 1000, london);
    EXPECT_LE(lo, orc_instant(after_shift, london).to_string());
    EXPECT_GE(hi, orc_instant(before_shift, london).to_string());

    // Every value between the bounds of a range loads between the decoded bounds, also across a three-hour backward
    // shift (Antarctica/Casey, UTC+11 -> UTC+8 at 2018-03-10 17:00 UTC) and past 2262 (no clamped lookup).
    cctz::time_zone casey;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Antarctica/Casey", casey));
    struct Range {
        const cctz::time_zone* tz;
        int64_t lo;
        int64_t hi;
    };
    for (Range r : {Range{&casey, 1520692200, 1520701200}, Range{&london, 10413792000LL, 10413792000LL + 86400 * 200},
                    Range{&london, 1729989000, 1729991400}, Range{&london, -86400 * 30, 86400 * 30}}) {
        auto [min_bound, max_bound] = decode_instant_min_max(r.lo * 1000, r.hi * 1000, *r.tz);
        for (int64_t v = r.lo; v <= r.hi; v += std::max<int64_t>(1, (r.hi - r.lo) / 997)) {
            EXPECT_LE(min_bound, orc_instant(v, *r.tz).to_string()) << v;
            EXPECT_GE(max_bound, orc_instant(v, *r.tz).to_string()) << v;
        }
        EXPECT_GE(max_bound, orc_instant(r.hi, *r.tz).to_string()) << r.hi;
    }
}

} // namespace starrocks
