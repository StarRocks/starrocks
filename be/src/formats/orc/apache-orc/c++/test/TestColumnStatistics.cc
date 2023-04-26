/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cmath>

#include "Statistics.hh"
#include "orc/OrcFile.hh"
#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

TEST(ColumnStatistics, intColumnStatistics) {
    std::unique_ptr<IntegerColumnStatisticsImpl> intStats(new IntegerColumnStatisticsImpl());

    // initial state
    EXPECT_EQ(0, intStats->getNumberOfValues());
    EXPECT_FALSE(intStats->hasNull());
    EXPECT_FALSE(intStats->hasMinimum());
    EXPECT_FALSE(intStats->hasMaximum());
    EXPECT_TRUE(intStats->hasSum());
    EXPECT_EQ(0, intStats->getSum());

    // normal operations
    intStats->increase(1);
    EXPECT_EQ(1, intStats->getNumberOfValues());

    intStats->increase(0);
    EXPECT_EQ(1, intStats->getNumberOfValues());

    intStats->increase(100);
    EXPECT_EQ(101, intStats->getNumberOfValues());

    intStats->increase(9999999999999899l);
    EXPECT_EQ(10000000000000000l, intStats->getNumberOfValues());

    intStats->update(0, 1);
    EXPECT_TRUE(intStats->hasMinimum());
    EXPECT_TRUE(intStats->hasMaximum());
    EXPECT_EQ(0, intStats->getMaximum());
    EXPECT_EQ(0, intStats->getMinimum());
    EXPECT_EQ(0, intStats->getSum());

    intStats->update(-100, 1);
    intStats->update(101, 1);
    EXPECT_EQ(101, intStats->getMaximum());
    EXPECT_EQ(-100, intStats->getMinimum());
    EXPECT_EQ(1, intStats->getSum());

    intStats->update(-50, 2);
    intStats->update(50, 3);
    EXPECT_EQ(101, intStats->getMaximum());
    EXPECT_EQ(-100, intStats->getMinimum());
    EXPECT_EQ(51, intStats->getSum());

    // test merge
    std::unique_ptr<IntegerColumnStatisticsImpl> other(new IntegerColumnStatisticsImpl());

    other->setHasNull(true);
    other->increase(100);
    other->setMaximum(9999);
    other->setMinimum(-9999);
    other->setSum(100000);
    EXPECT_EQ(100, other->getNumberOfValues());
    EXPECT_TRUE(other->hasNull());
    EXPECT_EQ(9999, other->getMaximum());
    EXPECT_EQ(-9999, other->getMinimum());
    EXPECT_TRUE(other->hasSum());
    EXPECT_EQ(100000, other->getSum());

    intStats->merge(*other);
    EXPECT_EQ(10000000000000100l, intStats->getNumberOfValues());
    EXPECT_TRUE(intStats->hasNull());
    EXPECT_EQ(100051, intStats->getSum());
    EXPECT_EQ(9999, intStats->getMaximum());
    EXPECT_EQ(-9999, intStats->getMinimum());

    // test overflow positively
    other->update(std::numeric_limits<int64_t>::max(), 1);
    EXPECT_FALSE(other->hasSum());

    intStats->merge(*other);
    EXPECT_FALSE(intStats->hasSum());

    // test overflow negatively
    intStats->setSum(-1000);
    other->setSum(std::numeric_limits<int64_t>::min() + 500);
    EXPECT_EQ(-1000, intStats->getSum());
    EXPECT_EQ(std::numeric_limits<int64_t>::min() + 500, other->getSum());
    intStats->merge(*other);
    EXPECT_FALSE(intStats->hasSum());

    std::unique_ptr<IntegerColumnStatisticsImpl> intStats2(new IntegerColumnStatisticsImpl());
    intStats2->update(1, 1);
    EXPECT_TRUE(intStats2->hasSum());
    intStats2->update(std::numeric_limits<int64_t>::max(), 3);
    EXPECT_FALSE(intStats2->hasSum());
}

TEST(ColumnStatistics, doubleColumnStatistics) {
    std::unique_ptr<DoubleColumnStatisticsImpl> dblStats(new DoubleColumnStatisticsImpl());

    // initial state
    EXPECT_EQ(0, dblStats->getNumberOfValues());
    EXPECT_FALSE(dblStats->hasNull());
    EXPECT_FALSE(dblStats->hasMinimum());
    EXPECT_FALSE(dblStats->hasMaximum());
    EXPECT_TRUE(dblStats->hasSum());
    EXPECT_TRUE(std::abs(0.0 - dblStats->getSum()) < 0.00001);

    // normal operations
    dblStats->increase(1);
    EXPECT_EQ(1, dblStats->getNumberOfValues());

    dblStats->increase(0);
    EXPECT_EQ(1, dblStats->getNumberOfValues());

    dblStats->increase(100);
    EXPECT_EQ(101, dblStats->getNumberOfValues());

    dblStats->increase(899);
    EXPECT_EQ(1000, dblStats->getNumberOfValues());

    dblStats->update(5.5);
    EXPECT_TRUE(dblStats->hasMinimum());
    EXPECT_TRUE(dblStats->hasMaximum());
    EXPECT_TRUE(std::abs(5.5 - dblStats->getMaximum()) < 0.00001);
    EXPECT_TRUE(std::abs(5.5 - dblStats->getMinimum()) < 0.00001);
    EXPECT_TRUE(std::abs(5.5 - dblStats->getSum()) < 0.00001);

    dblStats->update(13.25);
    dblStats->update(0.11117);
    dblStats->update(1000232.535);
    dblStats->update(-324.43);
    dblStats->update(-95454.5343);
    dblStats->update(63433.54543);

    EXPECT_TRUE(std::abs(967905.9773 - dblStats->getSum()) < 0.00001);
    EXPECT_TRUE(std::abs(1000232.535 - dblStats->getMaximum()) < 0.00001);
    EXPECT_TRUE(std::abs(-95454.5343 - dblStats->getMinimum()) < 0.00001);

    // test merge
    std::unique_ptr<DoubleColumnStatisticsImpl> other(new DoubleColumnStatisticsImpl());

    other->setHasNull(true);
    other->increase(987);
    other->setMaximum(1000232.5355);
    other->setMinimum(-9999.0);
    other->setSum(3435.343);
    EXPECT_EQ(987, other->getNumberOfValues());
    EXPECT_TRUE(other->hasNull());
    EXPECT_TRUE(std::abs(1000232.5355 - other->getMaximum()) < 0.00001);
    EXPECT_TRUE(std::abs(-9999.0 - other->getMinimum()) < 0.00001);
    EXPECT_TRUE(std::abs(3435.343 - other->getSum()) < 0.00001);

    dblStats->merge(*other);
    EXPECT_EQ(1987, dblStats->getNumberOfValues());
    EXPECT_TRUE(dblStats->hasNull());
    EXPECT_TRUE(std::abs(1000232.5355 - dblStats->getMaximum()) < 0.00001);
    EXPECT_TRUE(std::abs(-95454.5343 - dblStats->getMinimum()) < 0.00001);
    EXPECT_TRUE(std::abs(971341.3203 - dblStats->getSum()) < 0.00001);
}

TEST(ColumnStatistics, stringColumnStatistics) {
    std::unique_ptr<StringColumnStatisticsImpl> strStats(new StringColumnStatisticsImpl());

    EXPECT_FALSE(strStats->hasMinimum());
    EXPECT_FALSE(strStats->hasMaximum());
    EXPECT_EQ(0, strStats->getNumberOfValues());
    EXPECT_TRUE(strStats->hasTotalLength());
    EXPECT_EQ(0, strStats->getTotalLength());

    strStats->update("abc", 3);
    EXPECT_TRUE(strStats->hasMinimum());
    EXPECT_TRUE(strStats->hasMaximum());
    EXPECT_TRUE(strStats->hasTotalLength());
    EXPECT_EQ(3, strStats->getTotalLength());
    EXPECT_EQ("abc", strStats->getMaximum());
    EXPECT_EQ("abc", strStats->getMinimum());

    strStats->update("ab", 2);
    EXPECT_EQ(5, strStats->getTotalLength());
    EXPECT_EQ("abc", strStats->getMaximum());
    EXPECT_EQ("ab", strStats->getMinimum());

    strStats->update(nullptr, 0);
    EXPECT_EQ(5, strStats->getTotalLength());
    EXPECT_EQ("abc", strStats->getMaximum());
    EXPECT_EQ("ab", strStats->getMinimum());

    strStats->update("abcd", 4);
    EXPECT_EQ(9, strStats->getTotalLength());
    EXPECT_EQ("abcd", strStats->getMaximum());
    EXPECT_EQ("ab", strStats->getMinimum());

    strStats->update("xyz", 0);
    EXPECT_EQ(9, strStats->getTotalLength());
    EXPECT_EQ("abcd", strStats->getMaximum());
    EXPECT_EQ("", strStats->getMinimum());
}

TEST(ColumnStatistics, boolColumnStatistics) {
    std::unique_ptr<BooleanColumnStatisticsImpl> boolStats(new BooleanColumnStatisticsImpl());

    // initial state
    EXPECT_EQ(0, boolStats->getNumberOfValues());
    EXPECT_FALSE(boolStats->hasNull());
    EXPECT_EQ(0, boolStats->getTrueCount());
    EXPECT_EQ(0, boolStats->getFalseCount());

    // normal operations
    boolStats->increase(5);
    boolStats->update(true, 3);
    boolStats->update(false, 2);
    EXPECT_EQ(5, boolStats->getNumberOfValues());
    EXPECT_EQ(2, boolStats->getFalseCount());
    EXPECT_EQ(3, boolStats->getTrueCount());

    // test merge
    std::unique_ptr<BooleanColumnStatisticsImpl> other(new BooleanColumnStatisticsImpl());

    other->setHasNull(true);
    other->increase(100);
    other->update(true, 50);
    other->update(false, 50);

    boolStats->merge(*other);
    EXPECT_EQ(105, boolStats->getNumberOfValues());
    EXPECT_TRUE(boolStats->hasNull());
    EXPECT_EQ(53, boolStats->getTrueCount());
    EXPECT_EQ(52, boolStats->getFalseCount());
}

TEST(ColumnStatistics, timestampColumnStatistics) {
    std::unique_ptr<TimestampColumnStatisticsImpl> tsStats(new TimestampColumnStatisticsImpl());

    EXPECT_FALSE(tsStats->hasMaximum() || tsStats->hasMaximum());

    // normal operations
    tsStats->update(100);
    EXPECT_EQ(100, tsStats->getMaximum());
    EXPECT_EQ(100, tsStats->getMinimum());
    EXPECT_EQ(0, tsStats->getMinimumNanos());
    EXPECT_EQ(999999, tsStats->getMaximumNanos());

    tsStats->update(150);
    EXPECT_EQ(150, tsStats->getMaximum());
    EXPECT_EQ(100, tsStats->getMinimum());
    EXPECT_EQ(0, tsStats->getMinimumNanos());
    EXPECT_EQ(999999, tsStats->getMaximumNanos());

    // test merge
    std::unique_ptr<TimestampColumnStatisticsImpl> other(new TimestampColumnStatisticsImpl());

    other->setMaximum(160);
    other->setMinimum(90);

    tsStats->merge(*other);
    EXPECT_EQ(160, tsStats->getMaximum());
    EXPECT_EQ(90, tsStats->getMinimum());
    EXPECT_EQ(0, tsStats->getMinimumNanos());
    EXPECT_EQ(999999, tsStats->getMaximumNanos());
}

TEST(ColumnStatistics, dateColumnStatistics) {
    std::unique_ptr<DateColumnStatisticsImpl> tsStats(new DateColumnStatisticsImpl());

    EXPECT_FALSE(tsStats->hasMaximum() || tsStats->hasMaximum());

    // normal operations
    tsStats->update(100);
    EXPECT_EQ(100, tsStats->getMaximum());
    EXPECT_EQ(100, tsStats->getMinimum());

    tsStats->update(150);
    EXPECT_EQ(150, tsStats->getMaximum());
    EXPECT_EQ(100, tsStats->getMinimum());

    // test merge
    std::unique_ptr<DateColumnStatisticsImpl> other(new DateColumnStatisticsImpl());

    other->setMaximum(160);
    other->setMinimum(90);

    tsStats->merge(*other);
    EXPECT_EQ(160, other->getMaximum());
    EXPECT_EQ(90, other->getMinimum());
}

TEST(ColumnStatistics, otherColumnStatistics) {
    std::unique_ptr<ColumnStatisticsImpl> stats(new ColumnStatisticsImpl());

    EXPECT_EQ(0, stats->getNumberOfValues());
    EXPECT_FALSE(stats->hasNull());

    stats->increase(5);
    EXPECT_EQ(5, stats->getNumberOfValues());

    stats->increase(10);
    EXPECT_EQ(15, stats->getNumberOfValues());

    stats->setHasNull(true);
    EXPECT_TRUE(stats->hasNull());
}

TEST(ColumnStatistics, decimalColumnStatistics) {
    std::unique_ptr<DecimalColumnStatisticsImpl> decStats(new DecimalColumnStatisticsImpl());

    // initial state
    EXPECT_EQ(0, decStats->getNumberOfValues());
    EXPECT_FALSE(decStats->hasNull());
    EXPECT_FALSE(decStats->hasMinimum());
    EXPECT_FALSE(decStats->hasMaximum());
    EXPECT_TRUE(decStats->hasSum());
    EXPECT_EQ(Int128(0), decStats->getSum().value);
    EXPECT_EQ(0, decStats->getSum().scale);

    // normal operations
    decStats->update(Decimal(100, 1));
    EXPECT_TRUE(decStats->hasMinimum());
    EXPECT_TRUE(decStats->hasMaximum());
    EXPECT_TRUE(decStats->hasSum());
    EXPECT_EQ(Int128(100), decStats->getMaximum().value);
    EXPECT_EQ(1, decStats->getMaximum().scale);
    EXPECT_EQ(Int128(100), decStats->getMinimum().value);
    EXPECT_EQ(1, decStats->getMinimum().scale);
    EXPECT_EQ(Int128(100), decStats->getSum().value);
    EXPECT_EQ(1, decStats->getSum().scale);

    // update with same scale
    decStats->update(Decimal(90, 1));
    decStats->update(Decimal(110, 1));
    EXPECT_EQ(Int128(110), decStats->getMaximum().value);
    EXPECT_EQ(1, decStats->getMaximum().scale);
    EXPECT_EQ(Int128(90), decStats->getMinimum().value);
    EXPECT_EQ(1, decStats->getMinimum().scale);
    EXPECT_EQ(Int128(300), decStats->getSum().value);
    EXPECT_EQ(1, decStats->getSum().scale);

    // update with different scales
    decStats->update(Decimal(100, 2));
    decStats->update(Decimal(Int128(555), 3));
    decStats->update(Decimal(200, 2));
    EXPECT_EQ(Int128(110), decStats->getMaximum().value);
    EXPECT_EQ(1, decStats->getMaximum().scale);
    EXPECT_EQ(Int128(555), decStats->getMinimum().value);
    EXPECT_EQ(3, decStats->getMinimum().scale);
    EXPECT_EQ(Int128(33555), decStats->getSum().value);
    EXPECT_EQ(3, decStats->getSum().scale);

    // update with large values and scales
    decStats->update(Decimal(Int128(1000000000000l), 10));
    EXPECT_EQ(Int128(1335550000000l), decStats->getSum().value);
    EXPECT_EQ(10, decStats->getSum().scale);

    decStats->update(Decimal(Int128("100000000000000000000000"), 22));
    EXPECT_EQ(Int128("1435550000000000000000000"), decStats->getSum().value);
    EXPECT_EQ(22, decStats->getSum().scale);

    // update negative decimals
    decStats->update(Decimal(-1000, 2));
    EXPECT_EQ(Int128(-1000), decStats->getMinimum().value);
    EXPECT_EQ(2, decStats->getMinimum().scale);
    EXPECT_EQ(Int128("1335550000000000000000000"), decStats->getSum().value);
    EXPECT_EQ(22, decStats->getSum().scale);

    // test sum overflow
    decStats->update(Decimal(Int128("123456789012345678901234567890"), 10));
    EXPECT_FALSE(decStats->hasSum());
}

TEST(ColumnStatistics, timestampColumnStatisticsWithNanos) {
    std::unique_ptr<TimestampColumnStatisticsImpl> tsStats(new TimestampColumnStatisticsImpl());

    // normal operations
    for (int32_t i = 1; i <= 1024; ++i) {
        tsStats->update(i * 100, i * 1000);
        tsStats->increase(1);
    }
    EXPECT_EQ(102400, tsStats->getMaximum());
    EXPECT_EQ(1024000, tsStats->getMaximumNanos());
    EXPECT_EQ(100, tsStats->getMinimum());
    EXPECT_EQ(1000, tsStats->getMinimumNanos());

    // update with same milli but different nanos
    tsStats->update(102400, 1024001);
    tsStats->update(102400, 1023999);
    tsStats->update(100, 1001);
    tsStats->update(100, 999);
    EXPECT_EQ(102400, tsStats->getMaximum());
    EXPECT_EQ(1024001, tsStats->getMaximumNanos());
    EXPECT_EQ(100, tsStats->getMinimum());
    EXPECT_EQ(999, tsStats->getMinimumNanos());

    // test merge with no change
    std::unique_ptr<TimestampColumnStatisticsImpl> other1(new TimestampColumnStatisticsImpl());
    for (int32_t i = 1; i <= 1024; ++i) {
        other1->update(i * 100, i * 1000);
        other1->increase(1);
    }
    tsStats->merge(*other1);
    EXPECT_EQ(102400, tsStats->getMaximum());
    EXPECT_EQ(1024001, tsStats->getMaximumNanos());
    EXPECT_EQ(100, tsStats->getMinimum());
    EXPECT_EQ(999, tsStats->getMinimumNanos());

    // test merge with min/max change only in nano
    std::unique_ptr<TimestampColumnStatisticsImpl> other2(new TimestampColumnStatisticsImpl());
    other2->update(102400, 1024002);
    other2->update(100, 998);
    tsStats->merge(*other2);
    EXPECT_EQ(102400, tsStats->getMaximum());
    EXPECT_EQ(1024002, tsStats->getMaximumNanos());
    EXPECT_EQ(100, tsStats->getMinimum());
    EXPECT_EQ(998, tsStats->getMinimumNanos());

    // test merge with min/max change in milli
    std::unique_ptr<TimestampColumnStatisticsImpl> other3(new TimestampColumnStatisticsImpl());
    other3->update(102401, 1);
    other3->update(99, 1);
    tsStats->merge(*other3);
    EXPECT_EQ(102401, tsStats->getMaximum());
    EXPECT_EQ(1, tsStats->getMaximumNanos());
    EXPECT_EQ(99, tsStats->getMinimum());
    EXPECT_EQ(1, tsStats->getMinimumNanos());
}

TEST(ColumnStatistics, timestampColumnStatisticsProbubuf) {
    std::unique_ptr<TimestampColumnStatisticsImpl> tsStats(new TimestampColumnStatisticsImpl());
    tsStats->increase(2);
    tsStats->update(100);
    tsStats->update(200);

    proto::ColumnStatistics pbStats;
    tsStats->toProtoBuf(pbStats);
    EXPECT_EQ(100, pbStats.timestampstatistics().minimumutc());
    EXPECT_EQ(200, pbStats.timestampstatistics().maximumutc());
    EXPECT_FALSE(pbStats.timestampstatistics().has_minimumnanos());
    EXPECT_FALSE(pbStats.timestampstatistics().has_maximumnanos());

    StatContext ctx(true, nullptr);
    std::unique_ptr<TimestampColumnStatisticsImpl> tsStatsFromPb(new TimestampColumnStatisticsImpl(pbStats, ctx));
    EXPECT_EQ(100, tsStatsFromPb->getMinimum());
    EXPECT_EQ(200, tsStatsFromPb->getMaximum());
    EXPECT_EQ(0, tsStatsFromPb->getMinimumNanos());
    EXPECT_EQ(999999, tsStatsFromPb->getMaximumNanos());

    tsStats->update(50, 5555);
    tsStats->update(500, 9999);
    pbStats.Clear();
    tsStats->toProtoBuf(pbStats);
    EXPECT_EQ(50, pbStats.timestampstatistics().minimumutc());
    EXPECT_EQ(500, pbStats.timestampstatistics().maximumutc());
    EXPECT_TRUE(pbStats.timestampstatistics().has_minimumnanos());
    EXPECT_TRUE(pbStats.timestampstatistics().has_maximumnanos());
    EXPECT_EQ(5555 + 1, pbStats.timestampstatistics().minimumnanos());
    EXPECT_EQ(9999 + 1, pbStats.timestampstatistics().maximumnanos());

    tsStatsFromPb.reset(new TimestampColumnStatisticsImpl(pbStats, ctx));
    EXPECT_EQ(50, tsStatsFromPb->getMinimum());
    EXPECT_EQ(500, tsStatsFromPb->getMaximum());
    EXPECT_EQ(5555, tsStatsFromPb->getMinimumNanos());
    EXPECT_EQ(9999, tsStatsFromPb->getMaximumNanos());
}

} // namespace orc
