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

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "connector/hive_connector.h"
#include "exprs/runtime_filter.h"
#include "types/date_value.h"
#include "types/logical_type_infra.h"

namespace starrocks {

class HiveConnectorDppTest : public ::testing::Test {
protected:
    ObjectPool _pool;
};

// Build a MinMaxRuntimeFilter<ltype> covering [lo, hi] via insert().
// Using insert() keeps _always_true=false and has_min_max=true, which is
// what RfMinMaxFileStatsChecker requires to perform actual range checks.

TEST_F(HiveConnectorDppTest, IntPruneLeft) {
    auto* rf = _pool.add(new MinMaxRuntimeFilter<TYPE_INT>());
    rf->insert(10);
    rf->insert(20);

    TExprMinMaxValue fstats;
    fstats.all_null = false;
    fstats.min_int_value = 1;
    fstats.max_int_value = 9; // file [1,9] entirely below RF [10,20]

    connector::RfMinMaxFileStatsChecker checker{rf, fstats};
    EXPECT_TRUE(type_dispatch_filter(TYPE_INT, false, checker));
}

TEST_F(HiveConnectorDppTest, IntPruneRight) {
    auto* rf = _pool.add(new MinMaxRuntimeFilter<TYPE_INT>());
    rf->insert(10);
    rf->insert(20);

    TExprMinMaxValue fstats;
    fstats.all_null = false;
    fstats.min_int_value = 21;
    fstats.max_int_value = 30; // file [21,30] entirely above RF [10,20]

    connector::RfMinMaxFileStatsChecker checker{rf, fstats};
    EXPECT_TRUE(type_dispatch_filter(TYPE_INT, false, checker));
}

TEST_F(HiveConnectorDppTest, IntOverlap) {
    auto* rf = _pool.add(new MinMaxRuntimeFilter<TYPE_INT>());
    rf->insert(10);
    rf->insert(20);

    TExprMinMaxValue fstats;
    fstats.all_null = false;
    fstats.min_int_value = 15;
    fstats.max_int_value = 25; // file [15,25] overlaps RF [10,20]

    connector::RfMinMaxFileStatsChecker checker{rf, fstats};
    EXPECT_FALSE(type_dispatch_filter(TYPE_INT, false, checker));
}

TEST_F(HiveConnectorDppTest, IntExactMatch) {
    auto* rf = _pool.add(new MinMaxRuntimeFilter<TYPE_INT>());
    rf->insert(10);
    rf->insert(20);

    TExprMinMaxValue fstats;
    fstats.all_null = false;
    fstats.min_int_value = 10;
    fstats.max_int_value = 20; // file [10,20] == RF [10,20]: no prune

    connector::RfMinMaxFileStatsChecker checker{rf, fstats};
    EXPECT_FALSE(type_dispatch_filter(TYPE_INT, false, checker));
}

TEST_F(HiveConnectorDppTest, IntAllNull) {
    auto* rf = _pool.add(new MinMaxRuntimeFilter<TYPE_INT>());
    rf->insert(10);
    rf->insert(20);

    TExprMinMaxValue fstats;
    fstats.all_null = true; // all-null file must never be pruned

    connector::RfMinMaxFileStatsChecker checker{rf, fstats};
    EXPECT_FALSE(type_dispatch_filter(TYPE_INT, false, checker));
}

TEST_F(HiveConnectorDppTest, DatePrune) {
    auto* rf = _pool.add(new MinMaxRuntimeFilter<TYPE_DATE>());
    DateValue d1, d2;
    d1.from_date(2024, 1, 1);
    d2.from_date(2024, 12, 31);
    rf->insert(d1);
    rf->insert(d2);

    // File is in 2025 — entirely outside the RF range [2024-01-01, 2024-12-31].
    constexpr int64_t kEpochJulian = 2440588LL;
    DateValue d3, d4;
    d3.from_date(2025, 1, 1);
    d4.from_date(2025, 6, 30);

    TExprMinMaxValue fstats;
    fstats.all_null = false;
    fstats.min_int_value = static_cast<int64_t>(d3.julian()) - kEpochJulian;
    fstats.max_int_value = static_cast<int64_t>(d4.julian()) - kEpochJulian;

    connector::RfMinMaxFileStatsChecker checker{rf, fstats};
    EXPECT_TRUE(type_dispatch_filter(TYPE_DATE, false, checker));
}

TEST_F(HiveConnectorDppTest, DateOverlap) {
    auto* rf = _pool.add(new MinMaxRuntimeFilter<TYPE_DATE>());
    DateValue d1, d2;
    d1.from_date(2024, 1, 1);
    d2.from_date(2024, 12, 31);
    rf->insert(d1);
    rf->insert(d2);

    // File is in mid-2024 — overlaps the RF range.
    constexpr int64_t kEpochJulian = 2440588LL;
    DateValue d3, d4;
    d3.from_date(2024, 3, 1);
    d4.from_date(2024, 6, 30);

    TExprMinMaxValue fstats;
    fstats.all_null = false;
    fstats.min_int_value = static_cast<int64_t>(d3.julian()) - kEpochJulian;
    fstats.max_int_value = static_cast<int64_t>(d4.julian()) - kEpochJulian;

    connector::RfMinMaxFileStatsChecker checker{rf, fstats};
    EXPECT_FALSE(type_dispatch_filter(TYPE_DATE, false, checker));
}

TEST_F(HiveConnectorDppTest, FloatPrune) {
    auto* rf = _pool.add(new MinMaxRuntimeFilter<TYPE_FLOAT>());
    rf->insert(1.0f);
    rf->insert(2.0f);

    TExprMinMaxValue fstats;
    fstats.all_null = false;
    fstats.min_float_value = 3.0;
    fstats.max_float_value = 4.0; // file [3.0,4.0] above RF [1.0,2.0]

    connector::RfMinMaxFileStatsChecker checker{rf, fstats};
    EXPECT_TRUE(type_dispatch_filter(TYPE_FLOAT, false, checker));
}

TEST_F(HiveConnectorDppTest, VarcharNoPrune) {
    // Strings are unsupported: the checker always returns false for slice types.
    auto* rf = _pool.add(new MinMaxRuntimeFilter<TYPE_VARCHAR>());

    TExprMinMaxValue fstats;
    fstats.all_null = false;

    connector::RfMinMaxFileStatsChecker checker{rf, fstats};
    EXPECT_FALSE(type_dispatch_filter(TYPE_VARCHAR, false, checker));
}

} // namespace starrocks
