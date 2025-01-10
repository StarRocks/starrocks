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

#include "column/type_traits.h"
#include "exec/olap_common.h"
#include "gtest/gtest.h"
#include "testutil/assert.h"
#include "types/logical_type.h"

namespace starrocks {

class ColumnValueRangeTest : public ::testing::Test {
public:
    void SetUp() override {}

protected:
    std::stringstream _ss;
    static const auto _int32_min_value = RunTimeTypeLimits<TYPE_INT>::min_value();
    static const auto _int32_max_value = RunTimeTypeLimits<TYPE_INT>::max_value();
};

TEST_F(ColumnValueRangeTest, add_range_le_max) {
    ColumnValueRange<int32_t> range("c_int32", TYPE_INT, _int32_min_value, _int32_max_value, 5, _int32_max_value);

    ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LESS_OR_EQUAL, _int32_max_value));
    std::vector<TCondition> filters;
    range.to_olap_filter<false>(filters);

    ASSERT_EQ(filters.size(), 1);
    _ss << filters[0];
    ASSERT_EQ(_ss.str(),
              "TCondition(column_name=c_int32, condition_op=>=, condition_values=[5], is_index_filter_only=0)");
}

TEST_F(ColumnValueRangeTest, add_range_ge_min) {
    ColumnValueRange<int32_t> range("c_int32", TYPE_INT, _int32_min_value, _int32_max_value, _int32_min_value, 100);

    ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LARGER_OR_EQUAL, _int32_min_value));
    std::vector<TCondition> filters;
    range.to_olap_filter<false>(filters);

    ASSERT_EQ(filters.size(), 1);
    _ss << filters[0];
    ASSERT_EQ(_ss.str(),
              "TCondition(column_name=c_int32, condition_op=<=, condition_values=[100], is_index_filter_only=0)");
}

TEST(NormalizeRangeTest, RangeTest) {
    const constexpr LogicalType Type = TYPE_INT;
    using CppType = RunTimeCppType<Type>;
    {
        // where range in (1,2,3,4) and range not in (1, 2)
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_IN, {1, 2, 3, 4}));
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_NOT_IN, {1, 2}));
        std::set<CppType> values = {3, 4};
        ASSERT_EQ(range._fixed_values, values);
    }
    {
        // where range in (1, 2) and range > 1
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_IN, {1, 2}));
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LARGER, 1));

        ASSERT_TRUE(range.is_fixed_value_range());
        ASSERT_EQ(range._fixed_values.size(), 1);
    }
    {
        // where range in (1, 2) and range > 2
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_IN, {1, 2}));
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LARGER, 2));
        ASSERT_TRUE(range.is_empty_value_range());
    }
    {
        // where range in (1, 2) and range = 3
        // return empty
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_IN, {1, 2}));
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_IN, {3}));
        ASSERT_TRUE(range.is_empty_value_range());
    }
    {
        // where range in (1, 3) and range not in (3)
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_IN, {1, 3}));
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_NOT_IN, {3}));
        ASSERT_EQ(range._fixed_values.size(), 1);
        ASSERT_TRUE(range._fixed_values.count(1));
    }
    {
        // where range in (1, 2) and range not in (3)
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_IN, {1, 2}));
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_NOT_IN, {3}));
        ASSERT_EQ(range._fixed_values.size(), 2);
        ASSERT_TRUE(range._fixed_values.count(1));
        ASSERT_TRUE(range._fixed_values.count(2));
    }
    {
        // where range >= -limit and range in (1, 2, 3)
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LARGER_OR_EQUAL, std::numeric_limits<CppType>::lowest()));
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_IN, {1, 2, 3}));
        ASSERT_EQ(range._fixed_values.size(), 3);
        ASSERT_TRUE(range._fixed_values.count(1));
        ASSERT_TRUE(range._fixed_values.count(2));
        ASSERT_TRUE(range._fixed_values.count(3));
    }
    {
        // where range >= -limit and range not in (1, 2, 3)
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LESS, std::numeric_limits<CppType>::lowest()));
        bool ok = range.add_fixed_values(SQLFilterOp::FILTER_NOT_IN, {3}).ok();
        ASSERT_FALSE(ok);
    }
    {
        // where range > 1000 and range < 2000
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LESS, 1000));
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LARGER, 2000));
        ASSERT_TRUE(range.is_empty_value_range());
    }
}

TEST(NormalizeRangeTest, BoolRangeTest) {
    {
        // range not in (false) and range < false
        // not support for this range
        ColumnValueRange<int> range("test", TYPE_BOOLEAN, 0, 1);
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LESS, true));
        bool res = range.add_fixed_values(SQLFilterOp::FILTER_NOT_IN, {false}).ok();
        ASSERT_FALSE(res);
    }
    {
        // range not in (false) and range < empty
        // not support for this range
        ColumnValueRange<int> range("test", TYPE_BOOLEAN, 0, 1);
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_NOT_IN, {false}));
        bool res = range.add_range(SQLFilterOp::FILTER_LESS, true).ok();
        ASSERT_FALSE(res);
    }
    {
        // range != false and range < true
        // not support for this range
        ColumnValueRange<int> range("test", TYPE_BOOLEAN, 0, 1);
        ASSERT_OK(range.add_fixed_values(SQLFilterOp::FILTER_NOT_IN, {false}));
        ASSERT_ERROR(range.add_range(SQLFilterOp::FILTER_LESS, true));
        ASSERT_FALSE(range.is_empty_value_range());
    }
}

TEST(NormalizeRangeTest, ExtendScanKeyTest) {
    const constexpr LogicalType Type = TYPE_BIGINT;
    using CppType = RunTimeCppType<Type>;
    // Test OverFlow
    {
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LESS, 0));

        OlapScanKeys scan_keys;
        scan_keys._begin_scan_keys.emplace_back();
        scan_keys._begin_scan_keys.emplace_back();
        bool res = scan_keys.extend_scan_key(range, 1024).ok();
        ASSERT_TRUE(res);
    }
    {
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LARGER, std::numeric_limits<CppType>::max()));
        OlapScanKeys scan_keys;
        scan_keys._begin_scan_keys.emplace_back();
        scan_keys._begin_scan_keys.emplace_back();
        bool res = scan_keys.extend_scan_key(range, 1024).ok();
        ASSERT_TRUE(res);
    }
    {
        ColumnValueRange<CppType> range("test", Type, std::numeric_limits<CppType>::lowest(),
                                        std::numeric_limits<CppType>::max());
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LARGER_OR_EQUAL, std::numeric_limits<CppType>::max()));
        ASSERT_OK(range.add_range(SQLFilterOp::FILTER_LESS_OR_EQUAL, std::numeric_limits<CppType>::max()));
        OlapScanKeys scan_keys;
        scan_keys._begin_scan_keys.emplace_back();
        scan_keys._begin_scan_keys.emplace_back();
        scan_keys._end_scan_keys.emplace_back();
        scan_keys._end_scan_keys.emplace_back();
        bool res = scan_keys.extend_scan_key(range, 1024).ok();
        ASSERT_TRUE(res);
    }
}

} // namespace starrocks
