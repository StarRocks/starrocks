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

#include "base/testutil/assert.h"
#include "column/runtime_type_traits.h"
#include "exec/olap_common.h"
#include "types/logical_type.h"

namespace starrocks {

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
