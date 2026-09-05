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

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exec/aggregator.h"
#include "exprs/literal.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

bool could_apply_bitcompress_opt(
        const std::vector<ColumnType>& group_by_types,
        const std::vector<std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>>>& ranges,
        std::vector<std::any>& base, std::vector<int>& used_bytes, size_t* max_size, bool* has_null);

TEST(AggCompressedKey, could_bound) {
    // group by 1 columns
    {
        ObjectPool pool;

        std::vector<ColumnType> groupby;
        std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>> range;
        std::vector<std::any> bases;
        std::vector<int> used_bytes;
        size_t max_size;
        bool has_null;

        bases.resize(1);
        used_bytes.resize(1);

        auto type1 = TypeDescriptor(TYPE_INT);
        groupby.emplace_back(ColumnType{type1, false});
        std::vector<std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>>> ranges;
        auto* min = pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_INT>(0, 1), type1));
        auto* max = pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_INT>(100, 1), type1));
        range = {min, max};
        ranges.emplace_back(range);

        bool res = could_apply_bitcompress_opt(groupby, ranges, bases, used_bytes, &max_size, &has_null);
        EXPECT_EQ(max_size, 7);
        ASSERT_EQ(res, true);
    }
    // group by 2 columns
    {
        ObjectPool pool;

        std::vector<ColumnType> groupby;
        std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>> range;
        std::vector<std::any> bases;
        std::vector<int> used_bytes;
        size_t max_size;
        bool has_null;

        bases.resize(2);
        used_bytes.resize(2);

        auto type1 = TypeDescriptor(TYPE_INT);
        groupby.emplace_back(ColumnType{type1, false});
        groupby.emplace_back(ColumnType{type1, true});
        std::vector<std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>>> ranges;
        auto* min = pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_INT>(0, 1), type1));
        auto* max = pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_INT>(100, 1), type1));
        range = {min, max};
        ranges.emplace_back(range);
        ranges.emplace_back(range);

        bool res = could_apply_bitcompress_opt(groupby, ranges, bases, used_bytes, &max_size, &has_null);
        EXPECT_EQ(max_size, 15);
        ASSERT_EQ(res, true);
    }
    // group by decimal columns
    {
        ObjectPool pool;

        std::vector<ColumnType> groupby;
        std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>> range;
        std::vector<std::any> bases;
        std::vector<int> used_bytes;
        size_t max_size;
        bool has_null;

        bases.resize(2);
        used_bytes.resize(2);

        auto type1 = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 8, 4);
        groupby.emplace_back(ColumnType{type1, false});
        groupby.emplace_back(ColumnType{type1, true});
        std::vector<std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>>> ranges;
        auto* min = pool.add(
                new VectorizedLiteral(ColumnHelper::create_const_decimal_column<TYPE_DECIMAL128>(0, 8, 4, 1), type1));
        auto* max = pool.add(
                new VectorizedLiteral(ColumnHelper::create_const_decimal_column<TYPE_DECIMAL128>(100, 8, 4, 1), type1));
        range = {min, max};
        ranges.emplace_back(range);
        ranges.emplace_back(range);

        bool res = could_apply_bitcompress_opt(groupby, ranges, bases, used_bytes, &max_size, &has_null);
        EXPECT_EQ(max_size, 15);
        ASSERT_EQ(res, true);
    }
    // nullable single INT whose value range fills a full 16 bits ([0, 65535]).
    // The null flag pushes the packed width to 17 bits (int32 storage class), so the
    // level-reduction check alone would reject it -- but the single-INT direct-array
    // map keys on value - min and carries the null flag separately, so 16 value bits
    // still qualify for uint16. Regression guard for that boundary.
    {
        ObjectPool pool;

        std::vector<ColumnType> groupby;
        std::vector<std::any> bases;
        std::vector<int> used_bytes;
        size_t max_size;
        bool has_null;

        bases.resize(1);
        used_bytes.resize(1);

        auto type1 = TypeDescriptor(TYPE_INT);
        groupby.emplace_back(ColumnType{type1, true});
        std::vector<std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>>> ranges;
        auto* min = pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_INT>(0, 1), type1));
        auto* max = pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_INT>(65535, 1), type1));
        ranges.emplace_back(std::make_pair(min, max));

        bool res = could_apply_bitcompress_opt(groupby, ranges, bases, used_bytes, &max_size, &has_null);
        EXPECT_EQ(max_size, 17); // 16 value bits + 1 null bit
        ASSERT_EQ(res, true);
    }
    // nullable single INT one bit past uint16 ([0, 100000] -> 17 value bits). Neither the
    // direct-array map (no uint17) nor the slice path (int32 -> int32, no level reduction)
    // helps, so the opt must stay disabled. Guards the value-bits boundary against over-reach.
    {
        ObjectPool pool;

        std::vector<ColumnType> groupby;
        std::vector<std::any> bases;
        std::vector<int> used_bytes;
        size_t max_size;
        bool has_null;

        bases.resize(1);
        used_bytes.resize(1);

        auto type1 = TypeDescriptor(TYPE_INT);
        groupby.emplace_back(ColumnType{type1, true});
        std::vector<std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>>> ranges;
        auto* min = pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_INT>(0, 1), type1));
        auto* max = pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_INT>(100000, 1), type1));
        ranges.emplace_back(std::make_pair(min, max));

        bool res = could_apply_bitcompress_opt(groupby, ranges, bases, used_bytes, &max_size, &has_null);
        ASSERT_EQ(res, false);
    }
}
} // namespace starrocks
