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

#include "storage/column_and_predicate.h"

#include <gtest/gtest.h>

#include "testutil/column_test_helper.h"
#include "util/block_split_bloom_filter.h"

namespace starrocks {
class ColumnAndPredicateTest : public ::testing::Test {
public:
    void SetUp() override {
        _left.reset(new_column_ge_predicate(get_type_info(TYPE_INT), 0, "10"));
        _right.reset(new_column_le_predicate(get_type_info(TYPE_INT), 0, "20"));
        _pred = std::make_unique<ColumnAndPredicate>(get_type_info(TYPE_INT), 0);
        _pred->add_child(_left.get());
        _pred->add_child(_right.get());

        std::vector<int32_t> values = {5, 15, 17, 25, 0, 0};
        std::vector<uint8_t> null_values = {0, 0, 0, 0, 1, 1};
        _col = ColumnTestHelper::build_nullable_column<int32_t>(values, null_values);
    }

protected:
    std::unique_ptr<ColumnPredicate> _left;
    std::unique_ptr<ColumnPredicate> _right;
    std::unique_ptr<ColumnAndPredicate> _pred;
    ColumnPtr _col;
    BlockSplitBloomFilter _bf;
    ObjectPool _pool;
};

TEST_F(ColumnAndPredicateTest, basic) {
    ASSERT_TRUE(_pred->filter(_bf));
    ASSERT_FALSE(_pred->can_vectorized());
    ASSERT_EQ(_pred->type(), PredicateType::kAnd);
    ASSERT_TRUE(_pred->value().is_null());
    ASSERT_EQ(_pred->values().size(), 0);
    ASSERT_EQ(_pred->debug_string(), "AND(0:(columnId(0)>=10), 1:(columnId(0)<=20))");
}

TEST_F(ColumnAndPredicateTest, evaluate) {
    std::vector<uint8_t> buff = {0, 0, 0, 0, 0, 0};
    auto st = _pred->evaluate(_col.get(), buff.data(), 0, 6);
    ASSERT_TRUE(st.ok());

    std::vector<uint8_t> result = {0, 1, 1, 0, 0, 0};
    ASSERT_EQ(buff, result);
}

TEST_F(ColumnAndPredicateTest, evaluate_and) {
    std::vector<uint8_t> buff = {1, 1, 0, 1, 1, 1};
    auto st = _pred->evaluate_and(_col.get(), buff.data(), 0, 6);
    ASSERT_TRUE(st.ok());

    std::vector<uint8_t> result = {0, 1, 0, 0, 0, 0};
    ASSERT_EQ(buff, result);
}

TEST_F(ColumnAndPredicateTest, evaluate_or) {
    std::vector<uint8_t> buff = {0, 0, 0, 0, 1, 0};
    auto st = _pred->evaluate_or(_col.get(), buff.data(), 0, 6);
    ASSERT_TRUE(st.ok());

    std::vector<uint8_t> result = {0, 1, 1, 0, 1, 0};
    ASSERT_EQ(buff, result);
}

TEST_F(ColumnAndPredicateTest, zonemap_filter) {
    Datum min_value_1((int32_t)10);
    Datum max_value_1((int32_t)20);
    ZoneMapDetail zone_map_1(min_value_1, max_value_1, false);
    ASSERT_TRUE(_pred->zone_map_filter(zone_map_1));

    Datum min_value_2((int32_t)5);
    Datum max_value_2((int32_t)25);
    ZoneMapDetail zone_map_2(min_value_2, max_value_2, false);
    ASSERT_TRUE(_pred->zone_map_filter(zone_map_2));

    Datum min_value_3((int32_t)1);
    Datum max_value_3((int32_t)5);
    ZoneMapDetail zone_map_3(min_value_3, max_value_3, false);
    ASSERT_FALSE(_pred->zone_map_filter(zone_map_3));

    Datum min_value_4((int32_t)30);
    Datum max_value_4((int32_t)40);
    ZoneMapDetail zone_map_4(min_value_4, max_value_4, false);
    ASSERT_FALSE(_pred->zone_map_filter(zone_map_4));

    Datum min_value_5((int32_t)5);
    Datum max_value_5((int32_t)25);
    ZoneMapDetail zone_map_5(min_value_5, max_value_5, false);
    ASSERT_TRUE(_pred->zone_map_filter(zone_map_5));

    Datum min_value_6((int32_t)15);
    Datum max_value_6((int32_t)40);
    ZoneMapDetail zone_map_6(min_value_6, max_value_6, false);
    ASSERT_TRUE(_pred->zone_map_filter(zone_map_6));
}

TEST_F(ColumnAndPredicateTest, convert_to) {
    const ColumnPredicate* new_pred = nullptr;
    Status st = _pred->convert_to(&new_pred, get_type_info(TYPE_INT), &_pool);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(new_pred->debug_string(), "AND(0:(columnId(0)>=10), 1:(columnId(0)<=20))");
}

} // namespace starrocks
