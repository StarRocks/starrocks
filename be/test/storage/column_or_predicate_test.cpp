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

#include "storage/column_or_predicate.h"

#include <gtest/gtest.h>

#include "testutil/column_test_helper.h"

namespace starrocks {
class ColumnOrPredicateTest : public ::testing::Test {
public:
    void SetUp() override {
        _left.reset(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "100"));
        _right.reset(new_column_eq_predicate(get_type_info(TYPE_INT), 0, "200"));
        _pred = std::make_unique<ColumnOrPredicate>(get_type_info(TYPE_INT), 0);
        _pred->add_child(_left.get());
        _pred->add_child(_right.get());

        std::vector<int32_t> values = {10, 100, 200, 0, 0};
        std::vector<uint8_t> null_values = {0, 0, 0, 1, 1};
        _col = ColumnTestHelper::build_nullable_column<int32_t>(values, null_values);
    }

protected:
    std::unique_ptr<ColumnPredicate> _left;
    std::unique_ptr<ColumnPredicate> _right;
    std::unique_ptr<ColumnOrPredicate> _pred;
    ColumnPtr _col;
};

TEST_F(ColumnOrPredicateTest, basic) {
    ASSERT_EQ(PredicateType::kOr, _pred->type());
    ASSERT_FALSE(_pred->can_vectorized());
    ASSERT_EQ(_pred->debug_string(), "OR(0:(columnId(0)=100), 1:(columnId(0)=200))");
}

TEST_F(ColumnOrPredicateTest, evaluate) {
    std::vector<uint8_t> buff = {0, 0, 0, 0, 0};
    auto st = _pred->evaluate(_col.get(), buff.data(), 0, 5);
    ASSERT_TRUE(st.ok());

    std::vector<uint8_t> result = {0, 1, 1, 0, 0};
    ASSERT_EQ(buff, result);
}

TEST_F(ColumnOrPredicateTest, evaluate_and) {
    std::vector<uint8_t> buff = {1, 1, 0, 1, 1};
    auto st = _pred->evaluate_and(_col.get(), buff.data(), 0, 5);
    ASSERT_TRUE(st.ok());

    std::vector<uint8_t> result = {0, 1, 0, 0, 0};
    ASSERT_EQ(buff, result);
}

TEST_F(ColumnOrPredicateTest, evaluate_or) {
    std::vector<uint8_t> buff = {1, 1, 1, 1, 0};
    auto st = _pred->evaluate_or(_col.get(), buff.data(), 0, 5);
    ASSERT_TRUE(st.ok());

    std::vector<uint8_t> result = {1, 1, 1, 1, 0};
    ASSERT_EQ(buff, result);
}

} // namespace starrocks
