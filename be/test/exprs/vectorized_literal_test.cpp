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

#include <column/column_helper.h>
#include <gtest/gtest.h>

#include "exprs/literal.h"

namespace starrocks {
class VectorizedLiteralTest : public ::testing::Test {
public:
    void SetUp() override {}
};

TEST_F(VectorizedLiteralTest, literal_test) {
    TypeDescriptor type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128, 10, 5, 2);
    ColumnPtr column = ColumnHelper::create_const_decimal_column<TYPE_DECIMAL128>(10, 5, 2, 1);
    VectorizedLiteral literal{std::move(column), type};
    ASSERT_EQ(TExprNodeType::DECIMAL_LITERAL, literal.node_type());
}
} // namespace starrocks