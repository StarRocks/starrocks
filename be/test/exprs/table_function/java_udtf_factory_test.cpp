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

#include "exprs/table_function/table_function_factory.h"

namespace starrocks {

// A SRJAR table function resolves to the boxed JavaUDTF by default, and to the vectorized
// ("input"="arrow") variant when is_arrow_input is set — two distinct singletons.
TEST(JavaUDTFFactoryTest, SrjarBoxedVsArrowDistinct) {
    const auto* boxed = get_table_function("f", {TYPE_INT}, {TYPE_INT}, TFunctionBinaryType::SRJAR,
                                           /*is_arrow_input=*/false);
    const auto* arrow = get_table_function("f", {TYPE_INT}, {TYPE_INT}, TFunctionBinaryType::SRJAR,
                                           /*is_arrow_input=*/true);
    ASSERT_NE(nullptr, boxed);
    ASSERT_NE(nullptr, arrow);
    EXPECT_NE(boxed, arrow);
}

TEST(JavaUDTFFactoryTest, NonSrjarReturnsNull) {
    EXPECT_EQ(nullptr, get_table_function("f", {TYPE_INT}, {TYPE_INT}, TFunctionBinaryType::PYTHON,
                                          /*is_arrow_input=*/true));
}

} // namespace starrocks
