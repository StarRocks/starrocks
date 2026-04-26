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

#include "base/decimal_types.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(DecimalTypesTest, Exp10ClampsOutOfRange) {
    constexpr int max32 = decimal_precision_limit<int32_t>;
    constexpr int max64 = decimal_precision_limit<int64_t>;

    EXPECT_EQ(exp10_int32(0), exp10_int32(-1));
    EXPECT_EQ(exp10_int32(max32), exp10_int32(max32 + 1));

    EXPECT_EQ(exp10_int64(0), exp10_int64(-1));
    EXPECT_EQ(exp10_int64(max64), exp10_int64(max64 + 1));
}

} // namespace starrocks
