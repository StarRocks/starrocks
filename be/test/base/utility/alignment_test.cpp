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

#include "base/utility/alignment.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(AlignmentTest, HandlesNonPowerOfTwoAlignment) {
    EXPECT_EQ(12, ALIGN_DOWN(13, 6));
    EXPECT_EQ(18, ALIGN_UP(13, 6));
}

TEST(AlignmentTest, HandlesPowerOfTwoAlignment) {
    EXPECT_EQ(16, ALIGN_UP(13, 8));
    EXPECT_EQ(8, ALIGN_DOWN(13, 8));
}

} // namespace starrocks
