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

#include "storage/primitive/row_source_mask.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(RowSourceMaskPrimitiveTest, sourceAndAggFlagShareEncodedWord) {
    RowSourceMask mask(7, true);
    EXPECT_EQ(7, mask.get_source_num());
    EXPECT_TRUE(mask.get_agg_flag());
    EXPECT_EQ(RowSourceMask::MASK_FLAG | 7, mask.data);

    mask.set_source_num(RowSourceMask::MAX_SOURCES + 3);
    EXPECT_EQ(2, mask.get_source_num());
    EXPECT_TRUE(mask.get_agg_flag());

    mask.set_agg_flag(false);
    EXPECT_EQ(2, mask.get_source_num());
    EXPECT_FALSE(mask.get_agg_flag());
    EXPECT_EQ(2, mask.data);
}

} // namespace starrocks
