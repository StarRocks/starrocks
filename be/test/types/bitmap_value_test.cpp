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

#include "types/bitmap_value.h"

#include <gtest/gtest.h>

#include "util/phmap/phmap.h"

namespace starrocks {

class BitmapTest : public testing::Test {};

TEST_F(BitmapTest, Constructor) {
    BitmapValue bitmap;
    for (size_t i = 0; i < 64; i++) {
        bitmap.add(i);
    }

    BitmapValue shallow_bitmap(bitmap, false);
    shallow_bitmap.add(64);
    ASSERT_EQ(bitmap.cardinality(), 65);
}

} // namespace starrocks
