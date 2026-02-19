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

#include "base/random/random.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(RandomTest, RandomBinaryStringCoversHighBits) {
    Random rng(1);
    const std::string data = rng.RandomBinaryString(64);
    bool has_high_bit = false;
    for (unsigned char c : data) {
        if (c >= 128) {
            has_high_bit = true;
            break;
        }
    }
    EXPECT_TRUE(has_high_bit);
}

} // namespace starrocks
