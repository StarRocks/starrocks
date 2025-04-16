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

#include "util/bit_mask.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(BitMask, Basic) {
    BitMask bit_mask(10);
    for (int i = 0; i < 10; i++) {
        bit_mask.set_bit(i);
        ASSERT_TRUE(bit_mask.is_bit_set(i));
    }
    for (int i = 0; i < 10; i++) {
        ASSERT_FALSE(bit_mask.all_bits_zero());
        bit_mask.clear_bit(i);
        ASSERT_FALSE(bit_mask.is_bit_set(i));
    }
    ASSERT_TRUE(bit_mask.all_bits_zero());
}
} // namespace starrocks