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

#include "base/concurrency/limit_setter.h"

#include <gtest/gtest.h>

#include <cstring>
#include <type_traits>

namespace starrocks {

TEST(LimitSetterTest, DefaultInitialized) {
    EXPECT_FALSE(std::is_trivially_default_constructible_v<LimitSetter>);

    alignas(LimitSetter) unsigned char storage[sizeof(LimitSetter)];
    std::memset(storage, 0xA5, sizeof(storage));
    auto* setter = new (storage) LimitSetter();

    int32_t old_expect = -1;
    ASSERT_TRUE(setter->adjust_expect_num(0, &old_expect));
    EXPECT_EQ(0, old_expect);
}

} // namespace starrocks
