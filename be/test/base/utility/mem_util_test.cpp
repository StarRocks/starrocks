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

#include "base/utility/mem_util.hpp"
#include "base/utility/mysql_global.h"

#include <gtest/gtest.h>

#include <cstring>

namespace starrocks {

TEST(MemUtilTest, FixedSizeCopyUnaligned) {
    alignas(8) uint8_t buffer[32];
    for (size_t i = 0; i < sizeof(buffer); ++i) {
        buffer[i] = static_cast<uint8_t>(i);
    }
    uint8_t* src = buffer + 1;
    uint8_t* dst = buffer + 17;

    fixed_size_memory_copy<2>(dst, src);
    ASSERT_EQ(0, std::memcmp(dst, src, 2));

    fixed_size_memory_copy<4>(dst, src);
    ASSERT_EQ(0, std::memcmp(dst, src, 4));

    fixed_size_memory_copy<8>(dst, src);
    ASSERT_EQ(0, std::memcmp(dst, src, 8));
}

TEST(MemUtilTest, MysqlGlobalStoresUnaligned) {
    alignas(8) uint8_t buffer[32] = {};
    uint8_t* ptr = buffer + 1;

    int2store(ptr, 0x1234);
    uint16_t v16 = 0;
    std::memcpy(&v16, ptr, sizeof(v16));
    EXPECT_EQ(v16, 0x1234);

    int4store(ptr + 2, 0x12345678u);
    uint32_t v32 = 0;
    std::memcpy(&v32, ptr + 2, sizeof(v32));
    EXPECT_EQ(v32, 0x12345678u);

    int8store(ptr + 6, 0x1122334455667788ULL);
    uint64_t v64 = 0;
    std::memcpy(&v64, ptr + 6, sizeof(v64));
    EXPECT_EQ(v64, 0x1122334455667788ULL);

    float4store(ptr + 14, 1.5f);
    float f32 = 0.0f;
    std::memcpy(&f32, ptr + 14, sizeof(f32));
    EXPECT_FLOAT_EQ(f32, 1.5f);

    float8store(ptr + 18, 2.5);
    double f64 = 0.0;
    std::memcpy(&f64, ptr + 18, sizeof(f64));
    EXPECT_DOUBLE_EQ(f64, 2.5);
}

} // namespace starrocks
