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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "column/column_hash.h"

namespace starrocks {
namespace {

// memequal_padded may read up to 15 bytes past the end of either buffer, so
// every test buffer is allocated with this much trailing slack.
constexpr size_t kSlack = 16;

// Sizes covering every tier of the SIMD implementations: empty input, the
// short-key loop (< 32), exact multiples of 32, tails of <= 16 and > 16
// remaining bytes, and the memcmp fallback (>= 256).
constexpr size_t kSizes[] = {0, 1, 5, 15, 16, 17, 31, 32, 33, 40, 47, 48, 50, 63, 64, 100, 255, 256, 300, 1024};

std::vector<uint8_t> make_buffer(size_t size, uint8_t fill, uint8_t pad) {
    std::vector<uint8_t> buf(size + kSlack, pad);
    std::fill(buf.begin(), buf.begin() + size, fill);
    return buf;
}

} // namespace

TEST(MemequalPaddedTest, test_size_mismatch) {
    auto a = make_buffer(8, 0xab, 0x00);
    auto b = make_buffer(9, 0xab, 0x00);
    ASSERT_FALSE(memequal_padded(a.data(), 8, b.data(), 9));
}

TEST(MemequalPaddedTest, test_equal_ignores_padding) {
    for (size_t size : kSizes) {
        auto a = make_buffer(size, 0xab, 0x11);
        auto b = make_buffer(size, 0xab, 0x22);
        ASSERT_TRUE(memequal_padded(a.data(), size, b.data(), size)) << "size=" << size;
    }
}

TEST(MemequalPaddedTest, test_single_byte_mismatch) {
    for (size_t size : kSizes) {
        if (size == 0) {
            continue;
        }
        const size_t positions[] = {0, size / 2, size - 1};
        for (size_t pos : positions) {
            auto a = make_buffer(size, 0xab, 0x11);
            auto b = make_buffer(size, 0xab, 0x11);
            b[pos] ^= 0xff;
            ASSERT_FALSE(memequal_padded(a.data(), size, b.data(), size)) << "size=" << size << " pos=" << pos;
        }
    }
}

} // namespace starrocks
