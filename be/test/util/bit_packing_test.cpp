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

#include "util/bit_packing.inline.h"

namespace starrocks {
TEST(BitPacking, UnpackValue) {
    uint8_t data[BitPacking::MAX_BITWIDTH * 32 / 8];
    for (unsigned char& i : data) {
        i = 0x8;
    }

    ASSERT_EQ((UnpackValue<4, 0, true>(data)), 8);
    ASSERT_EQ((UnpackValue<4, 1, true>(data)), 0);

    ASSERT_EQ((UnpackValue<5, 0, false>(data)), 8);
    ASSERT_EQ((UnpackValue<5, 1, false>(data)), 0);
    ASSERT_EQ((UnpackValue<5, 2, false>(data)), 2);

    ASSERT_EQ((UnpackValue<31, 0, false>(data)), 134744072);
    ASSERT_EQ((UnpackValue<31, 1, false>(data)), 269488144);
}

TEST(BitPacking, Unpack32Values) {
    uint8_t data[BitPacking::MAX_BITWIDTH * 32 / 8];
    for (unsigned char& i : data) {
        i = 0x8;
    }

    uint64_t result[32];
    const uint8_t* pos = BitPacking::Unpack32Values<uint64_t, 4>(data, BitPacking::MAX_BITWIDTH * 32 / 8, result);
    ASSERT_EQ(pos, data + 4 * 32 / 8);

    for (size_t i = 0; i < 32; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result[i], 8);
        } else {
            ASSERT_EQ(result[i], 0);
        }
    }
}

TEST(BitPacking, UnpackUpTo31Values) {
    uint8_t data[4 * 15];
    for (unsigned char& i : data) {
        i = 0x8;
    }

    uint64_t result[15];
    const uint8_t* pos = BitPacking::UnpackUpTo31Values<uint64_t, 4>(data, 4 * 15, 15, result);
    ASSERT_EQ(pos, data + 4 * 15 / 8 + 1);

    for (size_t i = 0; i < 15; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result[i], 8);
        } else {
            ASSERT_EQ(result[i], 0);
        }
    }
}

TEST(BitPacking, UnpackValues) {
    uint8_t data[BitPacking::MAX_BITWIDTH * 48 / 8];
    for (unsigned char& i : data) {
        i = 0x8;
    }

    uint64_t result[48];
    const uint8_t* pos = nullptr;
    int64_t num = 0;
    std::tie(pos, num) = BitPacking::UnpackValues<uint64_t>(4, data, 4 * 48 / 8, 48, result);
    ASSERT_EQ(pos, data + 4 * 48 / 8);
    ASSERT_EQ(num, 48);

    for (size_t i = 0; i < 48; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result[i], 8);
        } else {
            ASSERT_EQ(result[i], 0);
        }
    }
}

} // namespace starrocks
