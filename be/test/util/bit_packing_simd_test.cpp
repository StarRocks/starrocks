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

#include <cstdint>
#include <random>
#include <vector>

#include "bench/bit_copy.h"
#include "util/bit_packing.inline.h"
#include "util/bit_packing_adapter.h"

namespace starrocks {

class BitPackingSIMDTest : public ::testing::Test {
public:
    void SetUp() override;
    void TearDown() override {}

private:
    void populateBitPacked();

    std::vector<uint32_t> randomInts_u32;
    // Array of bit packed representations of randomInts_u32. The array at index i
    // is packed i bits wide and the values come from the low bits of
    std::vector<std::vector<uint64_t>> bitPackedData;

    static const uint64_t kNumValues = 1024 * 4 + 31;
};

void BitPackingSIMDTest::SetUp() {
    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<uint32_t> distr(0, std::numeric_limits<uint32_t>::max());
    for (int32_t i = 0; i < kNumValues; i++) {
        auto randomInt = distr(gen);
        randomInts_u32.push_back(randomInt);
    }
    populateBitPacked();
}

void BitPackingSIMDTest::populateBitPacked() {
    bitPackedData.resize(33);
    for (auto bitWidth = 1; bitWidth <= 32; ++bitWidth) {
        auto numWords = (randomInts_u32.size() * bitWidth + 64 - 1) / 64;
        bitPackedData[bitWidth].resize(numWords);
        auto source = reinterpret_cast<uint64_t*>(randomInts_u32.data());
        auto destination = reinterpret_cast<uint64_t*>(bitPackedData[bitWidth].data());
        for (auto i = 0; i < randomInts_u32.size(); ++i) {
            BitCopy::copyBits(source, i * 32, destination, i * bitWidth, bitWidth);
        }
    }
}

#define BIT_PACKING_TEST(WIDTH)                                                                                    \
    TEST_F(BitPackingSIMDTest, test_##WIDTH##_width) {                                                             \
        auto max_result_width = std::min(32, (WIDTH));                                                             \
        for (auto bit_width = 1; bit_width <= max_result_width; bit_width++) {                                     \
            auto source = bitPackedData[bit_width];                                                                \
            std::vector<std::vector<uint##WIDTH##_t>> result;                                                      \
            result.resize(3);                                                                                      \
            result[0].resize(kNumValues);                                                                          \
            result[1].resize(kNumValues);                                                                          \
            result[2].resize(kNumValues);                                                                          \
            starrocks::BitPacking::UnpackValues(bit_width, reinterpret_cast<uint8_t*>(source.data()),              \
                                                source.size() * sizeof(uint64_t), kNumValues, result[0].data());   \
            starrocks::BitPackingAdapter::UnpackValues_ARROW(bit_width, reinterpret_cast<uint8_t*>(source.data()), \
                                                             source.size() * sizeof(uint64_t), kNumValues,         \
                                                             result[1].data());                                    \
            starrocks::BitPackingAdapter::UnpackValues(bit_width, reinterpret_cast<uint8_t*>(source.data()),       \
                                                       source.size() * sizeof(uint64_t), kNumValues,               \
                                                       result[2].data());                                          \
                                                                                                                   \
            for (auto i = 0; i < kNumValues; i++) {                                                                \
                ASSERT_EQ(result[0][i], result[1][i]);                                                             \
                ASSERT_EQ(result[0][i], result[2][i]);                                                             \
            }                                                                                                      \
        }                                                                                                          \
    }

BIT_PACKING_TEST(32);

BIT_PACKING_TEST(16);

BIT_PACKING_TEST(8);

BIT_PACKING_TEST(64);

} // namespace starrocks
