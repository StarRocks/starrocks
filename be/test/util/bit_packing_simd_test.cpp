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
#include "util/bit_packing.h"
#include "util/bit_packing_arrow.h"
#include "util/bit_packing_default.h"

namespace starrocks {

template <typename T, int Width>
struct ParamType {
    using Type = T;
    static constexpr int value_width = Width;
};

template <typename ParamType>
class BitPackingSIMDTest : public ::testing::Test {
public:
    using T = typename ParamType::Type;
    static constexpr int Width = ParamType::value_width;
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

template <typename ParamType>
void BitPackingSIMDTest<ParamType>::SetUp() {
    std::mt19937 gen(42);

    std::uniform_int_distribution<uint32_t> distr(0, std::numeric_limits<uint32_t>::max());
    for (int32_t i = 0; i < kNumValues; i++) {
        auto randomInt = distr(gen);
        randomInts_u32.push_back(randomInt);
    }
    populateBitPacked();
}

template <typename ParamType>
void BitPackingSIMDTest<ParamType>::populateBitPacked() {
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
using TestParams = ::testing::Types<ParamType<uint8_t, 8>, ParamType<uint16_t, 16>, ParamType<uint32_t, 32>,
                                    ParamType<uint64_t, 64>>;

TYPED_TEST_SUITE(BitPackingSIMDTest, TestParams);

TYPED_TEST(BitPackingSIMDTest, test_bit_packing) {
    using T = typename TestFixture::T;
    constexpr int width = TestFixture::Width;
    auto max_result_width = std::min(32, width);
    constexpr auto kNumValues = TestFixture::kNumValues;
    for (auto bit_width = 1; bit_width <= max_result_width; bit_width++) {
        std::cout << "testing bit_width: " << bit_width << std::endl;
        auto source = TestFixture::bitPackedData[bit_width];
        std::vector<std::vector<T>> result;
        result.resize(3);
        result[0].resize(kNumValues);
        result[1].resize(kNumValues);
        result[2].resize(kNumValues);

        starrocks::util::bitpacking_default::UnpackValues(bit_width, reinterpret_cast<uint8_t*>(source.data()),
                                                          source.size() * sizeof(uint64_t), kNumValues,
                                                          result[0].data());
        starrocks::util::bitpacking_arrow::UnpackValues(bit_width, reinterpret_cast<uint8_t*>(source.data()),
                                                        source.size() * sizeof(uint64_t), kNumValues, result[1].data());
        starrocks::BitPacking::UnpackValues(bit_width, reinterpret_cast<uint8_t*>(source.data()),
                                            source.size() * sizeof(uint64_t), kNumValues, result[2].data());

        for (auto i = 0; i < kNumValues; i++) {
            ASSERT_EQ(result[0][i], result[1][i]);
            ASSERT_EQ(result[0][i], result[2][i]);
        }
    }
}

} // namespace starrocks
