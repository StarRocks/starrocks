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

#include "base/simd/delta_decode.h"

#include <gtest/gtest.h>

namespace starrocks {
class DeltaDecodeTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(DeltaDecodeTest, test_int32) {
    std::vector<int32_t> values(233);
    for (int i = 0; i < values.size(); i++) {
        values[i] = 1;
    }
    [[maybe_unused]] std::vector<int32_t> avx2_values(values);
    [[maybe_unused]] std::vector<int32_t> avx2x_values(values);
    [[maybe_unused]] std::vector<int32_t> avx512_values(values);

    {
        int32_t last_value = 10;
        delta_decode_chain_scalar_prefetch<int32_t>(values.data(), values.size(), 10, last_value);
        ASSERT_EQ(values.back(), last_value);
    }
#ifdef __AVX2__
    {
        int32_t last_value = 10;
        delta_decode_chain_int32_avx2(avx2_values.data(), avx2_values.size(), 10, last_value);
        ASSERT_EQ(avx2_values.back(), last_value);
        ASSERT_EQ(avx2_values, values);
    }
#endif

#if defined(__AVX512F__) && defined(__AVX512VL__)
    {
        int32_t last_value = 10;
        delta_decode_chain_int32_avx2x(avx2x_values.data(), avx2x_values.size(), 10, last_value);
        ASSERT_EQ(avx2x_values.back(), last_value);
        ASSERT_EQ(avx2x_values, values);
    }
#endif

#ifdef __AVX512F__
    {
        int32_t last_value = 10;
        delta_decode_chain_int32_avx512(avx512_values.data(), avx512_values.size(), 10, last_value);
        ASSERT_EQ(avx512_values.back(), last_value);
        ASSERT_EQ(avx512_values, values);
    }
#endif
}

TEST_F(DeltaDecodeTest, test_int64) {
    std::vector<int64_t> values(223);
    for (int i = 0; i < values.size(); i++) {
        values[i] = 1;
    }
    [[maybe_unused]] std::vector<int64_t> avx512_values(values);
    {
        int64_t last_value = 10;
        delta_decode_chain_scalar_prefetch<int64_t>(values.data(), values.size(), 10, last_value);
        ASSERT_EQ(values.back(), last_value);
    }
#ifdef __AVX512F__
    {
        int64_t last_value = 10;
        delta_decode_chain_int64_avx512(avx512_values.data(), avx512_values.size(), 10, last_value);
        ASSERT_EQ(avx512_values.back(), last_value);
        ASSERT_EQ(avx512_values, values);
    }
#endif
}

} // namespace starrocks