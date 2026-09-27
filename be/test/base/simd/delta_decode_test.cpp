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

#include <vector>

namespace starrocks {
class DeltaDecodeTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

// The ISA-specific kernels now live in their own translation units and are always compiled (with
// per-file -m flags), so the guards below are *runtime* __builtin_cpu_supports checks rather than
// compile-time #ifdefs: this exercises the AVX-512 kernel on a capable CPU even when the binary was
// built with USE_AVX512=OFF, which is exactly the property the split is meant to provide. The
// AVX-512 kernels are skipped under ASan (known zmm-spill redzone trap), matching the dispatcher.

TEST_F(DeltaDecodeTest, test_int32) {
    std::vector<int32_t> values(233);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = 1;
    }

    // scalar reference
    std::vector<int32_t> expected(values);
    int32_t expected_last = 10;
    delta_decode_chain_scalar_prefetch<int32_t>(expected.data(), expected.size(), 10, expected_last);
    ASSERT_EQ(expected.back(), expected_last);

    // public dispatcher must match on any CPU
    {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }

#if defined(__x86_64__)
    if (__builtin_cpu_supports("avx2")) {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32_avx2(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }
#if !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl")) {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32_avx2x(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }
    if (__builtin_cpu_supports("avx512f")) {
        std::vector<int32_t> v(values);
        int32_t last_value = 10;
        delta_decode_chain_int32_avx512(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }
#endif
#endif
}

TEST_F(DeltaDecodeTest, test_int64) {
    std::vector<int64_t> values(223);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = 1;
    }

    // scalar reference
    std::vector<int64_t> expected(values);
    int64_t expected_last = 10;
    delta_decode_chain_scalar_prefetch<int64_t>(expected.data(), expected.size(), 10, expected_last);
    ASSERT_EQ(expected.back(), expected_last);

    // public dispatcher must match on any CPU
    {
        std::vector<int64_t> v(values);
        int64_t last_value = 10;
        delta_decode_chain_int64(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }

#if defined(__x86_64__) && !defined(ADDRESS_SANITIZER)
    if (__builtin_cpu_supports("avx512f")) {
        std::vector<int64_t> v(values);
        int64_t last_value = 10;
        delta_decode_chain_int64_avx512(v.data(), v.size(), 10, last_value);
        ASSERT_EQ(v, expected);
        ASSERT_EQ(v.back(), last_value);
    }
#endif
}

} // namespace starrocks
