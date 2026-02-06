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

#include "base/simd/expand.h"
#include "base/testutil/parallel_test.h"
#include "gtest/gtest.h"
#include "util/value_generator.h"

namespace starrocks {

template <class CppType>
void test_expand_load() {
    const size_t chunk_size = 4095;
    std::vector<CppType> srcs;
    std::vector<CppType> dsts;

    srcs.resize(chunk_size);
    dsts.resize(chunk_size);

    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i] = i;
    }

    {
        // test no nulls
        std::vector<uint8_t> nulls;
        nulls.resize(chunk_size);

        SIMD::Expand::expand_load(dsts.data(), srcs.data(), nulls.data(), chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            ASSERT_EQ(srcs[i], dsts[i]);
        }

        SIMD::Expand::expand_load_branchless(dsts.data(), srcs.data(), nulls.data(), chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            ASSERT_EQ(srcs[i], dsts[i]);
        }
    }
    {
        // test all nulls
        std::vector<uint8_t> nulls;
        nulls.resize(chunk_size, 1);
        // all nulls we don't have to check result
        SIMD::Expand::expand_load_branchless(dsts.data(), srcs.data(), nulls.data(), chunk_size);
        SIMD::Expand::expand_load(dsts.data(), srcs.data(), nulls.data(), chunk_size);
    }
    {
        // test interleave
        std::vector<uint8_t> nulls;
        nulls.resize(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            nulls[i] = i % 2;
        }

        std::vector<CppType> dsts1(chunk_size);
        std::vector<CppType> dsts2(chunk_size);

        SIMD::Expand::expand_load_branchless(dsts1.data(), srcs.data(), nulls.data(), chunk_size);
        SIMD::Expand::expand_load(dsts2.data(), srcs.data(), nulls.data(), chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            if (nulls[i] == 0) {
                ASSERT_EQ(dsts1[i], dsts2[i]);
            }
        }
    }
    {
        // test enum

        std::vector<uint8_t> nulls;
        nulls.resize(chunk_size);
        std::vector<uint8_t> pattern(8);
        size_t i = 0;
        for (int k = 1; k <= 8; ++k) {
            std::fill(pattern.begin(), pattern.end(), 0);
            std::fill(pattern.begin(), pattern.begin() + k, 1);

            do {
                for (int j = 0; j < 8; ++j) {
                    nulls[i++] = pattern[j];
                }
            } while (std::next_permutation(pattern.begin(), pattern.end()) && i + 8 < chunk_size);
        }

        std::vector<CppType> dsts1(chunk_size);
        std::vector<CppType> dsts2(chunk_size);

        SIMD::Expand::expand_load_branchless(dsts1.data(), srcs.data(), nulls.data(), chunk_size);
        SIMD::Expand::expand_load(dsts2.data(), srcs.data(), nulls.data(), chunk_size);

        for (size_t i = 0; i < chunk_size; ++i) {
            if (nulls[i] == 0) {
                ASSERT_EQ(dsts1[i], dsts2[i]);
            }
        }
    }

    // test full rand
    {
        using Gen = RandomGenerator<uint8_t, 2>;
        std::vector<uint8_t> nulls;
        nulls.resize(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            nulls[i] = Gen::next_value();
        }

        std::vector<CppType> dsts1(chunk_size);
        std::vector<CppType> dsts2(chunk_size);

        SIMD::Expand::expand_load_branchless(dsts1.data(), srcs.data(), nulls.data(), chunk_size);
        SIMD::Expand::expand_load(dsts2.data(), srcs.data(), nulls.data(), chunk_size);

        for (size_t i = 0; i < chunk_size; ++i) {
            if (nulls[i] == 0) {
                ASSERT_EQ(dsts1[i], dsts2[i]);
            }
        }
    }
}

PARALLEL_TEST(ExpandTest, All) {
    test_expand_load<int32_t>();
    test_expand_load<int64_t>();
    test_expand_load<float>();
    test_expand_load<double>();
}

} // namespace starrocks
