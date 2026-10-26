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

#include <cstring>
#include <random>
#include <vector>

#include "base/simd/expand.h"
#include "base/testutil/parallel_test.h"
#include "gtest/gtest.h"
#include "types/value_generator.h"

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
        // Enumerate every non-zero 8-bit null pattern. Iterating the masks directly instead of using
        // std::next_permutation avoids a GCC -Wstringop-overflow false positive on the vectorized std::reverse.
        size_t i = 0;
        for (uint32_t mask = 1; mask < 256 && i + 8 < chunk_size; ++mask) {
            for (int j = 0; j < 8; ++j) {
                nulls[i++] = (mask >> j) & 1;
            }
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

// The source may be misaligned for the element type (a parquet page's values start wherever the definition
// levels end): every variant must read it correctly, including the scalar tails.
template <class CppType>
void test_expand_load_misaligned() {
    std::mt19937 rng(7);
    for (size_t count : {1, 3, 17, 64, 4095}) {
        std::vector<uint8_t> nulls(count);
        std::vector<CppType> values;
        for (size_t i = 0; i < count; ++i) {
            nulls[i] = rng() % 3 == 0;
            if (!nulls[i]) values.push_back(static_cast<CppType>(rng()));
        }
        // trailing nulls would read one element past the values; stop at the last non-null row as callers do
        size_t expand_count = count;
        while (expand_count > 0 && nulls[expand_count - 1]) --expand_count;
        std::vector<char> bytes(1 + values.size() * sizeof(CppType));
        if (!values.empty()) memcpy(bytes.data() + 1, values.data(), values.size() * sizeof(CppType));
        const auto* src = reinterpret_cast<const CppType*>(bytes.data() + 1);

        std::vector<CppType> dst(count), dst_branchless(count);
        SIMD::Expand::expand_load(dst.data(), src, nulls.data(), expand_count);
        SIMD::Expand::expand_load_branchless(dst_branchless.data(), src, nulls.data(), expand_count);
        size_t k = 0;
        for (size_t i = 0; i < count; ++i) {
            if (!nulls[i]) {
                ASSERT_EQ(values[k], dst[i]) << "count " << count << " row " << i;
                ASSERT_EQ(values[k], dst_branchless[i]) << "count " << count << " row " << i;
                k++;
            }
        }
    }
}

PARALLEL_TEST(ExpandTest, Misaligned) {
    test_expand_load_misaligned<int32_t>();
    test_expand_load_misaligned<int64_t>();
    test_expand_load_misaligned<float>();
    test_expand_load_misaligned<double>();
}

} // namespace starrocks
