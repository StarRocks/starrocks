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

#include "gutil/cpu.h"

#include <gtest/gtest.h>

#include "util/cpu_info.h"

namespace starrocks {

TEST(CpuTest, hardware_support) {
    const base::CPU* cpu = base::CPU::instance();
    EXPECT_NE(nullptr, cpu);
    EXPECT_TRUE(cpu->has_avx());
#if defined(__x86_64__) && defined(__SSE4_2__)
    EXPECT_TRUE(cpu->has_sse42());
#else
    EXPECT_FALSE(cpu->has_sse42());
#endif
#if defined(__x86_64__) && defined(__AVX2__)
    EXPECT_TRUE(cpu->has_avx2());
#else
    EXPECT_FALSE(cpu->has_avx2());
#endif
#if defined(__x86_64__) && defined(__AVX512F__)
    EXPECT_TRUE(cpu->has_avx512f());
#else
    EXPECT_FALSE(cpu->has_avx512f());
#endif
#if defined(__x86_64__) && defined(__AVX512BW__)
    EXPECT_TRUE(cpu->has_avx512bw());
#else
    EXPECT_FALSE(cpu->has_avx512bw());
#endif
}

TEST(CpuTest, parse_cpus) {
    auto assert_cpu_equals = [](std::vector<size_t>& cpus, std::vector<size_t>& expected_cpus) {
        ASSERT_EQ(expected_cpus.size(), cpus.size());
        std::ranges::sort(cpus);
        std::ranges::sort(expected_cpus);
        for (size_t i = 0; i < cpus.size(); ++i) {
            EXPECT_EQ(expected_cpus[i], cpus[i]);
        }
    };

    {
        std::vector<size_t> cpus = CpuInfo::parse_cpus("0-3,5,7,9-10");
        std::vector<size_t> expected_cpus = {0, 1, 2, 3, 5, 7, 9, 10};
        assert_cpu_equals(cpus, expected_cpus);
    }

    {
        const std::vector<size_t> cpus = CpuInfo::parse_cpus("");
        EXPECT_TRUE(cpus.empty());
    }

    {
        std::vector<size_t> cpus = CpuInfo::parse_cpus("abc,1-,2-abc,3-5,,8");
        std::vector<size_t> expected_cpus = {3, 4, 5, 8};
        assert_cpu_equals(cpus, expected_cpus);
    }
}

} // namespace starrocks
