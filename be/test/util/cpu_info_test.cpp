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

#include "util/cpu_info.h"

#include "gtest/gtest.h"

namespace starrocks {

struct CpuInfoTest : public ::testing::Test {
    void SetUp() {
        CpuInfo::init();
        value = *CpuInfo::TEST_mutable_hardware_flags();
    }

    void TearDown() {
        int64_t* flag = CpuInfo::TEST_mutable_hardware_flags();
        *flag = value;
    }

    int64_t value;
};

TEST_F(CpuInfoTest, test_pass_cpu_flags_check) {
    // should be always success when the runtime env is the same as the env where the binary is built from
    auto sets = CpuInfo::unsupported_cpu_flags_from_current_env();
    EXPECT_TRUE(sets.empty());
}

TEST_F(CpuInfoTest, test_fail_cpu_flags_check) {
#if defined(__x86_64__) && defined(__AVX2__)
    int64_t* flags = CpuInfo::TEST_mutable_hardware_flags();
    EXPECT_TRUE(*flags & CpuInfo::AVX2);
    // clear AVX2 flags, simulate that the platform doesn't support avx2
    *flags &= ~CpuInfo::AVX2;
    EXPECT_FALSE(*flags & CpuInfo::AVX2);
    EXPECT_FALSE(CpuInfo::is_supported(CpuInfo::AVX2));
    auto unsupported_flags = CpuInfo::unsupported_cpu_flags_from_current_env();
    EXPECT_EQ(1, unsupported_flags.size());
    EXPECT_EQ("avx2", unsupported_flags.front());
    // restore the flag
    *flags |= CpuInfo::AVX2;
#else
    GTEST_SKIP() << "avx2 is not supported, skip the test!";
#endif
}
} // namespace starrocks
