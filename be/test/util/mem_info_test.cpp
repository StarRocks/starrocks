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

#include "util/mem_info.h"

#include <unistd.h>

#include <cstring>

#include "gtest/gtest.h"

#ifndef __APPLE__
#include <sys/mman.h>
#endif

namespace starrocks {

// Deliberately without MemInfo::init(): unlike physical_mem(), this reads /proc on every call and
// must not depend on the one-time initialization.

#ifndef __APPLE__

TEST(MemInfoProcessResidentTest, reports_whole_pages) {
    const int64_t rss = MemInfo::process_resident_bytes();
    ASSERT_GT(rss, 0);
    EXPECT_EQ(0, rss % sysconf(_SC_PAGESIZE)) << "statm counts pages, so the result is a whole number of them";
}

// statm's first field is the address space and its second is the resident set. A parser that took
// the first would still return a positive, page-aligned, growing number, so every cheap assertion
// would pass while every caller silently got the wrong quantity. An untouched mapping is what
// separates the two: it grows the address space and nothing else.
TEST(MemInfoProcessResidentTest, tracks_residency_not_address_space) {
    constexpr int64_t kMappingBytes = 64L * 1024 * 1024;
    // Half the mapping, so neither direction depends on the test binary being quiescent.
    constexpr int64_t kTolerance = kMappingBytes / 2;

    const int64_t before = MemInfo::process_resident_bytes();
    ASSERT_GT(before, 0);

    void* mapping = mmap(nullptr, kMappingBytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(MAP_FAILED, mapping);

    const int64_t mapped = MemInfo::process_resident_bytes();
    EXPECT_LT(mapped - before, kTolerance) << "an untouched anonymous mapping is address space, not resident memory";

    memset(mapping, 1, kMappingBytes);

    const int64_t touched = MemInfo::process_resident_bytes();
    EXPECT_GT(touched - mapped, kTolerance) << "touching every page of the mapping must show up in the resident set";

    ASSERT_EQ(0, munmap(mapping, kMappingBytes));
}

#else

TEST(MemInfoProcessResidentTest, unavailable_without_proc) {
    EXPECT_EQ(-1, MemInfo::process_resident_bytes()) << "macOS has no /proc/self/statm, so the read must fail cleanly";
}

#endif

} // namespace starrocks
