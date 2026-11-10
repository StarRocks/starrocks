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
#include <paimon/memory/memory_pool.h>

#include <cstdlib>
#include <memory>

#include "connector/hive/paimon/paimon_scanner.h"

namespace starrocks {

namespace {

class FixedPeakMemoryPool final : public paimon::MemoryPool {
public:
    explicit FixedPeakMemoryPool(uint64_t peak) : _peak(peak) {}

    void* Malloc(uint64_t size, uint64_t) override { return std::malloc(size); }
    void* Realloc(void* p, size_t, size_t new_size, uint64_t) override { return std::realloc(p, new_size); }
    void Free(void* p, uint64_t) override { std::free(p); }
    uint64_t CurrentUsage() const override { return 0; }
    uint64_t MaxMemoryUsage() const override { return _peak; }

private:
    uint64_t _peak;
};

constexpr int64_t kMiB = 1024 * 1024;

} // namespace

class PaimonScannerMemUsageTest : public ::testing::Test {
protected:
    static int64_t estimate_with_peak(std::shared_ptr<paimon::MemoryPool> pool) {
        PaimonScanner scanner;
        scanner._memory_pool = std::move(pool);
        return scanner.estimated_mem_usage();
    }
};

TEST_F(PaimonScannerMemUsageTest, no_pool_reports_no_observation) {
    ASSERT_EQ(0, estimate_with_peak(nullptr));
}

TEST_F(PaimonScannerMemUsageTest, zero_peak_reports_no_observation) {
    ASSERT_EQ(0, estimate_with_peak(std::make_shared<FixedPeakMemoryPool>(0)));
}

TEST_F(PaimonScannerMemUsageTest, peak_is_reported_as_is) {
    ASSERT_EQ(1 * kMiB, estimate_with_peak(std::make_shared<FixedPeakMemoryPool>(1 * kMiB)));
    ASSERT_EQ(640 * kMiB, estimate_with_peak(std::make_shared<FixedPeakMemoryPool>(640 * kMiB)));
}

} // namespace starrocks
