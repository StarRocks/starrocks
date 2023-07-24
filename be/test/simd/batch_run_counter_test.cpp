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

#include "simd/batch_run_counter.h"

#include <vector>

#include "gtest/gtest.h"
#include "simd/simd.h"

namespace starrocks {

class BatchRunCounterTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    size_t test_size = 20000;
};

TEST_F(BatchRunCounterTest, batch_size_32) {
    std::vector<uint8_t> filter;
    filter.resize(test_size);
    for (size_t i = 0; i < test_size; i++) {
        uint8_t set_or_not = rand() % 1000 > 3 ? 0 : 1;
        filter[i] = set_or_not;
    }

    size_t zero_count = SIMD::count_zero(filter.data(), test_size);
    BatchRunCounter counter(filter.data(), 0, test_size, zero_count);
    BatchCount batch = counter.next_batch();
#if defined(__AVX2__)
    double ratio = zero_count * 1.0 / test_size;
    if (ratio < 0.005 || ratio > 0.995) {
        EXPECT_EQ(batch.length, 32);
        std::cout << "batch size: " << batch.length << std::endl;
    }
#endif
    size_t count = 0;
    size_t index = 0;
    while (batch.length > 0) {
        if (batch.AllSet()) {
            //do nothing
        } else if (batch.NoneSet()) {
            count += batch.length;
        } else {
            for (int i = 0; i < batch.length; i++) {
                if (filter[index + i] == 0) {
                    count++;
                }
            }
        }
        index += batch.length;
        batch = counter.next_batch();
    }

    EXPECT_EQ(count, zero_count);
}

TEST_F(BatchRunCounterTest, batch_size_16) {
    std::vector<uint8_t> filter;
    filter.resize(test_size);
    for (size_t i = 0; i < test_size; i++) {
        uint8_t set_or_not = rand() % 1000 > 8 ? 1 : 0;
        filter[i] = set_or_not;
    }

    size_t zero_count = SIMD::count_zero(filter.data(), test_size);
    BatchRunCounter counter(filter.data(), 0, test_size, zero_count);
    BatchCount batch = counter.next_batch();
#if defined(__SSE2__)
    double ratio = zero_count * 1.0 / test_size;
    if ((ratio > 0.005 && ratio < 0.995) && (ratio < 0.01 || ratio > 0.99)) {
        EXPECT_EQ(batch.length, 16);
        std::cout << "batch size: " << batch.length << std::endl;
    }
#endif
    size_t count = 0;
    size_t index = 0;
    while (batch.length > 0) {
        if (batch.AllSet()) {
            //do nothing
        } else if (batch.NoneSet()) {
            count += batch.length;
        } else {
            for (int i = 0; i < batch.length; i++) {
                if (filter[index + i] == 0) {
                    count++;
                }
            }
        }
        index += batch.length;
        batch = counter.next_batch();
    }

    EXPECT_EQ(count, zero_count);
}

TEST_F(BatchRunCounterTest, batch_size_8) {
    std::vector<uint8_t> filter;
    filter.resize(test_size);
    for (size_t i = 0; i < test_size; i++) {
        uint8_t set_or_not = rand() % 1000 > 100 ? 1 : 0;
        filter[i] = set_or_not;
    }

    size_t zero_count = SIMD::count_zero(filter.data(), test_size);
    BatchRunCounter counter(filter.data(), 0, test_size, zero_count);
    BatchCount batch = counter.next_batch();
    double ratio = zero_count * 1.0 / test_size;
    if (ratio > 0.01 && ratio < 0.99) {
        EXPECT_EQ(batch.length, 8);
        std::cout << "batch size: " << batch.length << std::endl;
    }
    size_t count = 0;
    size_t index = 0;
    while (batch.length > 0) {
        if (batch.AllSet()) {
            //do nothing
        } else if (batch.NoneSet()) {
            count += batch.length;
        } else {
            for (int i = 0; i < batch.length; i++) {
                if (filter[index + i] == 0) {
                    count++;
                }
            }
        }
        index += batch.length;
        batch = counter.next_batch();
    }

    EXPECT_EQ(count, zero_count);
}

} // namespace starrocks
