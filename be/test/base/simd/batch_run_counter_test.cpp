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

#include "base/simd/batch_run_counter.h"

#include <vector>

#include "base/simd/simd.h"
#include "gtest/gtest.h"

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
    BatchRunCounter<32> counter(filter.data(), 0, test_size);
    BatchCount batch = counter.next_batch();
#if defined(__AVX2__) || (defined(__ARM_NEON) && defined(__aarch64__))
    EXPECT_EQ(batch.length, 32);
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
    BatchRunCounter<16> counter(filter.data(), 0, test_size);
    BatchCount batch = counter.next_batch();
#if defined(__SSE2__) || (defined(__ARM_NEON) && defined(__aarch64__))
    EXPECT_EQ(batch.length, 16);
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
    BatchRunCounter<8> counter(filter.data(), 0, test_size);
    BatchCount batch = counter.next_batch();
    EXPECT_EQ(batch.length, 8);
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

TEST_F(BatchRunCounterTest, deterministic_all_zero_all_one_and_single_flip) {
    {
        std::vector<uint8_t> zeros(64, 0);
        BatchRunCounter<32> c32(zeros.data(), 0, zeros.size());
        BatchCount b1 = c32.next_batch();
        EXPECT_TRUE(b1.NoneSet());
        EXPECT_FALSE(b1.AllSet());
        BatchRunCounter<16> c16(zeros.data(), 0, zeros.size());
        BatchCount b16 = c16.next_batch();
        EXPECT_TRUE(b16.NoneSet());
        EXPECT_FALSE(b16.AllSet());
    }
    {
        std::vector<uint8_t> ones(64, 1);
        BatchRunCounter<32> c32(ones.data(), 0, ones.size());
        BatchCount b1 = c32.next_batch();
        EXPECT_TRUE(b1.AllSet());
        EXPECT_FALSE(b1.NoneSet());
        BatchRunCounter<16> c16(ones.data(), 0, ones.size());
        BatchCount b16 = c16.next_batch();
        EXPECT_TRUE(b16.AllSet());
        EXPECT_FALSE(b16.NoneSet());
    }
    for (size_t k = 0; k < 32; ++k) {
        std::vector<uint8_t> single_one(32, 0);
        single_one[k] = 1;
        BatchRunCounter<32> c32(single_one.data(), 0, 32);
        BatchCount b = c32.next_batch();
#if defined(__AVX2__) || (defined(__ARM_NEON) && defined(__aarch64__))
        EXPECT_EQ(b.length, 32);
        EXPECT_FALSE(b.valid_count) << "lane k=" << k;
        EXPECT_FALSE(b.NoneSet()) << "lane k=" << k;
        EXPECT_FALSE(b.AllSet()) << "lane k=" << k;
#endif

        std::vector<uint8_t> single_zero(32, 1);
        single_zero[k] = 0;
        BatchRunCounter<32> c32_z(single_zero.data(), 0, 32);
        BatchCount bz = c32_z.next_batch();
#if defined(__AVX2__) || (defined(__ARM_NEON) && defined(__aarch64__))
        EXPECT_EQ(bz.length, 32);
        EXPECT_FALSE(bz.valid_count) << "lane k=" << k;
        EXPECT_FALSE(bz.NoneSet()) << "lane k=" << k;
        EXPECT_FALSE(bz.AllSet()) << "lane k=" << k;
#endif
    }
    for (size_t k = 0; k < 16; ++k) {
        std::vector<uint8_t> single_one(16, 0);
        single_one[k] = 1;
        BatchRunCounter<16> c16(single_one.data(), 0, 16);
        BatchCount b = c16.next_batch();
#if defined(__SSE2__) || (defined(__ARM_NEON) && defined(__aarch64__))
        EXPECT_EQ(b.length, 16);
        EXPECT_FALSE(b.valid_count) << "lane k=" << k;
        EXPECT_FALSE(b.NoneSet()) << "lane k=" << k;
        EXPECT_FALSE(b.AllSet()) << "lane k=" << k;
#endif

        std::vector<uint8_t> single_zero(16, 1);
        single_zero[k] = 0;
        BatchRunCounter<16> c16_z(single_zero.data(), 0, 16);
        BatchCount bz = c16_z.next_batch();
#if defined(__SSE2__) || (defined(__ARM_NEON) && defined(__aarch64__))
        EXPECT_EQ(bz.length, 16);
        EXPECT_FALSE(bz.valid_count) << "lane k=" << k;
        EXPECT_FALSE(bz.NoneSet()) << "lane k=" << k;
        EXPECT_FALSE(bz.AllSet()) << "lane k=" << k;
#endif
    }
    for (size_t tail_len : {size_t{0}, size_t{1}, size_t{7}, size_t{8}, size_t{15}, size_t{16}, size_t{31}}) {
        std::vector<uint8_t> tail(tail_len, 0);
        BatchRunCounter<32> c32(tail.data(), 0, tail_len);
        size_t total = 0;
        while (true) {
            BatchCount b = c32.next_batch();
            if (b.length == 0) break;
            total += b.length;
        }
        EXPECT_EQ(total, tail_len);
    }
}

} // namespace starrocks
