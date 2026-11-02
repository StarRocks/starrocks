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
#if defined(__AVX2__)
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
#if defined(__SSE2__)
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

TEST_F(BatchRunCounterTest, unsigned_filter_values) {
    // 1. All 0xFF (values with high bit set, previously broken by signed cmpgt_epi8)
    {
        std::vector<uint8_t> filter_ff(test_size, 0xFF);
        BatchRunCounter<32> counter32(filter_ff.data(), 0, test_size);
        BatchCount b32 = counter32.next_batch();
        EXPECT_TRUE(b32.AllSet());
        EXPECT_FALSE(b32.NoneSet());

        BatchRunCounter<16> counter16(filter_ff.data(), 0, test_size);
        BatchCount b16 = counter16.next_batch();
        EXPECT_TRUE(b16.AllSet());
        EXPECT_FALSE(b16.NoneSet());

        BatchRunCounter<8> counter8(filter_ff.data(), 0, test_size);
        BatchCount b8 = counter8.next_batch();
        EXPECT_TRUE(b8.AllSet());
        EXPECT_FALSE(b8.NoneSet());
    }

    // 2. All 0x80 (int8_t -128)
    {
        std::vector<uint8_t> filter_80(test_size, 0x80);
        BatchRunCounter<32> counter32(filter_80.data(), 0, test_size);
        BatchCount b32 = counter32.next_batch();
        EXPECT_TRUE(b32.AllSet());
        EXPECT_FALSE(b32.NoneSet());

        BatchRunCounter<16> counter16(filter_80.data(), 0, test_size);
        BatchCount b16 = counter16.next_batch();
        EXPECT_TRUE(b16.AllSet());
        EXPECT_FALSE(b16.NoneSet());

        BatchRunCounter<8> counter8(filter_80.data(), 0, test_size);
        BatchCount b8 = counter8.next_batch();
        EXPECT_TRUE(b8.AllSet());
        EXPECT_FALSE(b8.NoneSet());
    }

    // 3. Mixed unsigned values {0, 0xFF, 0x80, 1, 42}
    {
        std::vector<uint8_t> filter(test_size);
        const uint8_t choices[] = {0, 0xFF, 0x80, 1, 42};
        for (size_t i = 0; i < test_size; i++) {
            filter[i] = choices[rand() % 5];
        }

        size_t zero_count = SIMD::count_zero(filter.data(), test_size);

        auto count_zeros = [&](auto counter) {
            size_t count = 0;
            size_t index = 0;
            auto batch = counter.next_batch();
            while (batch.length > 0) {
                if (batch.AllSet()) {
                    // all non-zero
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
            return count;
        };

        EXPECT_EQ(count_zeros(BatchRunCounter<32>(filter.data(), 0, test_size)), zero_count);
        EXPECT_EQ(count_zeros(BatchRunCounter<16>(filter.data(), 0, test_size)), zero_count);
        EXPECT_EQ(count_zeros(BatchRunCounter<8>(filter.data(), 0, test_size)), zero_count);
    }
}

TEST_F(BatchRunCounterTest, tail_sizes_and_patterns) {
    const std::vector<size_t> sizes = {0, 1, 3, 7, 8, 11, 15, 16, 23, 24, 31, 32, 40, 48, 55, 63, 64, 75, 127};

    auto run_test = [](const std::vector<uint8_t>& filter) {
        size_t expected_zeros = 0;
        for (uint8_t b : filter) {
            if (b == 0) expected_zeros++;
        }

        auto count_zeros = [&](auto counter) {
            size_t count = 0;
            size_t index = 0;
            auto batch = counter.next_batch();
            while (batch.length > 0) {
                if (batch.AllSet()) {
                    // all non-zero
                } else if (batch.NoneSet()) {
                    count += batch.length;
                } else {
                    for (size_t i = 0; i < batch.length; i++) {
                        if (filter[index + i] == 0) {
                            count++;
                        }
                    }
                }
                index += batch.length;
                batch = counter.next_batch();
            }
            return count;
        };

        EXPECT_EQ(count_zeros(BatchRunCounter<32>(filter.data(), 0, filter.size())), expected_zeros);
        EXPECT_EQ(count_zeros(BatchRunCounter<16>(filter.data(), 0, filter.size())), expected_zeros);
        EXPECT_EQ(count_zeros(BatchRunCounter<8>(filter.data(), 0, filter.size())), expected_zeros);
    };

    for (size_t sz : sizes) {
        // 1. All zeros
        std::vector<uint8_t> all_zeros(sz, 0);
        run_test(all_zeros);

        // 2. All 1s
        std::vector<uint8_t> all_ones(sz, 1);
        run_test(all_ones);

        // 3. All 0xFF (unsigned high bit set)
        std::vector<uint8_t> all_ff(sz, 0xFF);
        run_test(all_ff);

        // 4. All 0x80 (int8_t -128)
        std::vector<uint8_t> all_80(sz, 0x80);
        run_test(all_80);

        // 5. Alternating 0 and 1
        std::vector<uint8_t> alternating(sz);
        for (size_t i = 0; i < sz; i++) {
            alternating[i] = (i % 2 == 0) ? 0 : 1;
        }
        run_test(alternating);

        // 6. Non-zero tail after 32 bytes of zeros
        if (sz >= 32) {
            std::vector<uint8_t> non_zero_tail(sz, 0);
            for (size_t i = 32; i < sz; i++) {
                non_zero_tail[i] = 0xFF;
            }
            run_test(non_zero_tail);
        }

        // 7. Zero tail after 32 bytes of non-zeros
        if (sz >= 32) {
            std::vector<uint8_t> zero_tail(sz, 0xFF);
            for (size_t i = 32; i < sz; i++) {
                zero_tail[i] = 0;
            }
            run_test(zero_tail);
        }
    }
}

} // namespace starrocks
