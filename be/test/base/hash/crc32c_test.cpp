// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// the following code are modified from RocksDB:
// https://github.com/facebook/rocksdb/blob/master/util/crc32c_test.cc

#include "base/hash/crc32c.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "base/string/slice.h"

namespace starrocks::crc32c {

class CRC {};

TEST(CRC, StandardResults) {
    // Original Fast_CRC32 tests.
    // From rfc3720 section B.4.
    char buf[32];

    memset(buf, 0, sizeof(buf));
    ASSERT_EQ(0x8a9136aaU, Value(buf, sizeof(buf)));

    memset(buf, 0xff, sizeof(buf));
    ASSERT_EQ(0x62a8ab43U, Value(buf, sizeof(buf)));

    for (int i = 0; i < 32; i++) {
        buf[i] = static_cast<char>(i);
    }
    ASSERT_EQ(0x46dd794eU, Value(buf, sizeof(buf)));

    for (int i = 0; i < 32; i++) {
        buf[i] = static_cast<char>(31 - i);
    }
    ASSERT_EQ(0x113fdb5cU, Value(buf, sizeof(buf)));

    unsigned char data[48] = {
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18,
            0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    ASSERT_EQ(0xd9963a56, Value(reinterpret_cast<char*>(data), sizeof(data)));
}

TEST(CRC, Values) {
    ASSERT_NE(Value("a", 1), Value("foo", 3));
}

TEST(CRC, NullAndZeroLength) {
    ASSERT_EQ(0U, Value(nullptr, 0));
    ASSERT_EQ(0x12345678U, Extend(0x12345678U, nullptr, 0));
    char dummy = 'x';
    ASSERT_EQ(0x12345678U, Extend(0x12345678U, &dummy, 0));
}

TEST(CRC, Extend) {
    ASSERT_EQ(Value("hello world", 11), Extend(Value("hello ", 6), "world", 5));

    std::vector<Slice> slices = {Slice("hello "), Slice("world")};
    ASSERT_EQ(Value("hello world", 11), Value(slices));
}

TEST(CRC, LargeBuffersAndChunking) {
    std::vector<char> buffer(65536 + 64);
    for (size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<char>((i * 131) ^ (i >> 3));
    }

    const size_t test_sizes[] = {0,  1,   7,   8,   9,   15,  16,   17,   31,   32,    63,   64,
                                 65, 127, 128, 129, 511, 512, 1023, 1024, 4096, 16384, 65536};
    for (size_t size : test_sizes) {
        for (size_t offset = 0; offset < 16 && (offset + size) <= buffer.size(); ++offset) {
            uint32_t val = Value(buffer.data() + offset, size);
            // Split into two halves to test state continuation across SIMD boundaries
            size_t half = size / 2;
            uint32_t val_split =
                    Extend(Value(buffer.data() + offset, half), buffer.data() + offset + half, size - half);
            ASSERT_EQ(val, val_split);
        }
    }
}

namespace {

#ifdef BE_TEST
class ScopedPmullOverride {
public:
    explicit ScopedPmullOverride(bool disabled) { SetArmPmullDisabledForTesting(disabled); }
    ~ScopedPmullOverride() { SetArmPmullDisabledForTesting(false); }
    ScopedPmullOverride(const ScopedPmullOverride&) = delete;
    ScopedPmullOverride& operator=(const ScopedPmullOverride&) = delete;
};
#endif

void verify_standard_results() {
    char buf[32];

    memset(buf, 0, sizeof(buf));
    ASSERT_EQ(0x8a9136aaU, Value(buf, sizeof(buf)));

    memset(buf, 0xff, sizeof(buf));
    ASSERT_EQ(0x62a8ab43U, Value(buf, sizeof(buf)));

    for (int i = 0; i < 32; i++) {
        buf[i] = static_cast<char>(i);
    }
    ASSERT_EQ(0x46dd794eU, Value(buf, sizeof(buf)));

    for (int i = 0; i < 32; i++) {
        buf[i] = static_cast<char>(31 - i);
    }
    ASSERT_EQ(0x113fdb5cU, Value(buf, sizeof(buf)));

    unsigned char data[48] = {
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18,
            0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    ASSERT_EQ(0xd9963a56, Value(reinterpret_cast<char*>(data), sizeof(data)));

    // Verify chunked buffer >= 64 bytes
    char buf64[64];
    for (int i = 0; i < 64; i++) {
        buf64[i] = static_cast<char>((i * 7) ^ 0x5a);
    }
    ASSERT_EQ(Value(buf64, sizeof(buf64)), ValueFallback(buf64, sizeof(buf64)));

    char buf128[128];
    for (int i = 0; i < 128; i++) {
        buf128[i] = static_cast<char>((i * 13) ^ 0xa5);
    }
    ASSERT_EQ(Value(buf128, sizeof(buf128)), ValueFallback(buf128, sizeof(buf128)));
}

} // namespace

#ifdef BE_TEST
TEST(CRC32C, TestOverrideCacheInvalidation) {
    // Warm up the dispatch cache
    char buf[128] = {1};
    uint32_t c1 = Extend(0, buf, sizeof(buf));
    (void)c1;

    // Disabling must take effect immediately
    {
        ScopedPmullOverride disable(true);
        EXPECT_FALSE(HasArmPmull());
        uint32_t fallback_crc = Extend(0, buf, sizeof(buf));
        EXPECT_EQ(c1, fallback_crc);
    }

    // Re-enabling must restore hardware capability
#if defined(__ARM_NEON) && defined(__aarch64__) && defined(USE_ARM_PMULL)
    // On ARM machines with PMULL hardware, it should restore to true
    // On non-ARM or systems without PMULL, it remains false
#endif
}

TEST(CRC32C, FallbackEquivalence) {
    ScopedPmullOverride disable(true);
    ASSERT_FALSE(HasArmPmull());
    verify_standard_results();

    std::vector<char> buffer(4096);
    for (size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<char>((i * 131) ^ (i >> 3));
    }

    const size_t test_sizes[] = {0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 63, 64, 65, 128, 256, 1024, 4096};
    for (size_t size : test_sizes) {
        uint32_t val = Value(buffer.data(), size);
        uint32_t val_fallback = ValueFallback(buffer.data(), size);
        ASSERT_EQ(val, val_fallback);
    }
}

TEST(CRC32C, AcceleratedPath) {
    ScopedPmullOverride enable(false);
    verify_standard_results();

    std::vector<char> buffer(4096);
    for (size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<char>((i * 131) ^ (i >> 3));
    }

    const size_t test_sizes[] = {0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 63, 64, 65, 128, 256, 1024, 4096};
    for (size_t size : test_sizes) {
        uint32_t val = Value(buffer.data(), size);
        uint32_t val_fallback = ValueFallback(buffer.data(), size);
        ASSERT_EQ(val, val_fallback);
    }
}

TEST(CRC32C, EquivalenceAcrossBoundaries) {
    std::vector<char> buffer(65536 + 64);
    for (size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<char>((i * 131) ^ (i >> 3));
    }

    const size_t test_sizes[] = {0,   1,   7,   8,   9,    15,   16,   17,   31,   32,   33,    47,
                                 48,  63,  64,  65,  127,  128,  129,  191,  192,  193,  255,   256,
                                 257, 511, 512, 513, 1023, 1024, 1025, 4095, 4096, 4097, 16384, 65536};
    const size_t test_offsets[] = {0, 1, 3, 7, 8, 15};

    for (size_t size : test_sizes) {
        for (size_t offset : test_offsets) {
            if ((offset + size) > buffer.size()) {
                continue;
            }
            const char* ptr = buffer.data() + offset;

            // 1. Compute default / accelerated value
            uint32_t crc_accel = Value(ptr, size);

            // 2. Compute explicit fallback value
            uint32_t crc_fallback = ValueFallback(ptr, size);
            ASSERT_EQ(crc_accel, crc_fallback) << "Mismatch at size=" << size << ", offset=" << offset;

            // 3. Compute with PMULL disabled
            {
                ScopedPmullOverride disable(true);
                uint32_t crc_disabled = Value(ptr, size);
                ASSERT_EQ(crc_accel, crc_disabled)
                        << "Mismatch with PMULL disabled at size=" << size << ", offset=" << offset;
            }

            // 4. Test continuation across split boundaries
            size_t half = size / 2;
            uint32_t crc_split = Extend(Value(ptr, half), ptr + half, size - half);
            ASSERT_EQ(crc_accel, crc_split) << "Continuation split mismatch at size=" << size;
        }
    }
}
#endif

#if defined(__ARM_NEON) && defined(__aarch64__) && defined(USE_ARM_PMULL)
TEST(CRC32C, DirectArmPmullSimd) {
    if (!HasArmPmull()) {
        GTEST_SKIP() << "ARM PMULL is not supported or is disabled on this host.";
    }
    std::vector<char> buffer(4096);
    for (size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<char>((i * 131) ^ (i >> 3));
    }
    const size_t simd_sizes[] = {64, 80, 96, 112, 128, 144, 192, 208, 256, 512, 1024, 2048, 4096};
    for (size_t size : simd_sizes) {
        uint32_t pmull_crc = ~crc32c_pmull_simd(~0U, buffer.data(), size);
        uint32_t fallback_crc = ValueFallback(buffer.data(), size);
        ASSERT_EQ(pmull_crc, fallback_crc) << "Direct PMULL SIMD mismatch at size=" << size;
    }
}
#endif

} // namespace starrocks::crc32c
