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

class ScopedEnv {
public:
    ScopedEnv(const char* name, const char* value) : _name(name) {
        const char* old_val = getenv(name);
        if (old_val != nullptr) {
            _had_old = true;
            _old_val = old_val;
        } else {
            _had_old = false;
        }
        if (value != nullptr) {
            setenv(name, value, 1);
        } else {
            unsetenv(name);
        }
        ResetArmPmullForTesting();
    }

    ~ScopedEnv() {
        if (_had_old) {
            setenv(_name.c_str(), _old_val.c_str(), 1);
        } else {
            unsetenv(_name.c_str());
        }
        ResetArmPmullForTesting();
    }

    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;

private:
    std::string _name;
    bool _had_old = false;
    std::string _old_val;
};

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

TEST(CRC32C, FallbackPathWithEnvVar) {
    ScopedEnv env("STARROCKS_DISABLE_PMULL", "1");
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
    ScopedEnv env("STARROCKS_DISABLE_PMULL", nullptr);
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

            // 3. Compute with STARROCKS_DISABLE_PMULL set
            {
                ScopedEnv env("STARROCKS_DISABLE_PMULL", "1");
                uint32_t crc_disabled = Value(ptr, size);
                ASSERT_EQ(crc_accel, crc_disabled)
                        << "Mismatch with STARROCKS_DISABLE_PMULL at size=" << size << ", offset=" << offset;
            }

            // 4. Test continuation across split boundaries
            size_t half = size / 2;
            uint32_t crc_split = Extend(Value(ptr, half), ptr + half, size - half);
            ASSERT_EQ(crc_accel, crc_split) << "Continuation split mismatch at size=" << size;
        }
    }
}

#if defined(__ARM_NEON) && defined(__aarch64__) && defined(USE_ARM_PMULL)
TEST(CRC32C, DirectArmPmullSimd) {
    if (!HasArmPmull()) {
        GTEST_SKIP() << "ARM PMULL is not supported or is disabled on this host.";
    }
    std::vector<char> buffer(4096);
    for (size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<char>((i * 131) ^ (i >> 3));
    }
    const size_t simd_sizes[] = {64, 128, 192, 256, 512, 1024, 2048, 4096};
    for (size_t size : simd_sizes) {
        uint32_t pmull_crc = ~crc32c_pmull_simd(~0U, buffer.data(), size);
        uint32_t fallback_crc = ValueFallback(buffer.data(), size);
        ASSERT_EQ(pmull_crc, fallback_crc) << "Direct PMULL SIMD mismatch at size=" << size;
    }
}
#endif

} // namespace starrocks::crc32c
