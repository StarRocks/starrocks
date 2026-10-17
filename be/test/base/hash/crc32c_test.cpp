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

#include <vector>

#include "base/string/slice.h"

namespace starrocks::crc32c {

#ifdef BE_TEST
class ScopedArmCrc32Disable {
public:
    explicit ScopedArmCrc32Disable(bool disabled = true) {
        SetArmCrc32DisabledForTesting(disabled);
        ResetArmCrc32cCacheForTesting();
    }
    ~ScopedArmCrc32Disable() {
        SetArmCrc32DisabledForTesting(false);
        ResetArmCrc32cCacheForTesting();
    }
    ScopedArmCrc32Disable(const ScopedArmCrc32Disable&) = delete;
    ScopedArmCrc32Disable& operator=(const ScopedArmCrc32Disable&) = delete;
};
#endif

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

TEST(CRC, SmallAndUnalignedBuffers) {
    std::string test_str = "StarRocks High-Performance Analytical Database Engine Checksum Test 1234567890";
    ASSERT_EQ(0xd17e79b6U, Value(test_str.data(), test_str.size()));
    ASSERT_EQ(0xe3069283U, Value("123456789", 9));

    for (size_t len = 0; len <= 64; ++len) {
        for (size_t offset = 0; offset < 16 && offset + len <= test_str.size(); ++offset) {
            const char* ptr = test_str.data() + offset;
            uint32_t val = Value(ptr, len);
            for (size_t split = 0; split <= len; ++split) {
                ASSERT_EQ(val, Extend(Extend(0, ptr, split), ptr + split, len - split));
            }
        }
    }
}

TEST(CRC, FallbackEquivalence) {
#if defined(BE_TEST)
#if defined(__aarch64__)
    std::string test_str = "StarRocks High-Performance Analytical Database Engine Checksum Test 1234567890";
    uint32_t hw_val = Value(test_str.data(), test_str.size());

    {
        ScopedArmCrc32Disable disable;
        uint32_t sw_val = Value(test_str.data(), test_str.size());
        ASSERT_EQ(hw_val, sw_val);
    }

    uint32_t hw_val_restored = Value(test_str.data(), test_str.size());
    ASSERT_EQ(hw_val, hw_val_restored);
#else
    std::string test_str = "StarRocks High-Performance Analytical Database Engine Checksum Test 1234567890";
    uint32_t initial_val = Value(test_str.data(), test_str.size());
    {
        ScopedArmCrc32Disable disable;
        uint32_t val = Value(test_str.data(), test_str.size());
        ASSERT_EQ(initial_val, val);
    }
    ASSERT_EQ(initial_val, Value(test_str.data(), test_str.size()));
#endif
#endif
}

TEST(CRC, FallbackEquivalenceAcrossBoundaries) {
#if defined(BE_TEST) && defined(__aarch64__)
    const std::string s = "StarRocks High-Performance Analytical Database Engine Checksum Test 1234567890";
    for (size_t len = 0; len <= 64; ++len) {
        for (size_t off = 0; off < 16 && off + len <= s.size(); ++off) {
            const uint32_t hw = Value(s.data() + off, len);
            uint32_t sw;
            {
                ScopedArmCrc32Disable disable;
                sw = Value(s.data() + off, len);
            }
            ASSERT_EQ(hw, sw) << "len=" << len << " off=" << off;
        }
    }
#endif
}

} // namespace starrocks::crc32c
