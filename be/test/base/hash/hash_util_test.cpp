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

#include "base/hash/hash_util.hpp"

#include <gtest/gtest.h>
#include <zlib.h>

#include <cstring>
#include <string_view>

#include "base/hash/murmur_hash3.h"
#include "gutil/cpu.h"

namespace starrocks {

struct HashCombineTag {
    int value;
};

namespace {

bool sse42_available() {
#ifdef __SSE4_2__
    base::CPU cpu;
    return cpu.has_sse42();
#else
    return false;
#endif
}

uint64_t crc64_fallback(const void* data, int32_t bytes, uint64_t hash) {
    uint32_t h1 = static_cast<uint32_t>(hash >> 32);
    uint32_t h2 = static_cast<uint32_t>(hash);
    h1 = HashUtil::zlib_crc_hash(data, bytes, h1);
    h2 = HashUtil::zlib_crc_hash(data, bytes, h2);
    return (static_cast<uint64_t>(h1) << 32) | h2;
}

} // namespace
} // namespace starrocks

namespace std {

template <>
struct hash<starrocks::HashCombineTag> {
    size_t operator()(const starrocks::HashCombineTag& tag) const noexcept {
        return static_cast<size_t>(tag.value) * 0x9e3779b1u;
    }
};

} // namespace std

namespace starrocks {
namespace {

TEST(HashUtilTest, Rotl32Fmix32Xorshift32) {
    EXPECT_EQ(HashUtil::rotl32(0x12345678u, 8), 0x34567812u);
    EXPECT_EQ(HashUtil::fmix32(0u), 0u);
    EXPECT_EQ(HashUtil::fmix32(1u), 0x514e28b7u);
    EXPECT_EQ(HashUtil::fmix32(0x12345678u), 0xe37cd1bcu);
    EXPECT_EQ(HashUtil::xorshift32(1u), 0x00042021u);
    EXPECT_EQ(HashUtil::xorshift32(0x12345678u), 0x87985aa5u);
}

TEST(HashUtilTest, ZlibCrcHash) {
    const std::string_view data = "starrocks";
    const uint32_t seed = 0;
    const uint32_t expected = crc32(seed, reinterpret_cast<const unsigned char*>(data.data()), data.size());
    EXPECT_EQ(HashUtil::zlib_crc_hash(data.data(), static_cast<int32_t>(data.size()), seed), expected);
}

TEST(HashUtilTest, FnvHash) {
    const std::string_view data = "hello";
    EXPECT_EQ(HashUtil::fnv_hash(data.data(), static_cast<int32_t>(data.size()), 0u), 0x1840de38u);
    EXPECT_EQ(HashUtil::fnv_hash(data.data(), static_cast<int32_t>(data.size()), HashUtil::FNV_SEED), 0x4f9f2cabu);
    EXPECT_EQ(HashUtil::fnv_hash(data.data(), 0, HashUtil::FNV_SEED), HashUtil::FNV_SEED);
}

TEST(HashUtilTest, MurmurHash3_32) {
    const std::string_view data = "hello";
    EXPECT_EQ(HashUtil::murmur_hash3_32(data.data(), static_cast<int32_t>(data.size()), 0u), 0x248bfa47u);
    EXPECT_EQ(HashUtil::murmur_hash3_32(data.data(), static_cast<int32_t>(data.size()), 123u), 0x5dc2bdfeu);
}

TEST(HashUtilTest, MurmurHash64A) {
    const std::string_view data = "hello";
    EXPECT_EQ(HashUtil::murmur_hash64A(data.data(), static_cast<int32_t>(data.size()), 0u), 0x1e68d17c457bf117ULL);
    EXPECT_EQ(HashUtil::murmur_hash64A(data.data(), static_cast<int32_t>(data.size()), 123u), 0x240cb1d62529fb86ULL);
}

TEST(HashUtilTest, XxHashDeterminism) {
    const std::string_view data = "starrocks";
    const uint64_t seed = 0x1234567890abcdefULL;
    const uint64_t h64 = HashUtil::xx_hash64(data.data(), static_cast<int32_t>(data.size()), seed);
    const uint64_t h64_repeat = HashUtil::xx_hash64(data.data(), static_cast<int32_t>(data.size()), seed);
    const uint64_t h3 = HashUtil::xx_hash3_64(data.data(), static_cast<int32_t>(data.size()), seed);
    const uint64_t h3_repeat = HashUtil::xx_hash3_64(data.data(), static_cast<int32_t>(data.size()), seed);
    EXPECT_EQ(h64, h64_repeat);
    EXPECT_EQ(h3, h3_repeat);
}

TEST(HashUtilTest, Hash64Fallback) {
    const std::string_view data = "fallback";
    const uint64_t seed = 0x1122334455667788ULL;
    uint64_t expected = 0;
    murmur_hash3_x64_64(data.data(), static_cast<int32_t>(data.size()), seed, &expected);
    EXPECT_EQ(HashUtil::hash64_fallback(data.data(), static_cast<int32_t>(data.size()), seed), expected);
}

TEST(HashUtilTest, Hash32Selection) {
    const std::string_view data = "starrocks";
    const uint32_t seed = 12345;

    const uint32_t hash_value = HashUtil::hash(data.data(), static_cast<int32_t>(data.size()), seed);

    if (sse42_available()) {
        EXPECT_EQ(hash_value, HashUtil::crc_hash(data.data(), static_cast<int32_t>(data.size()), seed));
    } else {
        EXPECT_EQ(hash_value, HashUtil::fnv_hash(data.data(), static_cast<int32_t>(data.size()), seed));
    }
}

TEST(HashUtilTest, Hash64Selection) {
    const std::string_view data = "starrocks";
    const uint64_t seed = 0x1234567890abcdefULL;

    const uint64_t hash_value = HashUtil::hash64(data.data(), static_cast<int32_t>(data.size()), seed);

    if (sse42_available()) {
        EXPECT_EQ(hash_value, HashUtil::crc_hash64(data.data(), static_cast<int32_t>(data.size()), seed));
    } else {
        EXPECT_EQ(hash_value, HashUtil::hash64_fallback(data.data(), static_cast<int32_t>(data.size()), seed));
    }
}

TEST(HashUtilTest, CrcHashSelection) {
    const std::string_view data = "crc";
    const uint32_t seed32 = 0x13579bdu;
    const uint64_t seed64 = 0x12345678abcdef90ULL;

    const uint32_t crc32_value = HashUtil::crc_hash(data.data(), static_cast<int32_t>(data.size()), seed32);
    const uint64_t crc64_value = HashUtil::crc_hash64(data.data(), static_cast<int32_t>(data.size()), seed64);

    if (sse42_available()) {
        EXPECT_EQ(crc32_value, HashUtil::hash(data.data(), static_cast<int32_t>(data.size()), seed32));
        EXPECT_EQ(crc64_value, HashUtil::hash64(data.data(), static_cast<int32_t>(data.size()), seed64));
    } else {
        EXPECT_EQ(crc32_value, HashUtil::zlib_crc_hash(data.data(), static_cast<int32_t>(data.size()), seed32));
        EXPECT_EQ(crc64_value, crc64_fallback(data.data(), static_cast<int32_t>(data.size()), seed64));
    }
}

TEST(HashUtilTest, HashCombine) {
    std::size_t seed = 0x12345678u;
    const HashCombineTag tag{7};
    const std::size_t expected = seed ^ (std::hash<HashCombineTag>{}(tag) + 0x9e3779b9u + (seed << 6) + (seed >> 2));
    HashUtil::hash_combine(seed, tag);
    EXPECT_EQ(seed, expected);
}

TEST(HashUtilTest, UnalignedLoad) {
    alignas(1) const unsigned char buffer[] = {0x00, 0x12, 0x34, 0x56, 0x78, 0x9a};
    uint32_t expected = 0;
    memcpy(&expected, buffer + 1, sizeof(expected));
    EXPECT_EQ(HashUtil::unaligned_load<uint32_t>(buffer + 1), expected);
}

} // namespace
} // namespace starrocks
