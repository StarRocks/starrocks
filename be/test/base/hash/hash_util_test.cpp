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

#include <array>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "base/hash/hash.h"
#include "base/hash/hash_std.hpp"

#if defined(__SSE4_2__)
#include <nmmintrin.h>
#elif defined(__aarch64__)
#include <arm_acle.h>
#endif

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
    const uint32_t seed32 = 0x12345678u;
    const uint64_t seed = 0x1234567890abcdefULL;
    const uint32_t h32 = HashUtil::xx_hash32(data.data(), static_cast<int32_t>(data.size()), seed32);
    const uint32_t h32_repeat = HashUtil::xx_hash32(data.data(), static_cast<int32_t>(data.size()), seed32);
    const uint64_t h64 = HashUtil::xx_hash64(data.data(), static_cast<int32_t>(data.size()), seed);
    const uint64_t h64_repeat = HashUtil::xx_hash64(data.data(), static_cast<int32_t>(data.size()), seed);
    const uint64_t h3 = HashUtil::xx_hash3_64(data.data(), static_cast<int32_t>(data.size()), seed);
    const uint64_t h3_repeat = HashUtil::xx_hash3_64(data.data(), static_cast<int32_t>(data.size()), seed);
    EXPECT_EQ(h32, h32_repeat);
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

// DataCache keys embed murmur_hash3_x64_64(file_name): they are persisted in the local disk cache
// and are sent to other BEs in PFetchDataCacheRequest.cache_key. If this test fails, the change
// under review silently orphans every cached block on upgrade and breaks peer-cache hits between
// mixed-version BEs -- fix the change, do not update the constants.
TEST(HashUtilTest, MurmurHash3X64_64GoldenVectors) {
    const std::string long_path(137, 'x');
    const struct {
        std::string_view data;
        uint64_t seed;
        uint64_t expected;
    } cases[] = {
            {"a", 0UL, 0x1d3547705c43c07eUL},
            {"abc", 0UL, 0x37cf0eb5f11c0ee3UL},
            {"hdfs://ns/warehouse/db/tbl/part-00000.parquet", 0UL, 0xd4436bccbc0623ceUL},
            {"s3://bucket/db/tbl/data/00000-0-1a2b3c.parquet", 0UL, 0xeb93e55c35c63745UL},
            {long_path, 0UL, 0x0b58ebb2a26c715dUL},
    };
    for (const auto& c : cases) {
        uint64_t hash = 0;
        murmur_hash3_x64_64(c.data.data(), static_cast<int>(c.data.size()), c.seed, &hash);
        EXPECT_EQ(hash, c.expected);
    }
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

TEST(HashUtilTest, StdHashSpecializations) {
    uint24_t u24 = 123456;
    EXPECT_EQ(std::hash<uint24_t>{}(u24), HashUtil::hash(&u24, sizeof(u24), 0));

    int96_t i96{};
    i96.lo = 0x1122334455667788ULL;
    i96.hi = 0x99aabbccU;
    EXPECT_EQ(std::hash<int96_t>{}(i96), HashUtil::hash(&i96, sizeof(i96), 0));

    decimal12_t d12{123, 456};
    EXPECT_EQ(std::hash<decimal12_t>{}(d12), HashUtil::hash(&d12, sizeof(d12), 0));

    TUniqueId uid;
    uid.__set_hi(7);
    uid.__set_lo(11);
    using UniqueIdPair = std::pair<TUniqueId, int64_t>;
    UniqueIdPair key{uid, 13};
    size_t expected_pair_hash = 0;
    expected_pair_hash = HashUtil::hash(&key.first.lo, sizeof(key.first.lo), expected_pair_hash);
    expected_pair_hash = HashUtil::hash(&key.first.hi, sizeof(key.first.hi), expected_pair_hash);
    expected_pair_hash = HashUtil::hash(&key.second, sizeof(key.second), expected_pair_hash);
    EXPECT_EQ(std::hash<UniqueIdPair>{}(key), expected_pair_hash);

    std::vector<int> values{1, 2, 3, 4};
    size_t expected_vector_hash = 0;
    for (int v : values) {
        boost::hash_combine(expected_vector_hash, v);
    }
    EXPECT_EQ(std::hash<std::vector<int>>{}(values), expected_vector_hash);
}

TEST(HashUtilTest, UnalignedLoad) {
    alignas(1) const unsigned char buffer[] = {0x00, 0x12, 0x34, 0x56, 0x78, 0x9a};
    uint32_t expected = 0;
    memcpy(&expected, buffer + 1, sizeof(expected));
    EXPECT_EQ(HashUtil::unaligned_load<uint32_t>(buffer + 1), expected);
}

TEST(HashUtilTest, CrcHashUnalignedInput) {
    std::array<uint8_t, 32> buffer{};
    for (size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<uint8_t>(i);
    }
    const uint8_t* unaligned = buffer.data() + 1;
    const int32_t len = static_cast<int32_t>(buffer.size() - 1);
    std::string aligned(reinterpret_cast<const char*>(unaligned), len);

    const uint32_t seed32 = 0x13579bdu;
    const uint64_t seed64 = 0x12345678abcdef90ULL;

    EXPECT_EQ(HashUtil::crc_hash(unaligned, len, seed32), HashUtil::crc_hash(aligned.data(), len, seed32));
    EXPECT_EQ(HashUtil::crc_hash64(unaligned, len, seed64), HashUtil::crc_hash64(aligned.data(), len, seed64));
}

#if (defined(__x86_64__) && defined(__SSE4_2__)) || (defined(__aarch64__) && defined(__ARM_FEATURE_CRC32))
TEST(HashUtilTest, CrcHash64UnmixedDoesNotDoubleHashTail) {
    const uint64_t seed = 0x12345678abcdef90ULL;
    const uint64_t data = 0x1122334455667788ULL;
    uint64_t expected = seed;
#if defined(__x86_64__) && defined(__SSE4_2__)
    expected = _mm_crc32_u64(expected, data);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    expected = __crc32cd(expected, data);
#endif
    const uint64_t actual = crc_hash_64_unmixed(&data, sizeof(data), seed);
    EXPECT_EQ(expected, actual);
}

TEST(HashUtilTest, CrcSoftwareFallbackMatchesHardwareAcrossLengths) {
    std::array<uint8_t, 65> buffer{};
    for (size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<uint8_t>((i * 31 + 17) & 0xff);
    }

    auto hw_raw_crc32 = [](const void* data, int32_t bytes, uint32_t hash) -> uint32_t {
        uint32_t words = bytes / sizeof(uint32_t);
        bytes = bytes % 4;
        auto* p = reinterpret_cast<const uint8_t*>(data);
        while (words--) {
#if defined(__x86_64__) && defined(__SSE4_2__)
            hash = _mm_crc32_u32(hash, HashUtil::unaligned_load<uint32_t>(p));
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
            hash = __crc32cw(hash, HashUtil::unaligned_load<uint32_t>(p));
#endif
            p += sizeof(uint32_t);
        }
        while (bytes--) {
#if defined(__x86_64__) && defined(__SSE4_2__)
            hash = _mm_crc32_u8(hash, *p);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
            hash = __crc32cb(hash, *p);
#endif
            ++p;
        }
        return hash;
    };

    auto sw_crc_hash_32 = [](const void* data, int32_t bytes, uint32_t hash) -> uint32_t {
        hash = ~starrocks::crc32c::Extend(~hash, reinterpret_cast<const char*>(data), bytes);
        hash = phmap_mix<4>()(hash);
        return hash;
    };

    auto sw_crc_hash_64_unmixed = [&](const void* data, int32_t length, uint64_t hash) -> uint64_t {
        if (UNLIKELY(length < 8)) {
            return sw_crc_hash_32(data, length, static_cast<uint32_t>(hash));
        }

        uint64_t words = length / sizeof(uint64_t);
        uint64_t remainder = length % sizeof(uint64_t);
        auto* p = reinterpret_cast<const uint8_t*>(data);
        auto* end = reinterpret_cast<const uint8_t*>(data) + length;
        while (words--) {
            hash = ~starrocks::crc32c::Extend(~hash, reinterpret_cast<const char*>(p), sizeof(uint64_t));
            p += sizeof(uint64_t);
        }
        if (remainder != 0) {
            p = end - 8;
            hash = ~starrocks::crc32c::Extend(~hash, reinterpret_cast<const char*>(p), sizeof(uint64_t));
        }

        return hash;
    };

    const std::vector<uint32_t> seeds32 = {0u, 1u, 0x12345678u, 0xdeadbeefu, 0xffffffffu, HashUtil::FNV_SEED};
    for (uint32_t seed : seeds32) {
        for (int32_t len = 0; len <= 64; ++len) {
            uint32_t hw = hw_raw_crc32(buffer.data(), len, seed);
            uint32_t sw = ~starrocks::crc32c::Extend(~seed, reinterpret_cast<const char*>(buffer.data()), len);
            EXPECT_EQ(hw, sw) << "raw CRC32 mismatch at len=" << len << ", seed=" << seed;
            EXPECT_EQ(crc_hash_32(buffer.data(), len, seed), sw_crc_hash_32(buffer.data(), len, seed))
                    << "crc_hash_32 mismatch at len=" << len << ", seed=" << seed;
        }
    }

    const std::vector<uint64_t> seeds64 = {0ULL,
                                           1ULL,
                                           0x12345678abcdef90ULL,
                                           0xdeadbeefcafebabeULL,
                                           0xffffffffffffffffULL,
                                           CRC_HASH_SEEDS::CRC_HASH_SEED1,
                                           CRC_HASH_SEEDS::CRC_HASH_SEED2};
    for (uint64_t seed : seeds64) {
        for (int32_t len = 0; len <= 64; ++len) {
            uint64_t hw = crc_hash_64_unmixed(buffer.data(), len, seed);
            uint64_t sw = sw_crc_hash_64_unmixed(buffer.data(), len, seed);
            EXPECT_EQ(hw, sw) << "crc_hash_64_unmixed mismatch at len=" << len << ", seed=" << seed;
            EXPECT_EQ(crc_hash_64(buffer.data(), len, seed), phmap_mix<8>()(sw))
                    << "crc_hash_64 mismatch at len=" << len << ", seed=" << seed;

            uint64_t hw_unaligned = crc_hash_64_unmixed(buffer.data() + 1, len, seed);
            uint64_t sw_unaligned = sw_crc_hash_64_unmixed(buffer.data() + 1, len, seed);
            EXPECT_EQ(hw_unaligned, sw_unaligned)
                    << "unaligned crc_hash_64_unmixed mismatch at len=" << len << ", seed=" << seed;
        }
    }
}
#endif

#if !(defined(__x86_64__) && !defined(__SSE4_2__))
TEST(HashUtilTest, CrcHash64UnmixedGoldenVectors) {
    const std::string_view s1 = "hello";                                      // 5 bytes (< 8)
    const std::string_view s2 = "12345678";                                   // 8 bytes (= 8)
    const std::string_view s3 = "hello world, starrocks crc64 unmixed test!"; // 42 bytes (> 8, remainder 2)
    const uint64_t seed = 0x12345678abcdef90ULL;

    EXPECT_EQ(crc_hash_64_unmixed(s1.data(), static_cast<int32_t>(s1.size()), seed), 0x7583172cULL);
    EXPECT_EQ(crc_hash_64_unmixed(s2.data(), static_cast<int32_t>(s2.size()), seed), 0x6a03190bULL);
    EXPECT_EQ(crc_hash_64_unmixed(s3.data(), static_cast<int32_t>(s3.size()), seed), 0xf369fa27ULL);
}
#endif

} // namespace
} // namespace starrocks
