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

#include "util/hash_util.hpp"

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#include "util/cpu_info.h"
#include "util/murmur_hash3.h"
#include "util/xxh3.h"

namespace starrocks {

// Static member definitions
HashUtil::Hash32Func HashUtil::g_hash32_func = nullptr;
HashUtil::Hash64Func HashUtil::g_hash64_func = nullptr;
#ifdef __SSE4_2__
HashUtil::Crc32Func HashUtil::g_crc32_func = nullptr;
HashUtil::Crc64Func HashUtil::g_crc64_func = nullptr;
#endif

// Initialize function pointers at program startup based on CPU capabilities.
// This avoids runtime CPU checks in the hot path (hash(), hash64(), crc_hash(), crc_hash64() functions).
// The constructor attribute ensures this runs before main(), but after CpuInfo::init()
// is called in Daemon::init(). If CpuInfo hasn't been initialized yet, we initialize it here.
__attribute__((constructor)) void SelectHashFunctions() {
    // Ensure CpuInfo is initialized
    CpuInfo::init();

#ifdef __SSE4_2__
    if (CpuInfo::is_supported(CpuInfo::SSE4_2)) {
        HashUtil::g_hash32_func = HashUtil::crc_hash_sse42;
        HashUtil::g_hash64_func = HashUtil::crc_hash64_sse42;
        HashUtil::g_crc32_func = HashUtil::crc_hash_sse42;
        HashUtil::g_crc64_func = HashUtil::crc_hash64_sse42;
    } else {
        HashUtil::g_hash32_func = HashUtil::fnv_hash;
        HashUtil::g_hash64_func = HashUtil::hash64_fallback;
        HashUtil::g_crc32_func = HashUtil::zlib_crc_hash;
        HashUtil::g_crc64_func = [](const void* data, int32_t bytes, uint64_t hash) -> uint64_t {
            // For 64-bit fallback, use zlib_crc_hash on both halves
            uint32_t h1 = hash >> 32;
            uint32_t h2 = (hash << 32) >> 32;
            h1 = HashUtil::zlib_crc_hash(data, bytes, h1);
            h2 = HashUtil::zlib_crc_hash(data, bytes, h2);
            return ((uint64_t)h1 << 32) | h2;
        };
    }
#else
    HashUtil::g_hash32_func = HashUtil::fnv_hash;
    HashUtil::g_hash64_func = HashUtil::hash64_fallback;
#endif
}

uint64_t HashUtil::xx_hash3_64(const void* key, int32_t len, uint64_t seed) {
    return XXH3_64bits_withSeed(key, len, seed);
}

uint64_t HashUtil::xx_hash64(const void* key, int32_t len, uint64_t seed) {
    return XXH64(key, len, seed);
}

uint64_t HashUtil::hash64_fallback(const void* data, int32_t bytes, uint64_t seed) {
    uint64_t hash = 0;
    murmur_hash3_x64_64(data, bytes, seed, &hash);
    return hash;
}

#ifdef __SSE4_2__
uint32_t HashUtil::crc_hash_sse42(const void* data, int32_t bytes, uint32_t hash) {
    uint32_t words = bytes / sizeof(uint32_t);
    bytes = bytes % sizeof(uint32_t);

    const uint32_t* p = reinterpret_cast<const uint32_t*>(data);

    while (words--) {
        hash = _mm_crc32_u32(hash, *p);
        ++p;
    }

    const uint8_t* s = reinterpret_cast<const uint8_t*>(p);

    while (bytes--) {
        hash = _mm_crc32_u8(hash, *s);
        ++s;
    }

    // The lower half of the CRC hash has poor uniformity, so swap the halves
    // for anyone who only uses the first several bits of the hash.
    hash = (hash << 16) | (hash >> 16);
    return hash;
}

uint64_t HashUtil::crc_hash64_sse42(const void* data, int32_t bytes, uint64_t hash) {
    uint32_t words = bytes / sizeof(uint32_t);
    bytes = bytes % sizeof(uint32_t);

    uint32_t h1 = hash >> 32;
    uint32_t h2 = (hash << 32) >> 32;

    const uint32_t* p = reinterpret_cast<const uint32_t*>(data);
    while (words--) {
        (words & 1) ? (h1 = _mm_crc32_u32(h1, *p)) : (h2 = _mm_crc32_u32(h2, *p));
        ++p;
    }

    const uint8_t* s = reinterpret_cast<const uint8_t*>(p);
    while (bytes--) {
        (bytes & 1) ? (h1 = _mm_crc32_u8(h1, *s)) : (h2 = _mm_crc32_u8(h2, *s));
        ++s;
    }

    h1 = (h1 << 16) | (h1 >> 16);
    h2 = (h2 << 16) | (h2 >> 16);
    ((uint32_t*)(&hash))[0] = h1;
    ((uint32_t*)(&hash))[1] = h2;
    return hash;
}

uint32_t HashUtil::crc_hash(const void* data, int32_t bytes, uint32_t hash) {
    return g_crc32_func(data, bytes, hash);
}

uint64_t HashUtil::crc_hash64(const void* data, int32_t bytes, uint64_t hash) {
    return g_crc64_func(data, bytes, hash);
}
#endif

} // namespace starrocks