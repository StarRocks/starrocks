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

#include "base/simd/gather.h"

namespace starrocks {

bool SIMDGather::should_prefetch_indexes(const uint32_t* indexes, size_t element_size) {
    static_assert(kPrefetchMinRows >= kPrefetchLocalitySampleRows);

    // Stage 0: Immediate rejection for uniform/identical keys (cardinality 1).
    // Prevents serial store-to-load forwarding stalls on repeated index updates.
    if (indexes[0] == indexes[1] && indexes[0] == indexes[2] && indexes[0] == indexes[3]) {
        return false;
    }

    // Stage 1: Register-only micro-window early rejection.
    // Uses a single 64-bit register (no stack memory read-modify-writes). For streams touching
    // <= 16 keys (or sequential cache lines), at most 12 unique lines are seen in 32 draws,
    // allowing exit in ~10 ns without building the full 128-row stack sketch.
    uint64_t seen_early = 0;
    for (size_t k = 0; k < kPrefetchEarlySampleRows; ++k) {
        const uint32_t cl = static_cast<uint32_t>((static_cast<size_t>(indexes[k]) * element_size) >> 6);
        const uint32_t h = (cl * 0x9E3779B1u) >> 26;
        seen_early |= (1ULL << h);
    }
    if (__builtin_popcountll(seen_early) <= kPrefetchEarlyMaxUniqueBits) {
        return false;
    }

    // Stage 2: 256-bit stack sketch up to midpoint (64 rows).
    // Streams with <= 64 distinct keys set at most 46 bits across 64 rows, while truly
    // high-cardinality streams (millions of keys) set >= 51 bits.
    uint64_t seen[4] = {0, 0, 0, 0};
    for (size_t k = 0; k < kPrefetchMidSampleRows; ++k) {
        const uint32_t cl = static_cast<uint32_t>((static_cast<size_t>(indexes[k]) * element_size) >> 6);
        const uint32_t h = (cl * 0x9E3779B1u) >> 24;
        seen[h >> 6] |= (1ULL << (h & 63));
    }
    int unique_bits = __builtin_popcountll(seen[0]) + __builtin_popcountll(seen[1]) + __builtin_popcountll(seen[2]) +
                      __builtin_popcountll(seen[3]);
    if (unique_bits <= kPrefetchMidMaxUniqueBits) {
        return false;
    }

    // Stage 3: Complete 128-row confirmation for candidate high-cardinality streams.
    for (size_t k = kPrefetchMidSampleRows; k < kPrefetchLocalitySampleRows; ++k) {
        const uint32_t cl = static_cast<uint32_t>((static_cast<size_t>(indexes[k]) * element_size) >> 6);
        const uint32_t h = (cl * 0x9E3779B1u) >> 24;
        seen[h >> 6] |= (1ULL << (h & 63));
    }
    unique_bits = __builtin_popcountll(seen[0]) + __builtin_popcountll(seen[1]) + __builtin_popcountll(seen[2]) +
                  __builtin_popcountll(seen[3]);
    return unique_bits >= kPrefetchMinUniqueSampleBits;
}

void SIMDGather::gather(uint32_t* dest, const uint32_t* src, const uint32_t* indexes, size_t num_rows,
                        SourcePoolSize pool_size) {
    size_t i = 0;
#if defined(__x86_64__) || defined(_M_X64)
    const bool is_large_pool = (pool_size.elements >= kPrefetchPoolBytes / sizeof(uint32_t));

    if (num_rows >= kPrefetchMinRows && is_large_pool && should_prefetch_indexes(indexes, sizeof(uint32_t))) {
        for (size_t p = 0; p < kPrefetchLookahead; ++p) {
            STARROCKS_PREFETCH(src + indexes[p]);
        }
        for (; i + kPrefetchLookahead + 8 <= num_rows; i += 8) {
            STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 0]);
            STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 1]);
            STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 2]);
            STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 3]);
            STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 4]);
            STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 5]);
            STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 6]);
            STARROCKS_PREFETCH(src + indexes[i + kPrefetchLookahead + 7]);

            gather_8_unrolled(dest, src, indexes, i);
        }
    }
#else
    (void)pool_size;
#endif

    gather_buffered(dest, src, indexes, num_rows, i);
}

void SIMDGather::gather(uint32_t* dest, const uint32_t* src, const uint32_t* indexes, const uint8_t* is_filtered,
                        size_t num_rows, SourcePoolSize pool_size) {
    (void)pool_size;
    size_t i = 0;
    gather_filtered_buffered(dest, src, indexes, is_filtered, num_rows, i);
}

void SIMDGather::gather(int32_t* dest, const int32_t* src, const uint32_t* indexes, size_t num_rows,
                        SourcePoolSize pool_size) {
    gather(reinterpret_cast<uint32_t*>(dest), reinterpret_cast<const uint32_t*>(src), indexes, num_rows, pool_size);
}

void SIMDGather::gather(int32_t* dest, const int32_t* src, const uint32_t* indexes, const uint8_t* is_filtered,
                        size_t num_rows, SourcePoolSize pool_size) {
    gather(reinterpret_cast<uint32_t*>(dest), reinterpret_cast<const uint32_t*>(src), indexes, is_filtered, num_rows,
           pool_size);
}

} // namespace starrocks
