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

#include "formats/parquet/parquet_block_split_bloom_filter.h"

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "common/util/debug_util.h"

namespace starrocks {

#if defined(__ARM_NEON) && defined(__aarch64__)
namespace {
static inline void compute_neon_masks(uint32_t key, uint32x4_t* mask_lo, uint32x4_t* mask_hi) {
    static constexpr uint32_t kSaltLo[4] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU};
    static constexpr uint32_t kSaltHi[4] = {0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};
    const uint32x4_t ones = vdupq_n_u32(1);
    int32x4_t shift_lo = vreinterpretq_s32_u32(vshrq_n_u32(vmulq_n_u32(vld1q_u32(kSaltLo), key), 27));
    int32x4_t shift_hi = vreinterpretq_s32_u32(vshrq_n_u32(vmulq_n_u32(vld1q_u32(kSaltHi), key), 27));
    *mask_lo = vshlq_u32(ones, shift_lo);
    *mask_hi = vshlq_u32(ones, shift_hi);
}
} // namespace
#endif

void ParquetBlockSplitBloomFilter::add_hash(uint64_t hash) {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    DCHECK(_num_bytes >= BYTES_PER_BLOCK);
    uint32_t num_blocks = _num_bytes / BYTES_PER_BLOCK;
    uint32_t block_index = (uint32_t)(((hash >> 32) * num_blocks) >> 32);
    auto key = (uint32_t)hash;

#if defined(__ARM_NEON) && defined(__aarch64__)
    auto* block_bytes = reinterpret_cast<uint8_t*>(_data + BYTES_PER_BLOCK * block_index);
    uint32x4_t mask_lo, mask_hi;
    compute_neon_masks(key, &mask_lo, &mask_hi);
    uint32x4_t block_lo = vreinterpretq_u32_u8(vld1q_u8(block_bytes));
    uint32x4_t block_hi = vreinterpretq_u32_u8(vld1q_u8(block_bytes + 16));
    vst1q_u8(block_bytes, vreinterpretq_u8_u32(vorrq_u32(block_lo, mask_lo)));
    vst1q_u8(block_bytes + 16, vreinterpretq_u8_u32(vorrq_u32(block_hi, mask_hi)));
#else
    // Calculate masks for bucket.
    uint32_t masks[8];
    _set_masks(key, masks);
    auto* block_offset = (uint32_t*)(_data + BYTES_PER_BLOCK * block_index);

#ifdef __AVX2__
    // OR the 8 mask lanes into the 8 block lanes in one 256-bit op instead of the loop.
    __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_offset));
    __m256i v_masks = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(masks));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(block_offset), _mm256_or_si256(v_block, v_masks));
#else
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        *(block_offset + i) |= masks[i];
    }
#endif
#endif
}

bool ParquetBlockSplitBloomFilter::test_hash(uint64_t hash) const {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    uint32_t num_blocks = _num_bytes / BYTES_PER_BLOCK;
    uint32_t block_index = (uint32_t)(((hash >> 32) * num_blocks) >> 32);
    auto key = (uint32_t)hash;

#if defined(__ARM_NEON) && defined(__aarch64__)
    const auto* block_bytes = reinterpret_cast<const uint8_t*>(_data + BYTES_PER_BLOCK * block_index);
    uint32x4_t mask_lo, mask_hi;
    compute_neon_masks(key, &mask_lo, &mask_hi);
    uint32x4_t block_lo = vreinterpretq_u32_u8(vld1q_u8(block_bytes));
    uint32x4_t block_hi = vreinterpretq_u32_u8(vld1q_u8(block_bytes + 16));
    uint32x4_t diff = vorrq_u32(vbicq_u32(mask_lo, block_lo), vbicq_u32(mask_hi, block_hi));
    return vmaxvq_u32(diff) == 0;
#else
    // Calculate masks for bucket.
    uint32_t masks[BITS_SET_PER_BLOCK];
    _set_masks(key, masks);
    auto* block_offset = (uint32_t*)(_data + BYTES_PER_BLOCK * block_index);

#ifdef __AVX2__
    // Hash is present iff every mask lane is also set in the block lane, i.e.
    // (~v_block) & v_masks == 0. PTEST sets CF on exactly that condition, so
    // _mm256_testc_si256 returns 1 when the hash may be present.
    __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_offset));
    __m256i v_masks = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(masks));
    return _mm256_testc_si256(v_block, v_masks);
#else
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        if ((*(block_offset + i) & masks[i]) == 0) {
            return false;
        }
    }
    return true;
#endif
#endif
}

} // namespace starrocks
