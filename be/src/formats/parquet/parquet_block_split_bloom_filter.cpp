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
#endif

#include "common/util/debug_util.h"

namespace starrocks {

#ifdef __AVX2__
namespace {
inline __m256i compute_masks_avx2(uint32_t key) {
    const __m256i v_salt = _mm256_setr_epi32(0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b,
                                             0x9efc4947, 0x5c6bfb31);
    const __m256i v_ones = _mm256_set1_epi32(1);
    __m256i v_key = _mm256_set1_epi32(key);
    __m256i v_hash = _mm256_mullo_epi32(v_key, v_salt);
    __m256i v_shift = _mm256_srli_epi32(v_hash, 27);
    return _mm256_sllv_epi32(v_ones, v_shift);
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
    auto* block_offset = (uint32_t*)(_data + BYTES_PER_BLOCK * block_index);

#ifdef __AVX2__
    // Compute mask lanes directly in AVX2 registers without stack spills.
    __m256i v_masks = compute_masks_avx2(key);
    __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_offset));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(block_offset), _mm256_or_si256(v_block, v_masks));
#else
    uint32_t masks[BITS_SET_PER_BLOCK];
    _set_masks(key, masks);
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        *(block_offset + i) |= masks[i];
    }
#endif
}

bool ParquetBlockSplitBloomFilter::test_hash(uint64_t hash) const {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    uint32_t num_blocks = _num_bytes / BYTES_PER_BLOCK;
    uint32_t block_index = (uint32_t)(((hash >> 32) * num_blocks) >> 32);
    auto key = (uint32_t)hash;
    auto* block_offset = (uint32_t*)(_data + BYTES_PER_BLOCK * block_index);

#ifdef __AVX2__
    // Hash is present iff every mask lane is also set in the block lane, i.e.
    // (~v_block) & v_masks == 0. PTEST sets CF on exactly that condition, so
    // _mm256_testc_si256 returns 1 when the hash may be present.
    __m256i v_masks = compute_masks_avx2(key);
    __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_offset));
    return _mm256_testc_si256(v_block, v_masks);
#else
    uint32_t masks[BITS_SET_PER_BLOCK];
    _set_masks(key, masks);
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        if ((*(block_offset + i) & masks[i]) == 0) {
            return false;
        }
    }
    return true;
#endif
}

} // namespace starrocks
