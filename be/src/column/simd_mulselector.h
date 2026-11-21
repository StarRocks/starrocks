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

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "base/simd/simd.h"
#include "base/simd/simd_utils.h"
#include "column/runtime_type_traits.h"
#include "glog/logging.h"
#include "types/logical_type.h"
#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

namespace starrocks {

template <LogicalType TYPE>
class SIMD_muti_selector {
public:
    using Container = typename RunTimeColumnType<TYPE>::Container;
    using CppType = RunTimeCppType<TYPE>;
    using SelectVec = uint8_t*;

    // a normal implements
    static void multi_select_if(SelectVec __restrict select_vec[], int select_vec_size, Container& dst,
                                const Container* __restrict select_list[], int select_list_size,
                                const std::vector<bool>& then_column_is_const, const int row_sz) {
        DCHECK_GT(select_list_size, 0);
        DCHECK_EQ(select_vec_size + 1, select_list_size);

        int processed_rows = 0;
        [[maybe_unused]] SelectVec __restrict handle_select_vec[select_vec_size];
        // copy select vector pointer
        for (int i = 0; i < select_vec_size; ++i) {
            handle_select_vec[i] = select_vec[i];
        }

        [[maybe_unused]] const CppType* __restrict handle_select_data[select_list_size];
        for (int i = 0; i < select_list_size; ++i) {
            handle_select_data[i] = select_list[i]->data();
        }

#ifdef __AVX2__
        // SIMD multi select if Algorithm
        // INPUT: select_vecs = [vec1, vec2] select_datas = [vdata1, vdata2, vdata3]
        // # padding len(selecter) == len(datas)
        // 1. add [1, 1, 1, 1] to select_vecs
        // # mark if it has been selected
        // 2. set selected = [0, 0, 0, 0];
        // # final select result
        // 3. set selected_dst = [undefined];
        // 4. for each select_vec in select_vecs, each select_data in select_datas:
        //          set not_selected = ~selected
        //          will_select = not_selected_vec & select_vec;
        //          selected_dst = select_if(will_select, selected_dst, select_data)
        // 5. store selected_dst
        constexpr int data_size = sizeof(RunTimeCppType<TYPE>);
        if constexpr (data_size == 1) {
            __m256i loaded_masks[select_vec_size];
            __m256i loaded_datas[select_list_size];
            int loaded_masks_value[select_vec_size];

            const __m256i all_zero_vec = _mm256_setzero_si256();

            while (processed_rows + 32 < row_sz) {
                __m256i selected_vec = all_zero_vec;
                // load select vector
                for (int i = 0; i < select_vec_size; ++i) {
                    loaded_masks[i] = _mm256_loadu_si256(reinterpret_cast<__m256i*>(handle_select_vec[i]));
                    loaded_masks[i] = _mm256_cmpgt_epi8(loaded_masks[i], _mm256_setzero_si256());
                    loaded_masks_value[i] = _mm256_movemask_epi8(loaded_masks[i]);
                }

                // load select data
                for (int i = 0; i < select_list_size; ++i) {
                    // date columns except the last column, if mask is zero, no need to load it
                    if (i < select_list_size - 1 && loaded_masks_value[i] == 0) {
                        continue;
                    }
                    if (then_column_is_const[i]) {
                        loaded_datas[i] = SIMDUtils::set_data(handle_select_data[i][0]);
                    } else {
                        loaded_datas[i] = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(handle_select_data[i]));
                    }

                    // if all 1, no need to load left data columns because they won't be selected
                    if (i < select_list_size - 1 && loaded_masks_value[i] == 0xffffffff) {
                        break;
                    }
                }

                __m256i selected_dst = loaded_datas[select_list_size - 1];

                for (int i = 0; i < select_list_size - 1; ++i) {
                    // all zero, skip this column
                    if (loaded_masks_value[i] == 0) {
                        continue;
                    }

                    // get will select vector in this loop
                    __m256i not_selected_vec = ~selected_vec;
                    __m256i will_select = not_selected_vec & loaded_masks[i];
                    will_select = _mm256_cmpeq_epi8(will_select, _mm256_setzero_si256());

                    will_select = ~will_select;
                    // select if
                    selected_dst = _mm256_blendv_epi8(selected_dst, loaded_datas[i], will_select);
                    // update select_vec
                    selected_vec |= will_select;

                    // no need to check other columns
                    if (_mm256_movemask_epi8(selected_vec) == 0xffffffff) {
                        break;
                    }
                }

                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst.data() + processed_rows), selected_dst);

                // update handle_select_vec
                for (int i = 0; i < select_vec_size; ++i) {
                    handle_select_vec[i] += 32;
                }

                for (int i = 0; i < select_list_size; ++i) {
                    if (!then_column_is_const[i]) {
                        handle_select_data[i] += 32;
                    }
                }
                processed_rows += 32;
            }
        } else if constexpr (data_size == 2 || data_size == 4 || data_size == 8) {
            constexpr int data_size = sizeof(RunTimeCppType<TYPE>);
            __m256i loaded_masks[select_vec_size];
            __m256i loaded_datas[select_list_size];
            int loaded_masks_value[select_vec_size];

            const __m256i all_zero_vec = _mm256_setzero_si256();
            while (processed_rows + 32 < row_sz) {
                // load select vector
                for (int i = 0; i < select_vec_size; ++i) {
                    loaded_masks[i] = _mm256_loadu_si256(reinterpret_cast<__m256i*>(handle_select_vec[i]));
                    loaded_masks[i] = _mm256_cmpgt_epi8(loaded_masks[i], _mm256_setzero_si256());
                    loaded_masks_value[i] = _mm256_movemask_epi8(loaded_masks[i]);
                }

                constexpr uint32_t mask_table[] = {0, 0xFFFFFFFF, 0xFFFF, 0, 0xFF, 0, 0, 0,   0x0F,
                                                   0, 0,          0,      0, 0,    0, 0, 0x03};
                constexpr uint8_t each_loop_handle_sz = 32 / sizeof(RunTimeCppType<TYPE>);

                // Process 'data_size' groups, each handling 'each_loop_handle_sz' int16
                // for example, if sizeof == 2,data_size is 2 and each_loop_handle_sz is 16
                for (int index = 0; index < data_size; index++) {
                    // load select data
                    for (int i = 0; i < select_list_size; ++i) {
                        // date columns except the last column, if mask is zero, no need to load it
                        if (i < select_list_size - 1 && loaded_masks_value[i] == 0) {
                            continue;
                        }

                        if (then_column_is_const[i]) {
                            loaded_datas[i] = SIMDUtils::set_data(handle_select_data[i][0]);
                        } else {
                            loaded_datas[i] =
                                    _mm256_loadu_si256(reinterpret_cast<const __m256i*>(handle_select_data[i]));
                        }

                        // if all 1, no need to load left data columns because they won't be selected
                        if (i < select_list_size - 1 && loaded_masks_value[i] == 0xffffffff) {
                            break;
                        }
                    }

                    // selected_vec[i] == 1 means this row is selected already
                    __m256i selected_vec = all_zero_vec;
                    bool all_selected = false;
                    // let the default value be the last data column, which is 'else' column
                    __m256i selected_dst = loaded_datas[select_list_size - 1];

                    for (int i = 0; i < select_list_size - 1; ++i) {
                        // every time, only get the first N bits of the mask
                        uint32_t select_mask = loaded_masks_value[i] & mask_table[data_size];
                        // select_mask all zero or all_selected, skip this column
                        if (select_mask == 0 || all_selected) {
                            loaded_masks_value[i] >>= each_loop_handle_sz;
                            continue;
                        }
                        __m256i expand_mask;
                        if constexpr (data_size == 2) {
                            expand_mask = _mm256_set1_epi16(select_mask);
                            const __m256i data_mask =
                                    _mm256_setr_epi16(0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x100, 0x200,
                                                      0x400, 0x800, 0x1000, 0x2000, 0x4000, 0x8000);
                            expand_mask &= data_mask;
                            expand_mask = _mm256_cmpeq_epi16(expand_mask, _mm256_setzero_si256());
                            expand_mask = ~expand_mask;
                        } else if constexpr (data_size == 4) {
                            expand_mask = _mm256_set1_epi8(select_mask);
                            const __m256i data_mask =
                                    _mm256_setr_epi8(0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
                                                     0x04, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00,
                                                     0x00, 0x20, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x80);
                            expand_mask &= data_mask;
                            expand_mask = _mm256_cmpeq_epi32(expand_mask, _mm256_setzero_si256());
                            expand_mask = ~expand_mask;
                        } else if constexpr (data_size == 8) {
                            expand_mask = _mm256_set1_epi8(select_mask);
                            const __m256i data_mask =
                                    _mm256_setr_epi8(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                                                     0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                                     0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08);
                            expand_mask &= data_mask;
                            expand_mask = _mm256_cmpeq_epi64(expand_mask, _mm256_setzero_si256());
                            expand_mask = ~expand_mask;
                        }

                        // get will select vector in this loop
                        __m256i not_selected_vec = ~selected_vec;
                        __m256i will_select = not_selected_vec & expand_mask;

                        // select if
                        selected_dst = _mm256_blendv_epi8(selected_dst, loaded_datas[i], will_select);
                        // update select_vec
                        selected_vec |= will_select;

                        // right shift mask
                        loaded_masks_value[i] >>= each_loop_handle_sz;

                        // check whether all row is selected, in this case we can do nothing
                        all_selected = _mm256_movemask_epi8(selected_vec) == 0xffffffff;
                    }
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst.data() + processed_rows), selected_dst);
                    processed_rows += each_loop_handle_sz;
                    for (int i = 0; i < select_list_size; ++i) {
                        if (!then_column_is_const[i]) {
                            handle_select_data[i] += each_loop_handle_sz;
                        }
                    }
                }

                // update handle_select_vec
                for (int i = 0; i < select_vec_size; ++i) {
                    handle_select_vec[i] += 32;
                }
            }
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        constexpr int data_size = sizeof(RunTimeCppType<TYPE>);
        if constexpr (data_size == 1) {
            uint8x16_t loaded_masks[select_vec_size];
            uint8x16_t loaded_datas[select_list_size];
            const uint8x16_t zero_vector = vdupq_n_u8(0);

            while (processed_rows + 16 <= row_sz) {
                uint8x16_t selected_vec = zero_vector;
                // load select vector
                for (int i = 0; i < select_vec_size; ++i) {
                    loaded_masks[i] = vld1q_u8(handle_select_vec[i]);
                    // loaded_mask[i] = selector[i] != 0 ? 0xFF : 0x00
                    loaded_masks[i] = vtstq_u8(loaded_masks[i], loaded_masks[i]);
                }

                loaded_datas[select_list_size - 1] = zero_vector;
                // load select data
                for (int i = 0; i < select_list_size; ++i) {
                    // date columns except the last column, if mask is zero, no need to load it
                    if (i < select_list_size - 1 && vmaxvq_u8(loaded_masks[i]) == 0) {
                        continue;
                    }

                    if (then_column_is_const[i]) {
                        loaded_datas[i] = vdupq_n_u8(handle_select_data[i][0]);
                    } else {
                        loaded_datas[i] = vld1q_u8(reinterpret_cast<const uint8_t*>(handle_select_data[i]));
                    }

                    // if all 1, no need to load left data columns because they won't be selected
                    if (i < select_list_size - 1 && vminvq_u8(loaded_masks[i])) {
                        break;
                    }
                }

                uint8x16_t selected_dst = loaded_datas[select_list_size - 1];
                for (int i = 0; i < select_list_size - 1; ++i) {
                    if (vmaxvq_u8(loaded_masks[i]) == 0) {
                        continue;
                    }

                    // get will select vector in this loop
                    uint8x16_t not_selected_vec = ~selected_vec;
                    uint8x16_t will_select = not_selected_vec & loaded_masks[i];
                    // will_select[i] = will_select[i] != 0 ? 0xFF : 0x00
                    will_select = vtstq_u8(will_select, will_select);

                    // select if: will_select[i] ? loaded_datas[i] : selected_dst
                    selected_dst = vbslq_u8(will_select, loaded_datas[i], selected_dst);
                    // update select_vec
                    selected_vec |= will_select;

                    // all 1
                    if (vminvq_u8(selected_vec)) {
                        break;
                    }
                }

                vst1q_u8(reinterpret_cast<uint8_t*>(dst.data() + processed_rows), selected_dst);

                // update handle_select_vec
                for (int i = 0; i < select_vec_size; ++i) {
                    handle_select_vec[i] += 16;
                }

                for (int i = 0; i < select_list_size; ++i) {
                    if (!then_column_is_const[i]) {
                        handle_select_data[i] += 16;
                    }
                }

                processed_rows += 16;
            }

        } else if constexpr (data_size == 2) {
            neon_disjoint16_select<uint16_t, uint16x8_t>(select_vec, select_vec_size, dst, handle_select_data,
                                                         select_list_size, then_column_is_const, row_sz,
                                                         processed_rows);
        } else if constexpr (data_size == 4) {
            neon_disjoint16_select<uint32_t, uint32x4_t>(select_vec, select_vec_size, dst, handle_select_data,
                                                         select_list_size, then_column_is_const, row_sz,
                                                         processed_rows);
        } else if constexpr (data_size == 8) {
            neon_disjoint16_select<uint64_t, uint64x2_t>(select_vec, select_vec_size, dst, handle_select_data,
                                                         select_list_size, then_column_is_const, row_sz,
                                                         processed_rows);
        }
#endif
        for (int i = processed_rows; i < row_sz; ++i) {
            int colIndex = 0;
            int j;
            for (j = 0; j < select_vec_size; ++j) {
                if (select_vec[j][i]) {
                    break;
                }
            }
            colIndex = j;
            if (then_column_is_const[colIndex]) {
                dst[i] = (*select_list[colIndex])[0];
            } else {
                dst[i] = (*select_list[colIndex])[i];
            }
        }
    }

#if defined(__ARM_NEON) && defined(__aarch64__)
private:
    // Disjoint16 multi-byte NEON selector for data_size in {2, 4, 8}:
    // - Hoists constant-branch vectors (const_vecs) outside the 16-row loop
    // - Resolves mutually disjoint 16-row winner masks and 64-bit nibble masks once per 16 rows
    // - Fast-copies all 16 rows when single_winner >= 0 or select_nibble_mask == 0 (all-ELSE)
    // - Lazily loads only active branches per 128-bit sub-chunk (skipping loads/blends when a sub-chunk
    //   has 0 or 1 active winner)
    template <typename UIntType, typename VecType>
    static void neon_disjoint16_select(SelectVec __restrict select_vec[], int select_vec_size, Container& dst,
                                       const CppType* __restrict handle_select_data[], int select_list_size,
                                       const std::vector<bool>& then_column_is_const, int row_sz, int& processed_rows) {
        constexpr int kDataSize = sizeof(UIntType);
        static_assert(kDataSize == 2 || kDataSize == 4 || kDataSize == 8);
        constexpr int kSubChunks = kDataSize;
        constexpr int kLanes = 16 / kDataSize;
        constexpr uint32_t kSubBits = 4 * kLanes;
        constexpr uint64_t kSubMask = (kSubBits == 64) ? ~uint64_t(0) : ((uint64_t(1) << kSubBits) - 1ull);
        constexpr int kMaxSimdWhenSize = 8;

        if (select_vec_size > kMaxSimdWhenSize) {
            return;
        }

        auto dup_vec = [](UIntType bits) -> VecType {
            if constexpr (kDataSize == 2) {
                return vdupq_n_u16(bits);
            } else if constexpr (kDataSize == 4) {
                return vdupq_n_u32(bits);
            } else {
                return vdupq_n_u64(bits);
            }
        };

        auto load_vec = [](const UIntType* ptr) -> VecType {
            if constexpr (kDataSize == 2) {
                return vld1q_u16(ptr);
            } else if constexpr (kDataSize == 4) {
                return vld1q_u32(ptr);
            } else {
                return vld1q_u64(ptr);
            }
        };

        auto store_vec = [](UIntType* ptr, VecType v) {
            if constexpr (kDataSize == 2) {
                vst1q_u16(ptr, v);
            } else if constexpr (kDataSize == 4) {
                vst1q_u32(ptr, v);
            } else {
                vst1q_u64(ptr, v);
            }
        };

        auto bsl_vec = [](uint8x16_t mask_bytes, VecType if_true, VecType if_false) -> VecType {
            if constexpr (kDataSize == 2) {
                return vbslq_u16(vreinterpretq_u16_u8(mask_bytes), if_true, if_false);
            } else if constexpr (kDataSize == 4) {
                return vbslq_u32(vreinterpretq_u32_u8(mask_bytes), if_true, if_false);
            } else {
                return vbslq_u64(vreinterpretq_u64_u8(mask_bytes), if_true, if_false);
            }
        };

        uint8x16_t win_masks[kMaxSimdWhenSize];
        uint64_t win_nibbles[kMaxSimdWhenSize];
        VecType const_vecs[kMaxSimdWhenSize + 1];
        if (processed_rows + 16 <= row_sz) {
            for (int i = 0; i < select_list_size; ++i) {
                if (then_column_is_const[i]) {
                    UIntType bits = 0;
                    std::memcpy(&bits, handle_select_data[i], sizeof(bits));
                    const_vecs[i] = dup_vec(bits);
                }
            }
        }

        uint8x16_t tbl_idx[kSubChunks];
        for (int s = 0; s < kSubChunks; ++s) {
            alignas(16) uint8_t idx_bytes[16];
            for (int b = 0; b < 16; ++b) {
                idx_bytes[b] = static_cast<uint8_t>(s * kLanes + (b / kDataSize));
            }
            tbl_idx[s] = vld1q_u8(idx_bytes);
        }

        const uint8x16_t zero_u8 = vdupq_n_u8(0);
        const VecType zero_vec = dup_vec(0);
        const int else_idx = select_list_size - 1;

        auto fill_branch_16 = [&](int branch_idx, UIntType* out_ptr) {
            if (then_column_is_const[branch_idx]) {
                VecType v = const_vecs[branch_idx];
                for (int s = 0; s < kSubChunks; ++s) {
                    store_vec(out_ptr + s * kLanes, v);
                }
            } else {
                std::memcpy(out_ptr, handle_select_data[branch_idx] + processed_rows, 16 * sizeof(UIntType));
            }
        };

        auto load_sub = [&](int branch_idx, int sub_idx) -> VecType {
            if (then_column_is_const[branch_idx]) {
                return const_vecs[branch_idx];
            }
            const auto* bp = reinterpret_cast<const UIntType*>(handle_select_data[branch_idx] + processed_rows);
            return load_vec(bp + sub_idx * kLanes);
        };

        while (processed_rows + 16 <= row_sz) {
            auto* out_ptr = reinterpret_cast<UIntType*>(dst.data() + processed_rows);
            uint8x16_t selected_16 = zero_u8;
            uint64_t select_nibble_mask = 0;
            int active_branches = 0;
            int single_winner = -1;

            for (int i = 0; i < select_vec_size; ++i) {
                uint8x16_t raw = vld1q_u8(select_vec[i] + processed_rows);
                uint8x16_t m = vtstq_u8(raw, raw);
                uint8x16_t win = vbicq_u8(m, selected_16);
                uint64_t win_nibble = SIMD::get_nibble_mask(win);
                win_masks[i] = win;
                win_nibbles[i] = win_nibble;
                selected_16 = vorrq_u8(selected_16, m);
                select_nibble_mask |= win_nibble;
                active_branches = i + 1;
                if (select_nibble_mask == 0xffff'ffff'ffff'ffffull) {
                    if (win_nibble == 0xffff'ffff'ffff'ffffull) {
                        single_winner = i;
                    }
                    break;
                }
            }

            if (single_winner >= 0) {
                fill_branch_16(single_winner, out_ptr);
                processed_rows += 16;
                continue;
            }
            if (select_nibble_mask == 0) {
                fill_branch_16(else_idx, out_ptr);
                processed_rows += 16;
                continue;
            }

            const uint64_t else_nibble_mask = ~select_nibble_mask;
            for (int s = 0; s < kSubChunks; ++s) {
                const uint32_t bit_shift = s * kSubBits;
                const uint64_t e_sub = (else_nibble_mask >> bit_shift) & kSubMask;
                if (e_sub == kSubMask) {
                    store_vec(out_ptr + s * kLanes, load_sub(else_idx, s));
                    continue;
                }
                uint64_t resolved = e_sub;
                VecType acc = (e_sub != 0) ? load_sub(else_idx, s) : zero_vec;
                for (int i = 0; i < active_branches; ++i) {
                    const uint64_t w_sub = (win_nibbles[i] >> bit_shift) & kSubMask;
                    if (w_sub == 0) {
                        continue;
                    }
                    VecType d_vec = load_sub(i, s);
                    if (resolved == 0) {
                        acc = d_vec;
                    } else {
                        uint8x16_t expand_mask = vqtbl1q_u8(win_masks[i], tbl_idx[s]);
                        acc = bsl_vec(expand_mask, d_vec, acc);
                    }
                    resolved |= w_sub;
                    if (resolved == kSubMask) {
                        break;
                    }
                }
                store_vec(out_ptr + s * kLanes, acc);
            }

            processed_rows += 16;
        }
    }
#endif
};
} // namespace starrocks
