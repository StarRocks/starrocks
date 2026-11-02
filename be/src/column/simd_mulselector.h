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
            uint32_t loaded_masks_value[select_vec_size];

            const __m256i all_zero_vec = _mm256_setzero_si256();

            while (processed_rows + 32 < row_sz) {
                __m256i selected_vec = all_zero_vec;
                // load select vector
                for (int i = 0; i < select_vec_size; ++i) {
                    loaded_masks[i] = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(handle_select_vec[i]));
                    loaded_masks[i] = ~_mm256_cmpeq_epi8(loaded_masks[i], all_zero_vec);
                    loaded_masks_value[i] = static_cast<uint32_t>(_mm256_movemask_epi8(loaded_masks[i]));
                }

                // Zero-initialise the ELSE slot before loading so an early all-true WHEN break never leaves it uninitialised.
                loaded_datas[select_list_size - 1] = all_zero_vec;

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
                    if (i < select_list_size - 1 && loaded_masks_value[i] == 0xffffffffu) {
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
                    __m256i will_select = _mm256_andnot_si256(selected_vec, loaded_masks[i]);
                    // select if
                    selected_dst = _mm256_blendv_epi8(selected_dst, loaded_datas[i], will_select);
                    // update select_vec
                    selected_vec |= will_select;

                    // no need to check other columns
                    if (static_cast<uint32_t>(_mm256_movemask_epi8(selected_vec)) == 0xffffffffu) {
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
            constexpr int kLanes = 32 / data_size;
            constexpr uint32_t kSubMask = (1u << kLanes) - 1u;
            const __m256i all_zero_vec = _mm256_setzero_si256();

            auto expand_sub_mask = [&](uint32_t sub_mask) -> __m256i {
                if constexpr (data_size == 2) {
                    const __m256i data_mask =
                            _mm256_setr_epi16(0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x100, 0x200, 0x400,
                                              0x800, 0x1000, 0x2000, 0x4000, static_cast<int16_t>(0x8000u));
                    __m256i expand_mask = _mm256_set1_epi16(static_cast<int16_t>(sub_mask)) & data_mask;
                    return _mm256_cmpeq_epi16(expand_mask, data_mask);
                } else if constexpr (data_size == 4) {
                    const __m256i data_mask = _mm256_setr_epi32(0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80);
                    __m256i expand_mask = _mm256_set1_epi32(static_cast<int32_t>(sub_mask)) & data_mask;
                    return _mm256_cmpeq_epi32(expand_mask, data_mask);
                } else {
                    const __m256i data_mask = _mm256_setr_epi64x(0x01, 0x02, 0x04, 0x08);
                    __m256i expand_mask = _mm256_set1_epi64x(static_cast<int64_t>(sub_mask)) & data_mask;
                    return _mm256_cmpeq_epi64(expand_mask, data_mask);
                }
            };

            // Disjoint32 optimisation: resolve mutually disjoint 32-row winner masks once per batch
            // and lazily load only active branches per YMM sub-chunk.
            __m256i const_vecs[select_list_size];
            if (processed_rows + 32 < row_sz) {
                for (int i = 0; i < select_list_size; ++i) {
                    if (then_column_is_const[i]) {
                        const_vecs[i] = SIMDUtils::set_data(handle_select_data[i][0]);
                    }
                }
            }

            auto load_sub = [&](int branch, int sub) -> __m256i {
                if (then_column_is_const[branch]) {
                    return const_vecs[branch];
                }
                return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(handle_select_data[branch] + sub * kLanes));
            };

            while (processed_rows + 32 < row_sz) {
                uint32_t win_bits[select_vec_size];
                __m256i claimed_vec = all_zero_vec;
                uint32_t claimed_bits = 0;
                for (int i = 0; i < select_vec_size; ++i) {
                    if (claimed_bits == 0xffffffffu) {
                        win_bits[i] = 0;
                        continue;
                    }
                    __m256i raw = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(handle_select_vec[i]));
                    raw = ~_mm256_cmpeq_epi8(raw, all_zero_vec);
                    __m256i win = _mm256_andnot_si256(claimed_vec, raw);
                    claimed_vec = _mm256_or_si256(claimed_vec, raw);
                    win_bits[i] = static_cast<uint32_t>(_mm256_movemask_epi8(win));
                    claimed_bits |= win_bits[i];
                }
                uint32_t else_bits = ~claimed_bits;

                for (int s = 0; s < data_size; ++s) {
                    const uint32_t shift = s * kLanes;
                    const uint32_t e_sub = (else_bits >> shift) & kSubMask;
                    if (e_sub == kSubMask) {
                        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst.data() + processed_rows + shift),
                                            load_sub(select_list_size - 1, s));
                        continue;
                    }
                    uint32_t resolved = e_sub;
                    __m256i acc = (e_sub != 0) ? load_sub(select_list_size - 1, s) : all_zero_vec;
                    for (int i = 0; i < select_vec_size; ++i) {
                        const uint32_t w_sub = (win_bits[i] >> shift) & kSubMask;
                        if (w_sub == 0) {
                            continue;
                        }
                        __m256i d_vec = load_sub(i, s);
                        if (resolved == 0) {
                            acc = d_vec;
                        } else {
                            acc = _mm256_blendv_epi8(acc, d_vec, expand_sub_mask(w_sub));
                        }
                        resolved |= w_sub;
                        if (resolved == kSubMask) {
                            break;
                        }
                    }
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst.data() + processed_rows + shift), acc);
                }

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
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        constexpr int data_size = sizeof(RunTimeCppType<TYPE>);
        if constexpr (data_size == 1) {
            uint8x16_t loaded_masks[select_vec_size];
            uint8x16_t loaded_datas[select_list_size];
            const uint8x16_t zero_vector = vdupq_n_u8(0);

            while (processed_rows + 16 < row_sz) {
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
            uint8x16_t loaded_masks[select_vec_size];
            uint16x8_t loaded_datas[select_list_size];

            const uint16x8_t zero_vector = vdupq_n_u16(0);
            // we handle 16 mask at one time
            while (processed_rows + 16 < row_sz) {
                // load select vector
                for (int i = 0; i < select_vec_size; ++i) {
                    loaded_masks[i] = vld1q_u8(handle_select_vec[i]);
                    // loaded_mask[i] = selector[i] != 0 ? 0xFF : 0x00
                    loaded_masks[i] = vtstq_u8(loaded_masks[i], loaded_masks[i]);
                }

                constexpr uint8_t each_loop_handle_sz = 16 / sizeof(RunTimeCppType<TYPE>);

                for (int j = 0; j < data_size; j++) {
                    loaded_datas[select_list_size - 1] = zero_vector;
                    // load select data
                    for (int i = 0; i < select_list_size; ++i) {
                        // date columns except the last column, if mask is zero, no need to load it
                        if (i < select_list_size - 1 && vmaxvq_u8(loaded_masks[i]) == 0) {
                            continue;
                        }

                        if (then_column_is_const[i]) {
                            loaded_datas[i] = vdupq_n_u16(handle_select_data[i][0]);
                        } else {
                            loaded_datas[i] = vld1q_u16(reinterpret_cast<const uint16_t*>(handle_select_data[i]));
                        }

                        // if all 1, no need to load left data columns because they won't be selected
                        if (i < select_list_size - 1 && vminvq_u8(loaded_masks[i])) {
                            break;
                        }
                    }

                    // selected_vec[i] == 1 means this row is selected already
                    uint16x8_t selected_vec = zero_vector;
                    bool all_selected = false;
                    // let the default value be the last data column, which is 'else' column
                    uint16x8_t selected_dst = loaded_datas[select_list_size - 1];
                    const uint8x16_t index = {0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7};
                    for (int i = 0; i < select_list_size - 1; ++i) {
                        uint8x16_t expand_mask = vqtbl1q_u8(loaded_masks[i], index);

                        if (vmaxvq_u8(expand_mask) == 0 || all_selected) {
                            loaded_masks[i] = vextq_u8(loaded_masks[i], loaded_masks[i], 8);
                            continue;
                        }

                        // get will select vector in this loop
                        uint16x8_t not_selected_vec = ~selected_vec;
                        uint16x8_t will_select = not_selected_vec & vreinterpretq_u16_u8(expand_mask);

                        // select if: will_select[i] ? loaded_datas[i] : selected_dst
                        selected_dst = vbslq_u16(will_select, loaded_datas[i], selected_dst);
                        // update select_vec
                        selected_vec |= will_select;

                        // right shift mask
                        loaded_masks[i] = vextq_u8(loaded_masks[i], loaded_masks[i], 8);

                        all_selected = vminvq_u16(selected_vec);
                    }
                    vst1q_u16(reinterpret_cast<uint16_t*>(dst.data() + processed_rows), selected_dst);
                    processed_rows += each_loop_handle_sz;
                    for (int i = 0; i < select_list_size; ++i) {
                        if (!then_column_is_const[i]) {
                            handle_select_data[i] += each_loop_handle_sz;
                        }
                    }
                }

                // update handle_select_vec
                for (int i = 0; i < select_vec_size; ++i) {
                    handle_select_vec[i] += 16;
                }
            }
        } else if constexpr (data_size == 4) {
            uint8x16_t loaded_masks[select_vec_size];
            uint32x4_t loaded_datas[select_list_size];

            const uint32x4_t zero_vector = vdupq_n_u32(0);
            // we handle 16 mask at one time
            while (processed_rows + 16 < row_sz) {
                // load select vector
                for (int i = 0; i < select_vec_size; ++i) {
                    loaded_masks[i] = vld1q_u8(handle_select_vec[i]);
                    // loaded_mask[i] = selector[i] != 0 ? 0xFF : 0x00
                    loaded_masks[i] = vtstq_u8(loaded_masks[i], loaded_masks[i]);
                }

                constexpr uint8_t each_loop_handle_sz = 16 / sizeof(RunTimeCppType<TYPE>);

                for (int j = 0; j < data_size; j++) {
                    loaded_datas[select_list_size - 1] = zero_vector;
                    // load select data
                    for (int i = 0; i < select_list_size; ++i) {
                        // date columns except the last column, if mask is zero, no need to load it
                        if (i < select_list_size - 1 && vmaxvq_u8(loaded_masks[i]) == 0) {
                            continue;
                        }

                        if (then_column_is_const[i]) {
                            uint32_t bits = 0;
                            std::memcpy(&bits, handle_select_data[i], sizeof(bits));
                            loaded_datas[i] = vdupq_n_u32(bits);
                        } else {
                            loaded_datas[i] = vld1q_u32(reinterpret_cast<const uint32_t*>(handle_select_data[i]));
                        }

                        // if all 1, no need to load left data columns because they won't be selected
                        if (i < select_list_size - 1 && vminvq_u8(loaded_masks[i])) {
                            break;
                        }
                    }

                    // selected_vec[i] == 1 means this row is selected already
                    uint32x4_t selected_vec = zero_vector;
                    bool all_selected = false;
                    // let the default value be the last data column, which is 'else' column
                    uint32x4_t selected_dst = loaded_datas[select_list_size - 1];
                    const uint8x16_t index = {0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
                    for (int i = 0; i < select_list_size - 1; ++i) {
                        uint8x16_t expand_mask = vqtbl1q_u8(loaded_masks[i], index);

                        if (vmaxvq_u8(expand_mask) == 0 || all_selected) {
                            loaded_masks[i] = vextq_u8(loaded_masks[i], loaded_masks[i], 4);
                            continue;
                        }

                        // get will select vector in this loop
                        uint32x4_t not_selected_vec = ~selected_vec;
                        uint32x4_t will_select = not_selected_vec & vreinterpretq_u32_u8(expand_mask);

                        // select if: will_select[i] ? loaded_datas[i] : selected_dst
                        selected_dst = vbslq_u32(will_select, loaded_datas[i], selected_dst);
                        // update select_vec
                        selected_vec |= will_select;

                        // right shift mask
                        loaded_masks[i] = vextq_u8(loaded_masks[i], loaded_masks[i], 4);

                        // all 1
                        all_selected = vminvq_u32(selected_vec);
                    }
                    vst1q_u32(reinterpret_cast<uint32_t*>(dst.data() + processed_rows), selected_dst);
                    processed_rows += each_loop_handle_sz;
                    for (int i = 0; i < select_list_size; ++i) {
                        if (!then_column_is_const[i]) {
                            handle_select_data[i] += each_loop_handle_sz;
                        }
                    }
                }

                // update handle_select_vec
                for (int i = 0; i < select_vec_size; ++i) {
                    handle_select_vec[i] += 16;
                }
            }
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
};
} // namespace starrocks
