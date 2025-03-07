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

#include "column/type_traits.h"
#include "glog/logging.h"
#include "simd/simd_utils.h"
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
                                Container* __restrict select_list[], int select_list_size,
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

            while (processed_rows + 16 < row_sz) {
                uint8x16_t selected_vec = zero_vector;
                // load select vector
                for (int i = 0; i < select_vec_size; ++i) {
                    loaded_masks[i] = vld1q_u8(handle_select_vec[i]);
                    // loaded_mask[i] = selector[i] != 0 ? 0xFF : 0x00
                    loaded_masks[i] = vtstq_u8(loaded_masks[i], loaded_masks[i]);
                }

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
                    // load select data
                    for (int i = 0; i < select_list_size; ++i) {
                        // date columns except the last column, if mask is zero, no need to load it
                        if (i < select_list_size - 1 && vmaxvq_u8(loaded_masks[i]) == 0) {
                            continue;
                        }

                        if (then_column_is_const[i]) {
                            loaded_datas[i] = vdupq_n_u32(*reinterpret_cast<const uint32_t*>(handle_select_data[i]));
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
