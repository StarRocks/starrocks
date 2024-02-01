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
#include "types/logical_type.h"
#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#elif defined(__aarch64__)
#include "avx2ki.h"
#endif

namespace starrocks {

template <LogicalType TYPE>
class SIMD_muti_selector {
public:
    using Container = typename RunTimeColumnType<TYPE>::Container;
    using CppType = RunTimeCppType<TYPE>;
    using SelectVec = uint8_t*;

    // a normal implements
    static void multi_select_if(SelectVec select_vec[], int select_vec_size, Container& dst, Container* select_list[],
                                int select_list_size) {
        DCHECK_GT(select_list_size, 0);
        DCHECK_EQ(select_vec_size + 1, select_list_size);

        int row_sz = select_list[0]->size();
        int processed_rows = 0;

#if defined(__AVX2__) || defined (__aarch64__)
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

        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            SelectVec handle_select_vec[select_vec_size];
            // copy select vector pointer
            for (int i = 0; i < select_vec_size; ++i) {
                handle_select_vec[i] = select_vec[i];
            }

            CppType* handle_select_data[select_list_size];
            for (int i = 0; i < select_list_size; ++i) {
                handle_select_data[i] = select_list[i]->data();
            }

            __m256i loaded_masks[select_vec_size + 1];
            loaded_masks[select_vec_size] = _mm256_set1_epi8(0xff);

            __m256i loaded_datas[select_list_size];

            const __m256i all_zero_vec = _mm256_setzero_si256();

            while (processed_rows + 32 < row_sz) {
                __m256i selected_vec = all_zero_vec;
                __m256i selected_dst = _mm256_undefined_si256();

                // load select vector
                for (int i = 0; i < select_vec_size; ++i) {
                    loaded_masks[i] = _mm256_loadu_si256(reinterpret_cast<__m256i*>(handle_select_vec[i]));
                    loaded_masks[i] = _mm256_cmpgt_epi8(loaded_masks[i], _mm256_setzero_si256());
                }

                // load select data
                for (int i = 0; i < select_list_size; ++i) {
                    loaded_datas[i] = _mm256_loadu_si256(reinterpret_cast<__m256i*>(handle_select_data[i]));
                }

                for (int i = 0; i < select_list_size; ++i) {
                    // get will select vector in this loop
                    __m256i not_selected_vec = ~selected_vec;
                    __m256i will_select = not_selected_vec & loaded_masks[i];
                    will_select = _mm256_cmpeq_epi8(will_select, _mm256_setzero_si256());

                    will_select = ~will_select;
                    // select if
                    selected_dst = _mm256_blendv_epi8(selected_dst, loaded_datas[i], will_select);
                    // update select_vec
                    selected_vec |= will_select;
                }

                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst.data() + processed_rows), selected_dst);

                // update handle_select_vec
                for (int i = 0; i < select_vec_size; ++i) {
                    handle_select_vec[i] += 32;
                }

                for (int i = 0; i < select_list_size; ++i) {
                    handle_select_data[i] += 32;
                }
                processed_rows += 32;
            }
        }
#endif

        auto get_select_index = [&](int idx) {
            for (int i = 0; i < select_vec_size; ++i) {
                if (select_vec[i][idx]) {
                    return i;
                }
            }
            return select_vec_size;
        };

        for (int i = processed_rows; i < row_sz; ++i) {
            int index = get_select_index(i);
            dst[i] = (*select_list[index])[i];
        }
    }
};
} // namespace starrocks
