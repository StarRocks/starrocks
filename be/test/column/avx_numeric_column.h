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

#ifdef __x86_64__
#include <glog/logging.h>
#include <immintrin.h>

#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {
class AvxNumericColumn {
public:
    void avx256_filter(const std::vector<int32_t>& data, const std::vector<uint8_t>& filter, size_t result_size_hint,
                       std::vector<int32_t>& result) {
        const int32_t* d_data = data.data();
        const uint8_t* f_data = filter.data();

        if (result_size_hint != 0) {
            result.reserve(result_size_hint > 0 ? result_size_hint : data.size());
        }

        int SIMD_BITS = 256;
        __m256i all0 = _mm256_setzero_si256();

        int loop = filter.size() * 8 / SIMD_BITS;
        int batch_nums = SIMD_BITS / (8 * sizeof(uint8_t));

        int result_pos = 0;
        for (int i = 0; i < loop; ++i) {
            __m256i f = _mm256_load_si256(reinterpret_cast<const __m256i*>(f_data + i * batch_nums));

            __m256i re = _mm256_cmpgt_epi8(f, all0);
            int mask = _mm256_movemask_epi8(re);

            if (mask == 0) {
                // pass
            } else if (mask == 0xFFFFFFFF) {
                /// copy all
                result.resize(result.size() + batch_nums);

                memcpy(result.data() + result_pos * batch_nums, d_data + i * batch_nums, batch_nums * sizeof(int32_t));

                result_pos++;
            } else {
                for (int j = 0; j < batch_nums; ++j) {
                    if (filter[i * batch_nums + j]) {
                        result.push_back(data[i * batch_nums + j]);
                        result_pos++;
                    }
                }
            }
        }

        for (int k = loop * batch_nums; k < filter.size(); ++k) {
            if (filter[k]) {
                result.push_back(data[k]);
            }
        }
    }

    void progma_filter(const std::vector<int32_t>& data, const Filter& filter, size_t result_size_hint,
                       std::vector<int32_t>& result) {
        if (result_size_hint != 0) {
            result.reserve(result_size_hint > 0 ? result_size_hint : data.size());
        }

#pragma simd
        for (int i = 0; i < filter.size(); ++i) {
            if (filter[i]) {
                result.push_back(data[i]);
            }
        }
    }

    void filter(const std::vector<int32_t>& data, const Filter& filter, size_t result_size_hint,
                std::vector<int32_t>& result) {
        if (result_size_hint != 0) {
            result.reserve(result_size_hint > 0 ? result_size_hint : data.size());
        }

        for (int i = 0; i < filter.size(); ++i) {
            if (filter[i]) {
                result.push_back(data[i]);
            }
        }
    }
};

} // namespace starrocks

#endif
