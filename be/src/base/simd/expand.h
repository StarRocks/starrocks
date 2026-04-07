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

#include <cstddef>
#include <cstdint>

namespace SIMD::Expand {

// SIMD implements for branchless selection

// dst_data[i] = src_data[cnt];
// cnt += !nulls[i];

void expand_load_simd(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls, size_t count);
void expand_load_simd(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls, size_t count);

template <class DataType>
inline void expand_load_branchless(DataType* dst_data, const DataType* src_data, const uint8_t* nulls, size_t count) {
    size_t cnt = 0;
    for (size_t i = 0; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
}

template <class DataType>
void expand_load(DataType* dst_data, const DataType* src_data, const uint8_t* nulls, size_t count) {
    if constexpr (sizeof(DataType) == 4) {
        expand_load_simd((int32_t*)dst_data, (int32_t*)src_data, nulls, count);
    } else if constexpr (sizeof(DataType) == 8) {
        expand_load_simd((int64_t*)dst_data, (int64_t*)src_data, nulls, count);
    } else {
        expand_load_branchless(dst_data, src_data, nulls, count);
    }
}

} // namespace SIMD::Expand
