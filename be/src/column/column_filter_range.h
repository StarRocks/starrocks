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

#include "column/vectorized_fwd.h"
#include "common/memory/column_allocator.h"
#include "gutil/cpu.h"

namespace starrocks::column_filter_range {

template <typename T, bool avx512f>
size_t t_filter_range(const Filter& filter, T* dst_data, const T* src_data, size_t from, size_t to);

template <typename T>
inline size_t filter_range(const Filter& filter, T* data, size_t from, size_t to) {
    if (base::CPU::instance()->has_avx512f()) {
        return t_filter_range<T, true>(filter, data, data, from, to);
    }
    return t_filter_range<T, false>(filter, data, data, from, to);
}

template <typename T>
inline size_t filter_range(const Filter& filter, T* dst_data, const T* src_data, size_t from, size_t to) {
    if (base::CPU::instance()->has_avx512f()) {
        return t_filter_range<T, true>(filter, dst_data, src_data, from, to);
    }
    return t_filter_range<T, false>(filter, dst_data, src_data, from, to);
}

} // namespace starrocks::column_filter_range
