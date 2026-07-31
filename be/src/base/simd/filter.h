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

namespace SIMD::Filter {

namespace detail {
// Type-erased implementation; defined in filter.cpp. Compaction is pure data
// movement, so only the element width matters -- this keeps the whole SIMD
// implementation in the .cpp and free of higher-level type dependencies.
size_t filter_range(void* dst, const void* src, size_t element_size, const uint8_t* selector, size_t from, size_t to);
} // namespace detail

// Stream compaction: copy the elements of `src` whose selector byte is non-zero,
// over the index range [from, to), to the front of `dst` starting at index
// `from`, and return the new end offset. Elements before `from` are left
// untouched, and `dst` may alias `src` (in-place). 4- and 8-byte elements take a
// vectorised path (AVX-512 vpcompress / AVX2 on x86, NEON on aarch64); other
// sizes use a scalar byte copy.
template <typename T>
inline size_t filter_range(T* dst, const T* src, const uint8_t* selector, size_t from, size_t to) {
    return detail::filter_range(dst, src, sizeof(T), selector, from, to);
}

} // namespace SIMD::Filter
