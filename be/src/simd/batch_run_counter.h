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

#include <type_traits>
#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#include <cstdint>

#include "simd/simd.h"

namespace starrocks {
/// Return value from BatchRunCounter: the total number of filter and
/// the number of set filter element.
struct BatchCount {
public:
    BatchCount(size_t length, size_t set_count, bool valid)
            : length(length), set_count(set_count), valid_count(valid) {}
    ~BatchCount() = default;

    bool NoneSet() const { return valid_count && set_count == 0; }
    bool AllSet() const { return valid_count && length == set_count; }

    size_t length;
    size_t set_count;
    bool valid_count = false;
};

template <int batch_size>
class BatchRunCounter {
public:
    BatchRunCounter(const uint8_t* filter, size_t start_offset, size_t length)
            : _filter(filter), _offset(start_offset), _left(length) {}

    BatchCount next_batch() {
        if (_left == 0) {
            return BatchCount(0, 0, true);
        }
#if defined(__AVX2__)
        const __m256i all0 = _mm256_setzero_si256();

        if (batch_size >= 32 && _left >= 32) {
            __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(_filter + _offset));
            uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));
            _offset += 32;
            _left -= 32;

            if (mask == 0) {
                // all zero
                return BatchCount(32, 0, true);
            } else if (mask == 0xffffffff) {
                // all one
                return BatchCount(32, 32, true);
            } else {
                return BatchCount(32, 0, false);
            }
        }
#endif

#if defined(__SSE2__)
        const __m128i zero16 = _mm_setzero_si128();

        if (batch_size >= 16 && _left >= 16) {
            const auto f = _mm_loadu_si128(reinterpret_cast<const __m128i*>(_filter + _offset));
            const auto mask = _mm_movemask_epi8(_mm_cmpgt_epi8(f, zero16));
            _offset += 16;
            _left -= 16;

            if (mask == 0) {
                // all zero
                return BatchCount(16, 0, true);
            } else if (mask == 0xffff) {
                // all one
                return BatchCount(16, 16, true);
            } else {
                return BatchCount(16, 0, false);
            }
        }
#endif

        if (_left >= 8) {
            uint64_t partital_value;
            memcpy(&partital_value, _filter + _offset, 8);
            _offset += 8;
            _left -= 8;
            if (partital_value == 0) {
                return BatchCount(8, 0, true);
            } else if (partital_value == 0x0101010101010101) {
                return BatchCount(8, 8, true);
            } else {
                return BatchCount(8, 0, false);
            }
        }

        size_t length = _left;
        _offset += _left;
        _left = 0;
        return BatchCount(length, 0, false);
    }

private:
    const uint8_t* _filter = nullptr;
    size_t _offset = 0;
    size_t _left = 0;
};

} // namespace starrocks