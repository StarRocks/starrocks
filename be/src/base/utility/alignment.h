// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Macros for dealing with memory alignment.
#pragma once

#include <limits>
#include <type_traits>

namespace starrocks {

template <typename T>
constexpr T AlignDown(T x, T align) {
    static_assert(std::is_integral_v<T>, "AlignDown requires an integral type");
    if (align <= 0) {
        return x;
    }
    return (x / align) * align;
}

template <typename T>
constexpr T AlignUp(T x, T align) {
    static_assert(std::is_integral_v<T>, "AlignUp requires an integral type");
    if (align <= 0) {
        return x;
    }
    if constexpr (std::is_signed_v<T>) {
        if (x < 0) {
            return x;
        }
    }
    const T rem = x % align;
    if (rem == 0) {
        return x;
    }
    const T max_aligned = (std::numeric_limits<T>::max() / align) * align;
    if (x > max_aligned) {
        return max_aligned;
    }
    return x + (align - rem);
}

} // namespace starrocks

// Round down 'x' to the nearest 'align' boundary
#define ALIGN_DOWN(x, align) (::starrocks::AlignDown((x), (align)))

// Round up 'x' to the nearest 'align' boundary. If rounding would overflow,
// returns the largest representable aligned value.
#define ALIGN_UP(x, align) (::starrocks::AlignUp((x), (align)))
