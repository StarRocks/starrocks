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

#include <bit>
#include <cstdint>

namespace starrocks::geo_exact {

// Finite binary64 input, scaled by 2^1074. The caller chooses the integer capacity and allocator.
template <class Integer>
Integer scaled_binary64(double value) {
    const uint64_t bits = std::bit_cast<uint64_t>(value);
    const uint64_t exponent = (bits >> 52) & 0x7ff;
    Integer result = bits & ((uint64_t{1} << 52) - 1);
    if (exponent != 0) {
        result += uint64_t{1} << 52;
        result <<= exponent - 1;
    }
    return bits >> 63 ? -result : result;
}

template <class Integer>
Integer orientation(const Integer& ax, const Integer& ay, const Integer& bx, const Integer& by, const Integer& cx,
                    const Integer& cy) {
    return (bx - ax) * (cy - ay) - (by - ay) * (cx - ax);
}

template <class Integer>
int sign(const Integer& value) {
    return (value > 0) - (value < 0);
}

} // namespace starrocks::geo_exact
