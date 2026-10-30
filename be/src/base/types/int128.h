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
#include <istream>
#include <ostream>
#include <string>

#include "base/utility/integer_util.h"

namespace starrocks {

using int128_t = __int128;
using uint128_t = unsigned __int128;

inline constexpr uint128_t abs_as_uint128(int128_t value) {
    uint128_t abs_value = static_cast<uint128_t>(value);
    if (value < 0) {
        abs_value = ~abs_value + 1;
    }
    return abs_value;
}

inline constexpr int128_t abs(int128_t value) {
    return static_cast<int128_t>(abs_as_uint128(value));
}

inline std::string int128_to_string(__int128 value) {
    return integer_to_string<__int128>(value);
}

inline std::ostream& operator<<(std::ostream& os, __int128 const& value) {
    return os << int128_to_string(value);
}

std::istream& operator>>(std::istream& is, __int128& value);

std::size_t hash_value(__int128 const& value);

} // namespace starrocks
