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

#include <limits>
#include <type_traits>

#include "common/compiler_util.h"

namespace starrocks {

DIAGNOSTIC_PUSH
#if defined(__clang__)
DIAGNOSTIC_IGNORE("-Wimplicit-int-float-conversion")
#endif

template <typename FromType, typename ToType>
static constexpr FromType floating_to_integral_lower_bound =
        static_cast<FromType>(std::numeric_limits<ToType>::lowest());

template <typename FromType, typename ToType>
static constexpr FromType floating_to_integral_upper_bound = static_cast<FromType>(2) *
                                                             (std::numeric_limits<ToType>::max() / 2 + 1);

/// Check whether the value of type `FromType` overflows when converted to type `ToType`.
/// If overflowed, return true; otherwise, return false.
/// NOTE that `FromType` and `ToType` must be signed number types.
template <typename FromType, typename ToType>
bool check_signed_number_overflow(FromType value) {
    if constexpr (std::numeric_limits<ToType>::max() >= std::numeric_limits<FromType>::max()) { // Widening conversion
        return false;
    } else { // Narrowing conversion
        if constexpr (std::is_floating_point_v<FromType> && std::is_integral_v<ToType>) {
            // For floating-point numbers, we cannot use `value > (Type)std::numeric_limits<ResultType>::max()` to
            // determine whether `value` exceeds the maximum value of ResultType. The reason is as follows:
            //
            // `std::numeric_limits<ResultType>::max()` is `2^n-1`, where n is 63, 31, 15 or 7, this number cannot be
            // exactly represented by floating-point numbers, so when converted to Type, it will be rounded up to `2^n`.
            // Therefore, when `value` is `2^n`, `value > (Type)std::numeric_limits<ResultType>::max()` will return false.
            // However, in actual conversion, overflow will occur, resulting in the maximum or minimum value of ResultType,
            // depending on the architecture, compiler, and compilation parameters.
            //
            // Because `2^n` can be exactly represented by floating-point numbers, we use `value >= (Type)2^n` to determine
            // whether it is overflow, rather than `value > (Type)2^n-1`.
            return !(value >= floating_to_integral_lower_bound<FromType, ToType> &&
                     value < floating_to_integral_upper_bound<FromType, ToType>);
        } else {
            // std::numeric_limits<T>::lowest() is a finite value x such that there is no other
            // finite value y where y < x.
            // This is different from std::numeric_limits<T>::min() for floating-point types.
            // So we use lowest instead of min for lower bound of all types.
            return (value < (FromType)std::numeric_limits<ToType>::lowest()) |
                   (value > (FromType)std::numeric_limits<ToType>::max());
        }
    }
}

DIAGNOSTIC_POP

} // namespace starrocks