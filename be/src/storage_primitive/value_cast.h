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

#include <boost/lexical_cast.hpp>
#include <sstream>
#include <string>
#include <type_traits>

#include "column/runtime_type_traits.h"
#include "types/date_value.h"
#include "types/datetime_value.h"
#include "types/decimalv3.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"

namespace starrocks {

template <class T>
inline std::string cast_to_string(T value) {
    return boost::lexical_cast<std::string>(value);
}

template <>
inline std::string cast_to_string<DateValue>(DateValue value) {
    return value.to_string();
}

template <>
inline std::string cast_to_string<TimestampValue>(TimestampValue value) {
    return value.to_string();
}

template <>
inline std::string cast_to_string<__int128>(__int128 value) {
    std::stringstream ss;
    ss << value;
    return ss.str();
}

// for decimal32/64/128, their underlying type is int32/64/128, so the decimal point
// depends on precision and scale when they are casted into strings
template <class T>
inline std::string cast_to_string(T value, [[maybe_unused]] LogicalType lt, [[maybe_unused]] int precision,
                                  [[maybe_unused]] int scale) {
    // According to https://rules.sonarsource.com/cpp/RSPEC-5275, it is better
    // to use static_cast to cast from integral/float/bool type to integral type, otherwise
    // reinterpret_cast may produce undefined behavior
    constexpr bool use_static_cast = std::is_integral_v<T> || std::is_floating_point_v<T> || std::is_same_v<T, bool>;
    switch (lt) {
    case TYPE_DECIMAL32: {
        using CppType = RunTimeCppType<TYPE_DECIMAL32>;
        if constexpr (use_static_cast) {
            return DecimalV3Cast::to_string<CppType>(static_cast<CppType>(value), precision, scale);
        } else {
            return DecimalV3Cast::to_string<CppType>(*reinterpret_cast<CppType*>(&value), precision, scale);
        }
    }
    case TYPE_DECIMAL64: {
        using CppType = RunTimeCppType<TYPE_DECIMAL64>;
        if constexpr (use_static_cast) {
            return DecimalV3Cast::to_string<CppType>(static_cast<CppType>(value), precision, scale);
        } else {
            return DecimalV3Cast::to_string<CppType>(*reinterpret_cast<CppType*>(&value), precision, scale);
        }
    }
    case TYPE_DECIMAL128: {
        using CppType = RunTimeCppType<TYPE_DECIMAL128>;
        if constexpr (use_static_cast) {
            return DecimalV3Cast::to_string<CppType>(static_cast<CppType>(value), precision, scale);
        } else {
            return DecimalV3Cast::to_string<CppType>(*reinterpret_cast<CppType*>(&value), precision, scale);
        }
    }
    case TYPE_DECIMAL256: {
        using CppType = RunTimeCppType<TYPE_DECIMAL256>;
        if constexpr (use_static_cast) {
            return DecimalV3Cast::to_string<CppType>(static_cast<CppType>(value), precision, scale);
        } else {
            return DecimalV3Cast::to_string<CppType>(*reinterpret_cast<CppType*>(&value), precision, scale);
        }
    }
    default:
        return cast_to_string<T>(value);
    }
}

} // namespace starrocks
