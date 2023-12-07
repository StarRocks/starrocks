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

namespace starrocks {
enum OverflowMode {
    // We don't check overflow in this mode
    IGNORE = 0,
    // We will output NULL when overflow happens
    OUTPUT_NULL = 1,
    // We will report error when overflow happens
    REPORT_ERROR = 2
};

template <OverflowMode mode>
inline constexpr bool check_overflow = false;
template <>
inline constexpr bool check_overflow<OverflowMode::OUTPUT_NULL> = true;
template <>
inline constexpr bool check_overflow<OverflowMode::REPORT_ERROR> = true;

template <OverflowMode mode>
inline constexpr bool null_if_overflow = false;
template <>
inline constexpr bool null_if_overflow<OverflowMode::OUTPUT_NULL> = true;

template <OverflowMode mode>
inline constexpr bool error_if_overflow = false;
template <>
inline constexpr bool error_if_overflow<OverflowMode::REPORT_ERROR> = true;
} // namespace starrocks
