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

enum SketchType {
    HLL = 0,
    QUANTILE = 1,
    FREQUENT = 2,
    THETA = 3,
};

template <LogicalType LT, SketchType ST>
struct DSSketchState {};

template <LogicalType LT>
struct SpecialCppType {
    using CppType = RunTimeCppType<LT>;
};

template <>
struct SpecialCppType<TYPE_BINARY> {
    using CppType = std::string;
};
template <>
struct SpecialCppType<TYPE_VARBINARY> {
    using CppType = std::string;
};
template <>
struct SpecialCppType<TYPE_CHAR> {
    using CppType = std::string;
};
template <>
struct SpecialCppType<TYPE_VARCHAR> {
    using CppType = std::string;
};

} // namespace starrocks