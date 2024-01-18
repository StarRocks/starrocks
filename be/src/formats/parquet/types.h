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

#include "gen_cpp/parquet_types.h"
#include "util/int96.h"
#include "util/slice.h"

namespace starrocks::parquet {

using level_t = int16_t;

template <tparquet::Type::type type>
struct PhysicalTypeTraits {};

template <>
struct PhysicalTypeTraits<tparquet::Type::BOOLEAN> {
    using CppType = bool;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT32> {
    using CppType = int32_t;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT64> {
    using CppType = int64_t;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT96> {
    using CppType = int96_t;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::FLOAT> {
    using CppType = float;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::DOUBLE> {
    using CppType = double;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::BYTE_ARRAY> {
    using CppType = Slice;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::FIXED_LEN_BYTE_ARRAY> {
    using CppType = Slice;
};

} // namespace starrocks::parquet
