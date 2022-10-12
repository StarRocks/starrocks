// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "gen_cpp/parquet_types.h"
#include "util/int96.h"

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
