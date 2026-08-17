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

#include "base/types/decimal12.h"
#include "base/types/uint24.h"
#include "column/vectorized_fwd.h"
#include "types/logical_type.h"

namespace starrocks {

// StorageColumnTraits: maps LogicalType to on-disk storage ColumnType.
template <LogicalType field_type>
struct StorageColumnTraits {};

template <>
struct StorageColumnTraits<TYPE_BOOLEAN> {
    using ColumnType = UInt8Column;
};

template <>
struct StorageColumnTraits<TYPE_NONE> {
    using ColumnType = BooleanColumn;
};

template <>
struct StorageColumnTraits<TYPE_TINYINT> {
    using ColumnType = Int8Column;
};

template <>
struct StorageColumnTraits<TYPE_UNSIGNED_TINYINT> {
    using ColumnType = UInt8Column;
};

template <>
struct StorageColumnTraits<TYPE_SMALLINT> {
    using ColumnType = Int16Column;
};

template <>
struct StorageColumnTraits<TYPE_UNSIGNED_SMALLINT> {
    using ColumnType = UInt16Column;
};

template <>
struct StorageColumnTraits<TYPE_INT> {
    using ColumnType = Int32Column;
};

template <>
struct StorageColumnTraits<TYPE_UNSIGNED_INT> {
    using ColumnType = UInt32Column;
};

template <>
struct StorageColumnTraits<TYPE_BIGINT> {
    using ColumnType = Int64Column;
};

template <>
struct StorageColumnTraits<TYPE_LARGEINT> {
    using ColumnType = Int128Column;
};

template <>
struct StorageColumnTraits<TYPE_INT256> {
    using ColumnType = Int256Column;
};

template <>
struct StorageColumnTraits<TYPE_FLOAT> {
    using ColumnType = FloatColumn;
};

template <>
struct StorageColumnTraits<TYPE_DOUBLE> {
    using ColumnType = DoubleColumn;
};

template <>
struct StorageColumnTraits<TYPE_DECIMAL> {
    using ColumnType = FixedLengthColumn<decimal12_t>;
};

template <>
struct StorageColumnTraits<TYPE_DECIMALV2> {
    using ColumnType = DecimalColumn;
};

template <>
struct StorageColumnTraits<TYPE_DECIMAL32> {
    using ColumnType = Decimal32Column;
};

template <>
struct StorageColumnTraits<TYPE_DECIMAL64> {
    using ColumnType = Decimal64Column;
};

template <>
struct StorageColumnTraits<TYPE_DECIMAL128> {
    using ColumnType = Decimal128Column;
};

template <>
struct StorageColumnTraits<TYPE_DECIMAL256> {
    using ColumnType = Decimal256Column;
};

template <>
struct StorageColumnTraits<TYPE_DATE_V1> {
    using ColumnType = FixedLengthColumn<uint24_t>;
};

template <>
struct StorageColumnTraits<TYPE_DATE> {
    using ColumnType = DateColumn;
};

template <>
struct StorageColumnTraits<TYPE_DATETIME_V1> {
    using ColumnType = Int64Column;
};

template <>
struct StorageColumnTraits<TYPE_DATETIME> {
    using ColumnType = TimestampColumn;
};

template <>
struct StorageColumnTraits<TYPE_CHAR> {
    using ColumnType = BinaryColumn;
    using LargeColumnType = LargeBinaryColumn;
};

template <>
struct StorageColumnTraits<TYPE_VARCHAR> {
    using ColumnType = BinaryColumn;
    using LargeColumnType = LargeBinaryColumn;
};

template <>
struct StorageColumnTraits<TYPE_HLL> {
    using ColumnType = HyperLogLogColumn;
};

template <>
struct StorageColumnTraits<TYPE_OBJECT> {
    using ColumnType = BitmapColumn;
};

template <>
struct StorageColumnTraits<TYPE_PERCENTILE> {
    using ColumnType = PercentileColumn;
};

template <>
struct StorageColumnTraits<TYPE_JSON> {
    using ColumnType = JsonColumn;
};

template <>
struct StorageColumnTraits<TYPE_VARIANT> {
    using ColumnType = BinaryColumn;
    using LargeColumnType = LargeBinaryColumn;
};

template <>
struct StorageColumnTraits<TYPE_VARBINARY> {
    using ColumnType = BinaryColumn;
    using LargeColumnType = LargeBinaryColumn;
};

template <LogicalType Type>
using StorageColumnType = typename StorageColumnTraits<Type>::ColumnType;

template <LogicalType Type>
using StorageLargeColumnType = typename StorageColumnTraits<Type>::LargeColumnType;

} // namespace starrocks