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

#include <memory>
#include <vector>

namespace starrocks {

class DecimalV2Value;
class HyperLogLog;
class BitmapValue;
class PercentileValue;
class JsonValue;

class DateValue;
class TimestampValue;

typedef __int128 int128_t;

class Chunk;
class Field;
class Column;
class Schema;
struct ProtobufChunkMeta;

// We may change the Buffer implementation in the future.
template <typename T>
using Buffer = std::vector<T>;

class ArrayColumn;
class MapColumn;
class StructColumn;
class NullableColumn;
class ConstColumn;

template <typename T>
class FixedLengthColumn;

template <typename T>
class FixedLengthColumnBase;

template <typename T>
class DecimalV3Column;

template <typename T>
class BinaryColumnBase;

using ColumnPtr = std::shared_ptr<Column>;
using MutableColumnPtr = std::unique_ptr<Column>;
using Columns = std::vector<ColumnPtr>;
using MutableColumns = std::vector<MutableColumnPtr>;

using Int8Column = FixedLengthColumn<int8_t>;
using UInt8Column = FixedLengthColumn<uint8_t>;
using BooleanColumn = UInt8Column;
using Int16Column = FixedLengthColumn<int16_t>;
using UInt16Column = FixedLengthColumn<uint16_t>;
using Int32Column = FixedLengthColumn<int32_t>;
using UInt32Column = FixedLengthColumn<uint32_t>;
using Int64Column = FixedLengthColumn<int64_t>;
using UInt64Column = FixedLengthColumn<uint64_t>;
using Int128Column = FixedLengthColumn<int128_t>;
using DoubleColumn = FixedLengthColumn<double>;
using FloatColumn = FixedLengthColumn<float>;
using DateColumn = FixedLengthColumn<DateValue>;
using DecimalColumn = FixedLengthColumn<DecimalV2Value>;
using TimestampColumn = FixedLengthColumn<TimestampValue>;
using Decimal32Column = DecimalV3Column<int32_t>;
using Decimal64Column = DecimalV3Column<int64_t>;
using Decimal128Column = DecimalV3Column<int128_t>;
using BinaryColumn = BinaryColumnBase<uint32_t>;
using LargeBinaryColumn = BinaryColumnBase<uint64_t>;

template <typename T>
constexpr bool is_decimal_column = false;
template <typename T>
inline constexpr bool is_decimal_column<DecimalV3Column<T>> = true;
template <typename ColumnType>
using DecimalColumnType = std::enable_if_t<is_decimal_column<ColumnType>, ColumnType>;

template <typename T>
class ObjectColumn;

using HyperLogLogColumn = ObjectColumn<HyperLogLog>;
using BitmapColumn = ObjectColumn<BitmapValue>;
using PercentileColumn = ObjectColumn<PercentileValue>;
using JsonColumnBase = ObjectColumn<JsonValue>;
class JsonColumn;

class MapColumn;
class StructColumn;

using ChunkPtr = std::shared_ptr<Chunk>;
using ChunkUniquePtr = std::unique_ptr<Chunk>;
using Chunks = std::vector<ChunkPtr>;

using SchemaPtr = std::shared_ptr<Schema>;

using Fields = std::vector<std::shared_ptr<Field>>;
using FieldPtr = std::shared_ptr<Field>;

using Filter = Buffer<uint8_t>;
using FilterPtr = std::shared_ptr<Filter>;

} // namespace starrocks
