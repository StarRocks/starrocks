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

#include "column/column_visitor_mutable.h"

namespace starrocks {

#define VISIT_IMPL(ClassName) \
    Status ColumnVisitorMutable::visit(ClassName* column) { return Status::NotSupported(#ClassName); }

VISIT_IMPL(NullableColumn)
VISIT_IMPL(ConstColumn)
VISIT_IMPL(ArrayColumn)
VISIT_IMPL(MapColumn)
VISIT_IMPL(StructColumn)
VISIT_IMPL(BinaryColumn)
VISIT_IMPL(LargeBinaryColumn)
VISIT_IMPL(Int8Column)
VISIT_IMPL(UInt8Column)
VISIT_IMPL(Int16Column)
VISIT_IMPL(UInt16Column)
VISIT_IMPL(Int32Column)
VISIT_IMPL(UInt32Column)
VISIT_IMPL(Int64Column)
VISIT_IMPL(UInt64Column)
VISIT_IMPL(Int128Column)
VISIT_IMPL(FloatColumn)
VISIT_IMPL(DoubleColumn)
VISIT_IMPL(DateColumn)
VISIT_IMPL(TimestampColumn)
VISIT_IMPL(DecimalColumn)
VISIT_IMPL(Decimal32Column)
VISIT_IMPL(Decimal64Column)
VISIT_IMPL(Decimal128Column)
VISIT_IMPL(HyperLogLogColumn)
VISIT_IMPL(BitmapColumn)
VISIT_IMPL(PercentileColumn)
VISIT_IMPL(JsonColumn)
VISIT_IMPL(ObjectColumn<JsonValue>)
VISIT_IMPL(FixedLengthColumn<int96_t>)
VISIT_IMPL(FixedLengthColumn<uint24_t>)
VISIT_IMPL(FixedLengthColumn<decimal12_t>)

VISIT_IMPL(FixedLengthColumnBase<int8_t>)
VISIT_IMPL(FixedLengthColumnBase<uint8_t>)
VISIT_IMPL(FixedLengthColumnBase<int16_t>)
VISIT_IMPL(FixedLengthColumnBase<uint16_t>)
VISIT_IMPL(FixedLengthColumnBase<int32_t>)
VISIT_IMPL(FixedLengthColumnBase<uint32_t>)
VISIT_IMPL(FixedLengthColumnBase<int64_t>)
VISIT_IMPL(FixedLengthColumnBase<uint64_t>)
VISIT_IMPL(FixedLengthColumnBase<int128_t>)
VISIT_IMPL(FixedLengthColumnBase<float>)
VISIT_IMPL(FixedLengthColumnBase<double>)
VISIT_IMPL(FixedLengthColumnBase<DateValue>)
VISIT_IMPL(FixedLengthColumnBase<DecimalV2Value>)
VISIT_IMPL(FixedLengthColumnBase<TimestampValue>)
VISIT_IMPL(FixedLengthColumnBase<uint24_t>)
VISIT_IMPL(FixedLengthColumnBase<int96_t>)
VISIT_IMPL(FixedLengthColumnBase<decimal12_t>)

} // namespace starrocks
