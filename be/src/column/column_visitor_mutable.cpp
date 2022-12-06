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

VISIT_IMPL(vectorized::NullableColumn)
VISIT_IMPL(vectorized::ConstColumn)
VISIT_IMPL(vectorized::ArrayColumn)
VISIT_IMPL(vectorized::MapColumn)
VISIT_IMPL(vectorized::StructColumn)
VISIT_IMPL(vectorized::BinaryColumn)
VISIT_IMPL(vectorized::LargeBinaryColumn)
VISIT_IMPL(vectorized::Int8Column)
VISIT_IMPL(vectorized::UInt8Column)
VISIT_IMPL(vectorized::Int16Column)
VISIT_IMPL(vectorized::UInt16Column)
VISIT_IMPL(vectorized::Int32Column)
VISIT_IMPL(vectorized::UInt32Column)
VISIT_IMPL(vectorized::Int64Column)
VISIT_IMPL(vectorized::UInt64Column)
VISIT_IMPL(vectorized::Int128Column)
VISIT_IMPL(vectorized::FloatColumn)
VISIT_IMPL(vectorized::DoubleColumn)
VISIT_IMPL(vectorized::DateColumn)
VISIT_IMPL(vectorized::TimestampColumn)
VISIT_IMPL(vectorized::DecimalColumn)
VISIT_IMPL(vectorized::Decimal32Column)
VISIT_IMPL(vectorized::Decimal64Column)
VISIT_IMPL(vectorized::Decimal128Column)
VISIT_IMPL(vectorized::HyperLogLogColumn)
VISIT_IMPL(vectorized::BitmapColumn)
VISIT_IMPL(vectorized::PercentileColumn)
VISIT_IMPL(vectorized::JsonColumn)
VISIT_IMPL(vectorized::ObjectColumn<JsonValue>)
VISIT_IMPL(vectorized::FixedLengthColumn<int96_t>)
VISIT_IMPL(vectorized::FixedLengthColumn<uint24_t>)
VISIT_IMPL(vectorized::FixedLengthColumn<decimal12_t>)

VISIT_IMPL(vectorized::FixedLengthColumnBase<int8_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<uint8_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<int16_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<uint16_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<int32_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<uint32_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<int64_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<uint64_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<vectorized::int128_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<float>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<double>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<vectorized::DateValue>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<DecimalV2Value>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<vectorized::TimestampValue>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<uint24_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<int96_t>)
VISIT_IMPL(vectorized::FixedLengthColumnBase<decimal12_t>)

} // namespace starrocks
