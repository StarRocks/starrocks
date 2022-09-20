// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "storage/decimal12.h"
#include "storage/uint24.h"
#include "util/int96.h"

namespace starrocks {

class ColumnVisitor {
public:
    virtual ~ColumnVisitor() = default;

    // The default implementation of `visit` will return `Status::NotSupported`
    virtual Status visit(const vectorized::NullableColumn& column);
    virtual Status visit(const vectorized::ConstColumn& column);
    virtual Status visit(const vectorized::ArrayColumn& column);
    virtual Status visit(const vectorized::MapColumn& column);
    virtual Status visit(const vectorized::BinaryColumn& column);
    virtual Status visit(const vectorized::LargeBinaryColumn& column);
    virtual Status visit(const vectorized::Int8Column& column);
    virtual Status visit(const vectorized::UInt8Column& column);
    virtual Status visit(const vectorized::Int16Column& column);
    virtual Status visit(const vectorized::UInt16Column& column);
    virtual Status visit(const vectorized::Int32Column& column);
    virtual Status visit(const vectorized::UInt32Column& column);
    virtual Status visit(const vectorized::Int64Column& column);
    virtual Status visit(const vectorized::UInt64Column& column);
    virtual Status visit(const vectorized::Int128Column& column);
    virtual Status visit(const vectorized::DoubleColumn& column);
    virtual Status visit(const vectorized::FloatColumn& column);
    virtual Status visit(const vectorized::DateColumn& column);
    virtual Status visit(const vectorized::TimestampColumn& column);
    virtual Status visit(const vectorized::DecimalColumn& column);
    virtual Status visit(const vectorized::Decimal32Column& column);
    virtual Status visit(const vectorized::Decimal64Column& column);
    virtual Status visit(const vectorized::Decimal128Column& column);
    virtual Status visit(const vectorized::HyperLogLogColumn& column);
    virtual Status visit(const vectorized::BitmapColumn& column);
    virtual Status visit(const vectorized::PercentileColumn& column);
    virtual Status visit(const vectorized::JsonColumn& column);
    virtual Status visit(const vectorized::FixedLengthColumn<int96_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumn<uint24_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumn<decimal12_t>& column);

    // NOTE: Inherited classes normally don't need to implement the following methods, they are
    // defined here mainly for successful compiling.
    virtual Status visit(const vectorized::FixedLengthColumnBase<int8_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<uint8_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<int16_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<uint16_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<int32_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<uint32_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<int64_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<uint64_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<vectorized::int128_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<float>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<double>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<vectorized::DateValue>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<DecimalV2Value>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<vectorized::TimestampValue>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<int96_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<uint24_t>& column);
    virtual Status visit(const vectorized::FixedLengthColumnBase<decimal12_t>& column);
    virtual Status visit(const vectorized::ObjectColumn<JsonValue>& column);
};

} // namespace starrocks
