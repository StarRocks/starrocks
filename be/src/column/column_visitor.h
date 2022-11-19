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
    virtual Status visit(const NullableColumn& column);
    virtual Status visit(const ConstColumn& column);
    virtual Status visit(const ArrayColumn& column);
    virtual Status visit(const MapColumn& column);
    virtual Status visit(const StructColumn& column);
    virtual Status visit(const BinaryColumn& column);
    virtual Status visit(const LargeBinaryColumn& column);
    virtual Status visit(const Int8Column& column);
    virtual Status visit(const UInt8Column& column);
    virtual Status visit(const Int16Column& column);
    virtual Status visit(const UInt16Column& column);
    virtual Status visit(const Int32Column& column);
    virtual Status visit(const UInt32Column& column);
    virtual Status visit(const Int64Column& column);
    virtual Status visit(const UInt64Column& column);
    virtual Status visit(const Int128Column& column);
    virtual Status visit(const DoubleColumn& column);
    virtual Status visit(const FloatColumn& column);
    virtual Status visit(const DateColumn& column);
    virtual Status visit(const TimestampColumn& column);
    virtual Status visit(const DecimalColumn& column);
    virtual Status visit(const Decimal32Column& column);
    virtual Status visit(const Decimal64Column& column);
    virtual Status visit(const Decimal128Column& column);
    virtual Status visit(const HyperLogLogColumn& column);
    virtual Status visit(const BitmapColumn& column);
    virtual Status visit(const PercentileColumn& column);
    virtual Status visit(const JsonColumn& column);
    virtual Status visit(const FixedLengthColumn<int96_t>& column);
    virtual Status visit(const FixedLengthColumn<uint24_t>& column);
    virtual Status visit(const FixedLengthColumn<decimal12_t>& column);

    // NOTE: Inherited classes normally don't need to implement the following methods, they are
    // defined here mainly for successful compiling.
    virtual Status visit(const FixedLengthColumnBase<int8_t>& column);
    virtual Status visit(const FixedLengthColumnBase<uint8_t>& column);
    virtual Status visit(const FixedLengthColumnBase<int16_t>& column);
    virtual Status visit(const FixedLengthColumnBase<uint16_t>& column);
    virtual Status visit(const FixedLengthColumnBase<int32_t>& column);
    virtual Status visit(const FixedLengthColumnBase<uint32_t>& column);
    virtual Status visit(const FixedLengthColumnBase<int64_t>& column);
    virtual Status visit(const FixedLengthColumnBase<uint64_t>& column);
    virtual Status visit(const FixedLengthColumnBase<int128_t>& column);
    virtual Status visit(const FixedLengthColumnBase<float>& column);
    virtual Status visit(const FixedLengthColumnBase<double>& column);
    virtual Status visit(const FixedLengthColumnBase<DateValue>& column);
    virtual Status visit(const FixedLengthColumnBase<DecimalV2Value>& column);
    virtual Status visit(const FixedLengthColumnBase<TimestampValue>& column);
    virtual Status visit(const FixedLengthColumnBase<int96_t>& column);
    virtual Status visit(const FixedLengthColumnBase<uint24_t>& column);
    virtual Status visit(const FixedLengthColumnBase<decimal12_t>& column);
    virtual Status visit(const ObjectColumn<JsonValue>& column);
};

} // namespace starrocks
