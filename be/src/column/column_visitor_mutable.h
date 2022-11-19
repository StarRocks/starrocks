// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "storage/decimal12.h"
#include "storage/uint24.h"
#include "util/int96.h"

namespace starrocks {

class ColumnVisitorMutable {
public:
    virtual ~ColumnVisitorMutable() = default;

    // The default implementation of `visit` will return `Status::NotSupported`
    virtual Status visit(NullableColumn* column);
    virtual Status visit(ConstColumn* column);
    virtual Status visit(ArrayColumn* column);
    virtual Status visit(MapColumn* column);
    virtual Status visit(StructColumn* column);
    virtual Status visit(BinaryColumn* column);
    virtual Status visit(LargeBinaryColumn* column);
    virtual Status visit(Int8Column* column);
    virtual Status visit(UInt8Column* column);
    virtual Status visit(Int16Column* column);
    virtual Status visit(UInt16Column* column);
    virtual Status visit(Int32Column* column);
    virtual Status visit(UInt32Column* column);
    virtual Status visit(Int64Column* column);
    virtual Status visit(UInt64Column* column);
    virtual Status visit(Int128Column* column);
    virtual Status visit(DoubleColumn* column);
    virtual Status visit(FloatColumn* column);
    virtual Status visit(DateColumn* column);
    virtual Status visit(TimestampColumn* column);
    virtual Status visit(DecimalColumn* column);
    virtual Status visit(Decimal32Column* column);
    virtual Status visit(Decimal64Column* column);
    virtual Status visit(Decimal128Column* column);
    virtual Status visit(HyperLogLogColumn* column);
    virtual Status visit(BitmapColumn* column);
    virtual Status visit(PercentileColumn* column);
    virtual Status visit(JsonColumn* column);
    virtual Status visit(FixedLengthColumn<int96_t>* column);
    virtual Status visit(FixedLengthColumn<uint24_t>* column);
    virtual Status visit(FixedLengthColumn<decimal12_t>* column);

    // NOTE: Inherited classes normally don't need to implement the following methods, they are
    // defined here mainly for successful compiling.
    virtual Status visit(FixedLengthColumnBase<int8_t>* column);
    virtual Status visit(FixedLengthColumnBase<uint8_t>* column);
    virtual Status visit(FixedLengthColumnBase<int16_t>* column);
    virtual Status visit(FixedLengthColumnBase<uint16_t>* column);
    virtual Status visit(FixedLengthColumnBase<int32_t>* column);
    virtual Status visit(FixedLengthColumnBase<uint32_t>* column);
    virtual Status visit(FixedLengthColumnBase<int64_t>* column);
    virtual Status visit(FixedLengthColumnBase<uint64_t>* column);
    virtual Status visit(FixedLengthColumnBase<int128_t>* column);
    virtual Status visit(FixedLengthColumnBase<float>* column);
    virtual Status visit(FixedLengthColumnBase<double>* column);
    virtual Status visit(FixedLengthColumnBase<DateValue>* column);
    virtual Status visit(FixedLengthColumnBase<DecimalV2Value>* column);
    virtual Status visit(FixedLengthColumnBase<TimestampValue>* column);
    virtual Status visit(FixedLengthColumnBase<int96_t>* column);
    virtual Status visit(FixedLengthColumnBase<uint24_t>* column);
    virtual Status visit(FixedLengthColumnBase<decimal12_t>* column);
    virtual Status visit(ObjectColumn<JsonValue>* column);
};

} // namespace starrocks
